__version__ = "0.6i"

"""
arbitrage_manager.py
====================

Подробное описание модуля
-------------------------
Этот модуль запускает и координирует поток обработки арбитража между несколькими биржами
для каждого торгового символа (например, `BTC/USDT:USDT`).

В модуле есть два основных класса:
1) `ExchangeInstrument`
   Отвечает за получение стакана (`watchOrderBook`) с конкретной биржи по конкретному символу.
   Результат работы экземпляра: регулярные события в очередь `orderbook_queue`
   вида `orderbook_update`, а также служебные события `exchange_stopped`.

2) `ArbitrageManager`
   По одному экземпляру на символ. Получает события от всех `ExchangeInstrument`
   этого символа, держит локальный кэш средних цен по биржам и считает текущий
   арбитражный спред (отношение лучшего bid к лучшему ask между биржами).

Ключевая идея архитектуры:
- На каждый символ создаётся отдельная задача `_ArbitrageTask|{symbol}`.
- Внутри неё запускаются задачи `_OrderbookTask|{symbol}|{exchange_id}` для всех бирж,
  где этот символ доступен.
- Задачи ордербуков пишут события в общую очередь символа.
- Задача арбитража читает очередь и принимает решение, можно ли считать спред.

Жизненный цикл символа:
1. Инициализация данных по символу и запуск задач ордербуков.
2. Получение обновлений по стакану и пересчёт лучшего ask/bid.
3. Если биржа перестаёт поставлять валидные данные, приходит `exchange_stopped`.
4. Если активных бирж стало меньше двух:
   - останавливаются задачи ордербуков символа;
   - по возможности закрывается `_ArbitrageTask|{symbol}`;
   - очищаются внутренние словари этого символа.

Почему требуется минимум 2 биржи:
- Арбитраж по определению требует минимум две площадки:
  на одной покупка (`ask`), на другой продажа (`bid`).
- При одной активной бирже расчёт спреда теряет смысл и может дать ложные сигналы.

Ограничения и технические заметки:
- `TaskManager` работает через `asyncio.all_tasks()` и имена задач.
- Классы используют много class-level словарей как общий state (shared state).
- Модуль ориентирован на длительную фоновую работу; shutdown реализован через signal.
"""

import signal

from app import task_manager
from modules import (cprint, round_down, get_average_orderbook_price, sync_time_with_exchange)
from pprint import pprint
import time
import ccxt.pro as ccxt
import asyncio
from decimal import Decimal
from typing import Any
from modules.exchange_instance import ExchangeInstance
from modules.task_manager import TaskManager
from modules.balance_manager import BalanceManager
from contextlib import AsyncExitStack
from modules.utils import to_decimal
from modules.exception_classes import (ReconnectLimitExceededError,
                                       InvalidOrEmptyOrderBookError,
                                       BaseArbitrageCalcException,
                                       InsufficientOrderBookVolumeError)


class ExchangeInstrument:
    """
    Рабочая единица "биржа + символ".

    Один экземпляр класса слушает стакан одного символа на одной бирже и
    публикует результат в очередь арбитража конкретного символа.
    """
    # Словарь флагов ордербук-сокетов вида {exchange_ids:{symbol: true/false}} - по нему можно собирать статистику открытых сокетов на всех биржах
    exchanges_instances_dict: dict[str, Any] = {}  # Словарь с экземплярами бирж
    balance_manager = None
    task_manager = None
    # Реестр экземпляров ExchangeInstrument:
    # {symbol: {exchange_id: {"obj": ExchangeInstrument, "task_name": str, "symbol_task_name": Task}}}
    exchange_instruments_obj_dict: dict[str, dict[str, dict[str, Any]]] = {}
    swap_processed_data_dict = None
    swap_raw_data_dict = None
    # Флаги активности ордербук-циклов:
    # {exchange_id: {symbol: bool}}
    orderbook_updating_status_dict: dict[str, dict[str, bool]] = {}
    # Счётчики принятых обновлений стакана:
    # {exchange_id: {symbol: int}}
    get_ex_orderbook_data_count: dict[str, dict[str, int]] = {}
    _configured = False

    _lock: asyncio.Lock | None = None

    @classmethod
    def get_lock(cls):
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    # Думаю надо взять за правило - передавать в экземпляр только аргументы экземпляра, аргументы класса передавать напрямую через имя класса, а лучше через отдельный метод
    @classmethod
    def get_configure(cls, *, exchanges_instances_dict=None, balance_manager=None, task_manager=None, swap_raw_data_dict=None, swap_processed_data_dict=None):
        if cls._configured:
            raise RuntimeError("ExchangeInstrument уже настроен")
        if exchanges_instances_dict is not None:
            cls.exchanges_instances_dict = exchanges_instances_dict
        if balance_manager is not None:
            cls.balance_manager = balance_manager
        if task_manager is not None:
            cls.task_manager = task_manager
        if swap_raw_data_dict is not None:
            cls.swap_raw_data_dict = swap_raw_data_dict
        if swap_processed_data_dict is not None:
            cls.swap_processed_data_dict = swap_processed_data_dict

        cls._configured = True
        return True

    def __init__(self, exchange_instance, symbol, orderbook_queue: asyncio.Queue, statistic_queue: asyncio.Queue = None):
        # ВНИМАНИЕ: экземпляр этого класса "привязан" к одной связке:
        # (exchange_id, symbol). Очередь общая для символа и читается ArbitrageManager.
        self.symbol = symbol
        self.exchange = exchange_instance
        self.exchange_id = exchange_instance.id
        self.orderbook_queue = orderbook_queue
        self.statistic_queue = statistic_queue
        self.orderbook_updating = False
        self.fee = None
        self.min_amount = None
        self.contract_size = None
        self.precision_amount = None
        if self.swap_raw_data_dict:
            self.update_swap_data()

    def update_swap_data(self):
        """
        Подтянуть биржевые параметры символа из `swap_raw_data_dict`.
        Используется для проверок лимитов и расчётов объёма/комиссии.
        """
        self.contract_size = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('contractSize', None)
        self.precision_amount = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('precision', {}).get('amount', None)
        self.min_amount = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('limits', {}).get('amount', {}).get('min', None)
        swap_data = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id, {})
        taker_fee = swap_data.get('taker_fee', None)
        taker = swap_data.get('taker', None)
        self.fee = taker_fee if taker_fee is not None else taker

    async def watch_orderbook(self):
        """
        Асинхронно следит за стаканом указанного символа.

        Что делает метод:
        1. Ждёт инициализацию баланса биржи.
        2. В цикле получает стакан по WebSocket.
        3. Если стакан изменился, считает среднюю цену покупки/продажи.
        4. Публикует обновление в `orderbook_queue`.
        5. Если данных для расчёта объёма недостаточно - отправляет событие остановки.
        6. При временных сетевых ошибках пробует reconnect с backoff.

        Важно:
        - Этот метод НИЧЕГО не считает по арбитражу напрямую.
        - Его задача только поставлять корректные входные данные для символа.
        """
        max_reconnect_attempts = 5
        reconnect_attempts = 0
        count = 0
        new_count = 0
        old_ask = tuple()
        old_bid = tuple()

        TICK_WINDOW_SEC = 10.0
        window_start_ts = time.monotonic()
        ticks_total = 0
        ticks_changed = 0

        try:
            print(f"[{self.exchange_id}] START watch_orderbook for {self.symbol}")

            await self.balance_manager.get_balance_instance(self.exchange_id).wait_initialized()

            self.__class__.orderbook_updating_status_dict.setdefault(self.exchange_id, {})[self.symbol] = True
            self.get_ex_orderbook_data_count.setdefault(self.exchange_id, {})[self.symbol] = 0

            while self.__class__.orderbook_updating_status_dict[self.exchange_id][self.symbol]:

                try:
                    bm = self.balance_manager.get_balance_instance(self.exchange_id)

                    if not await bm.is_balance_valid():
                        bal = await bm.get_balance()
                        cprint.warning_r(
                            f"[{self.exchange_id}] balance invalid, but continuing; current_balance={bal}"
                        )
                        # НЕ continue — просто читаем стакан дальше

                    max_deal_volume = self.balance_manager.max_deal_volume

                    # --- WebSocket ---
                    # Блокирующее ожидание нового стакана.
                    orderbook = await self.exchange.watchOrderBook(self.symbol)

                    # RAW тик
                    # print(f"[{self.exchange_id}] TICK_RAW ts={time.monotonic()}")

                    #
                    if not isinstance(orderbook, dict) or 'asks' not in orderbook or 'bids' not in orderbook:
                        raise InvalidOrEmptyOrderBookError(
                            exchange_id=self.exchange_id, symbol=self.symbol, orderbook_data=orderbook
                        )

                    self.get_ex_orderbook_data_count[self.exchange_id][self.symbol] += 1
                    reconnect_attempts = 0
                    ticks_total += 1
                    count += 1

                    depth = min(10, len(orderbook['asks']), len(orderbook['bids']))
                    new_ask = tuple(map(tuple, orderbook['asks'][:depth]))
                    new_bid = tuple(map(tuple, orderbook['bids'][:depth]))

                    # Пересчёт делаем только при реальном изменении top-N стакана.
                    if new_ask != old_ask or new_bid != old_bid:
                        # print(f"[{self.exchange_id}] CHANGE depth10")
                        ticks_changed += 1
                        new_count += 1

                        try:
                            average_ask = get_average_orderbook_price(
                                orderbook['asks'], money=max_deal_volume,
                                is_ask=True, log=True,
                                exchange=self.exchange_id, symbol=self.symbol
                            )
                            average_bid = get_average_orderbook_price(
                                orderbook['bids'], money=max_deal_volume,
                                is_ask=False, log=True,
                                exchange=self.exchange_id, symbol=self.symbol
                            )
                        except InsufficientOrderBookVolumeError as e:
                            print(f"[{self.exchange_id}] STOP insufficient volume: {e}")
                            # Сигнализируем арбитражному циклу символа, что эта биржа
                            # больше не может участвовать в расчёте.
                            stop_event = {
                                "type": "exchange_stopped",
                                "ts": time.monotonic(),
                                "symbol": self.symbol,
                                "exchange_id": self.exchange_id,
                                "reason": "insufficient_volume",
                            }
                            try:
                                self.orderbook_queue.put_nowait(stop_event)
                            except asyncio.QueueFull:
                                try:
                                    _ = self.orderbook_queue.get_nowait()
                                except asyncio.QueueEmpty:
                                    pass
                                await self.orderbook_queue.put(stop_event)
                            self.__class__.orderbook_updating_status_dict[self.exchange_id][self.symbol] = False
                            break

                        # Публикуем только полные данные:
                        # обе стороны (ask и bid) должны быть рассчитаны.
                        if average_ask is not None and average_bid is not None:
                            output_data = {
                                "type": "orderbook_update",
                                "ts": time.monotonic(),
                                "symbol": self.symbol,
                                "exchange_id": self.exchange_id,
                                "count": count,
                                "new_count": new_count,
                                "average_ask": average_ask,
                                "average_bid": average_bid,
                                "closed_error": False,
                            }

                            old_ask = new_ask
                            old_bid = new_bid

                            try:
                                self.orderbook_queue.put_nowait(output_data)
                            except asyncio.QueueFull:
                                try:
                                    _ = self.orderbook_queue.get_nowait()
                                except asyncio.QueueEmpty:
                                    pass
                                await self.orderbook_queue.put(output_data)

                    # --- статистика ---
                    # Здесь оставлено окно для будущих метрик качества стрима.
                    now = time.monotonic()
                    if now - window_start_ts >= TICK_WINDOW_SEC:
                        window_start_ts = now
                        ticks_total = 0
                        ticks_changed = 0

                except Exception as e:
                    method_name = "watchOrderBook"
                    method_params = {"symbol": self.symbol}

                    print(
                        f"[{self.exchange_id}][EXCEPTION_LOOP] "
                        f"method={method_name} params={method_params} error={repr(e)}"
                    )
                    print(
                        f"[{self.exchange_id}][EXCEPTION_TYPE] "
                        f"method={method_name} params={method_params} type={type(e)}"
                    )

                    import traceback
                    traceback.print_exc()

                    transient_errors = [
                        'RequestTimeout',
                        'ping-pong keepalive missing on time',
                        'timed out',
                        '1000', 'closed by remote server', 'Cannot write to closing transport',
                        'Connection closed', 'WebSocket is already closing', 'Transport closed',
                        'broken pipe', 'reset by peer'
                    ]

                    is_transient = any(x in str(e) for x in transient_errors)
                    print(
                        f"[{self.exchange_id}][TRANSIENT_MATCH] "
                        f"method={method_name} params={method_params} is_transient={is_transient}"
                    )

                    if is_transient:
                        reconnect_attempts += 1
                        print(
                            f"[{self.exchange_id}][RECONNECT] "
                            f"method={method_name} params={method_params} attempt={reconnect_attempts}"
                        )

                        if reconnect_attempts > max_reconnect_attempts:
                            print(f"[{self.exchange_id}][RECONNECT_LIMIT] exceeded")
                            raise ReconnectLimitExceededError(
                                exchange_id=self.exchange.id,
                                symbol=self.symbol,
                                attempts=reconnect_attempts
                            )

                        await asyncio.sleep(4 + (2 ** reconnect_attempts))
                        continue

                    print(
                        f"[{self.exchange_id}][FATAL_ERROR] "
                        f"method={method_name} params={method_params} stopping"
                    )
                    raise

        except Exception as e:
            print(f"[{self.exchange_id}][FATAL] watch_orderbook crashed: {repr(e)}")
            import traceback
            traceback.print_exc()

        finally:
            print(f"[{self.exchange_id}][STOPPED] watch_orderbook finished")
            self.__class__.orderbook_updating_status_dict.setdefault(self.exchange_id, {})[self.symbol] = False


class ArbitrageManager:
    """
    Координатор арбитража по символу.

    Экземпляр класса создаётся на конкретный symbol и:
    - запускает подписки на стакан по всем подходящим биржам;
    - держит локальный кэш цен `symbol_average_price_dict`;
    - вычисляет лучшие цены между биржами;
    - при невозможности продолжения корректно завершает symbol-task.
    """
    _lock = asyncio.Lock()
    exchanges_instances_dict: dict[str, Any] = {}  # Словарь с экземплярами бирж
    arbitrage_obj_dict: dict[str, "ArbitrageManager"] = {}
    # Флаги активности symbol-циклов:
    # {symbol: bool}
    symbol_arbitrage_enable_flag_dict: dict[str, bool] = {}
    task_manager = None
    balance_manager = None
    max_deal_slots = None
    free_deals_slots = None  # Количество доступных слотов сделок (активная сделка занимает один слот).
    # Открытия сделки доступно пока есть свободный слот.
    swap_processed_data_dict: dict[str, dict[str, dict[str, Any]]] = {}  # Словарь с данными символов для создания объектов класса
    swap_raw_data_dict: dict[str, dict[str, dict[str, Any]]] = {}  # Словарь с сырыми данными по символу маркета

    _configured = False

    @classmethod
    def get_configure(cls, *, exchanges_instances_dict=None, balance_manager=None,
                      task_manager=None, swap_raw_data_dict=None, swap_processed_data_dict=None, max_deal_slots=None):
        if cls._configured:
            raise RuntimeError("ArbitrageManager уже настроен")
        if exchanges_instances_dict is not None:
            cls.exchanges_instances_dict = exchanges_instances_dict
        if balance_manager is not None:
            cls.balance_manager = balance_manager
        if task_manager is not None:
            cls.task_manager = task_manager
        if swap_raw_data_dict is not None:
            cls.swap_raw_data_dict = swap_raw_data_dict
        if swap_processed_data_dict is not None:
            cls.swap_processed_data_dict = swap_processed_data_dict
        if max_deal_slots:
            cls.max_deal_slots = max_deal_slots
        cls._configured = True
        return True

    @classmethod
    # Основная точка входа в класс ArbitrageManager - асинхронная, метод класса
    async def create_all_arbitrage_objects(cls):
        """
        Создаёт по одному экземпляру ArbitrageManager на каждый символ и
        запускает отдельную задачу `_ArbitrageTask|{symbol}`.
        """
        for symbol, deal_data in cls.swap_processed_data_dict.items():
            instance = cls(symbol, deal_data)
            cls.arbitrage_obj_dict[symbol] = instance
            task_name = f"_ArbitrageTask|{symbol}"
            # Для каждого символа-экземпляра своя задача
            cls.task_manager.add_task(name=task_name, coro_func=instance.symbol_arbitrage)

    # init символа-экземпляра
    def __init__(self, symbol, deal_data):
        # Все поля ниже относятся ТОЛЬКО к одному symbol-экземпляру.
        self.symbol = symbol
        self.deal_data = deal_data
        self.task_manager = self.__class__.task_manager
        self.orderbook_queue = asyncio.Queue()
        self.statistic_queue = asyncio.Queue()
        # Локальный кэш последних средних цен по биржам:
        # {exchange_id: {"average_ask": Decimal, "average_bid": Decimal}}
        self.symbol_average_price_dict: dict[str, dict[str, Decimal]] = {}

        self.min_ask = Decimal('+Infinity')
        self.min_ask_exchange = ""
        self.max_bid = Decimal('-Infinity')
        self.max_bid_exchange = ""
        self.best_close_ask = Decimal('+Infinity')
        self.best_close_bid = Decimal('-Infinity')

    async def _shutdown_symbol_arbitrage(self, *, reason: str, active_exchange_ids: set[str]) -> None:
        """
        Полностью останавливает арбитраж по символу:
        - выключает флаг цикла;
        - отменяет orderbook-задачи символа;
        - по возможности отменяет `_ArbitrageTask|{symbol}`;
        - очищает локальный кэш и реестры экземпляров;
        - выводит диагностические сообщения.

        Почему отдельный метод:
        - остановка символа включает несколько шагов и должна выполняться единообразно;
        - в будущем сюда удобно добавить дополнительный cleanup (метрики, отчёты и т.д.).
        """
        print(
            f"[{self.symbol}] SHUTDOWN start | reason={reason} | "
            f"active_exchanges={len(active_exchange_ids)}"
        )

        type(self).symbol_arbitrage_enable_flag_dict[self.symbol] = False

        # Сначала останавливаем задачи по стаканам для этого символа.
        for exchange_id, data in ExchangeInstrument.exchange_instruments_obj_dict.get(self.symbol, {}).items():
            task_name = data.get("task_name")
            if task_name:
                print(f"[{self.symbol}] cancelling orderbook task: {task_name}")
                await self.task_manager.cancel_task(
                    name=task_name,
                    reason=f"{self.symbol}: {reason}"
                )

        # Пытаемся закрыть задачу арбитража по символу.
        arbitrage_task_name = f"_ArbitrageTask|{self.symbol}"
        current_task = asyncio.current_task()
        current_task_name = current_task.get_name() if current_task else ""
        if current_task_name == arbitrage_task_name:
            print(
                f"[{self.symbol}] arbitrage task is current task "
                f"({arbitrage_task_name}), it will finish by natural exit"
            )
        else:
            print(f"[{self.symbol}] cancelling arbitrage task: {arbitrage_task_name}")
            await self.task_manager.cancel_task(
                name=arbitrage_task_name,
                reason=f"{self.symbol}: {reason}"
            )

        # Затем чистим внутренние структуры символа.
        self.symbol_average_price_dict.clear()
        ExchangeInstrument.exchange_instruments_obj_dict.pop(self.symbol, None)
        type(self).symbol_arbitrage_enable_flag_dict.pop(self.symbol, None)
        type(self).arbitrage_obj_dict.pop(self.symbol, None)

        print(f"[{self.symbol}] SHUTDOWN complete | arbitrage instance closed")

    # Точка выхода в символ-экземпляр класса
    # Здесь происходит основная оценка арбитража и принятие решений, анализ и статистика символа и отсюда отправка на вывод в таблицу
    async def symbol_arbitrage(self):
        """
        Главный цикл расчёта арбитража для одного символа.

        Поток исполнения:
        1. Запускаем orderbook-задачи для бирж символа.
        2. Читаем события из `orderbook_queue`.
        3. Обновляем кэш средних цен по биржам.
        4. Когда есть минимум 2 биржи, ищем:
           - минимальный ask (где дешевле купить),
           - максимальный bid (где дороже продать).
        5. Считаем `open_ratio`.
        6. Если бирж стало < 2, делаем shutdown символа.
        """

        symbol_task_name = asyncio.current_task()
        print(f"Имя задачи: {symbol_task_name.get_name()}")

        type(self).symbol_arbitrage_enable_flag_dict[self.symbol] = True

        print("------------------------------")
        pprint(self.__class__.swap_processed_data_dict)
        print("------------------------------")

        # Запуск задач ордербуков.
        # active_exchange_ids отражает "кто ещё участвует" в текущем символе.
        active_exchange_ids = set(self.__class__.swap_processed_data_dict[self.symbol].keys())
        for exchange_id in self.__class__.swap_processed_data_dict[self.symbol].keys():
            exchange_instance = self.exchanges_instances_dict.get(exchange_id)
            _obj = ExchangeInstrument(
                exchange_instance=exchange_instance,
                symbol=self.symbol,
                orderbook_queue=self.orderbook_queue,
                statistic_queue=self.statistic_queue
            )
            task_name = f"_OrderbookTask|{self.symbol}|{exchange_id}"

            ExchangeInstrument.exchange_instruments_obj_dict.setdefault(self.symbol, {}) \
                .setdefault(exchange_id, {})

            ExchangeInstrument.exchange_instruments_obj_dict[self.symbol][exchange_id] = {
                "obj": _obj,
                "task_name": task_name,
                "symbol_task_name": symbol_task_name,
            }

            self.task_manager.add_task(name=task_name, coro_func=_obj.watch_orderbook)

        # Основной цикл событий символа.
        # Работает, пока флаг symbol_arbitrage_enable_flag_dict[self.symbol] == True.
        while type(self).symbol_arbitrage_enable_flag_dict[self.symbol]:

            orderbook_queue_data = await self.orderbook_queue.get()

            event_type = orderbook_queue_data.get("type")
            if event_type == "exchange_stopped":
                # Биржа сообщила, что больше не может поставлять корректный поток данных.
                stopped_exchange_id = orderbook_queue_data.get("exchange_id")
                reason = orderbook_queue_data.get("reason")

                if stopped_exchange_id in active_exchange_ids:
                    active_exchange_ids.remove(stopped_exchange_id)
                self.symbol_average_price_dict.pop(stopped_exchange_id, None)

                print(
                    f"[{self.symbol}] exchange stopped: {stopped_exchange_id}, "
                    f"reason={reason}, active_exchanges={len(active_exchange_ids)}"
                )

                if len(active_exchange_ids) < 2:
                    # Ниже двух бирж продолжать бессмысленно:
                    # арбитраж по определению невозможен.
                    await self._shutdown_symbol_arbitrage(
                        reason="active exchanges < 2",
                        active_exchange_ids=active_exchange_ids
                    )
                    break
                continue

            # DEBUG
            # print("FROM_QUEUE", self.symbol, orderbook_queue_data.get("exchange_id"), orderbook_queue_data.get("count"))

            # Защита от мусора
            if event_type != "orderbook_update":
                continue

            queue_exchange_id = orderbook_queue_data.get("exchange_id")
            if not queue_exchange_id:
                continue

            # Обновляем локальный кэш последних цен от конкретной биржи.
            self.symbol_average_price_dict[queue_exchange_id] = {
                "average_ask": orderbook_queue_data["average_ask"],
                "average_bid": orderbook_queue_data["average_bid"]
            }

            # print(queue_exchange_id, orderbook_queue_data)

            # Нужно минимум 2 биржи
            if len(self.symbol_average_price_dict) < 2:
                continue

            # Инициализация временных переменных для поиска лучших цен.
            min_ask = Decimal('+Infinity')
            max_bid = Decimal('-Infinity')
            min_ask_ex = None
            max_bid_ex = None

            # Поиск лучших цен
            for ex_id, data in self.symbol_average_price_dict.items():
                ask = data["average_ask"]
                bid = data["average_bid"]

                if ask < min_ask:
                    min_ask = ask
                    min_ask_ex = ex_id

                if bid > max_bid:
                    max_bid = bid
                    max_bid_ex = ex_id

            # Если что-то не определилось — пропускаем
            if not min_ask_ex or not max_bid_ex:
                continue

            # Расчёт процента "бумажного" спреда между лучшим bid и лучшим ask.
            open_ratio = round_down(100 * (max_bid - min_ask) / min_ask, 2)

            if open_ratio > .5:
                print(open_ratio, min_ask_ex, max_bid_ex, self.symbol, "%")

async def main():
    """
    Локальная точка запуска модуля.

    Что делает:
    1. Поднимает подключения к биржам.
    2. Запускает задачи мониторинга баланса.
    3. Формирует словари инструментов (`swap_raw_data_dict` / `swap_processed_data_dict`).
    4. Настраивает `ArbitrageManager` и `ExchangeInstrument`.
    5. Запускает задачи арбитража по символам.
    6. Ждёт системный сигнал завершения (SIGINT/SIGTERM).
    """
    TASK_MANAGER = TaskManager()
    MAX_DEAL_SLOTS = to_decimal('2')
    EXCHANGE_ID_LIST = ["okx", "htx", "gateio"]

    BalanceManager.task_manager = TASK_MANAGER
    BalanceManager.max_deal_slots = MAX_DEAL_SLOTS

    exchange_instance_dict = {}  # Словарь с экземплярами бирж {symbol:{exchange_obj}}
    swap_raw_data_dict = {}  # Словарь с сырыми данными {symbol:{exchange_id: <data>}})
    swap_processed_data_dict = {}  # Словарь с данными для открытия сделок

    # Основной контекст запуск экземпляров из списка бирж
    async with AsyncExitStack() as stack:
        async def open_exchange(exchange_id):
            exchange = await stack.enter_async_context(ExchangeInstance(ccxt, exchange_id, log=True))
            return exchange_id, exchange

        # Открываем все биржи параллельно для ускорения старта.
        results = await asyncio.gather(*(open_exchange(exchange_id) for exchange_id in EXCHANGE_ID_LIST))
        exchange_instance_dict = dict(results)

        started_balance_managers = []

        # После открытия всех бирж запускаем balance-задачи для всех бирж
        for exchange_id, exchange in exchange_instance_dict.items():
            balance_manager_obj = BalanceManager(exchange)
            task_name = f"_BalanceTask|{exchange.id}"
            TASK_MANAGER.add_task(name=task_name, coro_func=balance_manager_obj._watch_balance)
            started_balance_managers.append(balance_manager_obj)

            # Сборка сырого словаря по символам:
            # {symbol: {exchange_id: swap_market_data}}
            for pair_data in exchange.spot_swap_pair_data_dict.values():
                if swap_data := pair_data.get("swap"):
                    if not swap_data or swap_data.get("settle") != "USDT":
                        continue
                    symbol = swap_data["symbol"]
                    if swap_data.get('linear') and not swap_data.get('inverse'):
                        swap_raw_data_dict.setdefault(symbol, {})[exchange_id] = swap_data

        # Ожидание инициализации всех балансов после запуска всех watch-задач.
        # Так первичные fetch_balance идут одновременно, а не по очереди.
        await asyncio.gather(*(bm.wait_initialized() for bm in started_balance_managers))

        # Парсинг/нормализация данных для swap_processed_data_dict:
        # здесь оставляем только символы, присутствующие минимум на 2 биржах.
        for symbol, volume in swap_raw_data_dict.items():
            # Если символ только на одной бирже, он не попадает в переработанный словарь
            if len(volume) < 2:
                continue
            max_contractSize = 0  # Максимальный размер единичного контракта среди бирж символа
            # Объём сделки должен учитывать максимально "крупный" контракт среди бирж символа.
            # Идея: дальнейшие вычисления можно унифицировать через max_contractSize.
            for exchange_id, data in volume.items():
                contractSize = data.get('contractSize')
                if max_contractSize < contractSize:
                    max_contractSize = contractSize
                swap_processed_data_dict.setdefault(symbol, {}).setdefault(exchange_id, {})['contractSize'] = contractSize
            for exchange_id, data in volume.items():
                swap_processed_data_dict[symbol][exchange_id]['max_contractSize'] = max_contractSize

        print("*****************************")
        pprint(swap_processed_data_dict)
        print("*****************************")


        for symbol in list(swap_raw_data_dict):
            if len(swap_raw_data_dict[symbol]) < 2:
                swap_raw_data_dict.pop(symbol)
        c1 = 0
        c2 = 0
        c3 = 0
        for symbol in list(swap_raw_data_dict):
            if 'gateio' in swap_raw_data_dict[symbol]:
                c1 += 1
            if 'htx' in swap_raw_data_dict[symbol]:
                c2 += 1
            if 'okx' in swap_raw_data_dict[symbol]:
                c3 += 1

        # pprint(swap_raw_data_dict)

        # Инициализация аргументов класса ArbitrageManager и ExchangeInstrument
        if swap_processed_data_dict and swap_raw_data_dict:
            ArbitrageManager.get_configure(
                exchanges_instances_dict=exchange_instance_dict,
                balance_manager=BalanceManager,
                task_manager=TASK_MANAGER,
                max_deal_slots=MAX_DEAL_SLOTS,
                swap_raw_data_dict=swap_raw_data_dict,
                swap_processed_data_dict=swap_processed_data_dict

            )

            ExchangeInstrument.get_configure(
                exchanges_instances_dict=exchange_instance_dict,
                balance_manager=BalanceManager,
                task_manager=TASK_MANAGER,
                swap_raw_data_dict=swap_raw_data_dict,
                swap_processed_data_dict=swap_processed_data_dict
            )

            await ArbitrageManager.create_all_arbitrage_objects()  # Основная точка входа в класс ArbitrageManager
        print(len(swap_raw_data_dict))
        print(c1, c2, c3)

        # ---- graceful shutdown ----
        # Процесс остаётся жить, пока не придёт системный сигнал на завершение.
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, stop_event.set)
            loop.add_signal_handler(signal.SIGTERM, stop_event.set)
        except NotImplementedError:
            pass

        await stop_event.wait()


if __name__ == "__main__":
    asyncio.run(main())
