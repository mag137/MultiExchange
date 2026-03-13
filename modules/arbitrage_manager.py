__version__ = "0.9i"

"""Координация арбитражного потока по символу между несколькими биржами.

Модуль связывает две зоны ответственности:
- получение и нормализацию ордербуков по конкретной бирже и символу;
- координацию расчёта арбитражного спреда на уровне одного торгового символа.

Основные сущности:
- `ExchangeInstrument`: слушает `watchOrderBook` для пары
  `(exchange_id, symbol)` и публикует в единую торговую очередь либо
  валидный стакан, либо изменение состояния источника;
- `ArbitrageManager`: получает события от всех `ExchangeInstrument` этого
  символа, хранит локальный кэш средних цен и считает лучшие `ask` и `bid`
  между биржами.

Архитектурная модель:
- на каждый символ создаётся отдельная задача `_ArbitrageTask|{symbol}`;
- внутри символа создаются задачи `_OrderbookTask|{symbol}|{exchange_id}`;
- задачи ордербуков пишут события в общую очередь символа;
- символ-менеджер читает эту очередь и решает, можно ли учитывать биржу в
  поиске сигнала прямо сейчас.

Notes:
    Арбитражный расчёт имеет смысл только при наличии минимум двух активных
    бирж. Если валидный поток остаётся только у одной площадки, модуль
    останавливает symbol-level обработку и чистит связанное состояние.
    Для `stream_timeout` используется не мгновенный resume, а восстановление
    с гистерезисом. Это осознанная торговая политика: единичный вернувшийся
    тик не должен сразу возвращать биржу в поиск сигнала.
"""

import signal
import multiprocessing
import os

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
from modules.orderbook_interval_stats import OrderbookIntervalStatsWindow
from modules.WebGrid_Socket_Polling import run_web_grid_process
from modules.exception_classes import (ReconnectLimitExceededError,
                                       InsufficientOrderBookVolumeError)


class ExchangeInstrument:
    """Рабочая единица подписки на ордербук для одной биржи и одного символа.

    Экземпляр класса инкапсулирует всё, что относится к связке
    `(exchange_id, symbol)`:
    - ожидание готовности баланса;
    - чтение ордербука по WebSocket;
    - расчёт средних цен покупки и продажи;
    - публикацию торговых событий в очередь арбитража символа;
    - локальный контроль качества входящего стрима.

    Notes:
        Класс не принимает торговых решений и не считает спред между биржами.
        Его роль ограничена поставкой валидных входных данных вверх по пайплайну.
        Для причины `stream_timeout` восстановление намеренно делается более
        консервативным, чем для ошибок структуры стакана или нехватки объёма.
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
        """Вернуть общий lazy-initialized lock класса."""
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    def get_configure(cls, *, exchanges_instances_dict=None, balance_manager=None, task_manager=None, swap_raw_data_dict=None, swap_processed_data_dict=None):
        """One-time конфигурирование class-level зависимостей.

        Args:
            exchanges_instances_dict: Реестр экземпляров бирж.
            balance_manager: Менеджер балансов проекта.
            task_manager: Менеджер фоновых задач.
            swap_raw_data_dict: Сырые market-данные по swap-инструментам.
            swap_processed_data_dict: Подготовленные данные символов.

        Returns:
            `True`, если конфигурация успешно применена.

        Raises:
            RuntimeError: Если класс уже был сконфигурирован ранее.
        """
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

    # Реестр констант политики качества потока:
    #
    # STREAM_PAUSE_TIMEOUT_SEC:
    #   Сколько секунд можно не получать новый стакан, прежде чем источник
    #   считается подвисшим и переводится в `exchange_paused(stream_timeout)`.
    #
    # STREAM_TIMEOUT_RESUME_COOLDOWN_SEC:
    #   Минимальная выдержка после `stream_timeout`, в течение которой биржа
    #   не может вернуться в поиск сигнала, даже если пришёл одиночный тик.
    #
    # STREAM_TIMEOUT_RESUME_WINDOW_SEC:
    #   Длина окна наблюдения после cooldown. Внутри этого окна считаются
    #   валидные стаканы, подтверждающие восстановление потока.
    #
    # STREAM_TIMEOUT_RESUME_TICKS_REQUIRED:
    #   Минимум валидных стаканов в окне наблюдения, после которого источник
    #   считается действительно восстановившимся и получает `exchange_resumed`.
    STREAM_PAUSE_TIMEOUT_SEC = 30.0
    STREAM_TIMEOUT_RESUME_COOLDOWN_SEC = 300.0
    STREAM_TIMEOUT_RESUME_WINDOW_SEC = 30.0
    STREAM_TIMEOUT_RESUME_TICKS_REQUIRED = 5

    def __init__(self, exchange_instance, symbol, orderbook_queue: asyncio.Queue):
        """Инициализировать подписку на ордербук конкретной биржи.

        Args:
            exchange_instance: Экземпляр биржи, через который идёт подписка.
            symbol: Торговый символ, для которого слушается ордербук.
            orderbook_queue: Очередь публикации событий `orderbook_update` и
                служебных событий состояния источника.

        Notes:
            Текущий контракт событий в `orderbook_queue`:

            `orderbook_update`:
            {
                "type": "orderbook_update",
                "symbol": str,
                "exchange_id": str,
                "average_ask": Decimal,
                "average_bid": Decimal,
                "stream_status": "ok",
                "mean_dt": float | None,
                "count": int,
                "new_count": int,
                "ts": float,
            }

            `exchange_paused`:
            {
                "type": "exchange_paused",
                "symbol": str,
                "exchange_id": str,
                "stream_status": "paused",
                "reason": str,
                "ts": float,
                # Опционально для timeout-паузы:
                "last_interval_stats": dict | None,
            }

            `exchange_resumed`:
            {
                "type": "exchange_resumed",
                "symbol": str,
                "exchange_id": str,
                "stream_status": "ok",
                "reason": "stream_recovered" | "data_recovered",
                "ts": float,
            }
        """
        self.symbol = symbol
        self.exchange = exchange_instance
        self.exchange_id = exchange_instance.id
        self.orderbook_queue = orderbook_queue
        self.orderbook_updating = False
        self.fee = None
        self.min_amount = None
        self.contract_size = None
        self.precision_amount = None
        if self.swap_raw_data_dict:
            self.update_swap_data()

    async def _publish_orderbook_event(self, event: dict[str, Any]) -> None:
        """Положить торговое событие в основную очередь символа."""
        try:
            self.orderbook_queue.put_nowait(event)
        except asyncio.QueueFull:
            try:
                _ = self.orderbook_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            await self.orderbook_queue.put(event)

    def update_swap_data(self):
        """Обновить кэш параметров инструмента из `swap_raw_data_dict`.

        Notes:
            Метод подтягивает данные, которые позже используются для проверок
            лимитов, размера контракта, точности объёма и расчёта комиссии.
        """
        self.contract_size = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('contractSize', None)
        self.precision_amount = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('precision', {}).get('amount', None)
        self.min_amount = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('limits', {}).get('amount', {}).get('min', None)
        swap_data = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id, {})
        taker_fee = swap_data.get('taker_fee', None)
        taker = swap_data.get('taker', None)
        self.fee = taker_fee if taker_fee is not None else taker

    async def watch_orderbook(self):
        """Непрерывно читать ордербук, считать средние цены и публиковать события.

        Метод работает как основной цикл подписки для одной связки
        `(exchange_id, symbol)`.

        Поток работы:
        1. Дождаться инициализации баланса биржи.
        2. В цикле получать очередной ордербук из `watchOrderBook`.
        3. Валидировать структуру данных.
        4. Если top-N ордербук изменился, пересчитать `average_ask` и
           `average_bid`.
        5. Отправить событие `orderbook_update` в `orderbook_queue`.
        6. При деградации источника публиковать `exchange_paused`.
        7. При восстановлении потока публиковать `exchange_resumed`.
        8. При временных сетевых ошибках пробовать reconnect с backoff.

        Notes:
            Метод не считает арбитраж напрямую. Он только поставляет валидные
            входные данные и состояние источника для symbol-level координатора.
            В `orderbook_update` дополнительно передаётся `mean_dt` как
            компактная метрика средней плотности потока по последнему окну
            интервалов. Это вспомогательное поле для downstream-логики и
            диагностики, но не отдельный event stream.
            Для причины `stream_timeout` восстановление делается с гистерезисом:
            после паузы выдерживается cooldown 5 минут, затем в окне 30 секунд
            должно прийти минимум 3 валидных стакана.
            Это нормальная идея для торговой логики, если важнее не скорость
            возврата биржи, а защита от "дребезга" paused/resumed на рваном
            потоке. Цена такого решения: источник будет возвращаться в расчёт
            медленнее даже после честного восстановления канала.
        """
        max_reconnect_attempts = 5
        reconnect_attempts = 0
        count = 0
        new_count = 0
        old_ask = tuple()
        old_bid = tuple()

        interval_stats = OrderbookIntervalStatsWindow(max_intervals=50, emit_timeout_sec=30.0)
        latest_interval_stats = None
        mean_dt = None
        pause_reason: str | None = None
        stream_timeout_paused_at: float | None = None
        stream_timeout_resume_window_start: float | None = None
        stream_timeout_resume_valid_ticks = 0

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
                    # Блокирующее ожидание нового стакана с контролем таймаута
                    # деградации канала. Если данных нет слишком долго, биржа
                    # временно исключается из поиска сигнала.
                    orderbook = await asyncio.wait_for(
                        self.exchange.watchOrderBook(self.symbol),
                        timeout=self.STREAM_PAUSE_TIMEOUT_SEC,
                    )

                    #
                    if not isinstance(orderbook, dict) or 'asks' not in orderbook or 'bids' not in orderbook:
                        if pause_reason != "invalid_orderbook":
                            pause_reason = "invalid_orderbook"
                            await self._publish_orderbook_event({
                                "type": "exchange_paused",
                                "ts": time.monotonic(),
                                "symbol": self.symbol,
                                "exchange_id": self.exchange_id,
                                "stream_status": "paused",
                                "reason": pause_reason,
                            })
                        continue

                    self.get_ex_orderbook_data_count[self.exchange_id][self.symbol] += 1
                    reconnect_attempts = 0
                    count += 1

                    latest_interval_stats = interval_stats.observe()

                    depth = min(10, len(orderbook['asks']), len(orderbook['bids']))
                    if depth == 0:
                        if pause_reason != "empty_orderbook":
                            pause_reason = "empty_orderbook"
                            await self._publish_orderbook_event({
                                "type": "exchange_paused",
                                "ts": time.monotonic(),
                                "symbol": self.symbol,
                                "exchange_id": self.exchange_id,
                                "stream_status": "paused",
                                "reason": pause_reason,
                            })
                        continue

                    new_ask = tuple(map(tuple, orderbook['asks'][:depth]))
                    new_bid = tuple(map(tuple, orderbook['bids'][:depth]))

                    # В обычном режиме пересчитываем только при реальном
                    # изменении top-N стакана. После паузы делаем принудительный
                    # пересчёт на первом же валидном сообщении, даже если top-N
                    # совпал с последним состоянием до паузы.
                    #
                    # Это нужно по двум причинам:
                    # 1. Для non-timeout пауз можно быстро понять, что источник
                    #    снова пригоден к торговому расчёту.
                    # 2. Для timeout-паузы это сообщение участвует в счётчике
                    #    подтверждения восстановления потока.
                    if pause_reason is not None or new_ask != old_ask or new_bid != old_bid:
                        # print(f"[{self.exchange_id}] CHANGE depth10")
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
                            if pause_reason != "insufficient_volume":
                                print(f"[{self.exchange_id}] PAUSE insufficient volume: {e}")
                                pause_reason = "insufficient_volume"
                                await self._publish_orderbook_event({
                                    "type": "exchange_paused",
                                    "ts": time.monotonic(),
                                    "symbol": self.symbol,
                                    "exchange_id": self.exchange_id,
                                    "stream_status": "paused",
                                    "reason": pause_reason,
                                })
                            continue

                        # Публикуем только полные данные:
                        # обе стороны (ask и bid) должны быть рассчитаны.
                        if average_ask is not None and average_bid is not None:
                            if pause_reason is not None:
                                now = time.monotonic()
                                if pause_reason == "stream_timeout":
                                    # Resume после timeout делаем с гистерезисом:
                                    # сначала cooldown, затем окно подтверждения
                                    # потока с минимумом валидных стаканов.
                                    if stream_timeout_paused_at is None:
                                        stream_timeout_paused_at = now

                                    if now - stream_timeout_paused_at < self.STREAM_TIMEOUT_RESUME_COOLDOWN_SEC:
                                        continue

                                    if (
                                        stream_timeout_resume_window_start is None
                                        or now - stream_timeout_resume_window_start > self.STREAM_TIMEOUT_RESUME_WINDOW_SEC
                                    ):
                                        stream_timeout_resume_window_start = now
                                        stream_timeout_resume_valid_ticks = 0

                                    stream_timeout_resume_valid_ticks += 1
                                    if stream_timeout_resume_valid_ticks < self.STREAM_TIMEOUT_RESUME_TICKS_REQUIRED:
                                        continue

                                    pause_reason = None
                                    stream_timeout_paused_at = None
                                    stream_timeout_resume_window_start = None
                                    stream_timeout_resume_valid_ticks = 0
                                    await self._publish_orderbook_event({
                                        "type": "exchange_resumed",
                                        "ts": now,
                                        "symbol": self.symbol,
                                        "exchange_id": self.exchange_id,
                                        "stream_status": "ok",
                                        "reason": "stream_recovered",
                                    })
                                else:
                                    pause_reason = None
                                    stream_timeout_paused_at = None
                                    stream_timeout_resume_window_start = None
                                    stream_timeout_resume_valid_ticks = 0
                                    await self._publish_orderbook_event({
                                        "type": "exchange_resumed",
                                        "ts": now,
                                        "symbol": self.symbol,
                                        "exchange_id": self.exchange_id,
                                        "stream_status": "ok",
                                        "reason": "data_recovered",
                                    })

                            if latest_interval_stats:
                                mean_dt = latest_interval_stats["mean_dt"]

                            output_data = {
                                "type": "orderbook_update",
                                "ts": time.monotonic(),
                                "symbol": self.symbol,
                                "exchange_id": self.exchange_id,
                                "count": count,
                                "new_count": new_count,
                                "average_ask": average_ask,
                                "average_bid": average_bid,
                                "stream_status": "ok",
                                "mean_dt": mean_dt,
                            }

                            old_ask = new_ask
                            old_bid = new_bid

                            await self._publish_orderbook_event(output_data)

                except TimeoutError:
                    if pause_reason != "stream_timeout":
                        stream_timeout_paused_at = time.monotonic()
                        stream_timeout_resume_window_start = None
                        stream_timeout_resume_valid_ticks = 0
                        pause_reason = "stream_timeout"
                        timeout_event = {
                            "type": "exchange_paused",
                            "ts": stream_timeout_paused_at,
                            "symbol": self.symbol,
                            "exchange_id": self.exchange_id,
                            "stream_status": "paused",
                            "reason": pause_reason,
                        }
                        if latest_interval_stats is not None:
                            timeout_event["last_interval_stats"] = latest_interval_stats
                        await self._publish_orderbook_event(timeout_event)
                    continue

                except Exception as e:
                    print(f"[{self.exchange_id}][EXCEPTION_LOOP] {repr(e)}")
                    print(f"[{self.exchange_id}][EXCEPTION_TYPE] {type(e)}")

                    import traceback
                    traceback.print_exc()

                    transient_errors = [
                        '1000', 'closed by remote server', 'Cannot write to closing transport',
                        'Connection closed', 'WebSocket is already closing', 'Transport closed',
                        'broken pipe', 'reset by peer'
                    ]

                    is_transient = any(x in str(e) for x in transient_errors)
                    print(f"[{self.exchange_id}][TRANSIENT_MATCH] {is_transient}")

                    if is_transient:
                        reconnect_attempts += 1
                        print(f"[{self.exchange_id}][RECONNECT] attempt {reconnect_attempts}")

                        if reconnect_attempts > max_reconnect_attempts:
                            print(f"[{self.exchange_id}][RECONNECT_LIMIT] exceeded")
                            raise ReconnectLimitExceededError(
                                exchange_id=self.exchange.id,
                                symbol=self.symbol,
                                attempts=reconnect_attempts
                            )

                        await asyncio.sleep(4 + (2 ** reconnect_attempts))
                        continue

                    print(f"[{self.exchange_id}][FATAL_ERROR] stopping")
                    raise

        except Exception as e:
            print(f"[{self.exchange_id}][FATAL] watch_orderbook crashed: {repr(e)}")
            import traceback
            traceback.print_exc()
            await self._publish_orderbook_event({
                "type": "exchange_stopped",
                "ts": time.monotonic(),
                "symbol": self.symbol,
                "exchange_id": self.exchange_id,
                "reason": "fatal_watch_orderbook_error",
            })

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
    web_grid_table_queue = multiprocessing.Queue()
    web_grid_shared_values = {"shutdown": multiprocessing.Value('b', False)}
    web_grid_process = None
    web_grid_rows: dict[str, dict[str, Any]] = {}
    web_grid_event_mode = "snapshot"

    _configured = False

    @classmethod
    def _format_mean_dt(cls, mean_dt: float | None) -> str:
        if mean_dt is None:
            return "-"
        return f"{float(mean_dt):.3f}"

    @classmethod
    def _push_web_grid_snapshot(cls) -> None:
        if cls.web_grid_event_mode != "snapshot":
            return
        sorted_rows = sorted(
            cls.web_grid_rows.values(),
            key=lambda row: (-row["open_ratio_value"], row["symbol"])
        )

        grid_data: dict[Any, dict[int, dict[str, Any]]] = {
            "header": {
                0: {"text": "#", "align": "right"},
                1: {"text": "symbol", "align": "left"},
                2: {"text": "ask_ex", "align": "left"},
                3: {"text": "ask_mean_dt", "align": "right"},
                4: {"text": "bid_ex", "align": "left"},
                5: {"text": "bid_mean_dt", "align": "right"},
                6: {"text": "open_ratio", "align": "right"},
            }
        }

        for row_num, row in enumerate(sorted_rows, start=1):
            grid_data[row_num] = {
                0: {"text": row_num, "align": "right"},
                1: {"text": row["symbol"], "align": "left"},
                2: {"text": row["ask_exchange"], "align": "left"},
                3: {"text": row["ask_mean_dt"], "align": "right"},
                4: {"text": row["bid_exchange"], "align": "left"},
                5: {"text": row["bid_mean_dt"], "align": "right"},
                6: {"text": row["open_ratio"], "align": "right"},
            }

        cls.web_grid_table_queue.put(grid_data)

    @classmethod
    def _update_web_grid_row(
        cls,
        *,
        symbol: str,
        ask_exchange: str,
        ask_mean_dt: float | None,
        bid_exchange: str,
        bid_mean_dt: float | None,
        open_ratio: Decimal,
    ) -> None:
        row_data = {
            "symbol": symbol,
            "ask_exchange": ask_exchange,
            "ask_mean_dt": cls._format_mean_dt(ask_mean_dt),
            "bid_exchange": bid_exchange,
            "bid_mean_dt": cls._format_mean_dt(bid_mean_dt),
            "open_ratio": f"{open_ratio}%",
            "open_ratio_value": float(open_ratio),
        }
        cls.web_grid_rows[symbol] = row_data

        if cls.web_grid_event_mode == "event":
            cls.web_grid_table_queue.put({
                "grid_event": "upsert_row",
                "symbol": symbol,
                "row": row_data,
            })
            return

        cls._push_web_grid_snapshot()

    @classmethod
    def _remove_web_grid_row(cls, symbol: str) -> None:
        if cls.web_grid_rows.pop(symbol, None) is None:
            return

        if cls.web_grid_event_mode == "event":
            cls.web_grid_table_queue.put({
                "grid_event": "remove_row",
                "symbol": symbol,
            })
            return

        cls._push_web_grid_snapshot()

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
        # Важно: торговая логика живёт на одной очереди `orderbook_queue`.
        # В неё попадают только торгово значимые события по источнику:
        #
        # `orderbook_update`:
        # {
        #     "type": "orderbook_update",
        #     "symbol": str,
        #     "exchange_id": str,
        #     "average_ask": Decimal,
        #     "average_bid": Decimal,
        #     "stream_status": "ok",
        #     "mean_dt": float | None,
        #     "count": int,
        #     "new_count": int,
        #     "ts": float,
        # }
        #
        # `exchange_paused`:
        # {
        #     "type": "exchange_paused",
        #     "symbol": str,
        #     "exchange_id": str,
        #     "stream_status": "paused",
        #     "reason": str,
        #     "ts": float,
        # }
        #
        # `exchange_resumed`:
        # {
        #     "type": "exchange_resumed",
        #     "symbol": str,
        #     "exchange_id": str,
        #     "stream_status": "ok",
        #     "ts": float,
        # }
        # Локальный кэш последних торгуемых цен по биржам.
        # В словаре находятся только биржи, которые в данный момент можно
        # учитывать в поиске сигнала.
        # {exchange_id: {"average_ask": Decimal, "average_bid": Decimal, "mean_dt": float | None}}
        self.symbol_average_price_dict: dict[str, dict[str, Decimal | float | None]] = {}
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
        type(self)._remove_web_grid_row(self.symbol)
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
        2. Читаем торговые события из единой `orderbook_queue`.
        3. При `exchange_paused` исключаем биржу из поиска сигнала.
        4. При `orderbook_update` обновляем кэш последних торгуемых цен.
        5. Когда есть минимум 2 биржи, ищем:
           - минимальный ask (где дешевле купить),
           - максимальный bid (где дороже продать).
        6. Считаем `open_ratio`.
        7. Если бирж стало < 2 из-за `exchange_stopped`, делаем shutdown символа.

        Notes:
            `exchange_resumed` после `stream_timeout` приходит не мгновенно:
            источник должен выдержать cooldown и затем подтвердить
            восстановление несколькими валидными стаканами подряд.
            Это уменьшает ложное возвращение биржи в расчёт после единичного
            случайного тика на нестабильном канале.
        """

        symbol_task_name = asyncio.current_task()
        print(f"Имя задачи: {symbol_task_name.get_name()}")

        type(self).symbol_arbitrage_enable_flag_dict[self.symbol] = True

        # print("------------------------------")
        # pprint(self.__class__.swap_processed_data_dict)
        # print("------------------------------")

        # Запуск задач ордербуков.
        # active_exchange_ids отражает "кто ещё участвует" в текущем символе.
        active_exchange_ids = set(self.__class__.swap_processed_data_dict[self.symbol].keys())
        for exchange_id in self.__class__.swap_processed_data_dict[self.symbol].keys():
            exchange_instance = self.exchanges_instances_dict.get(exchange_id)
            _obj = ExchangeInstrument(
                exchange_instance=exchange_instance,
                symbol=self.symbol,
                orderbook_queue=self.orderbook_queue,
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
            if event_type == "exchange_paused":
                paused_exchange_id = orderbook_queue_data.get("exchange_id")
                reason = orderbook_queue_data.get("reason")
                if paused_exchange_id:
                    self.symbol_average_price_dict.pop(paused_exchange_id, None)
                    if len(self.symbol_average_price_dict) < 2:
                        type(self)._remove_web_grid_row(self.symbol)
                print(
                    f"[{self.symbol}] exchange paused: {paused_exchange_id}, "
                    f"reason={reason}"
                )
                continue

            if event_type == "exchange_resumed":
                resumed_exchange_id = orderbook_queue_data.get("exchange_id")
                print(f"[{self.symbol}] exchange resumed: {resumed_exchange_id}")
                continue

            if event_type == "exchange_stopped":
                # Биржа сообщила, что больше не может поставлять корректный поток данных.
                stopped_exchange_id = orderbook_queue_data.get("exchange_id")
                reason = orderbook_queue_data.get("reason")

                if stopped_exchange_id in active_exchange_ids:
                    active_exchange_ids.remove(stopped_exchange_id)
                self.symbol_average_price_dict.pop(stopped_exchange_id, None)
                if len(self.symbol_average_price_dict) < 2:
                    type(self)._remove_web_grid_row(self.symbol)

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
            if orderbook_queue_data.get("stream_status") != "ok":
                self.symbol_average_price_dict.pop(queue_exchange_id, None)
                if len(self.symbol_average_price_dict) < 2:
                    type(self)._remove_web_grid_row(self.symbol)
                continue

            # Обновляем локальный кэш последних цен от конкретной биржи.
            self.symbol_average_price_dict[queue_exchange_id] = {
                "average_ask": orderbook_queue_data["average_ask"],
                "average_bid": orderbook_queue_data["average_bid"],
                "mean_dt": orderbook_queue_data.get("mean_dt"),
            }

            # print(queue_exchange_id, orderbook_queue_data)

            # Нужно минимум 2 биржи
            if len(self.symbol_average_price_dict) < 2:
                type(self)._remove_web_grid_row(self.symbol)
                continue

            # Инициализация временных переменных для поиска лучших цен.
            min_ask = Decimal('+Infinity')
            max_bid = Decimal('-Infinity')
            min_ask_ex = None
            max_bid_ex = None
            min_ask_mean_dt = None
            max_bid_mean_dt = None

            # Поиск лучших цен
            for ex_id, data in self.symbol_average_price_dict.items():
                ask = data["average_ask"]
                bid = data["average_bid"]
                mean_dt = data.get("mean_dt")

                if ask < min_ask:
                    min_ask = ask
                    min_ask_ex = ex_id
                    min_ask_mean_dt = mean_dt

                if bid > max_bid:
                    max_bid = bid
                    max_bid_ex = ex_id
                    max_bid_mean_dt = mean_dt

            # Если что-то не определилось — пропускаем
            if not min_ask_ex or not max_bid_ex:
                continue

            # Расчёт процента "бумажного" спреда между лучшим bid и лучшим ask.
            open_ratio = round_down(100 * (max_bid - min_ask) / min_ask, 2)

            if open_ratio > 0.1:
                type(self)._update_web_grid_row(
                    symbol=self.symbol,
                    ask_exchange=min_ask_ex,
                    ask_mean_dt=min_ask_mean_dt,
                    bid_exchange=max_bid_ex,
                    bid_mean_dt=max_bid_mean_dt,
                    open_ratio=open_ratio,
                )
                print(open_ratio, min_ask_ex, max_bid_ex, self.symbol, "%")
            else:
                type(self)._remove_web_grid_row(self.symbol)

def calculate_worker_process_count(cpu_count: int | None = None) -> int:
    cpu_total = cpu_count or os.cpu_count() or 1
    if cpu_total <= 1:
        return 1
    if cpu_total <= 3:
        return 2
    return cpu_total - 2


def _reset_runtime_state(*, web_grid_queue=None, shared_values=None, web_grid_event_mode: str = "snapshot") -> None:
    BalanceManager.task_manager = None
    BalanceManager.exchange_balance_instance_dict = {}
    BalanceManager._lock = None
    BalanceManager.min_balance = None
    BalanceManager.exchange_min_balance = None
    BalanceManager.max_deal_volume = None
    BalanceManager.max_deal_slots = None
    BalanceManager._volume_ready_event = None

    ExchangeInstrument.exchanges_instances_dict = {}
    ExchangeInstrument.balance_manager = None
    ExchangeInstrument.task_manager = None
    ExchangeInstrument.exchange_instruments_obj_dict = {}
    ExchangeInstrument.swap_processed_data_dict = None
    ExchangeInstrument.swap_raw_data_dict = None
    ExchangeInstrument.orderbook_updating_status_dict = {}
    ExchangeInstrument.get_ex_orderbook_data_count = {}
    ExchangeInstrument._configured = False
    ExchangeInstrument._lock = None

    ArbitrageManager.exchanges_instances_dict = {}
    ArbitrageManager.arbitrage_obj_dict = {}
    ArbitrageManager.symbol_arbitrage_enable_flag_dict = {}
    ArbitrageManager.task_manager = None
    ArbitrageManager.balance_manager = None
    ArbitrageManager.max_deal_slots = None
    ArbitrageManager.free_deals_slots = None
    ArbitrageManager.swap_processed_data_dict = {}
    ArbitrageManager.swap_raw_data_dict = {}
    ArbitrageManager.web_grid_table_queue = web_grid_queue
    ArbitrageManager.web_grid_shared_values = shared_values or {"shutdown": multiprocessing.Value('b', False)}
    ArbitrageManager.web_grid_process = None
    ArbitrageManager.web_grid_rows = {}
    ArbitrageManager.web_grid_event_mode = web_grid_event_mode
    ArbitrageManager._configured = False
    ArbitrageManager._lock = asyncio.Lock()


async def _open_exchange_instances(
    stack: AsyncExitStack,
    exchange_id_list: list[str],
) -> tuple[dict[str, ExchangeInstance], list[tuple[str, Exception]]]:
    async def open_exchange(exchange_id: str):
        try:
            exchange = await stack.enter_async_context(ExchangeInstance(ccxt, exchange_id, log=True))
            return exchange_id, exchange, None
        except Exception as exc:
            return exchange_id, None, exc

    results = await asyncio.gather(*(open_exchange(exchange_id) for exchange_id in exchange_id_list))
    exchange_instance_dict: dict[str, ExchangeInstance] = {}
    failed: list[tuple[str, Exception]] = []
    for exchange_id, exchange, error in results:
        if exchange is None:
            if isinstance(error, Exception):
                failed.append((exchange_id, error))
            else:
                failed.append((exchange_id, Exception("unknown error")))
            continue
        exchange_instance_dict[exchange_id] = exchange

    if failed:
        for exchange_id, error in failed:
            cprint.warning_r(f"[worker] exchange init failed: {exchange_id}: {error}")

    return exchange_instance_dict, failed


async def _build_swap_data(
    exchange_instance_dict: dict[str, ExchangeInstance],
    task_manager: TaskManager,
) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, dict[str, dict[str, Any]]]]:
    swap_raw_data_dict: dict[str, dict[str, dict[str, Any]]] = {}
    swap_processed_data_dict: dict[str, dict[str, dict[str, Any]]] = {}
    started_balance_managers: list[BalanceManager] = []

    for exchange_id, exchange in exchange_instance_dict.items():
        balance_manager_obj = BalanceManager(exchange)
        task_name = f"_BalanceTask|{exchange.id}"
        task_manager.add_task(name=task_name, coro_func=balance_manager_obj._watch_balance)
        started_balance_managers.append(balance_manager_obj)

        for pair_data in exchange.spot_swap_pair_data_dict.values():
            swap_data = pair_data.get("swap")
            if not swap_data or swap_data.get("settle") != "USDT":
                continue
            if swap_data.get('linear') and not swap_data.get('inverse'):
                symbol = swap_data["symbol"]
                swap_raw_data_dict.setdefault(symbol, {})[exchange_id] = swap_data

    await asyncio.gather(*(bm.wait_initialized() for bm in started_balance_managers))

    for symbol, exchange_data in list(swap_raw_data_dict.items()):
        if len(exchange_data) < 2:
            swap_raw_data_dict.pop(symbol)
            continue

        max_contract_size = max(data.get('contractSize', 0) for data in exchange_data.values())
        for exchange_id, data in exchange_data.items():
            swap_processed_data_dict.setdefault(symbol, {}).setdefault(exchange_id, {})
            swap_processed_data_dict[symbol][exchange_id]['contractSize'] = data.get('contractSize')
            swap_processed_data_dict[symbol][exchange_id]['max_contractSize'] = max_contract_size

    return swap_raw_data_dict, swap_processed_data_dict


def split_symbols_between_processes(
    swap_raw_data_dict: dict[str, dict[str, dict[str, Any]]],
    swap_processed_data_dict: dict[str, dict[str, dict[str, Any]]],
    process_count: int,
    process_index: int,
) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, dict[str, dict[str, Any]]]]:
    if process_count <= 0:
        raise ValueError("process_count must be positive")
    if process_index < 0 or process_index >= process_count:
        raise ValueError("process_index out of range")

    selected_symbols = [
        symbol
        for index, symbol in enumerate(sorted(swap_processed_data_dict))
        if index % process_count == process_index
    ]

    worker_raw = {symbol: swap_raw_data_dict[symbol] for symbol in selected_symbols if symbol in swap_raw_data_dict}
    worker_processed = {
        symbol: swap_processed_data_dict[symbol]
        for symbol in selected_symbols
        if symbol in swap_processed_data_dict
    }
    return worker_raw, worker_processed


async def _wait_for_shared_shutdown(shared_values: dict[str, Any], poll_interval_sec: float = 0.5) -> None:
    while True:
        shutdown_value = shared_values.get("shutdown")
        if shutdown_value is not None and shutdown_value.value:
            return
        await asyncio.sleep(poll_interval_sec)


def _send_control_event(control_queue, payload: dict[str, Any]) -> None:
    if control_queue is None:
        return
    try:
        control_queue.put(payload)
    except Exception:
        return


async def _worker_heartbeat(
    *,
    control_queue,
    process_index: int,
    pid: int,
    shared_values: dict[str, Any],
    interval_sec: float = 5.0,
) -> None:
    while True:
        shutdown_value = shared_values.get("shutdown")
        if shutdown_value is not None and shutdown_value.value:
            return
        _send_control_event(
            control_queue,
            {
                "event": "worker_heartbeat",
                "worker_id": process_index,
                "pid": pid,
                "ts": time.time(),
            },
        )
        await asyncio.sleep(interval_sec)


async def run_arbitrage_worker(
    *,
    process_index: int,
    process_count: int,
    exchange_id_list: list[str],
    max_deal_slots: Decimal,
    web_grid_queue,
    control_queue,
    shared_values: dict[str, Any],
) -> None:
    task_manager = TaskManager()
    pid = os.getpid()
    _send_control_event(
        control_queue,
        {
            "event": "worker_started",
            "worker_id": process_index,
            "pid": pid,
            "ts": time.time(),
        },
    )
    _reset_runtime_state(
        web_grid_queue=web_grid_queue,
        shared_values=shared_values,
        web_grid_event_mode="event",
    )
    BalanceManager.task_manager = task_manager
    BalanceManager.max_deal_slots = max_deal_slots

    try:
        async with AsyncExitStack() as stack:
            heartbeat_task = asyncio.create_task(
                _worker_heartbeat(
                    control_queue=control_queue,
                    process_index=process_index,
                    pid=pid,
                    shared_values=shared_values,
                )
            )
            exchange_instance_dict, failed_exchanges = await _open_exchange_instances(stack, exchange_id_list)
            if len(exchange_instance_dict) < 2:
                cprint.warning_r(
                    f"[worker:{process_index}] not enough exchanges to run arbitrage: "
                    f"{list(exchange_instance_dict.keys())}"
                )
                _send_control_event(
                    control_queue,
                    {
                        "event": "worker_inactive",
                        "worker_id": process_index,
                        "pid": pid,
                        "reason": "not enough exchanges",
                        "exchanges_ok": len(exchange_instance_dict),
                        "exchanges_failed": len(failed_exchanges),
                        "symbols_assigned": 0,
                        "ts": time.time(),
                    },
                )
                await _wait_for_shared_shutdown(shared_values)
                return
            swap_raw_data_dict, swap_processed_data_dict = await _build_swap_data(exchange_instance_dict, task_manager)
            worker_raw_data_dict, worker_processed_data_dict = split_symbols_between_processes(
                swap_raw_data_dict=swap_raw_data_dict,
                swap_processed_data_dict=swap_processed_data_dict,
                process_count=process_count,
                process_index=process_index,
            )

            if worker_processed_data_dict and worker_raw_data_dict:
                ArbitrageManager.get_configure(
                    exchanges_instances_dict=exchange_instance_dict,
                    balance_manager=BalanceManager,
                    task_manager=task_manager,
                    max_deal_slots=max_deal_slots,
                    swap_raw_data_dict=worker_raw_data_dict,
                    swap_processed_data_dict=worker_processed_data_dict,
                )
                ExchangeInstrument.get_configure(
                    exchanges_instances_dict=exchange_instance_dict,
                    balance_manager=BalanceManager,
                    task_manager=task_manager,
                    swap_raw_data_dict=worker_raw_data_dict,
                    swap_processed_data_dict=worker_processed_data_dict,
                )
                await ArbitrageManager.create_all_arbitrage_objects()

            print(
                f"[worker:{process_index}] symbols={len(worker_processed_data_dict)} "
                f"process_count={process_count}"
            )
            _send_control_event(
                control_queue,
                {
                    "event": "worker_ready",
                    "worker_id": process_index,
                    "pid": pid,
                    "exchanges_ok": len(exchange_instance_dict),
                    "exchanges_failed": len(failed_exchanges),
                    "symbols_assigned": len(worker_processed_data_dict),
                    "ts": time.time(),
                },
            )
            await _wait_for_shared_shutdown(shared_values)
    except Exception as exc:
        _send_control_event(
            control_queue,
            {
                "event": "worker_error",
                "worker_id": process_index,
                "pid": pid,
                "text": str(exc),
                "ts": time.time(),
            },
        )
        raise
    finally:
        try:
            if "heartbeat_task" in locals():
                heartbeat_task.cancel()
        except Exception:
            pass
        for balance_manager in BalanceManager.exchange_balance_instance_dict.values():
            balance_manager.enable = False

        for symbol in list(ArbitrageManager.web_grid_rows):
            ArbitrageManager._remove_web_grid_row(symbol)

        await task_manager.cancel_all()


def run_arbitrage_worker_process(
    *,
    process_index: int,
    process_count: int,
    exchange_id_list: list[str],
    max_deal_slots: Decimal,
    web_grid_queue,
    control_queue,
    shared_values: dict[str, Any],
) -> None:
    asyncio.run(
        run_arbitrage_worker(
            process_index=process_index,
            process_count=process_count,
            exchange_id_list=exchange_id_list,
            max_deal_slots=max_deal_slots,
            web_grid_queue=web_grid_queue,
            control_queue=control_queue,
            shared_values=shared_values,
        )
    )
