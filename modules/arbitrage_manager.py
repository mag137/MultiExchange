__version__ = "0.7i"

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

    _configured = False

    @classmethod
    def _format_mean_dt(cls, mean_dt: float | None) -> str:
        if mean_dt is None:
            return "-"
        return f"{float(mean_dt):.3f}"

    @classmethod
    def _push_web_grid_snapshot(cls) -> None:
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
        cls.web_grid_rows[symbol] = {
            "symbol": symbol,
            "ask_exchange": ask_exchange,
            "ask_mean_dt": cls._format_mean_dt(ask_mean_dt),
            "bid_exchange": bid_exchange,
            "bid_mean_dt": cls._format_mean_dt(bid_mean_dt),
            "open_ratio": f"{open_ratio}%",
            "open_ratio_value": float(open_ratio),
        }
        cls._push_web_grid_snapshot()

    @classmethod
    def _remove_web_grid_row(cls, symbol: str) -> None:
        if cls.web_grid_rows.pop(symbol, None) is not None:
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
    ArbitrageManager.web_grid_shared_values["shutdown"].value = False

    exchange_instance_dict = {}  # Словарь с экземплярами бирж {symbol:{exchange_obj}}
    swap_raw_data_dict = {}  # Словарь с сырыми данными {symbol:{exchange_id: <data>}})
    swap_processed_data_dict = {}  # Словарь с данными для открытия сделок

    ArbitrageManager.web_grid_process = multiprocessing.Process(
        target=run_web_grid_process,
        kwargs={
            "table_queue_data": ArbitrageManager.web_grid_table_queue,
            "shared_values": ArbitrageManager.web_grid_shared_values,
            "queue_datadict_wrapper_key": None,
            "host": "127.0.0.1",
            "port": 8765,
            "row_header": False,
            "title": "Open Arbitrage Ratio",
            "transport": "sse",
            "max_fps": 2.0,
            "client_poll_interval_ms": 500,
        },
        daemon=True,
    )
    ArbitrageManager.web_grid_process.start()
    ArbitrageManager.web_grid_table_queue.put({"title": "Open Arbitrage Ratio"})
    ArbitrageManager._push_web_grid_snapshot()

    # Основной контекст запуск экземпляров из списка бирж
    try:
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
    finally:
        ArbitrageManager.web_grid_shared_values["shutdown"].value = True
        if ArbitrageManager.web_grid_process is not None:
            ArbitrageManager.web_grid_process.join(timeout=3)


if __name__ == "__main__":
    asyncio.run(main())
