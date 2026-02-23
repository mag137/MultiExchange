__version__ = "0.3"

import signal

from app import task_manager
from modules import (cprint, round_down, get_average_orderbook_price, sync_time_with_exchange)
from pprint import pprint
import time
import ccxt.pro as ccxt
import asyncio
from decimal import Decimal
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
    # Словарь флагов ордербук-сокетов вида {exchange_ids:{symbol: true/false}} - по нему можно собирать статистику открытых сокетов на всех биржах
    exchanges_instances_dict = {}  # Словарь с экземплярами бирж
    balance_manager = None
    task_manager = None
    exchange_instruments_obj_dict = {}  # Словарь с экземплярами класса ExchangeInstrument вида {symbol: {exchange_id: {obj: instance, task_name: task_name}}}
    swap_processed_data_dict = None
    swap_raw_data_dict = None
    orderbook_updating_status_dict = {}
    get_ex_orderbook_data_count = {}  # Словарь счетчиков пришедших стаканов целевого символа заданной биржи
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
        self.contract_size = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('contractSize', None)
        self.precision_amount = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('precision', {}).get('amount', None)
        self.min_amount = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('limits', {}).get('amount', {}).get('min', None)
        self.fee = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('taker_fee', None) or self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('taker', None)

    async def watch_orderbook(self):
        """
        Асинхронно следит за стаканом указанного символа.
        Добавлена расширенная диагностика.
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
                    orderbook = await self.exchange.watchOrderBook(self.symbol)

                    # RAW тик
                    # print(f"[{self.exchange_id}] TICK_RAW ts={time.monotonic()}")

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
                            print(f"[{self.exchange_id}] SKIP insufficient volume: {e}")
                            await asyncio.sleep(0)
                            continue

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
                    now = time.monotonic()
                    if now - window_start_ts >= TICK_WINDOW_SEC:
                        print(f"[{self.exchange_id}] TICK_STATS total={ticks_total} changed={ticks_changed}")
                        window_start_ts = now
                        ticks_total = 0
                        ticks_changed = 0

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

        finally:
            print(f"[{self.exchange_id}][STOPPED] watch_orderbook finished")
            self.__class__.orderbook_updating_status_dict.setdefault(self.exchange_id, {})[self.symbol] = False


class ArbitrageManager:
    _lock = asyncio.Lock()
    exchanges_instances_dict = {}  # Словарь с экземплярами бирж
    arbitrage_obj_dict = {}
    symbol_arbitrage_enable_flag_dict = {} # Словарь флагов для while вида {symbol: True/False}
    task_manager = None
    balance_manager = None
    max_deal_slots = None
    free_deals_slots = None  # Количество доступных слотов сделок (активная сделка занимает один слот).
    # Открытия сделки доступно пока есть свободный слот.
    swap_processed_data_dict = {}  # Словарь с данными символов для создания объектов класса
    swap_raw_data_dict = {}  # Словарь с сырыми данными по символу с маркета

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
        for symbol, deal_data in cls.swap_processed_data_dict.items():
            instance = cls(symbol, deal_data)
            cls.arbitrage_obj_dict[symbol] = instance
            task_name = f"_ArbitrageTask|{symbol}"
            # Для каждого символа-экземпляра своя задача
            cls.task_manager.add_task(name=task_name, coro_func=instance.symbol_arbitrage)

    # init символа-экземпляра
    def __init__(self, symbol, deal_data):
        self.symbol = symbol
        self.deal_data = deal_data
        self.task_manager = self.__class__.task_manager
        self.orderbook_queue = asyncio.Queue()
        self.statistic_queue = asyncio.Queue()
        self.symbol_average_price_dict = {} # Словарь со средними ценами символа на каждой бирже вида {exchange_id: {ask: ask, bid: bid}}

        self.min_ask = Decimal('+Infinity')
        self.min_ask_exchange = ""
        self.max_bid = Decimal('-Infinity')
        self.max_bid_exchange = ""
        self.best_close_ask = Decimal('+Infinity')
        self.best_close_bid = Decimal('-Infinity')


    # Точка выхода в символ-экземпляр класса
    # Здесь происходит основная оценка арбитража и принятие решений, анализ и статистика символа и отсюда отправка на вывод в таблицу
    async def symbol_arbitrage(self):

        symbol_task_name = asyncio.current_task()
        print(f"Имя задачи: {symbol_task_name.get_name()}")

        type(self).symbol_arbitrage_enable_flag_dict[self.symbol] = True

        print("------------------------------")
        pprint(self.__class__.swap_processed_data_dict)
        print("------------------------------")

        # Запуск задач ордербуков
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

        # Основной цикл
        while type(self).symbol_arbitrage_enable_flag_dict[self.symbol]:

            orderbook_queue_data = await self.orderbook_queue.get()

            # DEBUG
            # print("FROM_QUEUE", self.symbol, orderbook_queue_data.get("exchange_id"), orderbook_queue_data.get("count"))

            # Защита от мусора
            if orderbook_queue_data.get("type") != "orderbook_update":
                continue

            queue_exchange_id = orderbook_queue_data.get("exchange_id")
            if not queue_exchange_id:
                continue

            # Обновляем цены
            self.symbol_average_price_dict[queue_exchange_id] = {
                "average_ask": orderbook_queue_data["average_ask"],
                "average_bid": orderbook_queue_data["average_bid"]
            }

            print(queue_exchange_id, orderbook_queue_data)

            # Нужно минимум 2 биржи
            if len(self.symbol_average_price_dict) < 2:
                continue

            # Инициализация
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

            # Расчёт
            open_ratio = round_down(100 * (max_bid - min_ask) / min_ask, 2)

            if open_ratio > 1:
                print(open_ratio, min_ask_ex, max_bid_ex)







async def main():
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

        for exchange_id in EXCHANGE_ID_LIST:
            exchange = await stack.enter_async_context(ExchangeInstance(ccxt, exchange_id, log=True))
            exchange_instance_dict[exchange_id] = exchange

            # Создание объектов баланс-менеджера
            balance_manager_obj = BalanceManager.create_new_balance_obj(exchange)
            # Ожидание инициализации объектов балансов
            await balance_manager_obj.wait_initialized()

            # Сборка сырого словаря с данными по каждому символу на каждой бирже - swap_raw_data_dict
            for pair_data in exchange.spot_swap_pair_data_dict.values():
                if swap_data := pair_data.get("swap"):
                    if not swap_data or swap_data.get("settle") != "USDT":
                        continue
                    symbol = swap_data["symbol"]
                    if swap_data.get('linear') and not swap_data.get('inverse'):
                        swap_raw_data_dict.setdefault(symbol, {})[exchange_id] = swap_data

        # Парсинг данных для swap_processed_data_dict
        for symbol, volume in swap_raw_data_dict.items():
            # Если символ только на одной бирже, он не попадает в переработанный словарь
            if len(volume) < 2:
                continue
            if symbol == 'BTC/USDT:USDT':
                max_contractSize = 0  # Максимальный размер единичного контракта среди бирж символа
                # объем сделки должен быть рассчитан исходя из максимального размера контрактов в группе и кратен максимальному размеру
                # Мне кажется максимальный размер контракта имеет смысл вычислять оперативно при возникновении арбитража: возник - смотрим максимальный контракт и делаем последние вычисления
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