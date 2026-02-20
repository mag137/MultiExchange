__version__= "0.2"

import signal

from app import task_manager
from modules import (cprint, round_down, get_average_orderbook_price, sync_time_with_exchange)
from pprint import pprint
import time

import ccxt.pro as ccxt
import asyncio

from modules.exchange_instance import ExchangeInstance
from modules.task_manager import TaskManager
from modules.balance_manager import BalanceManager
from contextlib import AsyncExitStack


from modules.utils import to_decimal
from modules.exception_classes import ( ReconnectLimitExceededError,
                                        InvalidOrEmptyOrderBookError,
                                        BaseArbitrageCalcException,
                                        InsufficientOrderBookVolumeError)


class ExchangeInstrument:
    # Словарь флагов ордербук-сокетов вида {exchange_ids:{symbol: true/false}} - по нему можно собирать статистику открытых сокетов на всех биржах
    balance_manager = None
    swap_processed_data_dict = None
    swap_raw_data_dict = None
    orderbook_updating_status_dict = {}
    get_ex_orderbook_data_count = {} # Словарь счетчиков пришедших стаканов целевого символа заданной биржи
    _lock = asyncio.Lock()
    # Думаю надо взять за правило - передавать в экземпляр только аргументы экземпляра, аргументы класса передавать напрямую через имя класса
    def __init__(self, exchange_instance, symbol, orderbook_queue: asyncio.Queue, statistic_queue: asyncio.Queue = None):
        self.symbol = symbol
        self.exchange = exchange_instance
        self.exchange_id = exchange_instance.id
        self.balance_manager = self.__class__.balance_manager
        self.swap_processed_data_dict = self.__class__.swap_processed_data_dict
        self.swap_raw_data_dict = self.__class__.swap_raw_data_dict
        self.get_ex_orderbook_data_count = self.__class__.get_ex_orderbook_data_count
        self.orderbook_queue = orderbook_queue
        self.static_queue = statistic_queue
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
        Логика переподключений и проверки валидности баланса минимально блокирует общие данные.
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
        statistics_dict = {}

        try:
            print(self.symbol, self.exchange_id)
            # Ждем, пока баланс инициализируется
            await self.__class__.balance_manager.wait_initialized()

            # Флаг бесконечной подписки
            self.__class__.orderbook_updating_status_dict.setdefault(self.exchange_id, {})[self.symbol] = True
            self.get_ex_orderbook_data_count.setdefault(self.exchange_id, {})[self.symbol] = 0

            while self.__class__.orderbook_updating_status_dict[self.exchange_id][self.symbol]:
                try:
                    # Берем данные, требующие блокировки, атомарно
                    if not self.balance_manager.is_balance_valid:
                        await asyncio.sleep(5)
                        continue
                    # Получим максимально доступный баланс сде сделки на заданной бирже
                    max_deal_volume = self.balance_manager.max_deal_volume


                    # --- WebSocket и вычисления вне блокировки ---
                    orderbook = await self.exchange.watchOrderBook(self.symbol)
                    if not isinstance(orderbook, dict) or 'asks' not in orderbook or 'bids' not in orderbook:
                        raise InvalidOrEmptyOrderBookError(
                            exchange_id=self.exchange_id, symbol=self.symbol, orderbook_data=orderbook
                        )

                    # Счётчики и статистика

                    if self.exchange_id not in self.get_ex_orderbook_data_count:
                        self.get_ex_orderbook_data_count[self.exchange_id] = {}
                    if self.symbol not in self.get_ex_orderbook_data_count[self.exchange_id]:
                        self.get_ex_orderbook_data_count[self.exchange_id][self.symbol] = 0

                    self.get_ex_orderbook_data_count[self.exchange_id][self.symbol] += 1
                    reconnect_attempts = 0
                    ticks_total += 1
                    count += 1

                    depth = min(10, len(orderbook['asks']), len(orderbook['bids']))
                    new_ask = tuple(map(tuple, orderbook['asks'][:depth]))
                    new_bid = tuple(map(tuple, orderbook['bids'][:depth]))

                    if new_ask != old_ask or new_bid != old_bid:
                        ticks_changed += 1
                        new_count += 1

                        # Расчёт средних цен
                        try:
                            average_ask = get_average_orderbook_price(orderbook['asks'],
                                                                      money=max_deal_volume,
                                                                      is_ask=True,
                                                                      log=True,
                                                                      exchange=self.exchange_id,
                                                                      symbol=self.symbol)
                            average_bid = get_average_orderbook_price(orderbook['bids'],
                                                                      money=max_deal_volume,
                                                                      is_ask=False,
                                                                      log=True,
                                                                      exchange=self.exchange_id,
                                                                      symbol=self.symbol)
                        except InsufficientOrderBookVolumeError as e:
                            cprint.warning(f"[SKIP] Недостаточный объём стакана для {self.symbol} биржи {self.exchange_id}: {e}")
                            await asyncio.sleep(0)
                            break

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

                            # Отправка в очередь без блокировки
                            try:
                                self.orderbook_queue.put_nowait(output_data)
                            except asyncio.QueueFull:
                                try:
                                    _ = self.orderbook_queue.get_nowait()
                                except asyncio.QueueEmpty:
                                    pass
                                await self.orderbook_queue.put(output_data)

                    # --- статистика по тику ---
                    if self.statistic_queue:
                        now = time.monotonic()
                        if now - window_start_ts >= TICK_WINDOW_SEC:
                            statistics_dict = {
                                "type": "tick_stats",
                                "symbol": self.symbol,
                                "exchange_id": self.exchange_id,
                                "window_sec": TICK_WINDOW_SEC,
                                "ticks_total": ticks_total,
                                "ticks_changed": ticks_changed,
                                "ticks_per_sec": round(ticks_total / TICK_WINDOW_SEC, 2),
                                "changed_per_sec": round(ticks_changed / TICK_WINDOW_SEC, 2),
                                "ts_from": window_start_ts,
                                "ts_to": now,
                            }
                            window_start_ts = now
                            ticks_total = 0
                            ticks_changed = 0

                            try:
                                await self.statistic_queue.put_nowait({self.symbol: {self.exchange_id: statistics_dict}})
                            except asyncio.QueueFull:
                                try:
                                    _ = self.statistic_queue.get_nowait()
                                except asyncio.QueueEmpty:
                                    pass
                                await self.statistic_queue.put({self.symbol: {self.exchange_id: statistics_dict}})

                except Exception as e:
                    error_str = str(e)
                    transient_errors = [
                        '1000', 'closed by remote server', 'Cannot write to closing transport',
                        'Connection closed', 'WebSocket is already closing', 'Transport closed',
                        'broken pipe', 'reset by peer'
                    ]

                    if any(x in error_str for x in transient_errors):
                        reconnect_attempts += 1
                        if reconnect_attempts > max_reconnect_attempts:
                            raise ReconnectLimitExceededError(exchange_id=self.exchange.id, symbol=self.symbol, attempts=reconnect_attempts)
                        await asyncio.sleep(4 + (2 ** reconnect_attempts))
                        continue
                    raise

        except asyncio.CancelledError:
            cprint.success_w(f"[watch_orderbook] Задача отменена: {self.symbol}")
            raise
        except Exception as e:
            cprint.error_b(f"[watch_orderbook][FATAL] Ошибка для {self.symbol}: {e}")
            raise
        finally:
            self.__class__.orderbook_updating_status_dict.setdefault(self.exchange_id, {})[self.symbol] = False

class ArbitrageManager:
    _lock = asyncio.Lock()
    exchanges_instances_dict = {}    # Словарь с экземплярами бирж
    arbitrage_obj_dict = {}
    task_manager = None
    balance_manager = None
    max_deal_slots = None
    free_deals_slots = None          # Количество доступных слотов сделок (активная сделка занимает один слот).
                                    # При открытии сделки аргумент - декриментируется, При закрытии - инкрементируется.
                                    # Открытия сделки доступно пока есть свободный слот.
    swap_processed_data_dict = {} # Словарь с данными символов для создания объектов класса
    swap_raw_data_dict       = {}

    @classmethod
    async def create_all_arbitrage_objects(cls):
        cls._init_ExchangeInstrument_class()
        for symbol, deal_data in cls.swap_processed_data_dict.items():
            instance = cls(symbol, deal_data)
            cls.arbitrage_obj_dict[symbol] = instance
            task_name = f"_ArbitrageTask|{symbol}"
            task_manager.add_task(name=task_name, coro_func=instance.symbol_arbitrage)
        cls._init_ExchangeInstrunent_class() # При инициализации объектов заодно инициируем переменные класса ExchangeInstrument

    @classmethod
    def _init_ExchangeInstrument_class(cls):
        ExchangeInstrument.balance_manager = cls.balance_manager
        ExchangeInstrument.swap_raw_data_dict = cls.swap_raw_data_dict
        ExchangeInstrument.swap_processed_data_dict = cls.swap_processed_data_dict
        # Класс начальной инициализации аргументов класса ExchangeInstrument

    def __init__(self,symbol, deal_data):
        self.symbol = symbol
        self.deal_data = deal_data
        self.task_manager = self.__class__.task_manager
        self.orderbook_queue = asyncio.Queue()
        self.statistic_queue = asyncio.Queue()
        self.exchange_instrument_obj_dict = {}


    async def symbol_arbitrage(self):
        task = asyncio.current_task()
        print(f"Имя задачи: {task.get_name()}")

        # Запустим инициализацию задач текущего символа на биржах из словаря
        for exchange_id in self.__class__.swap_processed_data_dict[self.symbol].keys():
            exchange_instance = self.exchanges_instances_dict.get(exchange_id)
            _obj = ExchangeInstrument(exchange_instance=exchange_instance,
                                      symbol=self.symbol,
                                      orderbook_queue=self.orderbook_queue,
                                      statistic_queue=self.statistic_queue)

            self.exchange_instrument_obj_dict[exchange_id] = _obj
            task_name = f"_OrderbookTask|{self.symbol}|{exchange_id}"
            self.task_manager.add_task(name=task_name, coro_func=_obj.watch_orderbook)



async def main():
    TASK_MANAGER = TaskManager()
    MAX_DEAL_SLOTS =  to_decimal('2')
    EXCHANGE_ID_LIST = ["gateio", "okx", "poloniex"]

    BalanceManager.task_manager = TASK_MANAGER
    BalanceManager.max_deal_slots = MAX_DEAL_SLOTS
    ArbitrageManager.balance_manager = BalanceManager
    ArbitrageManager.task_manager = TASK_MANAGER
    ArbitrageManager.max_deal_slots = MAX_DEAL_SLOTS

    exchange_dict                   = {}  # Словарь с экземплярами бирж {symbol:{exchange_obj}}
    swap_raw_data_dict        = {}  # Словарь с сырыми данными {symbol:{exchange_id: <data>}})
    swap_processed_data_dict    = {}  # Словарь с данными для открытия сделок



    # Основной контекст запуск экземпляров из списка бирж
    async with AsyncExitStack() as stack:

        for exchange_id in EXCHANGE_ID_LIST:
            exchange = await stack.enter_async_context(ExchangeInstance(ccxt, exchange_id, log=True))
            exchange_dict[exchange_id] = exchange
            BalanceManager.create_new_balance_obj(exchange)
            print(exchange.id)

            for pair_data in exchange.spot_swap_pair_data_dict.values():
                if swap_data := pair_data.get("swap"):
                    if not swap_data or swap_data.get("settle") != "USDT":
                        continue
                    symbol = swap_data["symbol"]
                    if swap_data.get('linear') and not swap_data.get('inverse'):
                        swap_raw_data_dict.setdefault(symbol, {})[exchange_id] = swap_data

        ArbitrageManager.exchanges_instances_dict = exchange_dict

        for exchange_id, bm in BalanceManager.exchange_balance_instance_dict.items():
            await bm.wait_initialized()

        # pprint(swap_pair_data_dict)
        # Парсинг данных для swap_processed_data_dict
        for symbol, volume in swap_raw_data_dict.items():
            if len(volume) < 2:
                continue
            max_contractSize = 0 # Максимальный размер единичного контракта среди бирж символа
            # объем сделки должен быть рассчитан исходя из максимального размера контрактов в группе и кратен максимальному размеру
            for exchange_id, data in volume.items():
                contractSize = data.get('contractSize')
                if max_contractSize < contractSize:
                    max_contractSize = contractSize
                swap_processed_data_dict.setdefault(symbol, {}).setdefault(exchange_id, {})['contractSize'] = contractSize
            for exchange_id, data in volume.items():
                swap_processed_data_dict[symbol][exchange_id]['max_contractSize'] = max_contractSize

        # pprint(swap_processed_data_dict)

        for symbol in list(swap_raw_data_dict):
            if len(swap_raw_data_dict[symbol]) < 2:
                swap_raw_data_dict.pop(symbol)
        c1 = 0
        c2 = 0
        c3 = 0
        for symbol in list(swap_raw_data_dict):
            if 'gateio' in swap_raw_data_dict[symbol]:
                c1 += 1
            if 'poloniex' in swap_raw_data_dict[symbol]:
                c2 += 1
            if 'okx' in swap_raw_data_dict[symbol]:
                c3 += 1

        pprint(swap_raw_data_dict)

        if swap_processed_data_dict and swap_raw_data_dict:
            ArbitrageManager.swap_processed_data_dict = swap_processed_data_dict
            ArbitrageManager.swap_raw_data_dict = swap_raw_data_dict
            ArbitrageManager.create_all_arbitrage_objects()
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