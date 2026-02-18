__version__ = "3.0 OKX"

import signal
from modules import (cprint, round_down, get_average_orderbook_price, sync_time_with_exchange)
from pprint import pprint
import time

import ccxt.pro as ccxt
import os
import asyncio

from modules.logger import LoggerFactory
from modules.exchange_instance import ExchangeInstance
from modules.process_manager import ProcessManager
from modules.task_manager import TaskManager
from modules.balance_manager import BalanceManager

from contextlib import AsyncExitStack
from modules.ORJSON_file_manager import JsonFileManager
from modules.telegram_bot_message_sender import TelegramMessageSender
# from modules.TkGrid3 import TkGrid
# from modules.row_TkGrid import Row_TkGrid
from typing import List, Dict, Any, Optional
from functools import partial
from datetime import datetime, UTC

from modules.utils import to_decimal
from modules.exception_classes import ( ReconnectLimitExceededError,
                                        InvalidOrEmptyOrderBookError,
                                        BaseArbitrageCalcException,
                                        InsufficientOrderBookVolumeError)

# Пока делаем сисему заточенную на работу только с двумя биржами. Если все будет ок - будем немного менять архитектуру и масштабировать

# === Инициализация ===

task_manager = TaskManager()
p_manager = ProcessManager()


class ExchangeInstrument():
    # Словарь флагов ордербук-сокетов вида {exchange_ids:{symbol: true/false}} - по нему можно собирать статистику открытых сокетов на всех биржах
    orderbook_updating_status_dict = {}
    _lock = asyncio.Lock()

    def __init__(self, exchange, symbol, swap_data, balance_manager, orderbook_queue: asyncio.Queue, statistic_queue: asyncio.Queue = None):
        # Словарь счетчиков пришедших стаканов целевого символа заданной биржи
        self.get_ex_orderbook_data_count = {}
        fee = swap_data.get("taker")
        self.exchange = exchange
        self.exchange_id = exchange.id
        self.orderbook_queue = orderbook_queue # Очередь данных ордербуков, сообщений вида {exchange_id: {data_dict}}. На каждый символ своя очередь
        self.statistic_queue = statistic_queue # Очередь отсылки статистики работы watch_orderbook
        self.symbol = symbol
        self.balance_manager = balance_manager
        self.orderbook_updating = False
        self.contract_size = ''
        self.tick_size = ''
        self.qty_step = ''
        self.min_amount = ''
        self.fee = ''
        self.update_swap_data(swap_data=swap_data)
        self._lock = self.__class__._lock

    async def get_deal_volume_usdt(self):
        # Отдаем доступный объем для открытия сделки с учетом запаса и количества свободных слотов
        return await self.balance_manager.get_deal_balance(ArbitragePairs.free_deals_slot)

    # Метод обновления данных
    def update_swap_data(self, swap_data):
        fee = swap_data.get("taker")
        self.contract_size = to_decimal(swap_data.get("contractSize"))
        self.tick_size = to_decimal(swap_data.get("precision", {}).get("price"))
        self.qty_step = to_decimal(swap_data.get("precision", {}).get("amount", None))
        self.min_amount = to_decimal(swap_data.get("limits", {}).get("amount", {}).get('min', None))
        if fee is None:
            fee = swap_data.get("taker_fee")
        self.fee = fee

    # Метод возвращения словаря статусов бесконечного цикла получения стакана
    @classmethod
    def get_orderbook_updating_status_dict(cls):
        return cls.orderbook_updating_status_dict

    # Метод получения стаканов заданного символа на заданной бирже
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
            await self.balance_manager.wait_initialized()

            # Флаг бесконечной подписки
            self.__class__.orderbook_updating_status_dict.setdefault(self.exchange_id, {})[self.symbol] = True
            self.get_ex_orderbook_data_count.setdefault(self.exchange_id, {})[self.symbol] = 0

            while self.__class__.orderbook_updating_status_dict[self.exchange_id][self.symbol]:
                try:
                    # Берем данные, требующие блокировки, атомарно
                    async with self._lock:
                        balance_valid = await self.balance_manager.is_balance_valid()
                        volume = await self.get_deal_volume_usdt()

                    if not balance_valid or volume is None or volume <= 0:
                        await asyncio.sleep(0.5)
                        continue

                    # --- WebSocket и вычисления вне блокировки ---
                    orderbook = await self.exchange.watchOrderBook(self.symbol)
                    if not isinstance(orderbook, dict) or 'asks' not in orderbook or 'bids' not in orderbook:
                        raise InvalidOrEmptyOrderBookError(
                            exchange_id=self.exchange_id, symbol=self.symbol, orderbook_data=orderbook
                        )

                    # Счётчики и статистика
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
                            average_ask = get_average_orderbook_price(orderbook['asks'], money=volume, is_ask=True,
                                                                      log=True, exchange=self.exchange_id, symbol=self.symbol)
                            average_bid = get_average_orderbook_price(orderbook['bids'], money=volume, is_ask=False,
                                                                      log=True, exchange=self.exchange_id, symbol=self.symbol)
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


class ArbitragePairs:
    _lock = asyncio.Lock()
    exchanges_instance_dict = {}    # Словарь с экземплярами бирж
    balance_dict = {}               # Словарь балансов usdt вида {exchange_id: <balance_usdt>}
    arbitrage_obj_dict = {}
    task_manager = TaskManager()
    balance_managers = {}
    free_deals_slot = None          # Количество доступных слотов сделок (активная сделка занимает один слот).
                                    # При открытии сделки аргумент - декриментируется, При закрытии - инкрементируется.
                                    # Открытия сделки доступно пока есть свободный слот.


    def __init__(self, symbol, swap_pairs_raw_data_dict, swap_pairs_processed_data_dict):
        self.symbol = symbol
        self.balance_dict = self.__class__.balance_dict
        self.exchanges_instance_dict = self.__class__.exchanges_instance_dict
        self.swap_pairs_raw_data_dict = swap_pairs_raw_data_dict.get(symbol, None)
        self.swap_pairs_processed_data_dict = swap_pairs_processed_data_dict.get(symbol, None)
        self.task_manager = self.__class__.task_manager
        # Словарь с экземплярами нижнего класса для каждой биржи вида {exchange_id: <obj>}
        self.exchange_instrument_obj_dict = {}
        self.orderbook_queue = asyncio.Queue()
        self.statistic_queue = asyncio.Queue()

    # Основной метод арбитража символа
    async def start_symbol_arbitrage(self):
        symbol = self.symbol
        print(symbol)
        task = asyncio.current_task()
        print(f"Имя задачи: {task.get_name()}")

        # Запустим на исполнение экземпляры ExchangeInstrument для каждой биржи символа
        for exchange_id in self.swap_pairs_processed_data_dict.keys():
            exchange = self.exchanges_instance_dict.get(exchange_id)
            balance_manager = self.__class__.balance_managers.get(exchange_id)

            _obj = ExchangeInstrument  (
                                        exchange=exchange,
                                        symbol=symbol,
                                        swap_data=self.swap_pairs_raw_data_dict.get(exchange_id),
                                        balance_manager=balance_manager,
                                        orderbook_queue=self.orderbook_queue,
                                        statistic_queue=self.statistic_queue
                                       )
            # Передадим в класс максимальное количество одновременных сделок

            self.exchange_instrument_obj_dict[exchange_id] = _obj
            task_name = f"_OrderbookTask|{symbol}|{exchange_id}"
            self.task_manager.add_task(name=task_name, coro_func=_obj.watch_orderbook)

        await asyncio.sleep(30)
        pass

    # Инициализация данных списка бирж
    @classmethod
    def create_obj(cls, symbol: str, swap_pairs_raw_data_dict: Dict, swap_pairs_processed_data_dict: Dict):
        return cls(symbol, swap_pairs_raw_data_dict, swap_pairs_processed_data_dict)

    @classmethod
    async def init_exchanges_pairs_data(cls, exchanges_id_list):
        cls.swap_pairs_raw_data_dict = {}                # Словарь сырых с биржи данных для открытия сделок
        cls.swap_pairs_processed_data_dict = {}          # Словарь распарсенных с биржи данных для открытия сделок
        async with AsyncExitStack() as stack:
            async def open_exchange(exchange_id):
                return exchange_id, await stack.enter_async_context(ExchangeInstance(ccxt, exchange_id, log=True))

            results = await asyncio.gather(*(open_exchange(exchange_id) for exchange_id in exchanges_id_list))
            cls.exchanges_instance_dict = dict(results)

            for exchange_id, exchange in cls.exchanges_instance_dict.items():
                print(exchange.id)
                for pair_data in exchange.spot_swap_pair_data_dict.values():
                    if swap_data := pair_data.get("swap"):
                        if not swap_data or swap_data.get("settle") != "USDT":
                            continue
                        symbol = swap_data["symbol"]
                        if swap_data.get('linear') and not swap_data.get('inverse'):
                            cls.swap_pairs_raw_data_dict.setdefault(symbol, {})[exchange_id] = swap_data

            # Создаём менеджеры баланса
            cls.balance_managers = {}
            for exchange_id in cls.exchanges_instance_dict:
                exchange = cls.exchanges_instance_dict[exchange_id]
                cls.balance_managers[exchange_id] = BalanceManager(exchange, cls.task_manager)

            # Ожидаем первичную загрузку всех балансов
            for exchange_id in cls.balance_managers:
                await cls.balance_managers[exchange_id].wait_initialized()

            # Вывод текущих балансов
            for exchange_id in cls.balance_managers.keys():
                async with cls._lock:
                    balance = await cls.balance_managers[exchange_id].get_balance()
                    cprint.info_b(f"[MAIN] {exchange_id} balance = {balance} USDT")



            # Парсинг данных для cls.swap_pairs_processed_data_dict
            for symbol, volume in cls.swap_pairs_raw_data_dict.items():
                if len(volume) < 2:
                    continue
                max_contract_size = 0  # Максимальный размер единичного контракта среди бирж символа
                # объем сделки должен быть рассчитан исходя из максимального размера контрактов в группе и кратен максимальному размеру
                for exchange_id, data in volume.items():
                    contract_size = data.get('contractSize')
                    if max_contract_size < contract_size:
                        max_contract_size = contract_size
                    cls.swap_pairs_processed_data_dict.setdefault(symbol, {}).setdefault(exchange_id, {})['contractSize'] = contract_size
                for exchange_id, data in volume.items():
                    cls.swap_pairs_processed_data_dict[symbol][exchange_id]['max_contractSize'] = max_contract_size

            # Создаем экземпляры класса по каждому символу в ключах словаря
            for symbol in list(cls.swap_pairs_processed_data_dict.keys())[:12]:
                _obj =cls.create_obj (symbol=symbol,
                                      swap_pairs_raw_data_dict=cls.swap_pairs_raw_data_dict,
                                      swap_pairs_processed_data_dict=cls.swap_pairs_processed_data_dict)
                cls.arbitrage_obj_dict[symbol] = _obj
                task_arbitrage_symbol_name = f"_SymbolTask|{symbol}"
                cls.task_manager.add_task(name=task_arbitrage_symbol_name, coro_func=_obj.start_symbol_arbitrage)

            # pprint(cls.swap_pairs_processed_data_dict)

            await asyncio.sleep(5)
            # pprint(cls.task_manager.task_status_dict())

            # ---- graceful shutdown ----
            stop_event = asyncio.Event()
            loop = asyncio.get_running_loop()
            try:
                loop.add_signal_handler(signal.SIGINT, stop_event.set)
                loop.add_signal_handler(signal.SIGTERM, stop_event.set)
            except NotImplementedError:
                pass

            await stop_event.wait()

async def main():
    exchange_id_list = ["gateio", "okx", "poloniex"]
    swap_pair_data_dict = {}  # {symbol:{exchange_id: <data>}})
    swap_pair_for_deal_info_dict = {} # Словарь с данными для открытия сделок
    ArbitragePairs.free_slots = 2
    balance_managers = {}

    # Основной контекст запуск экземпляров из списка бирж
    async with AsyncExitStack() as stack:
        async def open_exchange(exchange_id):
            return exchange_id, await stack.enter_async_context(ExchangeInstance(ccxt, exchange_id, log=True))

        results = await asyncio.gather(*(open_exchange(exchange_id) for exchange_id in exchange_id_list))
        exchange_instance_dict = dict(results)

        for exchange_id, exchange in exchange_instance_dict.items():
            print(exchange.id)

            # Создадим экземпляр менеджера балансов для текущей биржи
            balance_managers[exchange_id] = BalanceManager(exchange, task_manager)

            for pair_data in exchange.spot_swap_pair_data_dict.values():
                if swap_data := pair_data.get("swap"):
                    if not swap_data or swap_data.get("settle") != "USDT":
                        continue
                    symbol = swap_data["symbol"]
                    if swap_data.get('linear') and not swap_data.get('inverse'):
                        swap_pair_data_dict.setdefault(symbol, {})[exchange_id] = swap_data

        for bm in balance_managers.values():
            await bm.wait_initialized()

        # pprint(swap_pair_data_dict)
        # Парсинг данных для swap_pair_for_deal_info_dict
        for symbol, volume in swap_pair_data_dict.items():
            if len(volume) < 2:
                continue
            max_contractSize = 0 # Максимальный размер единичного контракта среди бирж символа
            # объем сделки должен быть рассчитан исходя из максимального размера контрактов в группе и кратен максимальному размеру
            for exchange_id, data in volume.items():
                contractSize = data.get('contractSize')
                if max_contractSize < contractSize:
                    max_contractSize = contractSize
                swap_pair_for_deal_info_dict.setdefault(symbol, {}).setdefault(exchange_id, {})['contractSize'] = contractSize
            for exchange_id, data in volume.items():
                swap_pair_for_deal_info_dict[symbol][exchange_id]['max_contractSize'] = max_contractSize

        # pprint(swap_pair_for_deal_info_dict)

        for symbol in list(swap_pair_data_dict):
            if len(swap_pair_data_dict[symbol]) < 2:
                swap_pair_data_dict.pop(symbol)
        c1 = 0
        c2 = 0
        c3 = 0
        for symbol in list(swap_pair_data_dict):
            if 'gateio' in swap_pair_data_dict[symbol]:
                c1 += 1
            if 'poloniex' in swap_pair_data_dict[symbol]:
                c2 += 1
            if 'okx' in swap_pair_data_dict[symbol]:
                c3 += 1

        # pprint (swap_pair_data_dict)
        pprint (swap_pair_for_deal_info_dict)
        print(len(swap_pair_data_dict))
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

        for bm in balance_managers.values():
            await bm.stop()
        await task_manager.cancel_all()


if __name__ == "__main__":
    asyncio.run(main())


