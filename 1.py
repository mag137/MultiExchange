__version__ = '0.12'
import time
import asyncio
from contextlib import AsyncExitStack
from decimal import Decimal
import ccxt.pro as ccxt  # ccxt.pro нужен для ExchangeInstance
from modules.task_manager import TaskManager
from modules.balance_manager import BalanceManager
from pprint import pprint
from typing import Dict
import signal

# Импорт класса обработки экземпляра биржи - ExchangeInstance
from modules.exchange_instance import ExchangeInstance
from modules.utils import to_decimal
from modules import (get_average_orderbook_price, cprint, sync_time_with_exchange)
# Импортируем нижний класс - работа с символом на отдельной бирже
# from modules.exchange_instrument import ExchangeInstrument
from modules.exception_classes import ( ReconnectLimitExceededError,
                                        InvalidOrEmptyOrderBookError,
                                        BaseArbitrageCalcException,
                                        InsufficientOrderBookVolumeError)

class ExchangeInstrument():
    # Словарь флагов ордербук-сокетов вида {exchange_ids:{symbol: true/false}} - по нему можно собирать статистику открытых сокетов на всех биржах
    orderbook_updating_status_dict = {}
    # Словарь данных ордербуков для отправки в очередь вида {exchange_ids:{orderbook_data}}
    orderbook_data_dict = {}

    def __init__(self, exchange, symbol, swap_data, balance_manager, orderbook_queue: asyncio.Queue, statistic_queue: asyncio.Queue = None):
        # Словарь счетчиков пришедших стаканов целевого символа заданной биржи
        self.get_ex_orderbook_data_count = {}
        fee = swap_data.get("taker")
        self.exchange = exchange
        self.exchange_id = exchange.id
        self.orderbook_queue = orderbook_queue # Очередь данных ордербуков, сообщений вида {exchange_id: {data_dict}}. На каждый символ своя очередь
        self.statistic_queue = statistic_queue # Очередь отсылки статистики работы watch_orderbook
        self.symbol = symbol
        self.__class__.orderbook_data_dict.setdefault(symbol, {})
        self.balance_manager = balance_manager
        self.orderbook_updating = False
        self.contract_size = ''
        self.tick_size = ''
        self.qty_step = ''
        self.min_amount = ''
        self.fee = ''
        self.update_swap_data(swap_data=swap_data)

    async def get_balance_usdt(self):
        return await self.balance_manager.get_balance()

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
        При превышении числа попыток — завершает задачу.
        """
        max_reconnect_attempts = 5  # лимит переподключений
        reconnect_attempts = 0
        count = 0
        new_count = 0
        old_ask = tuple()
        old_bid = tuple()
        new_ask = tuple()
        new_bid = tuple()

        # ---- tick stats ----
        TICK_WINDOW_SEC = 10.0
        window_start_ts = time.monotonic()
        ticks_total = 0
        ticks_changed = 0
        # --------------------
        statistics_dict = {}


        try:
            # Ожидание баланса
            # todo реализовать получение экземпляром минимального баланса бирж
            await self.balance_manager.wait_initialized()

            balance = self.balance_usdt

            # Инициализируем бесконечный цикл подписки получения стаканов
            self.__class__.orderbook_updating_status_dict.setdefault(self.exchange_id, {})[self.symbol] = True

            # Инициализируем счетчик получения стаканов
            self.get_ex_orderbook_data_count.setdefault(self.exchange_id, {})[self.symbol] = 0

            # Цикл работает пока есть подписка
            while self.__class__.orderbook_updating_status_dict[self.exchange_id][self.symbol]:
                try:
                    if not await self.balance_manager.is_balance_valid():
                        continue

                    orderbook = await self.exchange.watchOrderBook(self.symbol)

                    # сбрасываем счётчик после успешного получения
                    reconnect_attempts = 0
                    ticks_total += 1
                    count += 1

                    # Проверим пришедшие данные стаканов - если левые, то выбрасываем исключение
                    if not isinstance(orderbook, dict) or len(orderbook) == 0 or 'asks' not in orderbook or 'bids' not in orderbook:
                        raise InvalidOrEmptyOrderBookError(exchange_id=self.exchange_id, symbol=self.symbol, orderbook_data=orderbook)

                    # Инкриминируем счетчик получения стаканов
                    self.get_ex_orderbook_data_count.setdefault(self.exchange_id, {})[self.symbol] += 1

                    # Обрезаем стакан для анализа до 10 ордеров
                    depth = min(10, len(orderbook['asks']), len(orderbook['bids']))
                    new_ask = tuple(map(tuple, orderbook['asks'][:depth]))
                    new_bid = tuple(map(tuple, orderbook['bids'][:depth]))

                    if new_ask != old_ask or new_bid != old_bid:
                        ticks_changed += 1
                        new_count += 1

                        if self.balance_usdt is None or self.balance_usdt <= 0:
                            await asyncio.sleep(0.5)
                            continue

                        try:
                            _, min_balance = await BalanceManager.get_min_balance()


                            if balance is None or balance <= 0:
                                continue

                            average_ask = get_average_orderbook_price(
                                data=orderbook['asks'],
                                money=min_balance, # Здесь используется минимальный баланс, получаемый из классного метода - минимум среди всех биржевых депозитов
                                is_ask=True,
                                log=True,
                                exchange=self.exchange_id,
                                symbol=self.symbol
                            )

                            average_bid = get_average_orderbook_price(
                                data=orderbook['bids'],
                                money=min_balance,
                                is_ask=False,
                                log=True,
                                exchange=self.exchange_id,
                                symbol=self.symbol
                            )

                        except InsufficientOrderBookVolumeError as e:
                            cprint.warning(f"[SKIP] Недостаточный объём стакана для {self.symbol} биржи {self.exchange_id}: {e}")
                            await asyncio.sleep(0)  # чтобы не перегружать цикл
                            self.__class__.orderbook_updating_status_dict[self.exchange_id][self.symbol] = False
                            # Отправляем сообщение о закрытии, теоретически можно отслеживать работу вебсокетов по флагу

                            break

                        if average_ask is None or average_bid is None:
                            await asyncio.sleep(1)
                            continue

                        self.__class__.orderbook_data_dict[self.symbol][self.exchange_id] = {
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
                            await self.orderbook_queue.put_nowait(self.__class__.orderbook_data_dict[self.symbol].copy())
                        except asyncio.QueueFull:
                            try:
                                _ = self.orderbook_queue.get_nowait()
                            except asyncio.QueueEmpty:
                                pass
                            await self.orderbook_queue.put(self.__class__.orderbook_data_dict[self.symbol].copy())

                    if self.statistic_queue:
                        # ---------- окно 10 секунд ----------
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
                        # -----------------------------------
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
                        '1000',
                        'closed by remote server',
                        'Cannot write to closing transport',
                        'Connection closed',
                        'WebSocket is already closing',
                        'Transport closed',
                        'broken pipe',
                        'reset by peer',
                    ]

                    if any(x in error_str for x in transient_errors):
                        reconnect_attempts += 1

                        if reconnect_attempts > max_reconnect_attempts:
                            raise ReconnectLimitExceededError(
                                exchange_id=self.exchange.id,
                                symbol=self.symbol,
                                attempts=reconnect_attempts
                            )
                        await asyncio.sleep(4 + (2 ** reconnect_attempts))
                        continue
                    raise

        except asyncio.CancelledError:
            current_task = asyncio.current_task()
            cancel_source = getattr(current_task, "_cancel_context", "неизвестно")
            cprint.success_w(f"[watch_orderbook] Задача отменена: {self.symbol}, источник: {cancel_source}")
            raise

        except Exception as e:
            cprint.error_b(f"[watch_orderbook][FATAL] Непредвиденная ошибка для {self.symbol}: {e}")
            raise

        finally:
            self.__class__.orderbook_updating_status_dict.setdefault(self.exchange_id, {})[self.symbol] = False

class ArbitragePairs:
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

            pprint(cls.swap_pairs_processed_data_dict)

            await asyncio.sleep(5)
            pprint(cls.task_manager.task_status_dict())

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
    exchanges_id_list = ["phemex", "okx", "gateio"]

    # Инициализация данных с бирж и создание объектов Arbitr
    await ArbitragePairs.init_exchanges_pairs_data(exchanges_id_list)
    # Зададим максимальное количество одновременных сделок
    ArbitragePairs.max_deals = 2

    # Доступ к объектам для конкретной пары
    for symbol, arbitrage_obj in ArbitragePairs.arbitrage_obj_dict.items():
        print(f"Объект для {symbol}:")
        print("Сырые данные:", arbitrage_obj.swap_pairs_raw_data_dict.get(symbol))
        print("Обработанные данные:", arbitrage_obj.swap_pairs_processed_data_dict.get(symbol))
        print("-" * 60)

if __name__ == "__main__":
    asyncio.run(main())
