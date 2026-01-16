__version__= "0.3"

import multiprocessing
import time
from asyncio import Queue
from itertools import count

import ccxt.pro as ccxt
import os
import asyncio
import random

from modules.logger import LoggerFactory
from modules.exchange_instance import ExchangeInstance
from modules.process_manager import ProcessManager
from modules.task_manager import TaskManager
from modules.ORJSON_file_manager import JsonFileManager
from modules.telegram_bot_message_sender import TelegramMessageSender
# from modules.TkGrid3 import TkGrid
from modules.row_TkGrid import Row_TkGrid
from typing import List, Dict, Any, Optional
from decimal import Decimal, getcontext, ROUND_HALF_UP, InvalidOperation
from functools import partial
from datetime import datetime, UTC

from modules.exception_classes import ( ReconnectLimitExceededError,
                                        InvalidOrEmptyOrderBookError,
                                        BaseArbitrageCalcException,
                                        InsufficientOrderBookVolumeError)
from modules import (cprint, round_down, get_average_orderbook_price, sync_time_with_exchange,  # is_valid_price,
                     )

# Установить нужную точность
getcontext().prec = 16  # Общая точность вычислений
getcontext().rounding = ROUND_HALF_UP  # Стандартное округление

def _safe_decimal(value, default=Decimal('0')):
    """Безопасное преобразование в Decimal"""
    if value is None or value == '':
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return default

class ArbitrageRootClass:
    exchange_id = None
    exchange = None
    arb_pair_num_dict = {}
    markets = None
    spot_usdt: Decimal = Decimal("0")
    swap_usdt: Decimal = Decimal("0")

    # Статистика задач
    tasks_orderbook = 0
    tasks_arb_pair = 0
    tasks_Task = 0
    all_tasks = 0

    arb_pair_obj_dict = {}  # словарь с объектами-парами
    task_manager = TaskManager()

    swap_orderbook_task_count = 0
    spot_orderbook_task_count = 0
    orderbook_socket_enable_dict = {} # Словарь с флагами работы ордербуков. При инициализации ордербука - создается флаг True - условие бесконечного цикла ордербука


    # Создадим для файла active_deals.json экземпляр файлового менеджера
    project_root: str = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    active_dir: str = os.path.join(project_root, "deals_log")
    os.makedirs(active_dir, exist_ok=True)
    active_path: str = os.path.join(active_dir, "active_deals.json")
    active_manager: JsonFileManager = JsonFileManager(active_path)
    active_deals_dict = active_manager.load()


    queue_spread_table = multiprocessing.Queue()                                   # Межпроцессная очередь для передачи данныхв таблицу спредов в отдельный процесс
    spread_grid = Row_TkGrid(queue_spread_table)
    spread_grid_process = multiprocessing.Process(target=spread_grid.run_gui)   # Создание отдельного процесса для таблицы спредов
    spread_grid_process.start()                                                 # запуск отдельного процесса таблицы спредов
    queue_spread_table.put({
        'header': {1: {'text': 'Coin'},
                   2: {'text': 'C/Size'},
                   3: {'text': 'Мax OpR'},
                   4: {'text': 'OpRatio'},
                   5: {'text': 'Мax ClR'},
                   6: {'text': 'ClRatio %'},
                   7: {'text': 'Delta'},
                   8: {'text': 'Fee %'},
                   9: {'text': 'SpFee %'},
                   10: {'text': 'SwFee %'},
                   11: {'text': 'SpTick/s'},
                   12: {'text': 'SwTick/s'}
                   }})
    all_queue_data_dict = {}                                                    # Словарь в который собираются данные очереди со всех арбитражных пар
    arb_pair_list_for_tkinter = ['Это нулевая арбитражная пара для того чтобы пары нумерация пар начиналась с единицы']# Список отображаемых арбитражных пар в таблице
    queue_pair_spread = asyncio.Queue()                                         # Очередь отправки данных таблицы из экземпляров в arb_data_to_tkgrid
    spread_table_title = ''                                                     # Заголовок окна


    @classmethod
    def create_object(cls, pair: List[str], instance_id):  # Возвращает созданный объект
        return cls(pair, instance_id=instance_id)

    @classmethod
    async def watch_balance(cls):
        try:
            while True:
                # balance = await exchange.watch_balance(params)
                balance = await cls.exchange.watch_balance()
                # print("Баланс обновлён:", {k: v for k, v in balance.get('total', {}).items() if v != 0})
                # print(balance.get('USDT').get('free'))
                cls.spot_usdt = cls.swap_usdt = balance.get('USDT').get('free') * 0.48
        except KeyboardInterrupt:
            print("Остановка по запросу пользователя")
        except Exception as e:
            print(f"Критическая ошибка: {e}")

    async def watch_orderbook(self, symbol):
        max_reconnect_attempts = 5
        reconnect_attempts = 0

        is_spot = False
        is_swap = False

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

        try:
            # Ожидание баланса
            while self.__class__.spot_usdt <= 0 or self.__class__.swap_usdt <= 0:
                await asyncio.sleep(0.1)

            # Определяем тип рынка
            if ':' in symbol:
                self.__class__.swap_orderbook_task_count += 1
                is_swap = True
                min_usdt = self.__class__.swap_usdt
            else:
                self.__class__.spot_orderbook_task_count += 1
                is_spot = True
                min_usdt = self.__class__.spot_usdt

            self.__class__.orderbook_socket_enable_dict[symbol] = True

            # ---------------- основной цикл ----------------
            while self.__class__.orderbook_socket_enable_dict.get(symbol, False):
                try:
                    orderbook = await self.exchange.watchOrderBook(
                        symbol,
                        params={'depth': 'books'}
                    )

                    reconnect_attempts = 0
                    ticks_total += 1
                    count += 1

                    depth = min(5, len(orderbook['asks']), len(orderbook['bids']))
                    new_ask = tuple(map(tuple, orderbook['asks'][:depth]))
                    new_bid = tuple(map(tuple, orderbook['bids'][:depth]))

                    if new_ask != old_ask or new_bid != old_bid:
                        ticks_changed += 1
                        new_count += 1

                        try:
                            average_ask = get_average_orderbook_price(
                                data=orderbook['asks'],
                                money=min_usdt,
                                is_ask=True,
                                log=True,
                                exchange=self.exchange_id,
                                symbol=symbol
                            )

                            average_bid = get_average_orderbook_price(
                                data=orderbook['bids'],
                                money=min_usdt,
                                is_ask=False,
                                log=True,
                                exchange=self.exchange_id,
                                symbol=symbol
                            )

                        except InsufficientOrderBookVolumeError as e:
                            cprint.warning(f"[SKIP] Недостаточный объём стакана для {symbol}: {e}")
                            await asyncio.sleep(0)  # чтобы не перегружать цикл
                            self.__class__.orderbook_socket_enable_dict[symbol] = False
                            if is_swap: # Если своп - закрываем спот
                                spot = symbol.split(':')[0]
                                self.__class__.orderbook_socket_enable_dict[spot] = False
                            if is_spot: # Если спот - закрываем своп
                                swap = f"{symbol}:USDT"
                                self.__class__.orderbook_socket_enable_dict[swap] = False
                            await self.orderbook_queue.put({
                                "closed_error": True,
                                "symbol": symbol,
                                "reason": "insufficient_liquidity"
                                })
                            continue

                        if average_ask is None or average_bid is None:
                            await asyncio.sleep(0)
                            continue

                        await self.orderbook_queue.put({
                            "type": "orderbook_update",
                            "ts": time.monotonic(),
                            "symbol": symbol,
                            "count": count,
                            "new_count": new_count,
                            "average_ask": average_ask,
                            "average_bid": average_bid,
                            "closed_error": False,
                        })

                        old_ask = new_ask
                        old_bid = new_bid

                    # ---------- окно 10 секунд ----------
                    now = time.monotonic()
                    if now - window_start_ts >= TICK_WINDOW_SEC:
                        await self.orderbook_queue.put({
                            "type": "tick_stats",
                            "symbol": symbol,
                            "window_sec": TICK_WINDOW_SEC,
                            "ticks_total": ticks_total,
                            "ticks_changed": ticks_changed,
                            "ticks_per_sec": round(ticks_total / TICK_WINDOW_SEC, 2),
                            "changed_per_sec": round(ticks_changed / TICK_WINDOW_SEC, 2),
                            "ts_from": window_start_ts,
                            "ts_to": now,
                        })

                        window_start_ts = now
                        ticks_total = 0
                        ticks_changed = 0
                    # -----------------------------------

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
                                symbol=symbol,
                                attempts=reconnect_attempts
                            )

                        await asyncio.sleep(4 + (2 ** reconnect_attempts))
                        continue

                    raise

            # очистка флага
            self.__class__.orderbook_socket_enable_dict.pop(symbol, None)

        except asyncio.CancelledError:
            cprint.success_w(f"[watch_orderbook] Задача отменена: {symbol}")
            raise

        except Exception as e:
            cprint.error_b(f"[watch_orderbook][FATAL] {symbol}: {e}")
            raise

        finally:
            if is_swap and self.__class__.swap_orderbook_task_count > 0:
                self.__class__.swap_orderbook_task_count -= 1
            if is_spot and self.__class__.spot_orderbook_task_count > 0:
                self.__class__.spot_orderbook_task_count -= 1

    async def orderbook_compare(self):
        pair_data = self.exchange.spot_swap_pair_data_dict.get(self.arb_pair)
        if not isinstance(pair_data, dict) or len(pair_data) != 2:
            raise ValueError(f"spot_swap_pair_data_dict[{self.arb_pair}] должен быть словарем из двух торговых пар.")
        cprint.info_b(f'[orderbooks_compare] task_name: {self.arb_pair}')

        # Создаем задачи подписки на стаканы
        self.__class__.task_manager.add_task(name=self.task_name_symbol_1, coro_func=partial(self.watch_orderbook, self.spot_symbol))
        self.__class__.task_manager.add_task(name=self.task_name_symbol_2, coro_func=partial(self.watch_orderbook, self.swap_symbol))
        old_spot_average_ask = False
        old_spot_average_bid = False
        old_swap_average_ask = False
        old_swap_average_bid = False
        queue_message = None

        count = 0
        while True:
            count +=1
            update_flag = False
            orderbook_average_price_event_data = await self.orderbook_queue.get()
            if orderbook_average_price_event_data.get('closed_error'): # Если в одном из ордербуков критическая ошибка - прекращаем арбитраж и закрываем задачи
                cprint.info_w(f"[orderbook_compare][{self.arb_pair}] Малая ликвидность стакана [{orderbook_average_price_event_data.get('symbol')}] - закрываем арбитраж")
                self.__class__.orderbook_socket_enable_dict[self.spot_symbol] = False
                self.__class__.orderbook_socket_enable_dict[self.swap_symbol] = False
                break

            if orderbook_average_price_event_data.get('type') == 'orderbook_update':
                # Обработка полученного ордербука
                if orderbook_average_price_event_data.get('symbol') == self.spot_symbol:
                    spot_average_ask = orderbook_average_price_event_data.get('average_ask')
                    spot_average_bid = orderbook_average_price_event_data.get('average_bid')
                    if old_spot_average_ask != spot_average_ask or old_spot_average_bid != spot_average_bid:
                        old_spot_average_ask = spot_average_ask
                        old_spot_average_bid = spot_average_bid
                        self.spot_average_ask = _safe_decimal(spot_average_ask)
                        self.spot_average_bid = _safe_decimal(spot_average_bid)
                        update_flag = True

                if orderbook_average_price_event_data.get('symbol') == self.swap_symbol:
                    swap_average_ask = orderbook_average_price_event_data.get('average_ask')
                    swap_average_bid = orderbook_average_price_event_data.get('average_bid')
                    if old_swap_average_ask != swap_average_ask or old_swap_average_bid != swap_average_bid:
                        old_swap_average_ask = swap_average_ask
                        old_swap_average_bid = swap_average_bid
                        self.swap_average_ask = _safe_decimal(swap_average_ask)
                        self.swap_average_bid = _safe_decimal(swap_average_bid)
                        update_flag = True

            if orderbook_average_price_event_data.get('type') == 'tick_stats':
                # Обработка статистики прихода данных ордербука из очереди
                self.symbols_orderbook_statistic['symbol'] = orderbook_average_price_event_data.get('symbol')
                self.symbols_orderbook_statistic['window_sec'] = orderbook_average_price_event_data.get('window_sec')
                self.symbols_orderbook_statistic['ticks_total'] = orderbook_average_price_event_data.get('ticks_total')
                self.symbols_orderbook_statistic['ticks_changed'] = orderbook_average_price_event_data.get('ticks_changed')
                self.symbols_orderbook_statistic['ticks_per_sec'] = orderbook_average_price_event_data.get('ticks_per_sec')
                self.symbols_orderbook_statistic['changed_per_sec'] = orderbook_average_price_event_data.get('changed_per_sec')
                if orderbook_average_price_event_data.get('symbol') == self.spot_symbol:
                    self.spot_tick_statistic = orderbook_average_price_event_data.get('ticks_per_sec')
                if orderbook_average_price_event_data.get('symbol') == self.swap_symbol:
                    self.swap_tick_statistic = orderbook_average_price_event_data.get('ticks_per_sec')
                # await self._to_tkinter_spread_table()
                print(self.symbols_orderbook_statistic['symbol'], self.symbols_orderbook_statistic['ticks_total'])

            if queue_message:
                self.__class__.queue_spread_table.put({
                    self.instance_id: {
                        1: {'text': self.arb_pair},
                        2: {'text': self.contract_size},
                        3: {'text': self.max_open_ratio},
                        4: {'text': self.open_ratio},
                        5: {'text': self.max_close_ratio},
                        6: {'text': self.close_ratio},
                        7: {'text': self.delta_ratios},
                        8: {'text': self.commission},
                        9: {'text': self.spot_fee},
                        10: {'text': self.swap_fee},
                        11: {'text': self.spot_tick_statistic},
                        12: {'text': self.swap_tick_statistic},
                    }
                })

            if not update_flag:
                continue

            # Если есть изменения в котировках - расчитываем арбитраж!



            # Проверяем наличие всех средних цен
            if any(v is None for v in [self.spot_average_ask, self.spot_average_bid, self.swap_average_ask, self.swap_average_bid]):
                cprint.error_w(self.arb_pair)
                await asyncio.sleep(5)
                continue

            # Определим предыдущие значения ратио открытия/закрытия
            self.last_open_ratio = self.open_ratio
            self.last_close_ratio = self.close_ratio

            # print(f"{self.arb_pair}, {self.open_ratio}, {self.close_ratio}, {orderbook_average_price_event_data.get(count)}")

            # Расчёт дельт и ratio

            self.open_ratio = round_down(100 * (self.swap_average_bid - self.spot_average_ask) / self.spot_average_ask, 2)
            self.close_ratio = round_down(100 * (self.spot_average_bid - self.swap_average_ask) / self.swap_average_ask, 2)
            self.max_open_ratio = max(self.max_open_ratio, self.open_ratio)
            self.max_close_ratio = max(self.max_close_ratio, self.close_ratio)
            self.min_open_ratio = min(self.min_open_ratio, self.open_ratio)
            self.min_close_ratio = min(self.min_close_ratio, self.close_ratio)
            self.delta_ratios = round_down(self.max_open_ratio + self.max_close_ratio, 2)

            # Расчёт объёмов сделки
            self.orders_data = self.exchange.deal_amounts_calc_func(deal_pair=self.arb_pair, spot_deal_usdt=type(self).spot_usdt, swap_deal_usdt=type(self).swap_usdt, price_spot=self.spot_average_ask, price_swap=self.swap_average_bid)
            self.spot_amount = Decimal(self.orders_data.get('spot_amount'))
            self.swap_contracts = Decimal(self.orders_data.get('swap_contracts'))
            self.contract_size = Decimal(self.orders_data.get('contract_size'))

            # Получаем минимумы объемов сделок
            self.spot_min_amt = Decimal(self.orders_data.get('spot_min_amt', '0'))
            self.spot_min_cost = Decimal(self.orders_data.get('spot_min_cost', '0'))
            self.swap_min_amt = Decimal(self.orders_data.get('swap_min_amt', '0'))
            self.swap_min_cost = Decimal(self.orders_data.get('swap_min_cost', '0'))

            # Отправим данные в таблицу спредов

            queue_message={
                self.instance_id: {
                    1: {'text': self.arb_pair},
                    2: {'text': self.contract_size},
                    3: {'text': self.max_open_ratio},
                    4: {'text': self.open_ratio},
                    5: {'text': self.max_close_ratio},
                    6: {'text': self.close_ratio},
                    7: {'text': self.delta_ratios},
                    8: {'text': self.commission},
                    9: {'text': self.spot_fee},
                    10: {'text': self.swap_fee},
                    11: {'text': self.spot_tick_statistic},
                    12: {'text': self.swap_tick_statistic},
                }
            }

            self.__class__.queue_spread_table.put(queue_message)

    def __init__(self, arb_pair: str, instance_id: int):
        self.exchange = self.__class__.exchange
        self.arb_pair = arb_pair
        self.instance_id = instance_id

        # Получение данных спот/своп безопасно
        spot_data = self.exchange.spot_swap_pair_data_dict.get(arb_pair, {}).get('spot', {})
        swap_data = self.exchange.spot_swap_pair_data_dict.get(arb_pair, {}).get('swap', {})

        self.spot_symbol = spot_data.get('symbol', None)
        self.swap_symbol = swap_data.get('symbol', None)
        self.spot_average_ask = None
        self.swap_average_ask = None
        self.spot_average_bid = None
        self.swap_average_bid = None
        self.delta_ratios = None
        self.open_ratio = None
        self.close_ratio = None
        self.last_open_ratio = None
        self.last_close_ratio = None
        self.max_open_ratio = Decimal('-Infinity')
        self.max_close_ratio = Decimal('-Infinity')
        self.min_open_ratio = Decimal('Infinity')
        self.min_close_ratio = Decimal('Infinity')
        self.task_name_symbol_1 = f"orderbook_{self.spot_symbol}"   # Имена задач для получения стаканов
        self.task_name_symbol_2 = f"orderbook_{self.swap_symbol}"   # Имена задач для получения стаканов
        self.spot_fee = _safe_decimal(spot_data.get('taker')) * Decimal('100')
        self.swap_fee = _safe_decimal(swap_data.get('info', {}).get('taker_fee_rate')) * Decimal('100')
        self.commission = self.spot_fee + self.swap_fee

        self.symbols_orderbook_statistic = {}   # Словарь хранения статистики тиков ордербуков арбитражной пары
        self.spot_tick_statistic = 0            # Количество тиков за 10 секунд
        self.swap_tick_statistic = 0            # Количество тиков за 10 секунд

        self.orderbook_queue = asyncio.Queue()                      # Очередь для watch_orderbook
        self.spread_queue_dict = {}                                 # Словарь хранения данных для таблицы спредов
        self.queue_pair_spread = self.__class__.queue_pair_spread   # Очередь отправки данных таблицы из экземпляров в arb_data_to_tkgrid

    @classmethod
    async def start_all_orderbooks_compares_tasks(cls):
        for idx, arb_pair in enumerate(list(cls.exchange.spot_swap_pair_data_dict)[:100], start=1):
            obj = cls.create_object(arb_pair, instance_id=idx)
            cls.arb_pair_obj_dict[arb_pair] = obj
            cls.arb_pair_num_dict[arb_pair] = idx
            obj.task_name = f"arb_pair_{idx}_{arb_pair}"
            cls.task_manager.add_task(name=obj.task_name, coro_func=obj.orderbook_compare)
            await asyncio.sleep(0.1)

    @classmethod
    async def run_analytic_process(cls, exchange_id: str):
        cls.exchange_id = exchange_id
        cls.task_manager = TaskManager()

        async with (ExchangeInstance(ccxt, cls.exchange_id, log=True) as cls.exchange):
            success = await sync_time_with_exchange(cls.exchange)
            if not success:
                cprint.error_b("Не удалось синхронизировать время с биржей.")
                return

            cls.task_manager.add_task(name="start_all_compares",        coro_func=cls.start_all_orderbooks_compares_tasks)
            cls.task_manager.add_task(name="watch_balance",             coro_func=cls.watch_balance)
            cls.task_manager.add_task(name="task_status_monitor",       coro_func=cls.task_status_monitor, interval = 20, log = False)
            # cls.task_manager.add_task(name="arb_data_to_tkgrid_task",   coro_func=cls.arb_data_to_tkgrid)

            await asyncio.Event().wait()

    @classmethod
    async def task_status_monitor(cls, interval, log):
        # Здесь запускаем счетчики типа асинхронных задач для отображения в заголовке окна
        while True:
            cls.tasks_orderbook = 0
            cls.tasks_arb_pair = 0
            cls.tasks_Task = 0
            cls.all_tasks = 0
            for t in cls.task_manager.list_tasks_with_status():
                if 'orderbook' in t['name']:
                    cls.tasks_orderbook += 1
                if 'arb_pair' in t['name'] :
                    cls.tasks_arb_pair += 1
                if 'Task-' in t['name']:
                    cls.tasks_Task += 1

                cls.all_tasks = len(cls.task_manager.list_tasks_with_status())
                if log:
                    print(f"{t['name']:40} {t['status']}")

            cls.spread_table_title = (
                        f"Spot:{cls.spot_usdt} "
                        f"Swap:{cls.swap_usdt} "
                        f"Tasks:{cls.tasks_arb_pair}/{cls.tasks_orderbook}/{cls.tasks_Task}/{cls.all_tasks}"
                    )
            cls.queue_spread_table.put({'title': cls.spread_table_title})  # Отправка словаря данных в процесс таблицы ткинтер через межпроцессную очередь


            if log:
                print("Всего асинхронных задач: ",len(cls.task_manager.list_tasks_with_status()))
                print('-----------------------------------------------------------------------------------')
            await asyncio.sleep(10)

if __name__ == "__main__":
    exchange_id = 'okx'
    asyncio.run(ArbitrageRootClass.run_analytic_process(exchange_id))