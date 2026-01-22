__version__ = "3.0 OKX"

import asyncio
import signal
import logging
from modules.task_manager import TaskManager
from modules.logger import LoggerFactory
from modules.process_manager import ProcessManager
from modules import (cprint, round_down, get_average_orderbook_price, sync_time_with_exchange)
from modules.arbitrage_pairs import run_analytic_process_wrapper
from modules.exchange_instance import ExchangeInstance
from modules.test_process_value_getter import shared_values_receiver
from modules.test_process_value_setter import string_writer_process
from modules.TkGrid3 import run_gui_grid_process
from functools import partial
import itertools
import multiprocessing
from pprint import pprint
import random

import multiprocessing
import time
from asyncio import Queue, ALL_COMPLETED
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

# Пока делаем сисему заточенную на работу только с двумя биржами. Если все будет ок - будем немного менять архитектуру и масштабировать

# === Инициализация ===

task_manager = TaskManager()
p_manager = ProcessManager()


class Arbitr:
    _initialized = False

    exchange_1 = None
    exchange_2 = None
    exchange_id_1 = None
    exchange_id_2 = None
    swap_pair_data_dict = None
    balance_usdt_dict = {}

    # # Переменные контроля и статистики
    # ex_1_orderbook_task_count = 0
    # ex_2_orderbook_task_count = 0
    orderbook_task_count_dict = {}

    ex_1_orderbook_get_data_count = 0
    ex_2_orderbook_get_data_count = 0
    orderbook_get_data_count_dict = {}
    orderbook_socket_enable_dict = {}  # Словарь с флагами работы ордербуков. При инициализации ордербука - создается флаг True - условие бесконечного цикла ордербука

    @classmethod
    async def shutdown(cls):
        if not cls._initialized:
            return

        await cls.exchange_1.close()
        await cls.exchange_2.close()

    @classmethod
    def init_arbitrage_pairs_data(cls, *, exchange_1, exchange_2, swap_pair_data_dict):
        if cls._initialized:
            raise RuntimeError("Arbitr context already initialized")

        cls.exchange_1 = exchange_1
        cls.exchange_2 = exchange_2
        cls.exchange_id_1 = exchange_1.id
        cls.exchange_id_2 = exchange_2.id

        cls.swap_pair_data_dict = swap_pair_data_dict
        cls._initialized = True

    def __init__(self, pair):
        # Названия пары и символа совпадают
        self.symbol = pair
        self.balance_usdt_dict = self.__class__.balance_usdt_dict
        self.exchange_1 = self.__class__.exchange_1
        self.exchange_2 = self.__class__.exchange_2
        self.queue_orderbook = asyncio.Queue()  # Локальная очередь для отправки стаканов

        self.queue_pair_spread = self.__class__.queue_pair_spread  # Очередь отправки данный в таблицу спредов
        self.spread_queue_dict = {}  # Словарь хранения данных для таблицы спредов

        self.task_name_symbol_ex_1 = f"orderbook_{self.exchange_id_1}_{self.symbol}"  # Имена задач для получения стаканов
        self.task_name_symbol_ex_2 = f"orderbook_{self.exchange_id_2}_{self.symbol}"  # Имена задач для получения стаканов

        self.fee_ex_1 = Decimal(str(self.swap_pair_data_dict.get(self.symbol, {}).get(self.exchange_id_1, {}).get('taker', None))) * Decimal('100')
        self.fee_ex_2 = Decimal(str(self.swap_pair_data_dict.get(self.symbol, {}).get(self.exchange_id_2, {}).get('taker', None))) * Decimal('100')
        self.commission = self.fee_ex_1 + self.fee_ex_2

        # Инициализация переменных обработки стаканов цен
        self.ex_1_average_ask = None
        self.ex_2_average_ask = None
        self.ex_1_average_bid = None
        self.ex_2_average_bid = None
        self.delta_ratios = None
        self.open_ratio = None
        self.close_ratio = None
        self.max_open_ratio = Decimal('-Infinity')
        self.max_close_ratio = Decimal('-Infinity')
        self.min_open_ratio = Decimal('Infinity')
        self.min_close_ratio = Decimal('Infinity')

        # Расчетные данные для открытия арбитражной сделки
        self.orders_data = {}  # Словарь с расчетными данными amount для открытия ордеров
        self.ex_1_swap_contracts = None
        self.ex_2_swap_contracts = None
        self.contract_size = None
        self.spot_cost = None
        self.ex_1_min_amt = None
        self.ex_1_min_cost = None
        self.ex_2_min_amt = None
        self.ex_2_min_cost = None

        self.orderbook_queue = asyncio.Queue()                      # Очередь для watch_orderbook
        self.spread_queue_dict = {}                                 # Словарь хранения данных для таблицы спредов
        self.queue_pair_spread = self.__class__.queue_pair_spread   # Очередь отправки данных таблицы из экземпляров в arb_data_to_tkgrid



    # Метод запроса цен заданного символа
    async def watch_orderbook(self, exchange):
        """
        Асинхронно следит за стаканом указанного символа.
        При превышении числа попыток — завершает задачу.
        """
        max_reconnect_attempts = 5  # лимит переподключений
        reconnect_attempts = 0
        counted_flag_ex_1 = False
        ex_1_orderbook_flag = False
        exchange_id = exchange.id
        # min_usdt = 0
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
            while self.balance_usdt_dict[exchange_id] <= 0:
                await asyncio.sleep(0.1)

            self.__class__.ex_1_orderbook_task_count += 1
                ex_1_orderbook_flag = True
                exchange_id = self.exchange_id_1

            if exchange.id == self.exchange_id_2:
                self.__class__.ex_2_orderbook_task_count += 1
                ex_2_orderbook_flag = True
                exchange_id = self.exchange_id_2

            self.__class__.orderbook_socket_enable_dict[self.symbol] = True

            while self.__class__.orderbook_socket_enable_dict[self.symbol]:
                try:
                    if ex_1_orderbook_flag:
                        min_usdt = self.__class__.usdt_ex_1
                    if ex_2_orderbook_flag:
                        min_usdt = self.__class__.usdt_ex_2

                    orderbook = await exchange.watchOrderBook(self.symbol)

                    # сбрасываем счётчик после успешного получения
                    reconnect_attempts = 0
                    ticks_total += 1
                    count += 1

                    # Проверим пришедшие данные стаканов - если левые, то выбрасываем исключение
                    if not isinstance(orderbook, dict) or len(orderbook) == 0 or 'asks' not in orderbook or 'bids' not in orderbook:
                        raise InvalidOrEmptyOrderBookError(exchange_id=exchange_id, symbol=self.symbol, orderbook_data=orderbook)

                    if exchange.id == self.exchange_id_1:
                        type(self).ex_1_orderbook_get_data_count += 1   # Инкрементируем счетчик получения стаканов
                        counted_flag_ex_1 = True                        # Определяем какой из бирж принадлежит стакан
                    if exchange.id == self.exchange_id_2:
                        type(self).ex_2_orderbook_get_data_count += 1   # Инкрементируем счетчик получения стаканов
                        counted_flag_ex_2 = True                        # Определяем какой из бирж принадлежит стакан

                    # Обрезаем стакан для анализа
                    depth = min(10, len(orderbook['asks']), len(orderbook['bids']))
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
                                exchange=exchange_id,
                                symbol=self.symbol
                            )

                            average_bid = get_average_orderbook_price(
                                data=orderbook['bids'],
                                money=min_usdt,
                                is_ask=False,
                                log=True,
                                exchange=exchange_id,
                                symbol=self.symbol
                            )


                        except InsufficientOrderBookVolumeError as e:
                            cprint.warning(f"[SKIP] Недостаточный объём стакана для {self.symbol} биржи {exchange_id}: {e}")
                            await asyncio.sleep(0)  # чтобы не перегружать цикл
                            self.__class__.orderbook_socket_enable_dict[self.symbol] = False
                            await self.orderbook_queue.put({
                                "closed_error": True,
                                "symbol": self.symbol,   # Если биржа 1 - закрываем сокет биржи 2,
                                "exchange": exchange_id,
                                "reason": "insufficient_liquidity"
                                })
                            continue

                        if average_ask is None or average_bid is None:
                            await asyncio.sleep(0)
                            continue

                        await self.orderbook_queue.put({
                            "type": "orderbook_update",
                            "ts": time.monotonic(),
                            "symbol": self.symbol,
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
                            "symbol": self.symbol,
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
            try:
                if counted_flag_ex_1:
                    type(self).ex_1_orderbook_get_data_count -= 1
                if counted_flag_ex_2:
                    type(self).ex_2_orderbook_get_data_count -= 1
            except Exception as e:
                cprint.error_b(f"[watch_orderbook][finally] Ошибка при уменьшении счётчика для {self.symbol}, {exchange_id}: {e}")

    async def orderbook_compare(self):
        pass


async def main():
    exchange_id_1 = "okx"
    exchange_id_2 = "gateio"
    swap_pair_data_dict = {}

    async with (
        ExchangeInstance(ccxt, exchange_id_1, log=True) as exchange_1,
        ExchangeInstance(ccxt, exchange_id_2, log=True) as exchange_2,
    ):
        print(exchange_1.id, exchange_2.id)

        dual_pair_list = list(exchange_1.spot_swap_pair_data_dict.keys() & exchange_2.spot_swap_pair_data_dict.keys())
        pprint(dual_pair_list)
        print(len(dual_pair_list))

        for key in dual_pair_list:
            swap_symbol = exchange_1.spot_swap_pair_data_dict[key]['swap']['symbol']
            data_1 = exchange_1.spot_swap_pair_data_dict[key]['swap']
            data_2 = exchange_2.spot_swap_pair_data_dict[key]['swap']
            print(swap_symbol)
            pprint(exchange_1.spot_swap_pair_data_dict[key]['swap'])
            swap_pair_data_dict.setdefault(swap_symbol, {})[exchange_1.id] = data_1
            swap_pair_data_dict.setdefault(swap_symbol, {})[exchange_2.id] = data_2
        Arbitr.init_arbitrage_pairs_data(exchange_1=exchange_1,exchange_2=exchange_2, swap_pair_data_dict=swap_pair_data_dict)
        pprint(swap_pair_data_dict)



        # здесь запускаются TaskManager, watch_* и т.п.
        await asyncio.Event().wait()





if __name__ == "__main__":
    asyncio.run(main())


