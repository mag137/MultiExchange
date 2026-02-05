__version__ = "3.0 OKX"

import asyncio
import signal
import logging
from modules.task_manager import TaskManager
from modules.logger import LoggerFactory
from modules.process_manager import ProcessManager
from modules import (cprint, round_down, get_average_orderbook_price, sync_time_with_exchange)
from modules.arbitrage_pairs import run_analytic_process_wrapper
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
from contextlib import AsyncExitStack
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

    # Словарь балансов вида {exchange_id: balance_usdt}
    balance_usdt_dict = {}

    # Минимальный баланс из балансов бирж
    min_usdt = None

    # # Переменные контроля и статистики
    orderbook_task_count_dict = {} # словарь счетчика задач по биржам


    orderbook_get_data_count_dict = {}

    # Словарь с флагами работы ордербуков.
    # Словарь вида {pair:[<exchange_ids>]}
    # При инициализации ордербука - добавляется в список id биржи
    orderbook_socket_enable_dict = {}

    # Словарь символов и бирж на которых они торгуются, если биржа закрывается - она удаляется из словаря
    symbol_in_exchanges_dict = {}



    @classmethod
    async def shutdown(cls):
        if not cls._initialized:
            return

        await cls.exchange_1.close()
        await cls.exchange_2.close()

    # Проброс словаря экземпляров бирж и словаря арбитражных пар бирж
    @classmethod
    def init_exchanges_pairs_data(cls, *, exchange_instance_dict, swap_pair_data_dict):
        if cls._initialized:
            raise RuntimeError("Arbitr context already initialized")

        cls.exchange_instance_dict = exchange_instance_dict
        cls.swap_pair_data_dict = swap_pair_data_dict
        cls._initialized = True

    def __init__(self, pair):
        # Экземпляр биржи - это работа с арбитражным символом на двух и более биржах
        # Названия пары и символа совпадают
        self.symbol = pair

        # Минимальный баланс из балансов бирж
        self.min_usdt = self.__class__.min_usdt

        # Список бирж участвующих арбитраже данной пары
        self.exchange_list = []

        # Словарь названий задач ордербуков
        self.task_name_symbol_dict = {}

        # Словарь комиссий вида {exchange_id: fee}
        self.fee_dict = {}

        # Словарь средних цен, вида {exchange_id: {ask: average_ask}, {bid: average_bid}}
        self.average_price_dict = {}

        # Словарь с данными для арбитража, ключи - ид биржи {exchange_id: {data1: data}, {data2: data}...}
        self.arbitrage_pair_data = {}

        # Локальная очередь для отправки стаканов
        self.queue_orderbook = asyncio.Queue()

        # Очередь отправки данный в таблицу спредов
        self.queue_pair_spread = self.__class__.queue_pair_spread

        # Словарь хранения данных для таблицы спредов
        self.spread_queue_dict = {}

        # Счетчик пришедших стаканов целевого символа заданной биржи
        self.get_ex_orderbook_data_count = {}

        # Словарь статистики ордербуков символа по exchange_id
        self.exchanges_orderbook_statistic = {}

        # Перебираем биржи данного символа для инициализации словарей
        for exchange_id in self.exchange_list:
            # Имена задач для получения стаканов
            self.task_name_symbol_dict[exchange_id] = f"orderbook_{self.exchange_id_1}_{self.symbol}"
            # Средние цены
            self.average_price_dict[exchange_id] = {'ask': None}
            self.average_price_dict[exchange_id] = {'bid': None}
            # Fee
            self.fee_dict[exchange_id] = Decimal(str(self.swap_pair_data_dict.get(self.symbol, {}).get(self.exchange_id_1, {}).get('taker', None))) * Decimal('100')
            # Счетчик пришедших стаканов целевого символа заданной биржи
            self.get_ex_orderbook_data_count[exchange_id] = None
            # Словарь статистики
            self.exchanges_orderbook_statistic[exchange_id] = {}

        # Инициализация переменных обработки стаканов цен
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
        exchange_id = exchange.id
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
            # todo реализовать получение экземпляром минимального баланса бирж
            while self.balance_usdt_dict[exchange_id] <= 0 or not self.min_usdt:
                await asyncio.sleep(0.1)

            # Инкрементируем счетчик активных ордербуков целевой биржи
            self.__class__.orderbook_task_count_dict[exchange_id] += 1

            # Инициализируем бесконечный цикл подписки получения стаканов
            self.__class__.orderbook_socket_enable_dict[exchange_id] = True

            # Цикл работает пока есть подписка
            while self.__class__.orderbook_socket_enable_dict[exchange_id]:
                try:
                    orderbook = await exchange.watchOrderBook(self.symbol)

                    # сбрасываем счётчик после успешного получения
                    reconnect_attempts = 0
                    ticks_total += 1
                    count += 1

                    # Проверим пришедшие данные стаканов - если левые, то выбрасываем исключение
                    if not isinstance(orderbook, dict) or len(orderbook) == 0 or 'asks' not in orderbook or 'bids' not in orderbook:
                        raise InvalidOrEmptyOrderBookError(exchange_id=exchange_id, symbol=self.symbol, orderbook_data=orderbook)

                    # Инкрементируем счетчик получения стаканов
                    self.get_ex_orderbook_data_count += 1

                    # Обрезаем стакан для анализа до 10 ордеров
                    depth = min(10, len(orderbook['asks']), len(orderbook['bids']))
                    new_ask = tuple(map(tuple, orderbook['asks'][:depth]))
                    new_bid = tuple(map(tuple, orderbook['bids'][:depth]))

                    if new_ask != old_ask or new_bid != old_bid:
                        ticks_changed += 1
                        new_count += 1

                        try:
                            average_ask = get_average_orderbook_price(
                                data=orderbook['asks'],
                                money=self.min_usdt, # Здесь используется минимальный баланс, получаемый из классного метода - минимум среди всех биржевых депозитов
                                is_ask=True,
                                log=True,
                                exchange=exchange_id,
                                symbol=self.symbol
                            )

                            average_bid = get_average_orderbook_price(
                                data=orderbook['bids'],
                                money=self.min_usdt,
                                is_ask=False,
                                log=True,
                                exchange=exchange_id,
                                symbol=self.symbol
                            )

                        except InsufficientOrderBookVolumeError as e:
                            cprint.warning(f"[SKIP] Недостаточный объём стакана для {self.symbol} биржи {exchange_id}: {e}")
                            await asyncio.sleep(0)  # чтобы не перегружать цикл
                            self.__class__.orderbook_socket_enable_dict[self.symbol] = False
                            # Отправляем сообщение о закрытии, теоретически можно отслеживать работу вебсокетов по флагу
                            await self.orderbook_queue.put({
                                "closed_error": True,
                                "symbol": self.symbol,
                                "exchange_id": exchange_id,
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
                            "exchange": exchange_id,
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
                            "exchange_id": exchange_id,
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
            type(self).orderbook_task_count_dict[exchange_id] -= 1

    async def orderbook_compare(self):
        symbol = self.symbol
        average_ask_dict = {}       # {exchange_id: average_ask}
        average_bid_dict = {}       # {exchange_id: average_bid}
        old_average_ask_dict = {}   # {exchange_id: average_ask}
        old_average_bid_dict = {}   # {exchange_id: average_bid}
        queue_message = None
        exchanges_in_pair_list = self.__class__.swap_pair_data_dict.get(symbol, []) # Биржи на которых торгуется символ

        # Создадим задачи ордербуков заданного символа на биржах из словаря
        for exchange_id in exchanges_in_pair_list:
            exchange = self.__class__.exchange_instance_dict[exchange_id] # Получим экземпляр биржи по ид из словаря
            task_name = f"orderbook_{symbol}_{exchange_id}"
            # Создаем задачи подписки на стаканы
            self.__class__.task_manager.add_task(name=task_name, coro_func=partial(self.watch_orderbook, exchange = exchange))

        while len(exchanges_in_pair_list) > 1: # Условие для бесконечного цикла - две и более бирж в списке
            orderbook_average_price_event_data = await self.orderbook_queue.get()

            # Обрабока ошибки ордербука
            if orderbook_average_price_event_data.get('closed_error'):  # Если в одном из ордербуков критическая ошибка - прекращаем арбитраж и закрываем задачи
                error_exchange_id = orderbook_average_price_event_data.get('exchange_id')
                cprint.info_w(f"[orderbook_compare][{symbol}] Малая ликвидность стакана [{error_exchange_id}] - закрываем вебсокет")

                # Удаляем биржу из списка бирж
                exchanges_in_pair_list = [exchanges_in_pair_list for item in exchanges_in_pair_list if item != error_exchange_id]

            # Обработка пришедшего обновления стаканов
            if orderbook_average_price_event_data.get('type') == 'orderbook_update':
                # Инициализируем имя биржи пришедшего стакана
                updated_exchange_id = orderbook_average_price_event_data.get('exchange_id')
                average_ask_dict[updated_exchange_id] = orderbook_average_price_event_data.get('average_ask')
                average_bid_dict[updated_exchange_id] = orderbook_average_price_event_data.get('average_bid')

                if old_average_ask_dict[updated_exchange_id] != average_ask_dict[updated_exchange_id] \
                    or old_average_bid_dict[updated_exchange_id] != average_bid_dict[updated_exchange_id]:
                    old_average_ask_dict[updated_exchange_id] = average_ask_dict[updated_exchange_id]
                    old_average_bid_dict[updated_exchange_id] = average_bid_dict[updated_exchange_id]

                    # TODO Здесь код анализа обновленного стакана и сравнения
                    # ...

            # Обработка статистики прихода стаканов
            if orderbook_average_price_event_data.get('type') == 'tick_stats':
                statistic_exchange_id = orderbook_average_price_event_data.get('exchange_id')
                self.exchanges_orderbook_statistic[statistic_exchange_id]['symbol'] = orderbook_average_price_event_data.get('symbol')
                self.exchanges_orderbook_statistic[statistic_exchange_id]['window_sec'] = orderbook_average_price_event_data.get('window_sec')
                self.exchanges_orderbook_statistic[statistic_exchange_id]['ticks_total']= orderbook_average_price_event_data.get('ticks_total')
                self.exchanges_orderbook_statistic[statistic_exchange_id]['ticks_changed']= orderbook_average_price_event_data.get('ticks_changed')
                self.exchanges_orderbook_statistic[statistic_exchange_id]['ticks_per_sec']= orderbook_average_price_event_data.get('ticks_per_sec')
                self.exchanges_orderbook_statistic[statistic_exchange_id]['changed_per_sec']= orderbook_average_price_event_data.get('changed_per_sec')







            pass


async def main():
    exchange_id_list = ["phemex", "okx", "gateio"]
    swap_pair_data_dict = {}  # {symbol:{exchange_id: <data>}}
    all_swap_symbols_set = set()

    # noinspection PyAbstractClass
    async with AsyncExitStack() as stack:

        async def open_exchange(exchange_id):
            return exchange_id, await stack.enter_async_context(ExchangeInstance(ccxt, exchange_id, log=True))

        results = await asyncio.gather(*(open_exchange(exchange_id) for exchange_id in exchange_id_list))
        exchange_instance_dict = dict(results)

        for exchange_id, exchange in exchange_instance_dict.items():
            print(exchange.id)
            for pair_data in exchange.spot_swap_pair_data_dict.values():
                if swap_data := pair_data.get("swap"):
                    symbol = swap_data["symbol"]
                    all_swap_symbols_set.add(symbol)
                    swap_pair_data_dict.setdefault(symbol, {})[exchange_id] = swap_data["symbol"]

        for symbol in list(swap_pair_data_dict):
            if len(swap_pair_data_dict[symbol]) < 2:
                swap_pair_data_dict.pop(symbol)
        c1 = 0
        c2 = 0
        c3 = 0
        for symbol in list(swap_pair_data_dict):
            if 'gateio' in swap_pair_data_dict[symbol]:
                c1 += 1
            if 'phemex' in swap_pair_data_dict[symbol]:
                c2 += 1
            if 'okx' in swap_pair_data_dict[symbol]:
                c3 += 1



        pprint (swap_pair_data_dict)
        print(len(swap_pair_data_dict))
        print(c1, c2, c3)

        # ---- graceful shutdown ----

        stop_event = asyncio.Event()

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)

        await stop_event.wait()


if __name__ == "__main__":
    asyncio.run(main())


