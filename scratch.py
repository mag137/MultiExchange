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
from modules.ORJSON_file_manager import JsonFileManager
from modules.telegram_bot_message_sender import TelegramMessageSender
# from modules.TkGrid3 import TkGrid
from modules.row_TkGrid import Row_TkGrid
from typing import List, Dict, Any, Optional
from decimal import Decimal, getcontext, ROUND_HALF_UP, InvalidOperation
from functools import partial
from datetime import datetime, UTC
from contextlib import AsyncExitStack
from modules.exception_classes import ( ReconnectLimitExceededError,
                                        InvalidOrEmptyOrderBookError,
                                        BaseArbitrageCalcException,
                                        InsufficientOrderBookVolumeError)

# Пока делаем сисему заточенную на работу только с двумя биржами. Если все будет ок - будем немного менять архитектуру и масштабировать

async def main():
    exchange_id_list = ["phemex", "okx", "gateio"]
    exchange_instance_dict = {}
    swap_pair_data_dict = {}  # {symbol:{exchange_id: <data>}}
    all_swap_symbols_set = set()

    # noinspection PyAbstractClass
    async with AsyncExitStack() as stack:

        async def open_exchange(exchange_id):
            exchange = await stack.enter_async_context(ExchangeInstance(ccxt, exchange_id, log=True))
            return exchange_id, exchange

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


