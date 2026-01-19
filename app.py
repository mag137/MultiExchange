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

# === Инициализация ===

task_manager = TaskManager()
p_manager = ProcessManager()
logger = LoggerFactory.get_logger(
        name="app",
        level=logging.DEBUG,
        split_levels=True,
        use_timed_rotating=True,
        use_dated_folder=True,
)
exchange_id = 'okx'  # Название биржи
max_active_deals = 1  # Максимальное количество сделок


async def run_exchange_instance(exchange_id: str):
    # Функция запуска и получения экземпляра вызванной биржи

    async with (ExchangeInstance(ccxt, exchange_id, log=True) as exchange):
        success = await sync_time_with_exchange(exchange)
        if not success:
            cprint.error_b("Не удалось синхронизировать время с биржей.")
            return
        return exchange



async def main():
    exchange_id_1 = "okx"
    exchange_id_2 = "gateio"
    swap_pair_data_dict = {}

    async with (
        ExchangeInstance(ccxt, exchange_id_1, log=True) as exchange_1,
        ExchangeInstance(ccxt, exchange_id_2, log=True) as exchange_2,
    ):
        # if not await sync_time_with_exchange(exchange_1):
        #     raise RuntimeError(f"Time sync failed for {exchange_1.id}")
        #
        # if not await sync_time_with_exchange(exchange_2):
        #     raise RuntimeError(f"Time sync failed for {exchange_2.id}")
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
        pprint(swap_pair_data_dict)



        # здесь запускаются TaskManager, watch_* и т.п.
        await asyncio.Event().wait()





if __name__ == "__main__":
    asyncio.run(main())


