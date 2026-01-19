__version__= "0.1 multi"

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
# from modules.row_TkGrid import TkGrid
from typing import List, Dict, Any, Optional
from decimal import Decimal, getcontext, ROUND_HALF_UP, InvalidOperation
from functools import partial
from datetime import datetime, UTC

from modules.exception_classes import ( ReconnectLimitExceededError,
                                        InvalidOrEmptyOrderBookError,
                                        BaseArbitrageCalcException,
                                        InsufficientOrderBookVolumeError)
from modules import (cprint, round_down, get_average_orderbook_price, sync_time_with_exchange,  # is_valid_price,


async def run_analytic_process(cls, exchange_id_1: str, exchange_id_2: str):
    cls.task_manager = TaskManager()

    async with (
        ExchangeInstance(ccxt, exchange_id_1, log=True) as exchange_1,
        ExchangeInstance(ccxt, exchange_id_2, log=True) as exchange_2
    ):
        cls.exchange_1 = exchange_1
        cls.exchange_2 = exchange_2

        if not await sync_time_with_exchange(exchange_1):
            cprint.error_b(f"Не удалось синхронизировать время с {exchange_id_1}")
            return

        if not await sync_time_with_exchange(exchange_2):
            cprint.error_b(f"Не удалось синхронизировать время с {exchange_id_2}")
            return

        cls.task_manager.add_task(
            name="start_all_compares",
            coro_func=cls.start_all_orderbooks_compares_tasks,
        )

        cls.task_manager.add_task(
            name=f"watch_balance_{exchange_id_1}",
            coro_func=partial(cls.watch_balance, exchange_1),
        )

        cls.task_manager.add_task(
            name=f"watch_balance_{exchange_id_2}",
            coro_func=partial(cls.watch_balance, exchange_2),
        )

        cls.task_manager.add_task(
            name="task_status_monitor",
            coro_func=cls.task_status_monitor,
            interval=20,
            log=False,
        )

        cls.task_manager.add_task(
            name="arb_data_to_tkgrid_task",
            coro_func=cls.arb_data_to_tkgrid,
        )

        await asyncio.Event().wait()
