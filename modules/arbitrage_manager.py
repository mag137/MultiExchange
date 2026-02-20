__version__= "0.1"

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


class ArbitrageManager:
    _lock = asyncio.Lock()
    exchanges_instance_dict = {}    # Словарь с экземплярами бирж
    balance_dict = {}               # Словарь балансов usdt вида {exchange_id: <balance_usdt>}
    arbitrage_obj_dict = {}
    task_manager = TaskManager()
    balance_managers = {}
    free_deals_slot = None          # Количество доступных слотов сделок (активная сделка занимает один слот).
                                    # При открытии сделки аргумент - декриментируется, При закрытии - инкрементируется.
                                    # Открытия сделки доступно пока есть свободный слот.


async def main():
    TASK_MANAGER = TaskManager()
    MAX_DEAL_SLOTS =  to_decimal('2')
    EXCHANGE_ID_LIST = ["gateio", "okx", "poloniex"]

    BalanceManager.task_manager = TASK_MANAGER
    BalanceManager.max_deal_slots = MAX_DEAL_SLOTS
    ArbitrageManager.task_manager = TASK_MANAGER
    ArbitrageManager.max_deal_slots = MAX_DEAL_SLOTS

    exchange_dict                   = {}  # Словарь с экземплярами бирж {symbol:{exchange_obj}}
    swap_pair_data_dict             = {}  # {symbol:{exchange_id: <data>}})
    swap_pair_for_deal_info_dict    = {}  # Словарь с данными для открытия сделок



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
                        swap_pair_data_dict.setdefault(symbol, {})[exchange_id] = swap_data

        for exchange_id, bm in BalanceManager.exchange_balance_instance_dict.items():
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


if __name__ == "__main__":
    asyncio.run(main())