import asyncio
import logging
from datetime import datetime
from modules.time_sync import sync_time_with_exchange
import ccxt.pro as ccxt
from modules.exchange_instance import ExchangeInstance
from modules.telegram_bot_message_sender import TelegramMessageSender
from modules.deal_recorder import DealRecorder
from modules.ORJSON_file_manager import JsonFileManager
from modules.logger import LoggerFactory
from pprint import pprint
from modules.exception_classes import (
    OpenSpotOrderError,
    OpenSwapOrderError,
    CloseSpotOrderError,
    CloseSwapOrderError,
    DealOpenError
)
import os
import json
import time
from decimal import Decimal



# Создадим для файла active_deals.json экземпляр файлового менеджера
project_root: str = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
active_dir: str = os.path.join(project_root, "deals_log")
os.makedirs(active_dir, exist_ok=True)
active_path: str = os.path.join(active_dir, "active_deals.json")
print(active_path)
active_manager: JsonFileManager = JsonFileManager(active_path)
active_deals_dict = active_manager.load()

pprint(active_deals_dict)