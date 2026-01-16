__version__ = "0.13"
import asyncio
import logging

import multiprocessing
from modules.time_sync import sync_time_with_exchange
import ccxt.pro as ccxt
from modules.exchange_instance import ExchangeInstance
from modules.telegram_bot_message_sender import TelegramMessageSender
from modules.deal_recorder import DealRecorder
from modules.ORJSON_file_manager import JsonFileManager
from modules.logger import LoggerFactory
from modules.colored_console import cprint
from modules.TkGrid3 import run_gui_grid_process
from pprint import pprint
from modules.exception_classes import (
    OpenSpotOrderError,
    OpenSwapOrderError,
    CloseSpotOrderError,
    CloseSwapOrderError,
    DealOpenError
)
from modules.exception_classes import ( ReconnectLimitExceededError,
                                        InvalidOrEmptyOrderBookError,
                                        BaseArbitrageCalcException,
                                        OpenSpotOrderError,
                                        OpenSwapOrderError,
                                        CloseSpotOrderError,
                                        CloseSwapOrderError,
                                        DealOpenError)
from modules.utils import safe_decimal, timestamp_to_print
from modules.process_manager import ProcessManager
import os
import json
import time
from decimal import Decimal, InvalidOperation

from typing import Dict, Any
from datetime import datetime, timezone

# TODO - в отладчик класса добавить баланс usdt спот, своп, и баланс GT для отображения в дампе сделки

def get_current_iso_time() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


class BaseDealManagerLogger:
    """Базовый миксин для классов, которым нужен собственный логгер."""
    _log_filename = "test_deals.log"
    _name = "base"

    def __init__(self):
        pass

    @property
    def open_deal_logger(self):
        if not hasattr(self.__class__, '_cached_logger'):
            self.__class__._cached_logger = LoggerFactory.get_logger(
                name=self._name,
                log_filename=self._log_filename,
                level=logging.DEBUG,
                split_levels=False,
                use_timed_rotating=True,
                use_dated_folder=True,
                add_date_to_filename=False,
                add_time_to_filename=True,
                base_logs_dir=os.path.abspath(
                    os.path.join(os.path.dirname(__file__), '..', 'deals_log')
                )
            )
        return self.__class__._cached_logger

class DealsManager(BaseDealManagerLogger):
    signal_close_ratio: None
    _log_filename = "deals.log"
    _name = "deals"

    # Создаём multiprocessing.Queue() на уровне класса
    deal_table_queue_data = multiprocessing.Queue()  # ✅ Правильно
    shared_values = {"shutdown": multiprocessing.Value('b', False)}


    dt_str = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    deal_table_queue_data.put(({'title': f'Ожидание данных открытия сделки:  {dt_str}'}))

    # Агент записи на диск инициируется только в jpen_spot_swap_deal
    def __init__(
        self,
        exchange,
        arb_pair: str,
        spot_symbol: str,
        swap_symbol: str,
        max_order_attempt: int = 3,
        max_active_deals: int = 1,
        order_attempt_interval: float = 0.5,
        active_deals_dict: dict | dict = None,
        telegram_sender =None,
        shared_values = None,
        active_deals_file_manager = None
    ):
        super().__init__()
        self.exchange = exchange
        self.arb_pair = arb_pair
        self.spot_symbol = spot_symbol
        self.swap_symbol = swap_symbol
        self.swap_order_id = None
        self.spot_order_id = None
        self.max_order_attempt = max_order_attempt
        self.max_active_deals = max_active_deals
        self.order_attempt_interval = order_attempt_interval
        self.active_deals_dict = active_deals_dict or {}
        self.active_deal_data_dict = {} # Словарь хранения данных сделки текущего экземпляра должен заполнятся при старте при наличии сделки по данному экземпляру
        self.open_position_spot_data = {}
        self.open_position_swap_data = {}
        self.available_for_sell_spot_balance = None # Количество доступных на продажу монет спот кошелька
        self.telegram_sender = telegram_sender
        self.active_deals_file_manager = active_deals_file_manager

        #     Инициализация переменных pnl
        # Инициализация атрибутов для PnL-мониторинга
        self.spot_average_bid = Decimal('0')
        self.swap_average_ask = Decimal('0')
        self.deal_open_ratio = Decimal('0')
        self.current_close_ratio = Decimal('0')
        self.pnl_spot = self.pnl_swap = self.gross_pnl = self.fees = self.net_pnl = Decimal('0')
        self.roi = Decimal('0')


        self.max_pnl = Decimal('-Infinity')
        self.min_pnl = Decimal('Infinity')
        self.max_roi = Decimal('-Infinity')
        self.min_roi = Decimal('Infinity')
        self.min_close_ratio = Decimal('Infinity')
        self.max_close_ratio = Decimal('-Infinity')
        self.old_pnl = Decimal('0')

        # Tk
        self.p_manager = ProcessManager()
        self.deal_table_queue_data = self.__class__.deal_table_queue_data
        self.deal_window_opened = False
        self.coin = self.spot_symbol.split('/')[0]

    async def _run_pnl_monitor(self):
        """
        Бесконечный цикл для отслеживания PnL в реальном времени через ccxt.pro.
        """
        cprint.info_b(f"[PNL MONITOR] Запуск мониторинга для {self.spot_symbol} / {self.swap_symbol}")
        while True:
            try:
                # Получаем спотовый orderbook (bid)
                spot_ob = await self.exchange.watch_order_book(self.spot_symbol)
                self.spot_average_bid = Decimal(str(spot_ob['bids'][0][0])) if spot_ob['bids'] else Decimal('0')

                # Получаем фьючерсный orderbook (ask)
                swap_ob = await self.exchange.watch_order_book(self.swap_symbol)
                self.swap_average_ask = Decimal(str(swap_ob['asks'][0][0])) if swap_ob['asks'] else Decimal('0')

                # Пересчитываем PnL
                self._get_PnL()  # логирование уже внутри

                # Не нужно await asyncio.sleep — watch_order_book сам по себе "ждёт" новый тик

            except ccxt.BaseError as e:
                cprint.error_b(f"[PNL MONITOR] CCXT Error: {e}")
                await asyncio.sleep(2)
            except asyncio.CancelledError:
                cprint.info_b("[PNL MONITOR] Остановлен по запросу.")
                break
            except Exception as e:
                cprint.error_b(f"[PNL MONITOR] Неизвестная ошибка: {e}")
                await asyncio.sleep(2)

    # Метод принятия решений - полная логика работы с арбитражными позициями
    async def decision_trigger(self, signal_deal_dict: dict):
        # Получаем текущие данные
        self.signal_spot_amount             = signal_deal_dict.get('signal_spot_amount',            None)
        self.signal_swap_contracts          = signal_deal_dict.get('signal_swap_contracts',         None)
        self.signal_average_spot_ask        = signal_deal_dict.get("signal_average_spot_ask",       None)
        self.signal_average_spot_bid        = signal_deal_dict.get("signal_average_spot_bid",       None)
        self.signal_average_swap_ask        = signal_deal_dict.get("signal_average_swap_ask",       None)
        self.signal_average_swap_bid        = signal_deal_dict.get("signal_average_swap_bid",       None)
        self.signal_open_ratio              = signal_deal_dict.get("signal_open_ratio",             None)
        self.signal_open_threshold_ratio    = signal_deal_dict.get("signal_open_threshold_ratio",   None)
        self.signal_close_ratio             = signal_deal_dict.get("signal_close_ratio",            None)
        self.signal_close_threshold_ratio   = signal_deal_dict.get("signal_close_threshold_ratio",  None)
        self.signal_max_open_ratio          = signal_deal_dict.get("signal_max_open_ratio",         None)
        self.signal_max_close_ratio         = signal_deal_dict.get("signal_max_close_ratio",        None)
        self.signal_min_open_ratio          = signal_deal_dict.get("signal_min_open_ratio",         None)
        self.signal_min_close_ratio         = signal_deal_dict.get("signal_min_close_ratio",        None)
        self.signal_delta_ratios            = signal_deal_dict.get("signal_delta_ratios",           None)
        self.signal_deal_dict               = signal_deal_dict


        # Запускаем метод принятия решений
        # Если нет открытой сделки по self.arb_pair и количество активных сделок меньше максимального, то идем на проверку условий открытия сделки по self.arb_pair
        if self.arb_pair not in self.active_deals_dict and len(self.active_deals_dict) < self.max_active_deals:
            # Проверка наличия открытоко окна сделок - если окно открыто
            if self.deal_window_opened:
                self.p_manager.stop_process(f"deal_table_{self.coin}")
            # Проверка условия открытия сделки, запуск self._open_spot_swap_deal
            # if self.signal_open_ratio > self.signal_open_threshold_ratio:

            if self.spot_symbol == "SOMI/USDT":
                self.active_deals_dict['signal_open_timestamp'] = (time.time()) # Добавим в словарь время принятия решения на открытие сделки
                await self._open_spot_swap_deal()
        """
        Ветка закрытия позиции
        """
        if self.arb_pair in self.active_deals_dict:
            # await self._run_pnl_monitor()
            # если активная сделка есть, а окно сделок еще не инициализировано
            if not self.deal_window_opened:
                cprint.info_w("окна нет")
                # Инициализируем окно сделки на уровне класса
                self.p_manager.start_process(
                    f"deal_table_{self.coin}",
                    run_gui_grid_process,
                    kwargs={
                        "table_queue_data": self.deal_table_queue_data,      # Очередь для каждого экземпляра должна быть уникальна
                        "shared_values": self.__class__.shared_values, # Общие межпроцессные переменные для проброса флага shutdown
                        "queue_datadict_wrapper_key": None,
                        "row_header": False,
                        "max_height": 300,
                        "fix_max_width_column": False
                    }
                )
                cprint.info_w("Процесс окна запустили")
                self.deal_window_opened = True # Взведен флаг открытого окна

            self._get_PnL()

            # # если ратио открытия меньше 0 и текущее ратио закрытия больше 0...
            # if self.deal_open_ratio < 0.5 and self.current_close_ratio >= 0:                 # Аварийное закрытие - сделка открылась не по плану
            #     self.active_deals_dict['signal_close_timestamp'] = int(time.time())  # Добавим в словарь время принятия решения по закрытию сделки
            #     await self._close_spot_swap_deal()

            # если ратио открытия меньше 1 и текущее ратио закрытия больше -0.4...
            if self.deal_open_ratio < 1 and self.current_close_ratio >= -0.4:
                self.active_deals_dict['signal_close_timestamp'] = int(time.time())  # Добавим в словарь время принятия решения по закрытию сделки
                await self._close_spot_swap_deal()

            # если ратио открытия меньше 1.5 и текущее ратио закрытия больше -0.6...
            if self.deal_open_ratio < 1.5 and self.current_close_ratio >= -0.6:
                self.active_deals_dict['signal_close_timestamp'] = int(time.time())  # Добавим в словарь время принятия решения по закрытию сделки
                await self._close_spot_swap_deal()

            # если ратио открытия меньше 2.5 и текущее ратио закрытия больше -0.8...
            if self.deal_open_ratio < 2.5 and self.current_close_ratio >= -0.8:
                self.active_deals_dict['signal_close_timestamp'] = int(time.time())  # Добавим в словарь время принятия решения по закрытию сделки
                await self._close_spot_swap_deal()

            # если ратио открытия меньше 4 и текущее ратио закрытия больше -ратио открытия/4...
            if self.deal_open_ratio < 4 and self.current_close_ratio >= -self.deal_open_ratio/4:
                self.active_deals_dict['signal_close_timestamp'] = int(time.time())  # Добавим в словарь время принятия решения по закрытию сделки
                await self._close_spot_swap_deal()

    # При открытии арбитражной сделки передаются необходимые для открытия переменные данные
    async def _open_spot_swap_deal(self):
        """
        При открытии спот ордера на покупку комиссия по умолчанию на gateio списывается в базовых монетах ордера.
        На данной бирже списывать комиссий в тетере я не знаю как. Альтернативный вариант списывать в токене GT.
        Комиссия в нем составляет не 0.1% а уже 0.09%, но при этом нужно обеспечить наличие этого токена хотя бы в количестве 0.1 попугая.
        На данный момент это на 1.2 usdt по курсу, но минимальный объем покупки - на 3 USDT!!!
        Иначе вылетаем в трубу по точности.
        Дальнейший вариант - работать арбитраж между двух свопов в котировках usdt и btc через курс.
        Но тут нюанс - сделка спот - среднее время 0.25-0.35 сек. Своп - 0.65-0.75 и доходит до 1 секунды, что не есть хорошо
        """

        # Логгер ошибок инициируется служебными аварийными исключениями и пишется под именами "orders_errors_TIME%"
        logger = self.open_deal_logger  # Инициализировали локальный логгер
        orders_recorder = DealRecorder(signal_deal_dict=self.signal_deal_dict)  # Инициализировали рекордер - агент сохранения результатов сделки
        logger.info("🚀 Начало открытия спот + своп позиций")
        """
        active_deal_data_dict - словарь сбора данных для последующего сохранения в active_deals.json с ключом по названию арбитражной пары.

        """
        active_deal_data_dict = {}  # Инициализируем словарь для передачи в active_deals.json
        active_deal_data_dict['signal_open_timestamp'] = (time.time())  # Сохраним время начала открытия ордеров

        spot_task = self._open_spot()  # Создали задачу открытия спот ордера
        swap_task = self._open_swap()  # Создали задачу открытия своп ордера
        # Запустим одноврменно задачи на выполнение
        spot_result, swap_result = await asyncio.gather(spot_task, swap_task, return_exceptions=True)

        # Если выполнение задачт не выбросило исключение - арбитражная сделка открылась удачно
        spot_ok = not isinstance(spot_result, Exception)
        swap_ok = not isinstance(swap_result, Exception)

        # Заполним словарь результатов выставления позиций.
        active_deal_data_dict.update(self.signal_deal_dict)  # Запишем в словарь сигнальные значения сделки
        # Даже если один из ордеров провален, попытаемся заполнить то, что есть — но только если оба успешны
        if spot_ok and swap_ok:
            active_deal_data_dict['arb_pair'] = self.arb_pair
            active_deal_data_dict['spot_symbol'] = self.spot_symbol
            active_deal_data_dict['swap_symbol'] = self.swap_symbol
            active_deal_data_dict['deal_open_spot_id'] = spot_result['order_data']['id']
            active_deal_data_dict['deal_open_swap_id'] = swap_result['order_data']['id']
            active_deal_data_dict['deal_open_spot_cost'] = spot_result['order_data']['cost']
            active_deal_data_dict['deal_open_swap_cost'] = swap_result['order_data']['cost']
            active_deal_data_dict['deal_open_spot_average_price'] = spot_result['order_data']['average']
            active_deal_data_dict['deal_open_swap_average_price'] = swap_result['order_data']['average']
            active_deal_data_dict['deal_open_spot_amount'] = spot_result['order_data']['amount']
            active_deal_data_dict['deal_open_swap_contracts'] = swap_result['order_data']['amount']
            active_deal_data_dict['deal_open_ratio'] = 100 * (Decimal(str(swap_result['order_data']["average"])) - Decimal(str(spot_result['order_data']["average"]))) / Decimal(str(spot_result['order_data']["average"]))

            # В любом случае успешности открытия ордеров сохраняем дампы ордеров
            spot_order = spot_result.get('order_data') if not isinstance(spot_result, Exception) else None
            swap_order = swap_result.get('order_data') if not isinstance(swap_result, Exception) else None

            # === вычисление комиссий в USDT ===
            spot_fee_usdt = await self._compute_spot_fee_usdt(spot_order)
            swap_fee_usdt = self._compute_swap_fee_usdt(swap_order)

            # === вычисление комиссий в процентах ===
            spot_cost = Decimal(str(spot_order.get("cost", "1")))
            swap_cost = Decimal(str(swap_order.get("cost", "1")))

            spot_fee_percent = (spot_fee_usdt / spot_cost * 100) if spot_cost > 0 else Decimal("0.0")
            swap_fee_percent = (swap_fee_usdt / swap_cost * 100) if swap_cost > 0 else Decimal("0.0")

            # Запись комиссий в словарь арбитражной сделки
            active_deal_data_dict['deal_open_spot_fee_usdt'] = str(spot_fee_usdt.quantize(Decimal("0.00000001")))
            active_deal_data_dict['deal_open_swap_fee_usdt'] = str(swap_fee_usdt.quantize(Decimal("0.00000001")))
            active_deal_data_dict['deal_open_spot_fee_percent'] = str(spot_fee_percent.quantize(Decimal("0.0001")))
            active_deal_data_dict['deal_open_swap_fee_percent'] = str(swap_fee_percent.quantize(Decimal("0.0001")))

            # 🔹 ДАЁМ ВРЕМЯ БИРЖЕ ОБНОВИТЬ БАЛАНС
            await asyncio.sleep(0.5)  # 500 мс — обычно достаточно
            base_currency = self.spot_symbol.split('/')[0]  # → 'SOMI'
            balance = await self.exchange.fetch_balance()

            # Невозможно использовать встроенный метод amount_to_precision.
            # - могут возникнуть проблемы с точностью при открытии ордера на продажу объемом available_for_sell_spot_balance
            available_for_sell_spot_balance = float(balance.get(base_currency, {}).get('free', 0.0))
            active_deal_data_dict['available_for_sell_spot_balance'] = available_for_sell_spot_balance

            # Дополнительные данные для записи дампа ордеров
            deal_data = {
                "spot_order": spot_order or {"error": str(spot_result) if isinstance(spot_result, Exception) else "unknown"},
                "swap_order": swap_order or {"error": str(swap_result) if isinstance(swap_result, Exception) else "unknown"},
                "signal_open_timestamp": active_deal_data_dict['signal_open_timestamp'],
                "available_for_sell_spot_balance": available_for_sell_spot_balance}

            # Сохраним доступное количество спот монет в аргумент self.available_for_sell_spot_balance
            self.available_for_sell_spot_balance = available_for_sell_spot_balance

            # Рекордер записывает дамп ордеров с пометкой "open_deal" в названии файла дампа
            dump_path = orders_recorder.record_orders_dump(deal_data, insertion_descriptor="open_deal")

            # Сохраняем временные параметры открытия арбитражной сделки
            active_deal_data_dict['open_dump_path'] = dump_path
            active_deal_data_dict['deal_open_spot_complete_timestamp'] = float(spot_result['order_data']['lastTradeTimestamp']) / 1000
            active_deal_data_dict['deal_open_swap_complete_timestamp'] = float(swap_result['order_data']['lastTradeTimestamp']) / 1000
            active_deal_data_dict['deal_open_spot_duration'] = (float(spot_result['order_data']['lastTradeTimestamp']) / 1000) - active_deal_data_dict['signal_open_timestamp']
            active_deal_data_dict['deal_open_swap_duration'] = (float(swap_result['order_data']['lastTradeTimestamp']) / 1000) - active_deal_data_dict['signal_open_timestamp']

            logger.info("✅ Обе позиции успешно открыты")
            # Фиксируем точный баланс монет, доступных для продажи при закрытии сделки.
            # Это необходимо, так как фактически купленный объём может отличаться от запрошенного
            # из-за частичного исполнения, комиссии или ограничений биржи.
            # Имхо если баланс ненулевой - значит была сделка, значит данные есть в active_deals.json и active_deals_dict,
            # потому проверяем баланс целевой монеты один раз после открытия арбитражной сделки,
            # так как баланс может измениться только поле открытия или закрытия арбитражной сделки



            # На данном этапе сохраняем словарь данных сделки как аргумент объекта класса для доступа через self из других методов
            self.active_deal_data_dict = active_deal_data_dict
            self.active_deals_dict[self.arb_pair] = self.active_deal_data_dict

            # Рекордер записывает полный лог данных в файл active_deals.json
            cprint.info_w("Записываем словарь в файл active_deals.json")

            orders_recorder.record_active_deal_dict(active_deal_data_dict)

            pprint(active_deal_data_dict)

            # Отправим результат сделки в телеграм бот
            await self.telegram_sender.send_numbered_message(f"Обе позиции {self.spot_symbol.split('/')[0]} открыты, сохраняем сделку в active_deals")
            # await self.telegram_sender.send_numbered_message(f"Данные сделки SPOT:\n{json.dumps(spot_order, indent=2, ensure_ascii=False)}")
            # await self.telegram_sender.send_numbered_message(f"Данные сделки SWAP:\n{json.dumps(swap_order, indent=2, ensure_ascii=False)}")
            await self.telegram_sender.send_numbered_message(f"Active_deals data:\n{json.dumps(deal_data, indent=2, ensure_ascii=False)}")

            # Получим id исполненных ордеров
            self.spot_order_id = spot_order.get("id", None)
            self.swap_order_id = swap_order.get("id", None)

            # -------------------------GET POSITION----------------------------------

            # 🔹 Проверяем swap-позицию после открытия
            try:
                position = None
                deal_data_position = {}
                swap_positions = await self.exchange.fetch_positions([self.swap_symbol])
                position = next((p for p in swap_positions if abs(float(p.get("contracts", 0))) > 0), None)
                if position:
                    logger.info(f"📊 Позиция по {self.swap_symbol} подтверждена: {json.dumps(position, indent=2)}")
                    await self.telegram_sender.send_numbered_message(f"📊 Подтверждена открытая позиция:\n{json.dumps(position, indent=2, ensure_ascii=False)}")
                    deal_data_position["swap_position"] = position
                else:
                    logger.warning(f"⚠️ Позиция по {self.swap_symbol} не найдена после открытия!")
                    await self.telegram_sender.send_numbered_message(f"⚠️ Не найдена открытая позиция по {self.swap_symbol} после открытия ордера!")
                    deal_data_position["swap_position"] = {"warning": "position_not_found"}

                # Рекордер записывает дамп запроса открытой позиции
                # orders_recorder.record_orders_dump(deal_data_position, insertion_descriptor="open_swap_position")

            except Exception as pos_e:
                logger.error(f"Ошибка при получении данных позиции: {pos_e}")
                await self.telegram_sender.send_numbered_message(
                    f"❌ Ошибка при получении данных позиции: {pos_e}")

            # Получим данные из истории сделок в части реальных заплаченных комиссий
            spot_order_history, swap_order_history = await self.fetch_spot_and_swap_order_by_id(spot_order_id=self.spot_order_id, swap_order_id=self.swap_order_id)
            await asyncio.sleep(1)
            deal_data_history = {"spot_order": spot_order_history or {"error": str(spot_order_history) if isinstance(spot_order_history, Exception) else "unknown"},
                                 "swap_order": swap_order_history or {"error": str(swap_order_history) if isinstance(swap_order_history, Exception) else "unknown"},
                                 "dump_time": time.time()}

            # Рекордер записывает дамп истории открытия позиций
            # orders_recorder.record_orders_dump(deal_data_history, insertion_descriptor="open_deal_history")
            # await self.telegram_sender.send_numbered_message(f"История сделки SPOT:\n{json.dumps(spot_order_history, indent=2, ensure_ascii=False)}")
            # await self.telegram_sender.send_numbered_message(f"История сделки SWAP:\n{json.dumps(swap_order_history, indent=2, ensure_ascii=False)}")
            return

        # 🔴 Если мы дошли сюда — хотя бы один ордер НЕ открыт.
        # Ниже обрабатываем ЧАСТИЧНЫЙ или ПОЛНЫЙ сбой.
        logger.error(f"Ошибка открытия: spot_ok={spot_ok}, swap_ok={swap_ok}")

        # Собираем минимальные данные для дампа в случае сбоя
        spot_order = spot_result.get('order_data') if spot_ok else None
        swap_order = swap_result.get('order_data') if swap_ok else None
        base_currency = self.spot_symbol.split('/')[0]
        available_for_sell_spot_balance = 0.0

        try:
            # Попытка получить баланс даже при частичном сбое (на случай, если спот частично исполнился)
            balance = await self.exchange.fetch_balance()
            available_for_sell_spot_balance = float(balance.get(base_currency, {}).get('free', 0.0))
        except Exception as e:
            logger.warning(f"Не удалось получить баланс при сбое: {e}")

        deal_data = {
            "spot_order": spot_order or {"error": str(spot_result) if isinstance(spot_result, Exception) else "unknown"},
            "swap_order": swap_order or {"error": str(swap_result) if isinstance(swap_result, Exception) else "unknown"},
            "signal_open_timestamp": active_deal_data_dict['signal_open_timestamp'],
            "available_for_sell_spot_balance": available_for_sell_spot_balance
        }

        # Сохраняем дамп даже при сбое
        dump_path = orders_recorder.record_orders_dump(deal_data, insertion_descriptor="open_deal_failure")


        # 🔹 Обработка частичного сбоя: аварийное закрытие открытой части
        try:
            if not spot_ok and swap_ok:
                logger.critical("🚨 Спот НЕ открыт! Аварийное закрытие своп-позиции...")
                try:
                    await self._close_swap()
                    logger.info("✅ Своп аварийно закрыт")
                except Exception as close_e:
                    logger.error(f"❌ Не удалось аварийно закрыть своп: {close_e}")
                    if self.telegram_sender:
                        await self.telegram_sender.send_numbered_message(
                            f"❌ АВАРИЙНОЕ ЗАКРЫТИЕ СВОПА ПРОВАЛЕНО\n{close_e}"
                        )

            elif not swap_ok and spot_ok:
                logger.critical("🚨 Своп НЕ открыт! Аварийная продажа спота...")
                try:
                    await self._close_spot()
                    logger.info("✅ Спот аварийно закрыт")
                except Exception as close_e:
                    logger.error(f"❌ Не удалось аварийно закрыть спот: {close_e}")
                    if self.telegram_sender:
                        await self.telegram_sender.send_numbered_message(
                            f"❌ АВАРИЙНАЯ ПРОДАЖА СПОТА ПРОВАЛЕНА\n{close_e}"
                        )

            else:
                logger.critical("🔥 Обе позиции НЕ открыты")

        except Exception as final_error:
            cause = str(final_error.__cause__) if final_error.__cause__ else str(final_error)
            if self.telegram_sender:
                await self.telegram_sender.send_numbered_message(
                    f"❌ ЧАСТИЧНЫЙ/ПОЛНЫЙ СБОЙ ОТКРЫТИЯ\n"
                    f"Спот: {'✅ OK' if spot_ok else '❌ FAIL'}\n"
                    f"Своп: {'✅ OK' if swap_ok else '❌ FAIL'}\n"
                    f"Ошибка: {cause}"
                )
            # Не подавляем исключение — оно будет проброшено ниже

        # 🔸 Теперь, после попытки аварийного закрытия, выбрасываем исходную ошибку
        if not spot_ok and not swap_ok:
            primary_error = spot_result if isinstance(spot_result, Exception) else swap_result
            raise DealOpenError("Не удалось открыть ни спот, ни своп") from primary_error
        elif not spot_ok:
            raise spot_result
        else:
            raise swap_result

    # Метод открытия спот-позиции
    async def _open_spot(self):
        logger = self.open_deal_logger
        for attempt in range(1, self.max_order_attempt + 1):
            send_time = time.time()
            try:
                # # Умножение только в Decimal
                # raw_price = self.signal_average_spot_ask * Decimal("1.05")
                #
                # # Приводим к правильной точности биржи
                # price_str = self.exchange.price_to_precision(self.spot_symbol, float(raw_price))
                #
                # order_data = await self.exchange.create_order(
                #     symbol=self.spot_symbol,
                #     type='limit',
                #     side='buy',
                #     amount=self.signal_spot_amount,
                #     price=price_str,
                #     params={}
                # )
                print("self.signal_spot_amount", self.signal_spot_amount)
                print(self.exchange.price_to_precision(self.spot_symbol, float(self.signal_average_spot_ask * Decimal("1.1"))))
                print(type(self.exchange.price_to_precision(self.spot_symbol, float(self.signal_average_spot_ask * Decimal("1.1")))))
                order_data = await self.exchange.create_order(
                    symbol  = self.spot_symbol,
                    type    = 'limit',
                    side    = 'buy',
                    amount  = self.signal_spot_amount,  # количество монет
                    price   = float(self.signal_average_spot_ask * Decimal("1.00")),  # лимит выше рынка → сработает как маркет
                    params  = {}
                )

                recv_time = time.time()

                status = order_data.get('status') or order_data.get('info', {}).get('finish_as')
                if status in ('closed', 'filled', 'finished'):
                    result = {
                        'order_data': order_data,
                        'spot_send_time': send_time,
                        'spot_recv_time': recv_time,


                        'duration': recv_time - send_time,
                        'attempts': attempt
                    }
                    if attempt > 1:
                        logger.info(f"✅ Спот открыт с {attempt}-й попытки")
                    return result
                else:
                    raise Exception(f"Ордер не исполнен, статус: {status}")

            except Exception as e:
                recv_time = time.time()
                duration = recv_time - send_time
                error_msg = str(e)
                logger.warning(f"Попытка {attempt}/{self.max_order_attempt} открытия спота упала через {duration:.3f}с")
                logger.warning(f"    Биржа: {error_msg}")
                if attempt < self.max_order_attempt:
                    await asyncio.sleep(self.order_attempt_interval)
                else:
                    logger.error(f"❌ Все {self.max_order_attempt} попыток открыть спот провалились")
                    logger.error(f"    Последняя ошибка от биржи: {error_msg}")
                    raise OpenSpotOrderError(
                        self.spot_symbol,
                        f"Не удалось открыть спот-ордер после всех попыток. Биржа: {error_msg}"
                    ) from e

    # Метод открытия своп-позиции
    async def _open_swap(self):
        """
        Метод возвращает словарь, содержащий дамп ордера
        result = {
            'order_data': order_data,
            'swap_send_time': send_time,
            'swap_recv_time': recv_time,
            'duration': recv_time - send_time,
            'attempts': attempt
            }
        """
        logger = self.open_deal_logger

        async def init_swap_settings(symbol):
            try:
                await self.exchange.set_margin_mode(
                    symbol=symbol,
                    marginMode='cross')
            except Exception as e1:
                logger.warning(f"Не удалось установить margin mode для {symbol}: {e1}")
            try:
                await self.exchange.set_leverage(1, symbol)
            except Exception as e1:
                logger.warning(f"Не удалось установить leverage для {symbol}: {e1}")

        await init_swap_settings(self.swap_symbol)

        for attempt in range(1, self.max_order_attempt + 1):
            send_time = time.time()
            try:
                order_data = await (self.exchange.create_order
                                    (symbol = self.swap_symbol,
                                     type   = 'market',
                                     side   = 'sell',
                                     amount = self.signal_swap_contracts,
                                     params = {})
                                    )
                recv_time = time.time()

                status = order_data.get('status') or order_data.get('info', {}).get('finish_as')
                if status in ('closed', 'filled', 'finished'):
                    result = {
                        'order_data': order_data,
                        'swap_send_time': send_time,
                        'swap_recv_time': recv_time,
                        'duration': recv_time - send_time,
                        'attempts': attempt
                    }
                    if attempt > 1:
                        logger.info(f"✅ Своп открыт с {attempt}-й попытки")
                    return result
                else:
                    raise Exception(f"Ордер не исполнен, статус: {status}")

            except Exception as e:
                recv_time = time.time()
                duration = recv_time - send_time
                error_msg = str(e)

                # 🔻 ДВЕ СТРОКИ
                logger.warning(
                    f"Попытка {attempt}/{self.max_order_attempt} открытия свопа упала через {duration:.3f}с"
                )
                logger.warning(f"    Биржа: {error_msg}")

                if attempt < self.max_order_attempt:
                    await asyncio.sleep(self.order_attempt_interval)
                else:
                    logger.error(f"❌ Все {self.max_order_attempt} попыток открыть своп провалились")
                    logger.error(f"    Последняя ошибка от биржи: {error_msg}")
                    raise OpenSwapOrderError(
                        self.swap_symbol,
                        f"Не удалось открыть своп-ордер после всех попыток. Биржа: {error_msg}"
                    ) from e

    async def _close_spot_swap_deal(self):
        """
        Закрытие спот происходит на весь доступный реальный объем
        """
        orders_recorder = DealRecorder(signal_deal_dict=self.signal_deal_dict)  # Инициализировали рекордер - агент сохранения результатов сделки
        logger = self.open_deal_logger
        self.active_deal_data_dict['signal_close_timestamp'] = (time.time()) # Сохраним время начала открытия ордеров

        try:
            logger.info("🚀 Начало закрытия спот + своп позиций")
            spot_result, swap_result = await asyncio.gather(self._close_spot(), self._close_swap())
            self.active_deal_data_dict.update(self.active_deals_dict.get(self.arb_pair, {}))
            self.active_deal_data_dict['arb_pair']                  = self.arb_pair
            self.active_deal_data_dict['deal_close_spot_id']        = spot_result['order_data']['id']
            self.active_deal_data_dict['deal_close_swap_id']        = swap_result['order_data']['id']
            self.active_deal_data_dict['deal_close_spot_cost']      = spot_result['order_data']['cost']
            self.active_deal_data_dict['deal_close_swap_cost']      = swap_result['order_data']['cost']
            self.active_deal_data_dict['deal_close_spot_avg']       = spot_result['order_data']['average']
            self.active_deal_data_dict['deal_close_swap_avg']       = swap_result['order_data']['average']
            self.active_deal_data_dict['deal_close_spot_amount']    = spot_result['order_data']['amount']
            self.active_deal_data_dict['deal_close_swap_contracts'] = swap_result['order_data']['amount']
            self.active_deal_data_dict['deal_close_ratio'] = 100 * (Decimal(str(spot_result['order_data']["average"])) - Decimal(str(swap_result['order_data']["average"]))) / Decimal(str(swap_result['order_data']["average"]))

            # В любом случае успешности открытия ордеров сохраняем дампы ордеров
            spot_order_data = spot_result.get('order_data') if not isinstance(spot_result, Exception) else None
            swap_order_data = swap_result.get('order_data') if not isinstance(swap_result, Exception) else None

            # === вычисление комиссий в USDT ===
            spot_fee_usdt = await self._compute_spot_fee_usdt(spot_order_data)
            swap_fee_usdt = self._compute_swap_fee_usdt(swap_order_data)

            # === вычисление комиссий в процентах ===
            spot_cost = Decimal(str(spot_order_data.get("cost", "1")))
            swap_cost = Decimal(str(swap_order_data.get("cost", "1")))

            spot_fee_percent = (spot_fee_usdt / spot_cost * 100) if spot_cost > 0 else Decimal("0.0")
            swap_fee_percent = (swap_fee_usdt / swap_cost * 100) if swap_cost > 0 else Decimal("0.0")

            self.active_deal_data_dict['deal_close_spot_fee_usdt']   = str(spot_fee_usdt.quantize(Decimal("0.00000001")))
            self.active_deal_data_dict['deal_close_swap_fee_usdt']   = str(swap_fee_usdt.quantize(Decimal("0.00000001")))
            self.active_deal_data_dict['deal_close_spot_fee_percent'] = str(spot_fee_percent.quantize(Decimal("0.0001")))
            self.active_deal_data_dict['deal_close_swap_fee_percent'] = str(swap_fee_percent.quantize(Decimal("0.0001")))

            self.active_deal_data_dict['deal_close_spot_complete_timestamp'] = float(spot_result['order_data']['lastTradeTimestamp']) / 1000
            self.active_deal_data_dict['deal_close_swap_complete_timestamp'] = float(swap_result['order_data']['lastTradeTimestamp']) / 1000
            self.active_deal_data_dict['deal_close_spot_duration'] = (float(spot_result['order_data']['lastTradeTimestamp']) / 1000) - self.active_deal_data_dict['signal_close_timestamp']
            self.active_deal_data_dict['deal_close_swap_duration'] = (float(swap_result['order_data']['lastTradeTimestamp']) / 1000) - self.active_deal_data_dict['signal_close_timestamp']

            spot_ok = not isinstance(spot_result, Exception)
            swap_ok = not isinstance(swap_result, Exception)

            # В любом случае успешности открытия ордеров сохраняем дампы ордеров
            spot_order = spot_result.get('order_data') if not isinstance(spot_result, Exception) else None
            swap_order = swap_result.get('order_data') if not isinstance(swap_result, Exception) else None
            deal_data = {
                "spot_order": spot_order or {"error": str(spot_result) if isinstance(spot_result, Exception) else "unknown"},
                "swap_order": swap_order or {"error": str(swap_result) if isinstance(swap_result, Exception) else "unknown"},
                "dump_time": time.time()
            }

            # Рекордер записывает дамп ордеров с пометкой "close_deal" в названии файла дампа
            dump_path = orders_recorder.record_orders_dump(deal_data, insertion_descriptor="close_deal")
            print(f"Дамп закрытия записан по адресу {dump_path}")
            self.active_deal_data_dict['close_dump_path'] = dump_path
            orders_recorder.record_deal_dump(self.active_deal_data_dict)


            if spot_ok and swap_ok:
                logger.info("✅ Обе позиции успешно закрыты!")
                # 🔹 ДАЁМ ВРЕМЯ БИРЖЕ ОБНОВИТЬ БАЛАНС
                await asyncio.sleep(0.5)  # 500 мс — обычно достаточно
                print("Здесь будет запись полного лога сделки в папку deals_log")
            # Временная отладочная запись словаря в файл active_deals
            orders_recorder.record_active_deal_dict(self.active_deal_data_dict)

            await self._check_gt_balance(orders_recorder=orders_recorder)

            # Удаляем арбитражную пару из словаря self.active_deals_dict
            del self.active_deals_dict[self.arb_pair]

            # Удаляем арбитражную пару из файла active_deals.json
            if self.active_deals_file_manager.remove(self.arb_pair):
                cprint.info_w(f"Запись {self.active_deals_dict} удалена из файла active_deals.json")
            else:
                cprint.error_w(f"Запись {self.active_deals_dict} не была удалена из файла active_deals.json")


            return {'spot_close': spot_result, 'swap_close': swap_result}

        except CloseSpotOrderError as e:
            logger.critical(
                f"🚨 Спот НЕ закрыт после {self.max_order_attempt} попыток! Своп, возможно, закрыт. ")
            if self.telegram_sender:
                await self.telegram_sender.send_numbered_message(
                    f"❌ ЗАКРЫТИЕ СПОТА ПРОВАЛЕНО\n"
                    f"Попыток: {self.max_order_attempt}\n"
                    f"Причина: {e.__cause__}"
                )
            raise

        except CloseSwapOrderError as e:
            logger.critical(
                f"🚨 Своп НЕ закрыт после {self.max_order_attempt} попыток! Спот, возможно, закрыт. "

            )
            if self.telegram_sender:
                await self.telegram_sender.send_numbered_message(
                    f"❌ ЗАКРЫТИЕ СВОПА ПРОВАЛЕНО\n"
                    f"Попыток: {self.max_order_attempt}\n"

                    f"Причина: {e.__cause__}"
                )
            raise

        except Exception as e:
            logger.error(f"Неизвестная ошибка при закрытии: {e}")
            if self.telegram_sender:
                await self.telegram_sender.send_numbered_message(f"🔥 НЕИЗВЕСТНАЯ ОШИБКА ЗАКРЫТИЯ\n{e}")
            raise

    def _get_PnL(self):
        """
        Для вычисления текущего pnl необходимо:
            текущие средние цены бид спота и аск свопа, берем через:
                - self.swap_average_ask
                - self.spot_average_bid
            цены открытия сделок:
                - цена открытия спот: active_deal_data_dict.get('deal_open_spot_average_price')
                - цена открытия своп: active_deal_data_dict.get('deal_open_swap_average_price')
            комиссии за открытие позиций, так как комиссии за открытие и закрытие равны, умножаем комиссии за открытие на два:
                - комиссия спот процентная - active_deal_data_dict.get('deal_open_spot_fee_percent')
                - комиссия своп процентная - active_deal_data_dict.get('deal_open_swap_fee_percent')
                - комиссия спот абсолютная - active_deal_data_dict.get('deal_open_spot_fee_usdt')
                - комиссия своп абсолютная - active_deal_data_dict.get('deal_open_swap_fee_usdt')
        Returns:
            dict or None
        """
        """
        Здесь для отладки вывода сделки на экран запустим запрос ордербуков, создадим словарь для ткинтера
        """

        active_deal_data_dict = self.active_deals_dict.get(self.arb_pair)
        deal_data = {}  # Инициализируем deal_data для возврата

        try:
            # --- Открываем данные из active_deal_data_dict с правильными ключами ---
            signal_ratio    = safe_decimal(active_deal_data_dict.get('signal_open_ratio'), 'signal_open_ratio')
            deal_open_ratio    = safe_decimal(active_deal_data_dict.get('deal_open_ratio'), 'deal_open_ratio')
            spot_open_price = safe_decimal(active_deal_data_dict.get('deal_open_spot_average_price'), 'deal_open_spot_average_price')
            swap_open_price = safe_decimal(active_deal_data_dict.get('deal_open_swap_average_price'), 'deal_open_swap_average_price')
            spot_amount = safe_decimal(active_deal_data_dict.get('deal_open_spot_amount'), 'deal_open_spot_amount')
            swap_contracts = safe_decimal(active_deal_data_dict.get('deal_open_swap_contracts'), 'deal_open_swap_contracts')
            open_deal_time = timestamp_to_print(active_deal_data_dict.get('deal_open_swap_complete_timestamp') * 1000)
            spot_fee_percent = safe_decimal(active_deal_data_dict.get('deal_open_spot_fee_percent'), 'deal_open_spot_fee_percent')
            swap_fee_percent = safe_decimal(active_deal_data_dict.get('deal_open_swap_fee_percent'), 'deal_open_swap_fee_percent')
            self.deal_open_ratio = deal_open_ratio

            # # Комиссии в процентах (уже в процентах, например "0.0900" = 0.09%)
            # spot_fee_percent = safe_decimal(active_deal_data_dict.get('deal_open_spot_fee_percent'))  # уже в %
            # swap_fee_percent = safe_decimal(active_deal_data_dict.get('deal_open_swap_fee_percent'))  # уже в %

            # Переводим в доли (0.09% → 0.0009)
            spot_fee = spot_fee_percent / Decimal('100')
            swap_fee = swap_fee_percent / Decimal('100')

            # Текущие рыночные цены для закрытия
            spot_close_price = safe_decimal(getattr(self, 'signal_average_spot_bid', None), 'signal_average_spot_bid')
            swap_close_price = safe_decimal(getattr(self, 'signal_average_swap_ask', None), 'signal_average_swap_ask')
            close_ratio = 100 * (spot_close_price - swap_close_price) / swap_close_price
            self.current_close_ratio = close_ratio

            # --- PnL по позициям ---
            # Спот: купили по spot_open_price, продаём по spot_close_price → (close - open) * amount
            self.pnl_spot = (spot_close_price - spot_open_price) * spot_amount

            # Своп: открыли шорт по swap_open_price, закрываем по swap_close_price → (open - close) * contracts
            self.pnl_swap = (swap_open_price - swap_close_price) * swap_contracts

            # Грязная прибыль
            self.gross_pnl = self.pnl_spot + self.pnl_swap

            # --- Комиссии (открытие + закрытие = ×2) ---
            # Комиссия считается от оборота: цена × объём × ставка
            spot_fee_total = 2 * spot_open_price * spot_amount * spot_fee
            swap_fee_total = 2 * swap_open_price * swap_contracts * swap_fee
            self.fees = spot_fee_total + swap_fee_total

            # Чистая прибыль
            self.net_pnl = self.gross_pnl - self.fees

            # ROI: инвестиции = стоимость открытых позиций в USDT
            invested_usdt = spot_open_price * spot_amount + swap_open_price * swap_contracts
            self.roi = (self.net_pnl / invested_usdt) * 100 if invested_usdt > 0 else Decimal('0')

            # --- Обновляем deal_data ---
            deal_data.update({
                'pnl_spot': self.pnl_spot,
                'pnl_swap': self.pnl_swap,
                'gross_pnl': self.gross_pnl,
                'fees': self.fees,
                'net_pnl': self.net_pnl,
                'roi': self.roi,
                'current_spot_price': spot_close_price,
                'current_swap_price': swap_close_price,
            })

            # --- Отслеживание min/max ---
            self.max_pnl = max(self.max_pnl, self.net_pnl)
            self.min_pnl = min(self.min_pnl, self.net_pnl)
            self.max_roi = max(self.max_roi, self.roi)
            self.min_roi = min(self.min_roi, self.roi)
            self.min_close_ratio = min(self.min_close_ratio, close_ratio)
            self.max_close_ratio = max(self.max_close_ratio, close_ratio)

            # тестовый словарь для таблицы
            pnl_data = {
                1: {
                    0: {'text': 'Параметр', 'fg': 'yellow', 'bg': 'black'},
                    1: {'text': 'Signal ratio', 'fg': 'yellow', 'bg': 'black'},
                    2: {'text': 'Current ratio', 'fg': 'yellow', 'bg': 'black'},
                    3: {'text': 'Fees USDT', 'fg': 'yellow', 'bg': 'black'}
                },
                2: {0: {'text': "Open  Data"}, 1: {'text': f"{signal_ratio:.4f}"}, 2: {'text': f"{deal_open_ratio:.4f}"}, 3: {'text': f"{self.fees:.4f}"}},
                3: {0: {'text': "Close Data"}, 1: {'text': f"{self.signal_close_threshold_ratio:.4f}"}, 2: {'text': f"{close_ratio:.4f}"}, 3: {'text': f"{self.fees:.4f}"}},
                4: {0: {'text': 'Параметр', 'fg': 'yellow', 'bg': 'black'}, 1: {'text': "Курс", 'fg': 'yellow', 'bg': 'black'}, 2: {'text': 'PnL', 'fg': 'yellow', 'bg': 'black'}, 3: {'text': 'Fee %.', 'fg': 'yellow', 'bg': 'black'}},
                5: {0: {'text': 'Текущий Spot'}, 1: {'text': f"{self.signal_average_spot_bid:.6f}"}, 2: {'text': f"{self.pnl_spot:.4f}"}, 3: {'text': f'{spot_fee_percent:.3f} %'}},
                6: {0: {'text': 'Текущий Swap'}, 1: {'text': f"{self.signal_average_swap_ask:.6f}"}, 2: {'text': f"{self.pnl_swap:.4f}"}, 3: {'text': f'{swap_fee_percent:.3f} %'}},
                7: {0: {'text':'Параметр','fg': 'yellow', 'bg': 'black'}, 1: {'text': "Min",'fg': 'yellow', 'bg': 'black'}, 2: {'text': 'Current','fg': 'yellow', 'bg': 'black'}, 3: {'text' :'Max','fg': 'yellow', 'bg': 'black'}},
                8: {0: {'text':'Close ratio'}, 1: {'text': f"{self.min_close_ratio:.4f}"}, 2: {'text': f"{close_ratio:.4f}"}, 3: {'text' :f"{self.max_close_ratio:.4f}"}},
                9: {0: {'text':'PnL USDT'}, 1: {'text': f"{self.min_pnl:.4f}"}, 2: {'text': f"{self.net_pnl:.4f}"}, 3: {'text' :f"{self.max_pnl:.4f}"}},
                10: {0: {'text':'ROI %'}, 1: {'text': f"{self.min_roi:.4f}"}, 2: {'text': f"{self.roi:.4f}"}, 3: {'text' :f"{self.max_roi:.4f}"}},

            }
            self.__class__.deal_table_queue_data.put({'title': f'{self.spot_symbol.split('/')[0]}  {open_deal_time}'})
            self.__class__.deal_table_queue_data.put(pnl_data)



            # --- Логирование при изменении ---
            if self.old_pnl != self.net_pnl:
                cprint.info_b(f"[PNL] {self.arb_pair}: "
                              f"net {self.net_pnl:.4f} USDT | ROI {float(self.roi):.3f}% | "
                              f"spot {self.pnl_spot:.4f} swap {self.pnl_swap:.4f} | "
                              f"net_pnl $: min {self.min_pnl:.4f}$, max {self.max_pnl:.4f}$ | "
                              f"roi %: min {self.min_roi:.4f}%, max {self.max_roi:.4f}%")
                self.old_pnl = self.net_pnl

            return deal_data

        except Exception as e:

            cprint.error_b(f"[_get_PnL] Ошибка при расчёте PnL для {self.arb_pair}: {e}")
            return None

    async def _check_gt_balance(self, orders_recorder):
        """
        Пополнение GT, если его доля < 0.5% от спот-USDT.
        Сумма пополнения:
          - 0.5% от депозита, если ≥ 3 USDT,
          - иначе — ровно 3 USDT (минимум Gate.io).
        """
        try:
            print("Проверим баланс GT и пополним при необходимости")
            self.open_deal_logger.debug("_check_gt_balance запущен")
            params_spot = {"type": "spot"}
            balance_spot = await self.exchange.fetch_balance(params_spot)
            ticker = await self.exchange.fetch_ticker('GT/USDT')

            spot_usdt = Decimal(str(balance_spot.get("USDT", {}).get("free", "0")))
            gt_amount = Decimal(str(balance_spot.get("GT", {}).get("free", "0")))
            gt_price = Decimal(str(ticker.get('last', '0')))

            if not spot_usdt or not gt_price:
                self.open_deal_logger.warning("Пропуск GT-проверки: недостаточно данных.")
                return

            gt_cost_usdt = gt_amount * gt_price
            gt_ratio = gt_cost_usdt / spot_usdt

            GT_THRESHOLD = Decimal('0.005')  # 0.5%
            TOP_UP_RATIO = Decimal('0.005')  # 0.5%
            MIN_USDT_FOR_BUY = 3.0  # Минимум Gate.io для market buy

            if gt_ratio < GT_THRESHOLD:
                planned_usdt = float(spot_usdt * TOP_UP_RATIO)

                if planned_usdt >= MIN_USDT_FOR_BUY:
                    usdt_to_spend = round(planned_usdt, 2)
                    log_msg = f"Покупка GT на {usdt_to_spend:.2f} USDT (0.5% от депозита)"
                else:
                    usdt_to_spend = MIN_USDT_FOR_BUY
                    log_msg = f"0.5% < {MIN_USDT_FOR_BUY} USDT → покупка на минимум: {usdt_to_spend:.2f} USDT"

                self.open_deal_logger.info(
                    f"GT = {gt_ratio:.4%} (<0.7%). {log_msg} по цене {gt_price}"
                )

                gt_order_data = await self.exchange.createMarketOrder(
                    symbol='GT/USDT',
                    side='buy',
                    amount=usdt_to_spend  # Gate.io: market buy → amount = сумма в USDT
                )
                orders_recorder.record_gt_order_dump(order_data=gt_order_data)

            else:
                self.open_deal_logger.debug(f"GT в норме: {gt_ratio:.4%}")

        except (InvalidOperation, TypeError, KeyError, ValueError) as e:
            self.open_deal_logger.error(f"Ошибка в _check_gt_balance: {e}")
        except Exception as e:
            self.open_deal_logger.error(f"Неожиданная ошибка: {e}", exc_info=True)

    async def _close_spot(self):
        logger = self.open_deal_logger
        self.exchange.options['createMarketBuyOrderRequiresPrice'] = False
        print(f"Время запроса закрытия спот: {get_current_iso_time()}")

        sell_spot_balance = self.active_deals_dict[self.arb_pair]['available_for_sell_spot_balance']
        if not sell_spot_balance:
            print(f"[⚠️] Баланс для закрытия {self.spot_symbol} отсутствует — операция пропущена")
            sell_spot_balance = self.active_deals_dict[self.arb_pair]['deal_spot_amount']
        for attempt in range(1, self.max_order_attempt + 1):
            send_time = time.time()
            try:
                print(f"Объем закрытия спот {sell_spot_balance}")
                order_data = await self.exchange.create_order(
                    self.spot_symbol,
                    type='market',
                    side='sell',
                    amount=self.exchange.amount_to_precision(self.spot_symbol, sell_spot_balance))
                recv_time = time.time()

                status = order_data.get('status') or order_data.get('info', {}).get('finish_as')
                if status in ('closed', 'filled', 'finished'):
                    result = {
                        'order_data': order_data,
                        'spot_send_time': send_time,
                        'spot_recv_time': recv_time,
                        'duration': recv_time - send_time,
                        'attempts': attempt
                    }
                    if attempt > 1:
                        logger.info(f"✅ Спот закрыт с {attempt}-й попытки")
                    return result
                else:
                    raise Exception(f"Ордер не исполнен, статус: {status}")

            except Exception as e:
                recv_time = time.time()
                duration = recv_time - send_time
                error_msg = str(e)
                logger.warning(
                    f"Попытка {attempt}/{self.max_order_attempt} закрытия спота упала через {duration:.3f}с: {error_msg}"
                )
                if attempt < self.max_order_attempt:
                    await asyncio.sleep(self.order_attempt_interval)
                else:
                    logger.error(
                        f"❌ Все {self.max_order_attempt} попыток закрыть спот провалились. "
                        f"Последняя ошибка от биржи: {error_msg}"
                    )
                    raise CloseSpotOrderError(symbol=self.spot_symbol, message= f"Не удалось закрыть спот-ордер после всех попыток, duration: {duration}") from e

    async def _close_swap(self):
        logger = self.open_deal_logger
        print(f"Время запроса закрытия своп: {get_current_iso_time()}")
        swap_contracts = self.active_deals_dict[self.arb_pair]["deal_open_swap_contracts"]
        async def init_swap_settings(symbol):
            try:
                await self.exchange.set_margin_mode(
                    symbol=symbol,
                    marginMode='cross')
            except Exception as e:
                logger.warning(f"Не удалось установить margin mode для {symbol}: {e}")
            try:
                await self.exchange.set_leverage(1, symbol)
            except Exception as e:
                logger.warning(f"Не удалось установить leverage для {symbol}: {e}")

        await init_swap_settings(self.swap_symbol)

        for attempt in range(1, self.max_order_attempt + 1):
            send_time = time.time()
            try:
                order_data = await self.exchange.create_order(
                    self.swap_symbol,
                    type='market',
                    side='buy',
                    amount=swap_contracts,
                    params={"reduce_only": True}  # 👈 важно
                )
                recv_time = time.time()

                status = order_data.get('status') or order_data.get('info', {}).get('finish_as')
                if status in ('closed', 'filled', 'finished'):
                    result = {
                        'order_data': order_data,
                        'swap_send_time': send_time,
                        'swap_recv_time': recv_time,
                        'duration': recv_time - send_time,
                        'attempts': attempt
                    }
                    if attempt > 1:
                        logger.info(f"✅ Своп закрыт с {attempt}-й попытки")
                    return result
                else:
                    raise Exception(f"Ордер не исполнен, статус: {status}")

            except Exception as e:
                recv_time = time.time()
                duration = recv_time - send_time
                error_msg = str(e)
                logger.warning(
                    f"Попытка {attempt}/{self.max_order_attempt} закрытия свопа упала через {duration:.3f}с: {error_msg}"
                )
                if attempt < self.max_order_attempt:
                    await asyncio.sleep(self.order_attempt_interval)
                else:
                    logger.error(
                        f"❌ Все {self.max_order_attempt} попыток закрыть своп провалились. "
                        f"Последняя ошибка от биржи: {error_msg}"
                    )
                    raise CloseSwapOrderError(symbol=self.swap_symbol, message=f"Не удалось закрыть своп-ордер после всех попыток, {duration}") from e

    async def _compute_spot_fee_usdt(self, order: Dict[str, Any]) -> Decimal:
        """
        Возвращает комиссию для спота в USDT:
        - если указана в fees в USDT, используется напрямую;
        - если комиссия в монете (например, SOMI), конвертируется через average.
        - если комиссия в GT - получаем курс GT и перемножаем на комиссию
        """
        fees = order.get("fees") or []
        avg_price = Decimal(str(order.get("average", "0")))
        for f in fees:
            cur = f.get("currency")
            cost = Decimal(str(f.get("cost", "0")))
            if cur == "USDT":
                return cost
            elif cur == "GT":
                ticker = await self.exchange.fetch_ticker('GT/USDT')
                gt_usdt_price = Decimal(str(ticker['last']))
                if gt_usdt_price:
                    return cost * gt_usdt_price
                return Decimal("0.0")
            elif cur and cur != "USDT" and cost > 0 and avg_price > 0:
                return cost * avg_price  # пересчёт в USDT через цену сделки
        return Decimal("0.0")

    @staticmethod
    def _compute_swap_fee_usdt(order: Dict[str, Any]) -> Decimal:
        """
        Возвращает комиссию для свопа в USDT:
        - если в fees есть USDT — берём напрямую;
        - иначе вычисляется как filled * fill_price * taker_fee_rate (tkfr).
        """
        fees = order.get("fees") or []
        for f in fees:
            if f.get("currency") == "USDT":
                return Decimal(str(f.get("cost", "0")))

        try:
            tkfr = Decimal(str(order["info"].get("tkfr", "0")))
            filled = Decimal(str(order.get("filled", "0")))
            fill_price = Decimal(str(order["info"].get("fill_price", order.get("average", "0"))))
            return filled * fill_price * tkfr
        except (KeyError, InvalidOperation):
            return Decimal("0.0")

    async def _handle_partial_failures(self, spot_result, swap_result):
        """Обработка частичных сбоев"""
        spot_ok = not isinstance(spot_result, Exception)
        swap_ok = not isinstance(swap_result, Exception)
        logger = self.open_deal_logger

        if not spot_ok and swap_ok:
            logger.critical("🚨 Спот НЕ открыт! Аварийное закрытие своп-позиции...")
            try:
                await self._close_swap()
                logger.info("✅ Своп аварийно закрыт")
            except Exception as e:
                logger.error(f"❌ Не удалось закрыть своп: {e}")
        elif not swap_ok and spot_ok:
            logger.critical("🚨 Своп НЕ открыт! Аварийная продажа спота...")
            try:
                await self._close_spot()
                logger.info("✅ Спот аварийно закрыт")
            except Exception as e:
                logger.error(f"❌ Не удалось закрыть спот: {e}")
        else:
            logger.critical("🔥 Обе позиции НЕ открыты")

    # Метод запроса истории ордера по id
    async def fetch_spot_and_swap_order_by_id(self, spot_order_id: str, swap_order_id: str):
        """
        Одновременно запрашивает информацию об ордере на споте и свопе по их ID и символам.

        :param spot_order_id: ID ордера на споте
        :param swap_order_id: ID ордера на свопе
        :return: Кортеж (spot_order, swap_order), где каждый элемент — dict или None
        """

        async def _fetch_single(order_id: str, symbol: str, market_type: str):
            try:
                order = await self.exchange.fetch_order(order_id, symbol=symbol)
                print(f"🔍 [{market_type}] Найден ордер {order_id} для {symbol}:")
                # pprint(order)
                return order
            except Exception as e:
                print(f"❌ [{market_type}] Не удалось найти ордер {order_id} для {symbol}: {e}")
                return None

        # Запускаем оба запроса параллельно
        spot_task = _fetch_single(spot_order_id, self.spot_symbol, "SPOT")
        swap_task = _fetch_single(swap_order_id, self.swap_symbol, "SWAP")

        spot_order, swap_order = await asyncio.gather(spot_task, swap_task)

        return spot_order, swap_order



async def main():

    # Создадим для файла active_deals.json экземпляр файлового менеджера
    project_root: str = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    active_dir: str = os.path.join(project_root, "deals_log")
    os.makedirs(active_dir, exist_ok=True)
    active_path: str = os.path.join(active_dir, "active_deals.json")
    active_manager: JsonFileManager = JsonFileManager(active_path)

    async with ExchangeInstance(ccxt, 'gateio', update_interval=10, log=True) as exchange:
        await sync_time_with_exchange(exchange)
        # Тут очередность:
        # - Создание экземпляра класса (после должен быть бесконечный цикл и все дальнейшие действия в нем)
        # - эмуляция сигнального словаря - заполнение данных ценами
        # - отправляем словарь в экземпляр DealsManager через метод get_signal_data,
        #       в нем же выбор открытие/PNL/закрытие методами open_spot_swap_deal/get_PNL/close_spot_swap_deal соответственно.)
        # - В нашем эмуляторе открываем сделку методом open_spot_swap_deal

        # Это эмуляция работы верхнего класса - ArbitragePair.orderbook_compare.
        # 1) Эмулируем исходные данные
        arb_pair = 'SOMI/USDT_SOMI/USDT:USDT'
        spot_symbol = 'SOMI/USDT'
        swap_symbol = "SOMI/USDT:USDT"
        # params_spot = {"type": "spot"}
        # params_swap = {"type": "swap"}
        # balance_spot, balance_swap = asyncio.gather(await exchange.fetch_balance(params_spot), await exchange.fetch_balance(params_swap))
        # spot_usdt = float(balance_spot.get("USDT", {}).get('free', 0.0))
        # swap_usdt = float(balance_swap.get("USDT", {}).get('free', 0.0))

        max_order_attempt = 2
        active_deals_dict = active_manager.load()
        telegram_sender = TelegramMessageSender(bot_token_env="DEAL_BOT_TOKEN", chat_id_env="DEAL_CHAT_ID")
        print("1")

        # Типа получили усредненные цены
        try:
            # получаем актуальную цену для эмуляции пришедшей цены аск спот
            ticker = await exchange.fetch_ticker('SOMI/USDT')
            market_price = ticker['last'] or ticker['close']
            limit_price = market_price# * 1.01  # +3%
            print(limit_price)
        except Exception as e:
            print(f'Ошибка: {e}')

        # 2) - Создали экземпляр класса DealsManager - передаем константные значения экземпляра класса
        deal = DealsManager(
            arb_pair                = arb_pair,
            exchange                = exchange,
            spot_symbol             = spot_symbol,
            swap_symbol             = swap_symbol,
            max_order_attempt       = 2,
            order_attempt_interval  = 1.0,
            active_deals_dict       = active_deals_dict,
            telegram_sender         = telegram_sender,
            shared_values           = None
        )

        # Ниже код типа в бесконечном цикле, типа while True:
            # 3) Эмулируем создание словаря с начальными - сигнальными данными открытия позиций внутри бесконечного цикла.
            # Эти данные получаются в результате получения стаканов цен, вычисления средних цен из них и их обработки.
            # Они служат сигналом открытия и закрытия сделки и анализа текущей прибыли/убытка


        signal_deal_dict = {
            "arb_pair": 'SOMI/USDT_SOMI/USDT:USDT',
            "spot_symbol": 'SOMI/USDT',
            "swap_symbol": 'SOMI/USDT:USDT',
            "signal_spot_amount": 11,
            "signal_swap_contracts": 11,
            "signal_average_spot_ask": limit_price,
            "signal_average_spot_bid": None,
            "signal_average_swap_ask": None,
            "signal_average_swap_bid": None,
            "signal_open_ratio": 1.1,
            "signal_open_threshold_ratio": 0.5,  # ← добавьте
            "signal_close_ratio": 1.1,
            "signal_close_threshold_ratio": 0.3,  # ← ОБЯЗАТЕЛЬНОЕ поле
            "signal_max_open_ratio": 1,
            "signal_max_close_ratio": 1,
            "signal_min_open_ratio": -1,
            "signal_min_close_ratio": -1,
            "signal_delta_ratios": 2
        }

        # Данный словарь передается в класс DealsManager используя метод decision_open_close_trigger.
        # Его данные используются для открытия позиций и им обновляется выходной словарь открытых сделок active_deals_dict

        # Класс DealsManager инициирует экземпляр внутри ArbitragePairs.orderbook_compare до бесконечного цикла.
        # Данный метод после парсинга данных словаря запускает внутренний метод
        await deal.decision_trigger(signal_deal_dict=signal_deal_dict)



        # Получим данные активной сделки из словаря
        # Для закрытия сделки нужно:
        # available_for_sell_spot_balance - доступное количество spot монет на продажу - получаем из .fetch_balance
        # deal_swap_contracts - объем контрактов открытой swap-short сделки - получаем из .fetch_positions
        # То есть .open_spot_swap_deal должна возвращать кортеж (available_for_sell_spot_balance, deal_swap_contracts)
        # Лучший вариант - получать данные закрытия сделок только из словаря active_deals_dict

if __name__ == "__main__":
    asyncio.run(main())
