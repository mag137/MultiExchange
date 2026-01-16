
__version__ = "6.4"

import ccxt.pro as ccxt
import multiprocessing
import multiprocessing.queues
import multiprocessing.synchronize
import os
import asyncio
import random

from typing import List, Dict, Any, Optional
from decimal import Decimal, getcontext, ROUND_HALF_UP, InvalidOperation
from functools import partial

from modules.logger import LoggerFactory
from modules.exchange_instance import ExchangeInstance
from modules.process_manager import ProcessManager
from modules.task_manager import TaskManager
from modules.ORJSON_file_manager import JsonFileManager
from modules.telegram_bot_message_sender import TelegramMessageSender
from modules.arbtrage_deal_manager import ArbitrageDealManager
from modules.deals_manager import DealsManager
from modules.deal_opener import DealOpener
# mark - обработка исключений получения и расчета данных ордеров переносится в market_sort_data
from modules.exception_classes import ( ReconnectLimitExceededError,
                                        InvalidOrEmptyOrderBookError,
                                        BaseArbitrageCalcException )
from datetime import datetime, UTC

datetime.now(UTC)
from modules import (cprint, round_down, get_average_orderbook_price, sync_time_with_exchange,  # is_valid_price,
                     )

# Установить нужную точность
getcontext().prec = 16  # Общая точность вычислений
getcontext().rounding = ROUND_HALF_UP  # Стандартное округление

logger = LoggerFactory.get_logger("app." + __name__)


def _safe_decimal(value, default=Decimal('0')):
    """Безопасное преобразование в Decimal"""
    if value is None or value == '':
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return default


class ArbitragePairs:
    """Данный класс создает экземпляры арбитражных пар,
        подписывается на получение стаканов цен,
        получает средние цены и вызывает обработчик сделок.
        Также отправляет данные в таблицу спредов"""

    # Создадим для файла active_deals.json экземпляр файлового менеджера
    project_root: str = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    active_dir: str = os.path.join(project_root, "deals_log")
    os.makedirs(active_dir, exist_ok=True)
    active_path: str = os.path.join(active_dir, "active_deals.json")
    active_manager: JsonFileManager = JsonFileManager(active_path)
    active_deals_dict = active_manager.load()

    max_order_attempt = 2

    # Телеграмм-Бот
    telegram_sender = TelegramMessageSender(bot_token_env="DEAL_BOT_TOKEN", chat_id_env="DEAL_CHAT_ID")

    exchange_id = None
    exchange = None
    markets = None
    arb_pair_obj_dict = {}  # словарь с объектами-парами

    # Список отображаемых арбитражных пар в таблице
    arb_pair_list_for_tkinter = ['Это нулевая арбитражная пара для того чтобы пары нумерация пар начиналась с единицы']

    # Переменные очереди таблицы спредов
    all_queue_data_dict = {}  # Словарь в который собираются данные очереди со всех арбитражных пар
    tkinter_data = {}  # Словарь класса с данными для очереди в таблицу gui tkinter
    queue_pair_spread = asyncio.Queue()  # Общая очередь класса для общения между экземплярами и методами
    queue_spread_table = multiprocessing.Queue()  # Очередь для передачи данных для отображения в таблице

    # Балансы счетов
    spot_usdt = 0
    swap_usdt = 0

    # Расчетные средства на каждую сделку
    deal_spot_usdt = 0  #
    deal_swap_usdt = 0

    # Переменные контроля и статистики
    spot_orderbook_task_count = 0
    swap_orderbook_task_count = 0
    spot_orderbook_get_data_count = 0
    swap_orderbook_get_data_count = 0

    open_threshold_ratio = 2
    close_threshold_ratio = 2


    def __init__(self, arb_pair: List[str]):
        self.logger = logger
        self.exchange = self.__class__.exchange
        self.arb_pair = str(arb_pair)  # Упрощаем - заменяем этим аргументом self.task_name и self.deal_pair
        self.queue_orderbook = asyncio.Queue()  # Локальная очередь для отправки стаканов
        self.queue_pair_spread = self.__class__.queue_pair_spread  # Очередь отправки данных в таблицу спредов
        self.deal_in_active = False  # Флаг наличия сделки по данной арбитражной паре
        self.spread_queue_dict = {}  # Словарь хранения данных для таблицы спредов

        # Получение данных спот/своп безопасно
        spot_data = self.exchange.spot_swap_pair_data_dict.get(self.arb_pair, {}).get('spot', {})
        swap_data = self.exchange.spot_swap_pair_data_dict.get(self.arb_pair, {}).get('swap', {})

        self.spot_symbol = spot_data.get('symbol', None)
        self.swap_symbol = swap_data.get('symbol', None)
        self.task_name_symbol_1 = f"orderbook_{self.spot_symbol}"  # Имена задач для получения стаканов
        self.task_name_symbol_2 = f"orderbook_{self.swap_symbol}"  # Имена задач для получения стаканов

        # Безопасные комиссии
        self.spot_fee = _safe_decimal(spot_data.get('taker')) * Decimal('100')
        self.swap_fee = _safe_decimal(swap_data.get('info', {}).get('taker_fee_rate')) * Decimal('100')
        self.commission = self.spot_fee + self.swap_fee

        # Инициализация переменных обработки стаканов цен
        self.spot_average_ask = None
        self.swap_average_ask = None
        self.spot_average_bid = None
        self.swap_average_bid = None
        self.delta_ratios = None
        self.open_ratio = None
        self.close_ratio = None
        self.max_open_ratio = Decimal('-Infinity')
        self.max_close_ratio = Decimal('-Infinity')
        self.min_open_ratio = Decimal('Infinity')
        self.min_close_ratio = Decimal('Infinity')

        # Последние значения ратио
        self.last_open_ratio = None
        self.last_close_ratio = None

        # Расчетные данные для открытия арбитражной сделки
        self.orders_data = {}  # Словарь с расчетными данными amount для открытия ордеров
        self.spot_amount = None
        self.swap_contracts = None
        self.contract_size = None
        self.spot_cost = None
        self.spot_min_amt = None
        self.spot_min_cost = None
        self.swap_min_amt = None
        self.swap_min_cost = None

    # При арбитражной ситуации метод отправляет данные арбитражной пары в очередь
    async def orderbooks_compare(self):
        """
        Основной метод работы с каждой отдельной арбитражной парой.
        Теперь с точной классификацией причин отмены:
        - InsufficientOrderBookError — малый объем стакана
        - InsufficientFundsError — недостаточно средств для минимального объема
        """

        deal = DealsManager(
            arb_pair=self.arb_pair,
            exchange=self.exchange,
            spot_symbol=self.spot_symbol,
            swap_symbol=self.swap_symbol,
            max_order_attempt=2,
            order_attempt_interval=1.0,
            active_deals_dict=self.active_deals_dict,
            telegram_sender=self.__class__.telegram_sender
        )

        deal_opener = DealOpener(
            arb_pair=self.arb_pair,
            exchange=self.exchange,
            spot_symbol=self.spot_symbol,
            swap_symbol=self.swap_symbol,
            max_order_attempt=2,
            order_attempt_interval=0.5,
            active_deals_dict=self.active_deals_dict,
            telegram_sender=self.__class__.telegram_sender
        )

        pair_data = self.exchange.spot_swap_pair_data_dict.get(self.arb_pair)
        if not isinstance(pair_data, dict) or len(pair_data) != 2:
            raise ValueError(f"spot_swap_pair_data_dict[{self.arb_pair}] должен быть словарем из двух торговых пар.")

        cprint.info_b(f'[orderbooks_compare] task_name: {self.arb_pair}')

        # Создаем задачи подписки на стаканы
        self.__class__.task_manager.add_task(name=self.task_name_symbol_1, coro_func=partial(self.watch_orderbook, self.spot_symbol))
        self.__class__.task_manager.add_task(name=self.task_name_symbol_2, coro_func=partial(self.watch_orderbook, self.swap_symbol))

        try:
            # Запуск бесконечного цикла получения стаканов арбитражной пары...
            while True:
                # Ждём наличия данных по счетам
                while not type(self).deal_spot_usdt or not type(self).deal_swap_usdt:
                    await asyncio.sleep(1)

                # Определим предыдущие значения ратио открытия/закрытия
                self.last_open_ratio = self.open_ratio
                self.last_close_ratio = self.close_ratio

                # Определим минимальный объем между спот/своп - он будет одинаковым объемом ордеров
                min_usdt = min(type(self).deal_spot_usdt, type(self).deal_swap_usdt)

                # Пробуем получить данные по стаканам цен из специальной очереди ордербуков
                symbol, orderbook = await self.queue_orderbook.get()

                # Проверка данных из очереди
                if symbol is None:
                    cprint.error_w(f"[orderbooks_compare][{self.arb_pair}] Очередь вернула None, пауза 1 сек")
                    await asyncio.sleep(1)
                    continue

                # Попытка вычислить средние цены. Если стаканы
                if symbol == self.spot_symbol:
                    self.spot_average_ask = get_average_orderbook_price(orderbook['asks'], min_usdt, self.spot_symbol, True, self.exchange_id)
                    self.spot_average_bid = get_average_orderbook_price(orderbook['bids'], min_usdt, self.spot_symbol, False, self.exchange_id)
                elif symbol == self.swap_symbol:
                    self.swap_average_ask = get_average_orderbook_price(orderbook['asks'], min_usdt, self.swap_symbol, True, self.exchange_id)
                    self.swap_average_bid = get_average_orderbook_price(orderbook['bids'], min_usdt, self.swap_symbol, False, self.exchange_id)

                # Проверяем наличие всех средних цен
                if any(v is None for v in [self.spot_average_ask, self.spot_average_bid, self.swap_average_ask, self.swap_average_bid]):
                    await asyncio.sleep(5)
                    continue

                # Расчёт дельт и ratio
                if self.spot_average_ask != int(0) and self.swap_average_ask != int(0):
                    self.open_ratio         = round_down(100 * (self.swap_average_bid - self.spot_average_ask) / self.spot_average_ask, 2)
                    self.close_ratio        = round_down(100 * (self.spot_average_bid - self.swap_average_ask) / self.swap_average_ask, 2)
                    self.max_open_ratio     = max(self.max_open_ratio, self.open_ratio)
                    self.max_close_ratio    = max(self.max_close_ratio, self.close_ratio)
                    self.min_open_ratio     = min(self.min_open_ratio, self.open_ratio)
                    self.min_close_ratio    = min(self.min_close_ratio, self.close_ratio)
                    self.delta_ratios       = round_down(self.max_open_ratio + self.max_close_ratio, 2)

                # Расчёт объёмов сделки
                self.orders_data    = self.exchange.deal_amounts_calc_func(deal_pair=self.arb_pair, spot_deal_usdt=type(self).deal_spot_usdt, swap_deal_usdt=type(self).deal_swap_usdt, price_spot=self.spot_average_ask, price_swap=self.swap_average_bid)
                self.spot_amount    = Decimal(self.orders_data.get('spot_amount'))
                self.swap_contracts = Decimal(self.orders_data.get('swap_contracts'))
                self.contract_size  = Decimal(self.orders_data.get('contract_size'))
                # Получаем минимумы объемов сделок
                self.spot_min_amt   = Decimal(self.orders_data.get('spot_min_amt', '0'))
                self.spot_min_cost  = Decimal(self.orders_data.get('spot_min_cost', '0'))
                self.swap_min_amt   = Decimal(self.orders_data.get('swap_min_amt', '0'))
                self.swap_min_cost  = Decimal(self.orders_data.get('swap_min_cost', '0'))

                # Отправим данные в таблицу спредов
                await self._to_tkinter_spread_table()

                # Проверим результаты вычислений объемов перед запуском менеджера сделок
                values = {'spot_average_ask': self.spot_average_ask, 'spot_average_bid': self.spot_average_bid, 'swap_average_ask': self.swap_average_ask, 'swap_average_bid': self.swap_average_bid, }

                if any(v is None for v in values.values()):
                    cprint.warning_r(f"[orderbooks_compare][{self.arb_pair}] Пропуск run_management — есть None в средних ценах: {values}")
                    await asyncio.sleep(1)
                    continue

                # Заполним сигнальный словарь мгновенными значениями параметров
                signal_deal_dict = {
                    "arb_pair": self.arb_pair,
                    "spot_symbol": self.spot_symbol,
                    "swap_symbol": self.swap_symbol,
                    "signal_spot_amount": self.spot_amount,
                    "signal_swap_contracts": self.swap_contracts,
                    "signal_average_spot_ask": self.spot_average_ask,
                    "signal_average_spot_bid": self.spot_average_bid,
                    "signal_average_swap_ask": self.swap_average_ask,
                    "signal_average_swap_bid": self.swap_average_ask,
                    "signal_open_ratio": self.open_ratio,
                    "signal_open_threshold_ratio": self.open_threshold_ratio,
                    "signal_close_ratio": self.close_ratio,
                    "signal_close_threshold_ratio": self.close_threshold_ratio,
                    "signal_max_open_ratio": self.max_open_ratio,
                    "signal_max_close_ratio": self.max_close_ratio,
                    "signal_min_open_ratio": self.min_open_ratio,
                    "signal_min_close_ratio": self.min_close_ratio,
                    "signal_delta_ratios": self.delta_ratios
                }

                # Данный словарь передается в класс DealOpener используя метод.
                # Его данные используются для открытия позиций и им обновляется выходной словарь открытых сделок active_deals_dict

                # Условия открытия позиций
                if self.arb_pair not in self.active_deals_dict \
                        and len(self.active_deals_dict) < self.max_active_deals \
                        and  self.last_open_ratio \
                        and self.last_open_ratio > 2 and self.open_ratio > 2:
                    await deal_opener.open_deal(signal_deal_dict=signal_deal_dict)


        except asyncio.CancelledError:
            cprint.success_w(f"[orderbooks_compare] Задача отменена: {self.arb_pair}")
            raise

        # Здесь обрабатываются исключения недостатка средств
        # Проверка минимального объема осуществляется в market_sort_data подъемом сюда исключений.
        # Ловим исключения ошибочных данных стакана полученные при расчете средних цен - get_average_orderbook_price
        # через родительский класс BaseArbitrageCalcException
        except BaseArbitrageCalcException as e:
            cprint.warning_r(f"Арбитражная ошибка: {e}")
            cprint.warning_r(f"Залогировано в: {e.logged_file}")
            # НЕТ continue — задача завершится

        # ...проверка деления на ноль
        except (ZeroDivisionError, TypeError) as e:
            cprint.error_b(f"[{type(e).__name__}] {e}")

        except Exception as e:
            cprint.error_b(f"[orderbooks_compare] Неожиданная ошибка для {self.arb_pair}: {e}")
            logger.exception(e)

        finally:
            # Всегда отменяем задачи стаканов и удаляем из GUI
            await type(self).task_manager.cancel_task(self.task_name_symbol_1)
            await type(self).task_manager.cancel_task(self.task_name_symbol_2)
            if self.arb_pair in type(self).arb_pair_list_for_tkinter:
                await self._delete_spread_tkinter_pair()

    # Метод удаления пары из таблицы спредов
    async def _delete_spread_tkinter_pair(self):
        cprint.error_w(f"{self.arb_pair} - Метод удаления!!! *************************************************************************************************************")
        await self.__class__.queue_pair_spread.put({'delete': self.arb_pair})

    # Метод запроса цен заданного символа
    async def watch_orderbook(self):
        """
        Асинхронно следит за стаканом указанного символа.
        При превышении числа попыток — завершает задачу.
        """

        max_reconnect_attempts = 5  # лимит переподключений
        reconnect_attempts = 0
        counted_flag_spot = False
        counted_flag_swap = False

        last_spot_orderbook = {}
        last_swap_orderbook = {}
        old_spot_orderbook = {}
        old_swap_orderbook = {}

        try:
            # Увеличиваем счётчик активных задач

            type(self).swap_orderbook_task_count += 1
            type(self).spot_orderbook_task_count += 1

            while True:
                try:
                    # Проверим наличие полученных балансов перед запросом стаканов
                    if not self.__class__.spot_usdt or not self.__class__.swap_usdt:
                        await asyncio.sleep(3)
                        continue

                    orderbook = await self.exchange.watchOrderBookForSymbols([self.spot_symbol, self.swap_symbol], params={'depth': 'books5'})
                    print(orderbook)
                    # print(orderbook)
                    # Проверим пришедшие данные стаканов - если левые, то выбрасываем исключение
                    if not isinstance(orderbook, dict) or len(orderbook) == 0 or 'asks' not in orderbook or 'bids' not in orderbook:
                        raise InvalidOrEmptyOrderBookError(exchange_id=self.exchange.id, orderbook_data=orderbook)

                    # сбрасываем счётчик после успешного получения
                    reconnect_attempts = 0

                    if not counted_flag_swap and ':' in orderbook['symbol']:
                        type(self).swap_orderbook_get_data_count += 1
                        counted_flag_swap = True
                    if not counted_flag_spot and ':' not in orderbook['symbol']:
                        type(self).spot_orderbook_get_data_count += 1
                        counted_flag_spot = True

                    if orderbook['symbol'] == self.spot_symbol:
                        last_spot_orderbook = orderbook
                    if orderbook['symbol'] == self.swap_symbol:
                        last_swap_orderbook = orderbook


                    # обработаем полученные стаканы - высчитаем среднюю цену
                    if counted_flag_spot and counted_flag_swap:
                        if old_spot_orderbook != last_spot_orderbook:
                            self.spot_average_ask = get_average_orderbook_price(last_spot_orderbook['asks'], 10, self.spot_symbol, True, self.exchange_id)
                            self.spot_average_bid = get_average_orderbook_price(last_spot_orderbook['bids'], 10, self.spot_symbol, False, self.exchange_id)
                            old_spot_orderbook = last_spot_orderbook
                        elif old_swap_orderbook != last_swap_orderbook:
                            self.swap_average_ask = get_average_orderbook_price(last_swap_orderbook['asks'], 10, self.swap_symbol, True, self.exchange_id)
                            self.swap_average_bid = get_average_orderbook_price(last_swap_orderbook['bids'], 10, self.swap_symbol, False, self.exchange_id)
                            old_swap_orderbook = last_swap_orderbook

                    else:
                        continue



                    await self.queue_orderbook.put((orderbook['symbol'], orderbook))
                    await asyncio.sleep(0.01)

                except Exception as e:
                    error_str = str(e)
                    # Список признаков "временных" ошибок, при которых можно переподключиться
                    transient_errors = ['1000', 'closed by remote server', 'Cannot write to closing transport',
                                        'Connection closed', 'WebSocket is already closing', 'Transport closed',
                                        'broken pipe', 'reset by peer', ]

                    if any(phrase in error_str for phrase in transient_errors):
                        reconnect_attempts += 1
                        if reconnect_attempts > max_reconnect_attempts:
                            cprint.error_w(
                                f"[{orderbook['symbol']}] ❌ Превышено число переподключений ({max_reconnect_attempts}), закрываем задачу.")
                            # ...поднимаем исключение наверх
                            raise ReconnectLimitExceededError(exchange_id=self.exchange.id, symbol=orderbook['symbol'], attempts=reconnect_attempts)

                        cprint.warning_b(f"[{orderbook['symbol']}] Временная ошибка соединения: {e}. "
                                         f"Таймаут {4 + (2 ** reconnect_attempts)} сек, попытка {reconnect_attempts}/{max_reconnect_attempts}...")
                        await asyncio.sleep(4 + (2 ** reconnect_attempts))
                        continue

                    # Любая другая ошибка — критическая
                    cprint.error_w(f"Критическая ошибка watchOrderBook [{orderbook['symbol']}]: {e}")
                    break

        except asyncio.CancelledError:
            current_task = asyncio.current_task()
            cancel_source = getattr(current_task, "_cancel_context", "неизвестно")
            cprint.success_w(f"[watch_orderbook] Задача отменена: {orderbook['symbol']}, источник: {cancel_source}")
            raise

        except Exception as e:
            cprint.error_b(f"[watch_orderbook][FATAL] Непредвиденная ошибка для {orderbook['symbol']}: {e}")
            raise

        finally:
            try:
                type(self).swap_orderbook_task_count -= 1
                type(self).spot_orderbook_task_count -= 1
                    # cprint.warning_b(f"[watch_orderbook][finally] Счётчик задач обновлён: {symbol}")
            except Exception as e:
                cprint.error_b(f"[watch_orderbook][finally] Ошибка при уменьшении счётчика для {orderbook['symbol']}: {e}")

    @classmethod
    async def watch_balance(cls):
        try:
            while True:
                # balance = await exchange.watch_balance(params)
                balance = await cls.exchange.watch_balance()
                print("Баланс обновлён:", {k: v for k, v in balance.get('total', {}).items() if v != 0})
                print(balance.get('USDT').get('free'))
                cls.spot_usdt = cls.swap_usdt = balance.get('USDT').get('free') * 0.48
        except KeyboardInterrupt:
            print("Остановка по запросу пользователя")
        except Exception as e:
            print(f"Критическая ошибка: {e}")

    # *******************************************************************************************************************************************************************
    #  Метод, отвечающий за отправку данных в таблицу спредов
    async def _to_tkinter_spread_table(self):
        # Ниже - код отвечающий за отправку данных в таблицу спредов
        # Условие-сигнал для отображения в таблице - превысили порог...
        if self.max_open_ratio > 0.5:  # and self.max_close_ratio > 0:
            # cprint.info_b(f'{task_name} превысили порог - отслеживаем в таблице')

            new_data_queue_dict = {
                'task_name': self.arb_pair,
                'symbol_1': self.spot_symbol,
                'symbol_2': self.swap_symbol,
                'open_ratio': self.open_ratio,
                'close_ratio': self.close_ratio,
                'delta_ratios': self.delta_ratios,
                'max_open_ratio': self.max_open_ratio,
                'max_close_ratio': self.max_close_ratio,
                'commission': self.commission,
                'spot_fee': self.spot_fee,
                'swap_fee': self.swap_fee}

            outer_data = {
                0: {'text': self.arb_pair.split('/')[0]},
                1: {'text': self.contract_size},
                2: {'text': self.max_open_ratio},
                3: {'text': self.open_ratio},
                4: {'text': self.max_close_ratio},
                5: {'text': self.close_ratio},
                6: {'text': self.delta_ratios},
                7: {'text': self.commission},
                8: {'text': self.spot_fee},
                9: {'text': self.swap_fee}}

            if outer_data != self.spread_queue_dict:
                self.spread_queue_dict.clear()
                self.spread_queue_dict.update(outer_data)
                cprint.info_b(f'{self.arb_pair} отправка данных в таблицу')
                await self.queue_pair_spread.put(outer_data)

    @classmethod
    def rebuild_tkinter_data(cls) -> dict:
        """
        Пересобирает словарь данных для Tkinter таблицы из списка арбитражных пар.
        Используется при изменении состава списка отображаемых арбитражных пар arb_pair_list - при удалении элемента списка
        пересоздаются только строки с данными, заголовки остаются изначальные
        """
        for i in range(1, len(cls.arb_pair_list_for_tkinter)):
            pair = cls.arb_pair_list_for_tkinter[i]
            cls.tkinter_data[i] = {0: {'text': pair}}
            for column in range(1, len(cls.all_queue_data_dict[pair])):
                cls.tkinter_data[i][column] = {'text': cls.all_queue_data_dict[pair][column]['text']}
        # cprint.info(f"INFO: 'rebuild_tkinter_data': Пересчет списка отображаемых пар: {cls.arb_pair_list}")
        # pprint(cls.tkinter_data)
        return cls.tkinter_data

    @classmethod
    async def arb_data_to_tkgrid(cls):
        try:
            last_sent_time = 0  # время последней отправки данных
            while True:
                """
                Так как этот метод бесконечный и асинхронный, значит сюда можно засунуть опрос межпроцессных переменных
                """
                try:
                    # print(f"cls.arb_pair_list: {cls.arb_pair_list}")
                    headers = ['Header 1', 'Header 2', 'Header 3']
                    test_data = {'header': {i: {'text': headers[i]} for i in range(len(headers))},
                                 1: {0: {'text': f'Row 1, Col 1: {random.randint(1, 100)}'},
                                     1: {'text': f'Row 1, Col 2: {random.randint(1, 100)}'},
                                     2: {'text': f'Row 1, Col 3: {random.randint(1, 100)}'}},
                                 2: {0: {'text': f'Row 2, Col 1: {random.randint(1, 100)}'},
                                     1: {'text': f'Row 2, Col 2: {random.randint(1, 100)}'},
                                     2: {'text': f'Row 2, Col 3: {random.randint(1, 100)}'}}}
                    # Здесь заполняется содержимое заголовков
                    cls.tkinter_data = {
                        'header': {0: {'text': 'Coin'}, 1: {'text': 'ContractSize'},
                                    2: {'text': 'Мax OpRatio'},3: {'text': 'OpRatio'},
                                    4: {'text': 'Мax ClRatio'},5: {'text': 'ClRatio %'},
                                    6: {'text': 'Delta'}, 7: {'text': 'Fee %'},
                                   8: {'text': 'SpotFee %'}, 9: {'text': 'SwapFee %'}, }}

                    data = await cls.queue_pair_spread.get()
                    if isinstance(data, dict):
                        # Если пришел словарь с ключом delete
                        if 'delete' in data:
                            cprint.info_w(f"[arbitrage_pairs][arb_data_to_grid] пришел словарь с ключом delete {data}")
                            deleted = data['delete']
                            if deleted in cls.arb_pair_list_for_tkinter:
                                cls.arb_pair_list_for_tkinter.remove(deleted)
                            cls.all_queue_data_dict.pop(deleted, None)
                            cls.tkinter_data = cls.rebuild_tkinter_data()
                            continue

                        # Если пришли новые данные.
                        if 0 in data:
                            # Получаем арбитражную пару из данных
                            arb_pair = data[0]['text']
                            # Если пара отсутствует в списке - добавляем ее в список в конец
                            if arb_pair not in cls.arb_pair_list_for_tkinter:
                                cls.arb_pair_list_for_tkinter.append(arb_pair)
                            # Собираем список пар приходящих в очереди в словарь.
                            # Добавляем в словарь - словарь {'имя арбитражной пары':{словарь с данными из очереди}}
                            # Этот словарь в дальнейшем будем использовать для формирования из него данных для отправки таблицы.
                            cls.all_queue_data_dict[data[0]['text']] = data

                        # Получаем текущее время
                        current_time = asyncio.get_event_loop().time()

                        # Отправляем данные в очередь, если прошло больше 0.5 секунды с последней отправки
                        if current_time - last_sent_time >= 0.2:
                            cls.tkinter_data = cls.rebuild_tkinter_data()  # Обновляем данные для отправки
                            cls.queue_spread_table.put(cls.tkinter_data)
                            last_sent_time = current_time  # Обновляем время последней отправки  # Это мониторинг очереди отправки в greed  # cprint.info_w(f"отправляем в GRID {cls.tkinter_data}")

                    await asyncio.sleep(0)

                except Exception as e:
                    cprint.error_b(f"Error: arb_data_to_tkgrid: {e}")
        except asyncio.CancelledError:
            cprint.info_w("Задача arb_data_to_tkgrid отменена")
            raise

    @classmethod
    def create_object(cls, pair: List[str]):  # Возвращает созданный объект
        return cls(pair)

    @classmethod
    async def start_all_orderbooks_compares_tasks(cls):
        """
        Только запускает задачи для каждой арбитражной пары.
        Задача arb_data_to_tkgrid_task уже добавлена ранее.
        """
        count = 0
        for arb_pair in list(cls.exchange.spot_swap_pair_data_dict)[:25]:
            count +=1
            obj = cls.create_object(arb_pair)
            cls.arb_pair_obj_dict[arb_pair] = obj
            obj.task_name = arb_pair
            print(count)
            cls.task_manager.add_task(name=arb_pair, coro_func=obj.orderbooks_compare)
            await asyncio.sleep(0.1)

        cprint.info(
            f"Запущено {len(cls.exchange.spot_swap_pair_data_dict)} задач арбитража для биржи '{cls.exchange_id}'")

    @classmethod
    async def run_analytic_process(cls, exchange_id: str,
                                   spread_table_queue_data: multiprocessing.Queue,
                                   shared_values: Dict,
                                   deal_table_queue_data: Optional[multiprocessing.Queue]) -> Any:
        cls.exchange_id = exchange_id
        cls.queue_spread_table = spread_table_queue_data
        cls.queue_deal_table = deal_table_queue_data
        cls.shared_values = shared_values
        cls.task_manager = TaskManager(shared=shared_values)

        async with (ExchangeInstance(ccxt, cls.exchange_id, log=True) as cls.exchange):
            success = await sync_time_with_exchange(cls.exchange)
            if not success:
                cprint.error_b("Не удалось синхронизировать время с биржей.")
                return

            # ✅ Добавляем задачи
            cls.task_manager.add_task(name="start_all_compares",        coro_func=cls.start_all_orderbooks_compares_tasks)
            cls.task_manager.add_task(name="watch_balance",             coro_func=cls.watch_balance)
            cls.task_manager.add_task(name="arb_data_to_tkgrid_task",   coro_func=cls.arb_data_to_tkgrid)
            cls.task_manager.add_task(name="monitor_event_loop_tasks",  coro_func=partial(monitor_event_loop_tasks, interval=10))

            # Инициализация класса ArbitrageDealManager
            ArbitrageDealManager.open_deal_enable = asyncio.Event()
            ArbitrageDealManager.open_deal_enable.set()  # разрешить открытие по умолчанию
            ArbitrageDealManager.exchange = cls.exchange
            ArbitrageDealManager.shared_values = cls.shared_values
            ArbitrageDealManager.max_active_deals = ProcessManager.read_decimal(
                cls.shared_values.get("max_active_deals", Decimal('0')))
            ArbitrageDealManager.telegram_sender = TelegramMessageSender(bot_token_env="DEAL_BOT_TOKEN",
                                                                         chat_id_env="DEAL_CHAT_ID")
            await ArbitrageDealManager.telegram_sender.send_numbered_message("Арбитражный робот запущен!")
            cls.telegram_sender = ArbitrageDealManager.telegram_sender
            # При старте процесса необходимо проверить наличие открытых сделок с предыдущей сессии из файла active_deals.json
            try:
                while True:
                    # Проверка изменения балансов для отображения в шапке окна - обновляется раз в секунду
                    if cls.spot_usdt != getattr(cls.shared_values.get("spot_usdt"), "value", 0) \
                            or cls.swap_usdt != getattr(cls.shared_values.get("swap_usdt"), "value", 0):
                        cls.spot_usdt = ProcessManager.read_decimal(cls.shared_values.get("spot_usdt", Decimal('0')))
                        cls.deal_spot_usdt = ProcessManager.read_decimal(
                            cls.shared_values.get("deal_spot_usdt", Decimal('0')))
                        cls.swap_usdt = ProcessManager.read_decimal(cls.shared_values.get("swap_usdt", Decimal('0')))
                        cls.deal_swap_usdt = ProcessManager.read_decimal(
                            cls.shared_values.get("deal_swap_usdt", Decimal('0')))
                        cls.max_active_deals = ProcessManager.read_decimal(
                            cls.shared_values.get("max_active_deals", Decimal('0')))
                        cls.active_deals_count = ProcessManager.read_decimal(
                            cls.shared_values.get("active_deals_count", Decimal('0')))
                        cls.queue_spread_table.put({'config': {
                            'title': f"Exchange: {cls.exchange.id}   Free spot: {Decimal(cls.spot_usdt):.3f} ({Decimal(cls.deal_spot_usdt):.3f}) USDT   Free swap: {Decimal(cls.swap_usdt):.3f} ({Decimal(cls.deal_swap_usdt):.3f}) USDT"}})
                        cls.queue_deal_table.put({'config': {
                            'title': f"Активные сделки, Free spot: {Decimal(cls.spot_usdt):.3f} ({Decimal(cls.deal_spot_usdt):.3f}) USDT   Free swap: {Decimal(cls.swap_usdt):.3f} ({Decimal(cls.deal_swap_usdt):.3f}) USDT"}})  # cprint.success_w(f"{cls.spot_usdt} {cls.swap_usdt}")
                    if shared_values.get("shutdown", False) and shared_values["shutdown"].value:
                        cprint.info_w("[run_analytic_process] shutdown flag detected")
                        break
                    await asyncio.sleep(1)
            finally:
                await cls.task_manager.cancel_all()


# Синхронная обертка над точкой входа в ArbitragePairs - ожидает получения exchange_id из shared_values
def run_analytic_process_wrapper(*,
                                 spread_table_queue_data: "multiprocessing.queues.Queue",
                                 shared_values: Dict,
                                 deal_table_queue_data: Optional["multiprocessing.queues.Queue"] = None
                                 ) -> None:
    exchange_id = False
    logger.debug("'run_analytic_process': запуск")
    while exchange_id == False:
        print(f"Тип очереди: {type(spread_table_queue_data)}")
        if shared_values.get("exchange_id", False):
            exchange_id = ProcessManager.read_str(shared_values.get('exchange_id', False))
            cprint.info_b(f"[arbitrage_pairs] Получено имя биржи: {exchange_id}")
            asyncio.run(ArbitragePairs.run_analytic_process(exchange_id,
                                                            spread_table_queue_data,
                                                            shared_values,
                                                            deal_table_queue_data))


async def monitor_event_loop_tasks(interval: float = 20.0, loop: Optional[asyncio.AbstractEventLoop] = None):
    """
    Периодически отслеживает активные задачи в цикле событий и выводит их количество.

    :param interval: Интервал между проверками (в секундах)
    :param loop: Цикл событий (по умолчанию текущий)
    """
    loop = loop or asyncio.get_running_loop()
    previous_count = -1  # Для отслеживания изменений

    while True:
        # Получаем текущие задачи
        try:
            if hasattr(asyncio, 'all_tasks'):
                current_tasks = asyncio.all_tasks(loop)
            else:
                # Python 3.12+
                current_tasks = {task for task in asyncio.all_tasks() if task.get_loop() is loop}
        except Exception:
            # На всякий случай
            current_tasks = set()

        active_count = len([t for t in current_tasks if not t.done()])

        # Печатаем только если количество изменилось
        if active_count != previous_count:
            print(f"  • Active tasks: {active_count}")
            previous_count = active_count

        await asyncio.sleep(interval)



"""
При открытии сделки мы записываем в словарь файла active_deals.json 
данные открытия сделки:
Signal - данные, полученные из расчетов и от биржи.
Deal - данные вернувшиеся от биржи после выполнения торговых операций
{
arb_pair,               # Название арбитражной пары
spot_symbol,            # Название спот символа
swap_symbol,            # Название своп символа
spot_opening balance    # Баланс спот перед началом сделки
swap_opening balance    # Баланс своп перед началом сделки
swap_contract_size,     # Размер своп контракта в количестве монет

signal_time,            # Время получения сигнала на открытие сделки
signal_spot_amount,     # Рассчитанное количество монет для открытия сделки
sigal_swap_contracts,   # Рассчитанное количество контрактов своп, уравновешенное со спот
signal_spot_price,      # Сигнальная спот цена для открытия спот сделки
signal_swap_price,      # Сигнальная своп цена для открытия своп сделки
signal_open_ratio       # Процентная разница между ask spot и bid swap
signal_spot_fee,        # Полученный от биржи размер комиссии спот
signal_swap_fee,        # Полученный от биржи размер комиссии своп

deal_spot_time,         #
deal_swap_time,         #
deal_spot_amount        #
deal_swap_contracts     #
deal_open_spot_price    #
deal_open_swap_price    #
deal_open_ratio
deal_spot_time,         # Время спот сделки, возвращенное ордером
deal_swap_time,         # Время своп сделки, возвращенное ордером
deal_spot_amount        # Объем спот сделки, возвращенный ордером
deal_swap_contracts     # Количество контрактов своп сделки, возвращенное ордером
deal_open_spot_price    # Средняя цена покупки спот, возвращенная ордером
deal_open_swap_price    # Средняя цена шот своп, возвращенная ордером
deal_open_ratio         # Вычисленная процентная разница возвращенных ордерами цен ask spot и bid swap
deal_spot_fee,          # Размер комиссии спот, возвращенный ордером
deal_swap_fee,          # Размер комиссии своп, возвращенный ордером
}
Данный словарь после подтверждения открытия сделок сохраняется в файл active_deals.json в виде:
{
    deal_pair:{
                open_deal_data
                }
}
"""