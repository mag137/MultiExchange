__version__ = "0.1"


import logging
import os
import asyncio
import time

from decimal import Decimal, InvalidOperation


from modules.logger import LoggerFactory
from modules.ORJSON_file_manager import JsonFileManager
from modules.utils import safe_decimal
# mark - обработка исключений получения и расчета данных ордеров переносится в market_sort_data
from modules.exception_classes import ( OpenSpotOrderError,
                                        OpenSwapOrderError,
                                        CloseSpotOrderError,
                                        CloseSwapOrderError,
                                        DealOpenError
                                        )
from pprint import pprint
from modules import (cprint,  # is_valid_price,
                     )

class ArbitrageDealManager:
    # Пробрасываем экземпляр класса
    exchange = None
    # Пробрасываем межпроцессные переменные
    shared_values = None
    # Инициализируем событие блокировки арбитражных пар
    open_deal_enable = None  # Событие-блокировщик открытия новых сделок инициализируется в ArbitragePairs.run_analitic_process
    # Создадим путь для файла-хранителя текущих открытых сделок и объект класса сопровождения файла
    active_deals_file_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'deals_log', 'active_deals.json'))
    active_deals_file_manager = JsonFileManager(active_deals_file_path)
    # Словарь хранитель данных по открытым сделкам. Его длина - количество открытых сделок
    active_deals_dict = active_deals_file_manager.load()
    # Максимальное количество одновременных сделок
    max_active_deals = None
    # Логгер для записи результатов сделки
    deal_logger = LoggerFactory.get_logger(name="deal", log_filename="deal.log", level=logging.DEBUG,
                                           split_levels=False, use_timed_rotating=True, use_dated_folder=True,
                                           add_date_to_filename=False, add_time_to_filename=True,
                                           base_logs_dir=os.path.abspath(
                                               os.path.join(os.path.dirname(__file__), '..', 'deals_log')))
    # Телеграмм-Бот
    telegram_sender = None

    def __init__(self, arb_pair: str, spot_symbol: str, swap_symbol: str):
        self.pnl_spot = None
        self.arb_pair = arb_pair
        self.spot_symbol = spot_symbol
        self.swap_symbol = swap_symbol

        # Расчетные данные для открытия арбитражной сделки
        self.spot_amount = None
        self.swap_contracts = None
        self.contract_size = None
        self.spot_calc_fee = None
        self.swap_calc_fee = None

        # Инициализация переменных стаканов и анализа
        self.spot_average_ask: Decimal = Decimal("0")
        self.spot_average_bid: Decimal = Decimal("0")
        self.swap_average_ask: Decimal = Decimal("0")
        self.swap_average_bid: Decimal = Decimal("0")

        self.open_ratio: Decimal = Decimal("0")
        self.close_ratio: Decimal = Decimal("0")
        self.delta_ratios: Decimal = Decimal("0")

        self.max_open_ratio = Decimal("-Infinity")
        self.min_open_ratio = Decimal("Infinity")
        self.max_close_ratio = Decimal("-Infinity")
        self.min_close_ratio = Decimal("Infinity")

        self.pnl_spot = Decimal("0")
        self.pnl_swap = Decimal("0")
        self.gross_pnl = Decimal("0")
        self.net_pnl = Decimal("0")
        self.fees = Decimal("0")
        self.roi = Decimal("0")
        self.old_pnl = Decimal('0')

        self.open_deal_enable = self.__class__.open_deal_enable  # Событие-блокировщик открытия новых сделок
        self.deal_in_active = False  # Флаг наличия сделки по данной арбитражной паре

        # Переменные торговых ордеров
        self.max_order_attempt = 3
        self.order_attempt_interval = 0.5

        # Логгер для записи результатов сделки
        self.deal_logger = self.__class__.deal_logger
        self.deal_logfile_path = ""

        # Телеграмм-Бот
        self.telegram_sender = self.__class__.telegram_sender

        # Словарь для хранения данных сделки
        self.open_deal_data_dict = {}

    async def run_management(self,
                             open_ratio: Decimal = Decimal("0"),
                             close_ratio: Decimal = Decimal("0"),
                             max_open_ratio: Decimal = Decimal("0"),
                             max_close_ratio: Decimal = Decimal("0"),
                             min_open_ratio: Decimal = Decimal("0"),
                             min_close_ratio: Decimal = Decimal("0"),
                             delta_ratios: Decimal = Decimal("0"),
                             spot_amount: Decimal = Decimal("0"),
                             swap_contracts: Decimal = Decimal("0"),
                             contract_size: Decimal = Decimal("0"),
                             spot_average_ask: Decimal = Decimal("0"),
                             spot_average_bid: Decimal = Decimal("0"),
                             swap_average_ask: Decimal = Decimal("0"),
                             swap_average_bid: Decimal = Decimal("0"),
                             spot_fee: Decimal = Decimal("0"),
                             swap_fee: Decimal = Decimal("0"),
                             ):

        """Синхронный метод анализа. Решает, когда вызывать open/close."""
        self.open_ratio = open_ratio
        self.close_ratio = close_ratio
        self.delta_ratios = delta_ratios
        self.max_open_ratio = Decimal(max_open_ratio)
        self.min_open_ratio = Decimal(min_open_ratio)
        self.max_close_ratio = Decimal(max_close_ratio)
        self.min_close_ratio = Decimal(min_close_ratio)
        self.spot_amount = spot_amount
        self.swap_contracts = swap_contracts
        self.contract_size = contract_size
        self.spot_calc_fee = spot_fee  # Полученные данные из экземпляра биржи
        self.swap_calc_fee = swap_fee  # Полученные данные из экземпляра биржи

        # Инициализация переменных стаканов и анализа
        self.spot_average_ask: Decimal = Decimal(spot_average_ask)
        self.spot_average_bid: Decimal = Decimal(spot_average_bid)
        self.swap_average_ask: Decimal = Decimal(swap_average_ask)
        self.swap_average_bid: Decimal = Decimal(swap_average_bid)

        await self.open_deal_enable.wait()  # Ждем разрешения дальнейшего выполнения экземпляра

        # Если нет активной сделки по паре и есть свободный слот...
        if not self.deal_in_active and not self.arb_pair in self.__class__.active_deals_dict and len(
                self.__class__.active_deals_dict) < self.__class__.max_active_deals and self.max_open_ratio > 1 and self.min_open_ratio < 0 < self.max_close_ratio:

            # Здесь проверка условий и открытие сделки
            if self.max_open_ratio <= 1 or self.min_open_ratio >= 0 or self.max_close_ratio <= 0:
                return False

            self.open_deal_enable.clear()
            try:
                """
                Открывает спотовую и своп-позиции параллельно.
                Возвращает данные по обоим ордерам или обрабатывает частичный сбой.
                """
                # Отправляем сделку на исполнение
                try:
                    open_results = await self._open_spot_swap_deal()
                    spot_result = open_results['spot_open']
                    swap_result = open_results['swap_open']
                except Exception:
                    # Блокировка остаётся снятой только в случае успеха
                    self.open_deal_enable.set()
                    raise
                await asyncio.sleep(0.1)  # Мне кажется надо дать паузу перед запросом результатов сделки
                spot_order_answer_dict = self._parse_order(spot_result)
                swap_order_answer_dict = self._parse_order(swap_result)

                await self.telegram_sender.send_numbered_message(f"⏳ Ордера {self.arb_pair} отправлены на исполнение")

                # Выводим краткий лог сделки в консоль
                cprint.info_w(f"[{self.arb_pair}][_check_and_open_position] Ордера отправлены на выполнение")
                cprint.info_w(
                    f"[{self.arb_pair}][_check_and_open_position] Объем спот: {spot_amount}, своп: {swap_contracts} контрактов")
                cprint.info_w(f"[{self.arb_pair}][_check_and_open_position] Результат спот: {self.spot_symbol}")
                await self.telegram_sender.send_numbered_message(f"✅ Отчет о выставлении спот-ордера:")
                await self.telegram_sender.append_to_last_message(spot_order_answer_dict)
                await self.telegram_sender.send_numbered_message(f"✅ Отчет о выставлении своп-ордера:")
                await self.telegram_sender.append_to_last_message(swap_order_answer_dict)
                pprint(spot_order_answer_dict)
                cprint.info_w(f"[{self.arb_pair}][_check_and_open_position] Результат своп: {self.swap_symbol}")
                pprint(swap_order_answer_dict)

                # Задержка на обработку ордеров биржей
                await asyncio.sleep(0.5)

                # Ждем получения подтверждения совершения сделки, после этого получаем ее данные.
                open_deal_data_dict = await self._get_actual_deal_data(self.exchange, self.spot_symbol,
                                                                       self.swap_symbol, safe_decimal)

                # Проверяем данные -
                if open_deal_data_dict.get('get_swap_data', False) and open_deal_data_dict.get('get_spot_data', False):
                    open_deal_data_dict['spot_deal_ask'] = self.spot_average_ask  # Цена принятия решения
                    open_deal_data_dict['swap_deal_bid'] = self.swap_average_bid  # Цена принятия решения
                    open_deal_data_dict['calculated_order_property']['spot_amount'] = spot_amount
                    open_deal_data_dict['calculated_order_property']['swap_contracts'] = swap_contracts
                    await self.telegram_sender.send_numbered_message(
                        f"✅ Полученные фактические данные размещенных ордеров")
                    await self.telegram_sender.append_to_last_message(open_deal_data_dict)
                    open_deal_data_dict['spot_order_answer'] = spot_order_answer_dict
                    open_deal_data_dict['swap_order_answer'] = swap_order_answer_dict
                    self.deal_in_active = True
                    # Запись в словарь
                    self.__class__.active_deals_dict.setdefault(str(self.arb_pair), {}).update(open_deal_data_dict)
                    # Запись в файл
                    self.__class__.active_deals_file_manager.add(str(self.arb_pair), open_deal_data_dict)
                    open_deal_data_dict = {}

                else:
                    await self.telegram_sender.send_numbered_message(f"❌ Ошибка в размещении ордеров")
                    await self.telegram_sender.append_to_last_message(open_deal_data_dict)
                    open_deal_data_dict = {}
                    return str(f"❌ Ошибка в размещении ордеров")

                # После завершения выставления ордеров и подтверждения сделки снимаем блокировку
                self.open_deal_enable.set()
                """
                          {
                    # Всегда присутствуют:
                    'deal_spot_coin_volume': Decimal,                           # значение из safe_decimal(spot_free)
                    'spot_symbol': str,                                         # например, "BTC/USDT"

                    # Зависит от spot_free:
                    'get_spot_data': bool,                                      # True, если spot_free != 0 и не None

                    # Присутствуют ТОЛЬКО если найдена активная своп-позиция с symbol == swap_symbol:
                    'get_swap_data': True,                                      # bool
                    'swap_symbol': str,                                         # например, "BTC/USDT:USDT"
                    'swap_side': str,                                           # "long" или "short"
                    ['calculated_order_property']['swap_contracts']: Union[str, float, int],  # количество контрактов (часто строка из API)
                    ['calculated_order_property']['spot_amount']
                    'swap_entry_price': Optional[Union[str, float, Decimal]],
                    'swap_mark_price': Optional[Union[str, float, Decimal]],
                    'swap_open_position_fee': Optional[str],                    # из pos['info']['pnl_fee'], если есть
                    'swap_datetime': Optional[str],                             # ISO-строка, например "2025-10-16T12:00:00.000Z"
                    'swap_timestamp': Optional[int],                            # Unix-время в мс или сек
                    'swap_leverage': Optional[Union[str, int, float]],
                    'swap_contract_size': Optional[Union[str, float, Decimal]],
                }
                """

            except OpenSpotOrderError as e:
                # Спот не открылся, но своп, возможно, открыт → критическая ситуация!
                self.deal_logger.critical(
                    f"🚨 Спот НЕ открыт после {self.max_order_attempt} попыток! Своп, возможно, открыт. ")
                cause = str(e.__cause__) if e.__cause__ else "Нет деталей"
                await self.telegram_sender.send_numbered_message(f"❌ ОТКРЫТИЕ СПОТА ПРОВАЛЕНО\n"
                                                                 f"Попыток: {self.max_order_attempt}\n"
                                                                 f"Причина: {cause}")
                # ⚠️ Здесь можно добавить аварийное закрытие свопа, если он был открыт
                raise
            except OpenSwapOrderError as e:
                # Своп не открылся, но спот, возможно, открыт → тоже критично!
                self.deal_logger.critical(
                    f"🚨 Своп НЕ открыт после {self.max_order_attempt} попыток! Спот, возможно, открыт. ")
                cause = str(e.__cause__) if e.__cause__ else "Нет деталей"
                await self.telegram_sender.send_numbered_message(f"❌ ОТКРЫТИЕ СВОПА ПРОВАЛЕНО\n"
                                                                 f"Попыток: {self.max_order_attempt}\n"
                                                                 f"Причина: {cause}")
                # ⚠️ Здесь можно добавить аварийное закрытие спота
                raise
            except Exception as e:
                self.deal_logger.error(f"Неизвестная ошибка при открытии: {e}", log_file=self.deal_logfile_path)
                await self.telegram_sender.send_numbered_message(f"🔥 НЕИЗВЕСТНАЯ ОШИБКА ОТКРЫТИЯ\n{e}")
                raise

        #  Если есть активная сделка и пара в словаре сделок...
        if self.deal_in_active or self.arb_pair in self.__class__.active_deals_dict:
            """
            Здесь проверка прибыли/убытка и закрытие сделки. 
            Отслеживаем PnL по высчитанным объемам ордеров, а закрываем по фактическому наличию монет на счетах.
            """
            deal_data = self.__class__.active_deals_dict[self.arb_pair]  # Словарь со всеми данными открытой сделки
            spot_open_sigal_price = safe_decimal(
                deal_data.get('spot_deal_ask', '0'))  # Средняя цена сработки сигнала открытия спот сделки
            swap_open_sigal_price = safe_decimal(
                deal_data.get('swap_deal_bid', '0'))  # Средняя цена сработки сигнала открытия своп сделки
            swap_open_price = safe_decimal(
                deal_data.get('swap_entry_price', '0'))  # Фактическая цена исполнения открытия своп сделки
            swap_contracts = safe_decimal(deal_data.get('swap_contracts', '0'))
            contract_size = safe_decimal(
                deal_data.get('swap_contract_size', '0'))  # Количество монет в одном своп контракте
            swap_fee_usdt = safe_decimal(
                deal_data.get('swap_open_position_fee', '0'))  # Реальная вычтенная из pnl сделки комиссия
            spot_amount = safe_decimal(deal_data.get('calculated_order_property', {}).get('spot_amount', '0'))
            swap_contracts = safe_decimal(deal_data.get('calculated_order_property', {}).get('swap_contracts',
                                                                                             '0'))  # Объем своп сделки в контрактах

            swap_amount = swap_contracts * contract_size  # Количество монет в своп сделке

            # Текущие цены
            spot_close_price = safe_decimal(self.spot_average_bid, 'spot_average_bid')
            swap_close_price = safe_decimal(self.swap_average_ask, 'swap_average_ask')

            # PnL
            # Пока мы не можем получить из истории фактическую цену открытия сделки спот, потому используем сигнальную цену
            self.pnl_spot = (spot_close_price - spot_open_sigal_price) * spot_amount
            self.pnl_swap = (swap_open_price - swap_close_price) * swap_amount

            # Грязная прибыль
            self.gross_pnl = self.pnl_spot + self.pnl_swap

            # Комиссии
            # swap_fee_usdt взято из возвращаемого при выставлении ордера словаря
            fees_open = (spot_open_sigal_price * spot_amount * spot_fee + abs(swap_fee_usdt))
            fees_close = (spot_close_price * spot_amount * spot_fee + abs(swap_fee_usdt))
            self.fees = fees_open + fees_close

            # Чистая прибыль
            self.net_pnl = self.gross_pnl - self.fees

            invested_usdt = spot_open_sigal_price * spot_amount + swap_open_price * swap_amount
            self.roi = (self.net_pnl / invested_usdt) * 100 if invested_usdt > 0 else 0

            # Обновляем данные
            deal_data.update(
                {'pnl_spot': self.pnl_spot,
                 'pnl_swap': self.pnl_swap,
                 'gross_pnl': self.gross_pnl,
                 'fees': self.fees,
                 'net_pnl': self.net_pnl,
                 'roi': self.roi,
                 'current_spot_price': spot_close_price,
                 'current_swap_price': swap_close_price, })

            # min/max
            if self.net_pnl != Decimal('0'):
                self.max_pnl = max(self.max_pnl, self.net_pnl)
                self.min_pnl = min(self.min_pnl, self.net_pnl)
            if self.roi != Decimal('0'):
                self.max_roi = max(self.max_roi, self.roi)
                self.min_roi = min(self.min_roi, self.roi)

            if self.old_pnl != self.net_pnl:
                cprint.info_b(f"[PNL] {self.arb_pair}: "
                              f"net {self.net_pnl:.4f} USDT | ROI {float(self.roi):.3f}% | "
                              f"spot {self.pnl_spot:.4f} swap {self.pnl_swap:.4f} | "
                              f"net_pnl $: min {self.min_pnl:.4f}$, max {self.max_pnl:.4f}$ | "
                              f"roi %: min {self.min_roi:.4f}%, max {self.max_roi:.4f}%")
                self.old_pnl = self.net_pnl

    # Метод сброса всей статистики по открытой сделке после ее закрытия
    def _reset_pnl(self):
        # Если был логгер — закрываем все его хэндлеры
        if self.deal_logger:
            for handler in self.deal_logger.handlers[:]:
                handler.close()
                self.deal_logger.removeHandler(handler)
            self.deal_logger = None
        self.max_pnl = Decimal('-Infinity')
        self.min_pnl = Decimal('Infinity')
        self.max_roi = Decimal('-Infinity')
        self.min_roi = Decimal('Infinity')
        self.old_pnl = Decimal('0')
        self.roi = None  # Прибыль в процентах
        self.net_pnl = None  # Прибыль в USDT
        self.pnl_spot = None  # Прибыль spot в USDT
        self.pnl_swap = None  # Прибыль swap в USDT
        self.pnl_gross = None  # Грязная Прибыль в USDT
        self.fees = None  # Общая рассчитанная комиссия за сделку в USDT
        self.deal_logger = None  # Логгер для сохранения данных сделки
        self.deal_in_active = False  # Флаг наличия сделки по данной арбитражной паре
        self.deal_pnl_data_dict = {}
        self.deal_logfile_path = None

    # Метод открытия арбитражной сделки. Возвращает данные по открытию, которые вернул сервер
    async def _open_spot_swap_deal(self):
        """
        Открывает спотовую и своп-позиции параллельно.
        В случае частичного сбоя — аварийно закрывает успешно открытую позицию.
        """
        self.deal_logger.info("🚀 Начало открытия спот + своп позиций", log_file=self.deal_logfile_path)

        # Запускаем обе операции параллельно, сохраняя исключения как объекты
        spot_task = self._open_spot()
        swap_task = self._open_swap()
        results = await asyncio.gather(spot_task, swap_task, return_exceptions=True)

        spot_result, swap_result = results

        spot_ok = not isinstance(spot_result, Exception)
        swap_ok = not isinstance(swap_result, Exception)

        # Оба успешны
        if spot_ok and swap_ok:
            self.deal_logger.info("✅ Обе позиции успешно открыты", log_file=self.deal_logfile_path)
            return {'spot_open': spot_result, 'swap_open': swap_result}

        # Частичный или полный сбой — обрабатываем
        try:
            if not spot_ok and swap_ok:
                # Спот не открылся, своп — открыт → аварийное закрытие свопа
                self.deal_logger.critical(
                    "🚨 Спот НЕ открыт! Аварийное закрытие своп-позиции...",
                    log_file=self.deal_logfile_path
                )
                try:
                    await self._close_swap()
                    self.deal_logger.info("✅ Своп аварийно закрыт", log_file=self.deal_logfile_path)
                except Exception as close_e:
                    self.deal_logger.error(
                        f"❌ Не удалось аварийно закрыть своп: {close_e}",
                        log_file=self.deal_logfile_path
                    )
                    await self.telegram_sender.send_numbered_message(
                        f"❌ АВАРИЙНОЕ ЗАКРЫТИЕ СВОПА ПРОВАЛЕНО\n{close_e}"
                    )
                # Выбрасываем исходную ошибку открытия спота
                raise spot_result

            elif not swap_ok and spot_ok:
                # Своп не открылся, спот — открыт → аварийная продажа спота
                self.deal_logger.critical(
                    "🚨 Своп НЕ открыт! Аварийная продажа спота...",
                    log_file=self.deal_logfile_path
                )
                try:
                    await self._close_spot()
                    self.deal_logger.info("✅ Спот аварийно закрыт", log_file=self.deal_logfile_path)
                except Exception as close_e:
                    self.deal_logger.error(
                        f"❌ Не удалось аварийно закрыть спот: {close_e}",
                        log_file=self.deal_logfile_path
                    )
                    await self.telegram_sender.send_numbered_message(
                        f"❌ АВАРИЙНАЯ ПРОДАЖА СПОТА ПРОВАЛЕНА\n{close_e}"
                    )
                # Выбрасываем исходную ошибку открытия свопа
                raise swap_result

            else:
                # Оба провалились
                self.deal_logger.critical(
                    "🔥 Обе позиции НЕ открыты",
                    log_file=self.deal_logfile_path
                )
                # Выбираем первую ошибку для репорта
                primary_error = spot_result if isinstance(spot_result, Exception) else swap_result
                raise DealOpenError("Не удалось открыть ни спот, ни своп") from primary_error

        except Exception as final_error:
            # Формируем сообщение для Telegram
            cause = str(final_error.__cause__) if final_error.__cause__ else str(final_error)
            await self.telegram_sender.send_numbered_message(
                f"❌ ЧАСТИЧНЫЙ/ПОЛНЫЙ СБОЙ ОТКРЫТИЯ\n"
                f"Спот: {'✅ OK' if spot_ok else '❌ FAIL'}\n"
                f"Своп: {'✅ OK' if swap_ok else '❌ FAIL'}\n"
                f"Ошибка: {cause}"
            )
            raise

    async def _open_spot(self):
        self.exchange.options['createMarketBuyOrderRequiresPrice'] = False

        for attempt in range(1, self.max_order_attempt + 1):
            send_time = time.time()
            try:
                order_data = await self.exchange.create_order(self.spot_symbol, type='market', side='buy',
                                                              amount=self.spot_amount)
                recv_time = time.time()

                status = order_data.get('status') or order_data.get('info', {}).get('finish_as')
                if status in ('closed', 'filled', 'finished'):
                    result = {'order_data': order_data, 'spot_send_time': send_time, 'spot_recv_time': recv_time,
                              'duration': recv_time - send_time, 'attempts': attempt}
                    if attempt > 1:
                        self.deal_logger.info(f"✅ Спот открыт с {attempt}-й попытки", log_file=self.deal_logfile_path)
                    return result
                else:
                    raise Exception(f"Ордер не исполнен, статус: {status}")

            except Exception as e:
                recv_time = time.time()
                duration = recv_time - send_time
                self.deal_logger.warning(
                    f"Попытка {attempt}/{self.max_order_attempt} открытия спота упала через {duration:.3f}с: {e}",
                    log_file=self.deal_logfile_path)
                if attempt < self.max_order_attempt:
                    await asyncio.sleep(self.order_attempt_interval)
                else:
                    self.deal_logger.error(f"❌ Все {self.max_order_attempt} попыток открыть спот провалились",
                                           log_file=self.deal_logfile_path)
                    raise OpenSpotOrderError(self.spot_symbol, "Не удалось открыть спот-ордер после всех попыток") from e
        return None

    async def _open_swap(self):
        async def init_swap_settings(symbol):
            try:
                await self.exchange.set_margin_mode('cross', symbol)
            except Exception as e1:
                self.deal_logger.warning(f"Не удалось установить margin mode для {symbol}: {e1}")
            try:
                await self.exchange.set_leverage(1, symbol)
            except Exception as e1:
                self.deal_logger.warning(f"Не удалось установить leverage для {symbol}: {e1}")

        await init_swap_settings(self.swap_symbol)

        for attempt in range(1, self.max_order_attempt + 1):
            send_time = time.time()
            try:
                order_data = await self.exchange.create_order(self.swap_symbol, type='market', side='sell',
                                                              amount=self.swap_contracts, params={})
                recv_time = time.time()

                status = order_data.get('status') or order_data.get('info', {}).get('finish_as')
                if status in ('closed', 'filled', 'finished'):
                    result = {'order_data': order_data, 'swap_send_time': send_time, 'swap_recv_time': recv_time,
                              'duration': recv_time - send_time, 'attempts': attempt}
                    if attempt > 1:
                        self.deal_logger.info(f"✅ Своп открыт с {attempt}-й попытки", log_file=self.deal_logfile_path)
                    return result
                else:
                    raise Exception(f"Ордер не исполнен, статус: {status}")

            except Exception as e:
                recv_time = time.time()
                duration = recv_time - send_time
                self.deal_logger.warning(
                    f"Попытка {attempt}/{self.max_order_attempt} открытия свопа упала через {duration:.3f}с: {e}",
                    log_file=self.deal_logfile_path)
                if attempt < self.max_order_attempt:
                    await asyncio.sleep(self.order_attempt_interval)
                else:
                    self.deal_logger.error(f"❌ Все {self.max_order_attempt} попыток открыть своп провалились",
                                           log_file=self.deal_logfile_path)
                    raise OpenSwapOrderError(self.swap_symbol, "Не удалось открыть своп-ордер после всех попыток") from e

    async def _close_spot(self):
        self.exchange.options['createMarketBuyOrderRequiresPrice'] = False

        for attempt in range(1, self.max_order_attempt + 1):
            send_time = time.time()
            try:
                order_data = await self.exchange.create_order(self.spot_symbol, type='market', side='sell',
                                                              amount=self.spot_amount)
                recv_time = time.time()

                status = order_data.get('status') or order_data.get('info', {}).get('finish_as')
                if status in ('closed', 'filled', 'finished'):
                    result = {'order_data': order_data, 'spot_send_time': send_time, 'spot_recv_time': recv_time,
                              'duration': recv_time - send_time, 'attempts': attempt}
                    if attempt > 1:
                        self.deal_logger.info(f"✅ Спот закрыт с {attempt}-й попытки", log_file=self.deal_logfile_path)
                    return result
                else:
                    raise Exception(f"Ордер не исполнен, статус: {status}")

            except Exception as e:
                recv_time = time.time()
                duration = recv_time - send_time
                self.deal_logger.warning(
                    f"Попытка {attempt}/{self.max_order_attempt} закрытия спота упала через {duration:.3f}с: {e}",
                    log_file=self.deal_logfile_path)
                if attempt < self.max_order_attempt:
                    await asyncio.sleep(self.order_attempt_interval)
                else:
                    self.deal_logger.error(f"❌ Все {self.max_order_attempt} попыток закрыть спот провалились",
                                           log_file=self.deal_logfile_path)
                    raise CloseSpotOrderError(self.spot_symbol, "Не удалось закрыть спот-ордер после всех попыток") from e

    async def _close_swap(self):
        async def init_swap_settings(symbol):
            try:
                await self.exchange.set_margin_mode('cross', symbol)
            except Exception as e:
                self.deal_logger.warning(f"Не удалось установить margin mode для {symbol}: {e}")
            try:
                await self.exchange.set_leverage(1, symbol)
            except Exception as e:
                self.deal_logger.warning(f"Не удалось установить leverage для {symbol}: {e}")

        await init_swap_settings(self.swap_symbol)

        for attempt in range(1, self.max_order_attempt + 1):
            send_time = time.time()
            try:
                order_data = await self.exchange.create_order(self.swap_symbol, type='market', side='buy',
                                                              amount=self.swap_contracts, params={})
                recv_time = time.time()

                status = order_data.get('status') or order_data.get('info', {}).get('finish_as')
                if status in ('closed', 'filled', 'finished'):
                    result = {'order_data': order_data, 'swap_send_time': send_time, 'swap_recv_time': recv_time,
                              'duration': recv_time - send_time, 'attempts': attempt}
                    if attempt > 1:
                        self.deal_logger.info(f"✅ Своп закрыт с {attempt}-й попытки", log_file=self.deal_logfile_path)
                    return result
                else:
                    raise Exception(f"Ордер не исполнен, статус: {status}")

            except Exception as e:
                recv_time = time.time()
                duration = recv_time - send_time
                self.deal_logger.warning(
                    f"Попытка {attempt}/{self.max_order_attempt} закрытия свопа упала через {duration:.3f}с: {e}",
                    log_file=self.deal_logfile_path)
                if attempt < self.max_order_attempt:
                    await asyncio.sleep(self.order_attempt_interval)
                else:
                    self.deal_logger.error(f"❌ Все {self.max_order_attempt} попыток закрыть своп провалились",
                                           log_file=self.deal_logfile_path)
                    raise CloseSwapOrderError(self.swap_symbol, "Не удалось закрыть своп-ордер после всех попыток") from e

    # Метод закрытия арбитражной сделки. Возвращает данные по закрытию, которые вернул сервер
    async def _close_spot_swap_deal(self):
        try:
            spot_result, swap_result = await asyncio.gather(self._close_spot(), self._close_swap())
            self.deal_logger.info("✅ Обе позиции успешно закрыты", log_file=self.deal_logfile_path)
            return {'spot_close': spot_result, 'swap_close': swap_result}

        except CloseSpotOrderError as e:
            self.deal_logger.critical(
                f"🚨 Спот НЕ закрыт после {self.max_order_attempt} попыток! Своп, возможно, закрыт. ")

            await self.telegram_sender.send_numbered_message(

                f"❌ ЗАКРЫТИЕ СПОТА ПРОВАЛЕНО\n"
                f"Попыток: {self.max_order_attempt}\n"
                f"Причина: {e.__cause__}")
            raise

        except CloseSwapOrderError as e:
            self.deal_logger.critical(
                f"🚨 Своп НЕ закрыт после {self.max_order_attempt} попыток! Спот, возможно, закрыт. ")
            await self.telegram_sender.send_numbered_message(
                f"❌ ЗАКРЫТИЕ СВОПА ПРОВАЛЕНО\n"
                f"Попыток: {self.max_order_attempt}\n"
                f"Последняя длительность: {e.duration:.3f} с\n"
                f"Причина: {e.__cause__}")
            raise

        except Exception as e:
            self.deal_logger.error(f"Неизвестная ошибка при закрытии: {e}", log_file=self.deal_logfile_path)
            await self.telegram_sender.send_numbered_message(f"🔥 НЕИЗВЕСТНАЯ ОШИБКА ЗАКРЫТИЯ\n{e}")
            raise

    @staticmethod
    def _parse_order(order: dict) -> dict:
        """Парсинг ключевых и оригинальных данных ордера с безопасными числами"""

        def safe_decimal(value):
            """Безопасное преобразование в Decimal"""
            if value is None:
                return Decimal("0")
            try:
                return Decimal(str(value))
            except (InvalidOperation, ValueError, TypeError):
                return Decimal("0")

        return {
            'id': order.get('id'),
            'clientOrderId': order.get('clientOrderId'),
            'symbol': order.get('symbol'),
            'type': order.get('type'),
            'side': order.get('side'),
            'price': safe_decimal(order.get('price')),
            'average': safe_decimal(order.get('average')),
            'amount': safe_decimal(order.get('amount')),
            'filled': safe_decimal(order.get('filled')),
            'remaining': safe_decimal(order.get('remaining')),
            'cost': safe_decimal(order.get('cost')),
            'status': order.get('status'),
            'fee': safe_decimal(order.get('fee')),
            'trades': order.get('trades', []),
            'timestamp': order.get('timestamp'),
            'datetime': order.get('datetime'),
            'info': order.get('info'),
        }

    @staticmethod
    async def _get_actual_deal_data(exchange, spot_symbol, swap_symbol, safe_decimal):
        """ По непонятным причинам gateio перестала возвращать сделки спот методом fetch_my_trades.
        По этой причине мы получаем только баланс монеты в кошельке и открытую своп сделку.
        Параллельно запрашивает: спотовый баланс по spot, открытые позиции по swap.
        Возвращает: - Реальное количество монет в спот кошельке; - Реальный объем своп сделки """
        """
        Возвращает фактические данные по сделке:
          {
            # Всегда присутствуют:
            'deal_spot_coin_volume': Decimal,                           # значение из safe_decimal(spot_free)
            'spot_symbol': str,                                         # например, "BTC/USDT"

            # Зависит от spot_free:
            'get_spot_data': bool,                                      # True, если spot_free != 0 и не None

            # Присутствуют ТОЛЬКО если найдена активная своп-позиция с symbol == swap_symbol:
            'get_swap_data': True,                                      # bool
            'swap_symbol': str,                                         # например, "BTC/USDT:USDT"
            'swap_side': str,                                           # "long" или "short"
            'swap_contracts': Union[str, float, int],                   # количество контрактов (часто строка из API)
            'swap_entry_price': Optional[Union[str, float, Decimal]],
            'swap_mark_price': Optional[Union[str, float, Decimal]],
            'swap_open_position_fee': Optional[str],                    # из pos['info']['pnl_fee'], если есть
            'swap_datetime': Optional[str],                             # ISO-строка, например "2025-10-16T12:00:00.000Z"
            'swap_timestamp': Optional[int],                            # Unix-время в мс или сек
            'swap_leverage': Optional[Union[str, int, float]],
            'swap_contract_size': Optional[Union[str, float, Decimal]],
        }
        """
        result = {}

        params_spot_balance = {"type": "spot"}
        balance_task = exchange.fetch_balance(params_spot_balance)
        positions_task = exchange.fetch_positions(symbols=[swap_symbol])

        try:
            spot_balance, positions = await asyncio.gather(balance_task, positions_task)
        except Exception as e:
            print(f"❌ Ошибка при параллельном получении данных: {e}")
            raise

        spot_base = spot_symbol.split('/')[0]
        spot_free = spot_balance.get(spot_base, {}).get("free", None)
        result['deal_spot_coin_volume'] = safe_decimal(spot_free)
        result['spot_symbol'] = spot_symbol
        if spot_free is not None and Decimal(spot_free) != "0":
            result.update({'get_spot_data': True})
        else:
            result.update({'get_spot_data': False})

        active_positions = [p for p in positions if p.get('contracts') and float(p['contracts']) != 0]

        if active_positions:
            print(f"🔔 Обнаружено {len(active_positions)} активная своп-позиция")
            for pos in active_positions:
                if pos['symbol'] == swap_symbol:
                    result.update({
                        'get_swap_data': True,
                        'swap_symbol': pos['symbol'],
                        'swap_side': pos['side'],
                        'swap_contracts': pos['contracts'],
                        'swap_entry_price': pos.get('entryPrice'),
                        'swap_mark_price': pos.get('markPrice'),
                        'swap_open_position_fee': pos.get('info', {}).get('pnl_fee', None),
                        'swap_datetime': pos.get('datetime', None),
                        'swap_timestamp': pos.get('timestamp', None),
                        'swap_leverage': pos.get('leverage', None),
                        'swap_contract_size': pos.get('contractSize', None),
                    })
        else:
            print(f"📭 Нет активных своп-позиций по символу {swap_symbol}")
            result.update({'get_swap_data': False})

        return result