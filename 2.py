__version__ = '0.12'
import time
import asyncio
from contextlib import AsyncExitStack
from decimal import Decimal
import ccxt.pro as ccxt  # ccxt.pro нужен для ExchangeInstance
from modules.task_manager import TaskManager
from modules.balance_manager import BalanceManager
from pprint import pprint
from typing import Dict
import signal

# Импорт класса обработки экземпляра биржи - ExchangeInstance
from modules.exchange_instance import ExchangeInstance
from modules.utils import to_decimal
from modules import (get_average_orderbook_price, cprint, sync_time_with_exchange)
# Импортируем нижний класс - работа с символом на отдельной бирже
# from modules.exchange_instrument import ExchangeInstrument
from modules.exception_classes import ( ReconnectLimitExceededError,
                                        InvalidOrEmptyOrderBookError,
                                        BaseArbitrageCalcException,
                                        InsufficientOrderBookVolumeError)

class ExchangeInstrument():
    # Обязательна передача ссылки на класс BalanceManager
    balance_manager = None
    # Словарь флагов ордербук-сокетов вида {exchange_ids:{symbol: true/false}} - по нему можно собирать статистику открытых сокетов на всех биржах
    orderbook_updating_status_dict = {}

    def __init__(self, exchange, symbol, swap_data, orderbook_queue: asyncio.Queue, statistic_queue: asyncio.Queue = None):
        # Словарь счетчиков пришедших стаканов целевого символа заданной биржи
        self.get_ex_orderbook_data_count = {}
        fee = swap_data.get("taker")
        self.exchange = exchange
        self.exchange_id = exchange.id
        self.symbol = symbol
        self.orderbook_queue = orderbook_queue # Очередь данных ордербуков, сообщений вида {exchange_id: {data_dict}}. На каждый символ своя очередь
        self.statistic_queue = statistic_queue # Очередь отсылки статистики работы watch_orderbook
        if self.__class__.balance_manager:
            self.balance_manager_instance = self.__class__.balance_manager.get_instance(exchange.id)
        else:
            cprint.error(f"[ExchangeInstrument][_init_] Не передан экземпляр balance_manager [{exchange.id}][{symbol}]")
        self.orderbook_updating = False
        self.contract_size = ''
        self.tick_size = ''
        self.qty_step = ''
        self.min_amount = ''
        self.fee = ''
        self.update_swap_data(swap_data=swap_data)
        self.deal_balance = None
        self.exchange_balance = None
        self.min_exchanges_balance = None

    async def get_exchange_balance(self):
        if await self.balance_manager_instance.is_balance_valid:
            self.exchange_balance = await self.balance_manager_instance.get_balance()
            return self.exchange_balance
        else:
            cprint.error_w(f"[ExchangeInstrument][get_exchange_balance] Нет баланса [{self.exchange_id}]")
            return False

    async def get_min_exchanges_balance(self):
        if await self.balance_manager_instance.is_balance_valid:
            self.min_exchanges_balance = await self.balance_manager_instance.get_min_balance()
            return self.min_exchanges_balance
        else:
            cprint.error_w(f"[ExchangeInstrument][get_min_exchanges_balance] Нет баланса [{self.exchange_id}]")
            return False

    async def get_deal_balance(self):
        if await self.balance_manager_instance.is_balance_valid:
            self.deal_balance = await self.balance_manager_instance.get_deal_balance()
            return self.deal_balance
        else:
            cprint.error_w(f"[ExchangeInstrument][get_deal_balance] Нет баланса [{self.exchange_id}]")
            return False

    # Метод обновления данных
    def update_swap_data(self, swap_data):
        fee = swap_data.get("taker")
        self.contract_size = to_decimal(swap_data.get("contractSize"))
        self.tick_size = to_decimal(swap_data.get("precision", {}).get("price"))
        self.qty_step = to_decimal(swap_data.get("precision", {}).get("amount", None))
        self.min_amount = to_decimal(swap_data.get("limits", {}).get("amount", {}).get('min', None))
        if fee is None:
            fee = swap_data.get("taker_fee")
        self.fee = fee

    # Метод возвращения словаря статусов бесконечного цикла получения стакана
    @classmethod
    def get_orderbook_updating_status_dict(cls):
        return cls.orderbook_updating_status_dict

    # Метод получения стаканов заданного символа на заданной бирже
    async def watch_orderbook(self):
        """
        Асинхронно следит за стаканом указанного символа.
        При превышении числа попыток — завершает задачу.
        """
        max_reconnect_attempts = 5  # лимит переподключений
        reconnect_attempts = 0
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
        statistics_dict = {}


        try:
            # Ожидание баланса
            # todo реализовать получение экземпляром минимального баланса бирж
            await self.balance_manager.wait_initialized()

            await self.balance_manager.get_deal_balance()


            # Инициализируем бесконечный цикл подписки получения стаканов
            self.__class__.orderbook_updating_status_dict.setdefault(self.exchange_id, {})[self.symbol] = True

            # Инициализируем счетчик получения стаканов
            self.get_ex_orderbook_data_count.setdefault(self.exchange_id, {})[self.symbol] = 0

            # Цикл работает пока есть подписка
            while self.__class__.orderbook_updating_status_dict[self.exchange_id][self.symbol]:
                try:
                    if not await self.balance_manager.is_balance_valid():
                        continue

                    orderbook = await self.exchange.watchOrderBook(self.symbol)

                    # сбрасываем счётчик после успешного получения
                    reconnect_attempts = 0
                    ticks_total += 1
                    count += 1

                    # Проверим пришедшие данные стаканов - если левые, то выбрасываем исключение
                    if not isinstance(orderbook, dict) or len(orderbook) == 0 or 'asks' not in orderbook or 'bids' not in orderbook:
                        raise InvalidOrEmptyOrderBookError(exchange_id=self.exchange_id, symbol=self.symbol, orderbook_data=orderbook)

                    # Инкриминируем счетчик получения стаканов
                    self.get_ex_orderbook_data_count.setdefault(self.exchange_id, {})[self.symbol] += 1

                    # Обрезаем стакан для анализа до 10 ордеров
                    depth = min(10, len(orderbook['asks']), len(orderbook['bids']))
                    new_ask = tuple(map(tuple, orderbook['asks'][:depth]))
                    new_bid = tuple(map(tuple, orderbook['bids'][:depth]))

                    if new_ask != old_ask or new_bid != old_bid:
                        ticks_changed += 1
                        new_count += 1

                        if self.get_deal_balance is None or self.balance_usdt <= 0:
                            await asyncio.sleep(0.5)
                            continue

                        try:
                            _, min_balance = await BalanceManager.get_min_balance()


                            if balance is None or balance <= 0:
                                continue

                            average_ask = get_average_orderbook_price(
                                data=orderbook['asks'],
                                money=min_balance, # Здесь используется минимальный баланс, получаемый из классного метода - минимум среди всех биржевых депозитов
                                is_ask=True,
                                log=True,
                                exchange=self.exchange_id,
                                symbol=self.symbol
                            )

                            average_bid = get_average_orderbook_price(
                                data=orderbook['bids'],
                                money=min_balance,
                                is_ask=False,
                                log=True,
                                exchange=self.exchange_id,
                                symbol=self.symbol
                            )

                        except InsufficientOrderBookVolumeError as e:
                            cprint.warning(f"[SKIP] Недостаточный объём стакана для {self.symbol} биржи {self.exchange_id}: {e}")
                            await asyncio.sleep(0)  # чтобы не перегружать цикл
                            self.__class__.orderbook_updating_status_dict[self.exchange_id][self.symbol] = False
                            # Отправляем сообщение о закрытии, теоретически можно отслеживать работу вебсокетов по флагу

                            break

                        if average_ask is None or average_bid is None:
                            await asyncio.sleep(1)
                            continue

                        self.__class__.orderbook_data_dict[self.symbol][self.exchange_id] = {
                            "type": "orderbook_update",
                            "ts": time.monotonic(),
                            "symbol": self.symbol,
                            "exchange_id": self.exchange_id,
                            "count": count,
                            "new_count": new_count,
                            "average_ask": average_ask,
                            "average_bid": average_bid,
                            "closed_error": False,
                        }

                        old_ask = new_ask
                        old_bid = new_bid

                        try:
                            await self.orderbook_queue.put_nowait(self.__class__.orderbook_data_dict[self.symbol].copy())
                        except asyncio.QueueFull:
                            try:
                                _ = self.orderbook_queue.get_nowait()
                            except asyncio.QueueEmpty:
                                pass
                            await self.orderbook_queue.put(self.__class__.orderbook_data_dict[self.symbol].copy())

                    if self.statistic_queue:
                        # ---------- окно 10 секунд ----------
                        now = time.monotonic()
                        if now - window_start_ts >= TICK_WINDOW_SEC:
                            statistics_dict = {
                                "type": "tick_stats",
                                "symbol": self.symbol,
                                "exchange_id": self.exchange_id,
                                "window_sec": TICK_WINDOW_SEC,
                                "ticks_total": ticks_total,
                                "ticks_changed": ticks_changed,
                                "ticks_per_sec": round(ticks_total / TICK_WINDOW_SEC, 2),
                                "changed_per_sec": round(ticks_changed / TICK_WINDOW_SEC, 2),
                                "ts_from": window_start_ts,
                                "ts_to": now,
                            }

                            window_start_ts = now
                            ticks_total = 0
                            ticks_changed = 0
                        # -----------------------------------
                        try:
                            await self.statistic_queue.put_nowait({self.symbol: {self.exchange_id: statistics_dict}})
                        except asyncio.QueueFull:
                            try:
                                _ = self.statistic_queue.get_nowait()
                            except asyncio.QueueEmpty:
                                pass
                            await self.statistic_queue.put({self.symbol: {self.exchange_id: statistics_dict}})

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
            self.__class__.orderbook_updating_status_dict.setdefault(self.exchange_id, {})[self.symbol] = False


