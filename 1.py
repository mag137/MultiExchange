import asyncio
import ccxt.pro as ccxt
from modules.exchange_instrument import ExchangeInstrument
class ExchangeInstrument:
    # Словарь флагов ордербук-сокетов вида {exchange_ids:{symbol: true/false}} - по нему можно собирать статистику открытых сокетов на всех биржах
    exchanges_instances_dict = {}  # Словарь с экземплярами бирж
    balance_manager = None
    task_manager = None
    exchange_instruments_obj_dict = {} # Словарь с экземплярами класса ExchangeInstrument вида {symbol: {exchange_id: {obj: instance, task_name: task_name}}}
    swap_processed_data_dict = None
    swap_raw_data_dict = None
    orderbook_updating_status_dict = {}
    get_ex_orderbook_data_count = {} # Словарь счетчиков пришедших стаканов целевого символа заданной биржи
    _configured = False

    _lock: asyncio.Lock | None = None

    @classmethod
    def get_lock(cls):
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    # Думаю надо взять за правило - передавать в экземпляр только аргументы экземпляра, аргументы класса передавать напрямую через имя класса, а лучше через отдельный метод
    @classmethod
    def get_configure(cls, *, exchanges_instances_dict=None, balance_manager=None, task_manager=None, swap_raw_data_dict=None, swap_processed_data_dict=None):
        if cls._configured:
            raise RuntimeError("ExchangeInstrument уже настроен")
        if exchanges_instances_dict is not None:
            cls.exchanges_instances_dict = exchanges_instances_dict
        if balance_manager is not None:
            cls.balance_manager = balance_manager
        if task_manager is not None:
            cls.task_manager = task_manager
        if swap_raw_data_dict is not None:
            cls.swap_raw_data_dict = swap_raw_data_dict
        if swap_processed_data_dict is not None:
            cls.swap_processed_data_dict = swap_processed_data_dict

        cls._configured = True
        return True


    def __init__(self, symbol, exchange_id,  orderbook_queue: asyncio.Queue, statistic_queue: asyncio.Queue = None):
        self.symbol = symbol
        self.exchange_id = exchange_id
        self.exchange = type(self).exchanges_instances_dict.get(exchange_id, None)
        self.orderbook_queue = orderbook_queue
        self.statistic_queue = statistic_queue
        self.orderbook_updating = False
        self.fee = None
        self.min_amount = None
        self.contract_size = None
        self.precision_amount = None
        if self.swap_raw_data_dict:
            self.update_swap_data()

    def update_swap_data(self):
        self.contract_size = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('contractSize', None)
        self.precision_amount = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('precision', {}).get('amount', None)
        self.min_amount = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('limits', {}).get('amount', {}).get('min', None)
        self.fee = self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('taker_fee', None) or self.swap_raw_data_dict.get(self.symbol, {}).get(self.exchange_id).get('taker', None)

    async def watch_orderbook(self, ):
        """
        Асинхронно следит за стаканом указанного символа.
        Логика переподключений и проверки валидности баланса минимально блокирует общие данные.
        """
        max_reconnect_attempts = 5
        reconnect_attempts = 0
        count = 0
        new_count = 0
        old_ask = tuple()
        old_bid = tuple()

        TICK_WINDOW_SEC = 10.0
        window_start_ts = time.monotonic()
        ticks_total = 0
        ticks_changed = 0
        statistics_dict = {}

        try:
            print(self.symbol, self.exchange_id)
            # Ждем, пока баланс инициализируется
            await self.balance_manager.get_balance_instance(self.exchange_id).wait_initialized()

            # Флаг бесконечной подписки
            self.__class__.orderbook_updating_status_dict.setdefault(self.exchange_id, {})[self.symbol] = True
            self.get_ex_orderbook_data_count.setdefault(self.exchange_id, {})[self.symbol] = 0

            while self.__class__.orderbook_updating_status_dict[self.exchange_id][self.symbol]:
                try:
                    # Берем данные, требующие блокировки, атомарно
                    bm = self.balance_manager.get_balance_instance(self.exchange_id)

                    if not await bm.is_balance_valid():
                        await asyncio.sleep(5)
                        continue

                    max_deal_volume = self.balance_manager.max_deal_volume


                    # --- WebSocket и вычисления вне блокировки ---
                    orderbook = await self.exchange.watchOrderBook(self.symbol)
                    if not isinstance(orderbook, dict) or 'asks' not in orderbook or 'bids' not in orderbook:
                        raise InvalidOrEmptyOrderBookError(
                            exchange_id=self.exchange_id, symbol=self.symbol, orderbook_data=orderbook
                        )

                    # Счётчики и статистика

                    if self.exchange_id not in self.get_ex_orderbook_data_count:
                        self.get_ex_orderbook_data_count[self.exchange_id] = {}
                    if self.symbol not in self.get_ex_orderbook_data_count[self.exchange_id]:
                        self.get_ex_orderbook_data_count[self.exchange_id][self.symbol] = 0

                    self.get_ex_orderbook_data_count[self.exchange_id][self.symbol] += 1
                    reconnect_attempts = 0
                    ticks_total += 1
                    count += 1

                    depth = min(10, len(orderbook['asks']), len(orderbook['bids']))
                    new_ask = tuple(map(tuple, orderbook['asks'][:depth]))
                    new_bid = tuple(map(tuple, orderbook['bids'][:depth]))

                    if new_ask != old_ask or new_bid != old_bid:
                        ticks_changed += 1
                        new_count += 1

                        # Расчёт средних цен
                        try:
                            average_ask = get_average_orderbook_price(orderbook['asks'],
                                                                      money=max_deal_volume,
                                                                      is_ask=True,
                                                                      log=True,
                                                                      exchange=self.exchange_id,
                                                                      symbol=self.symbol)
                            average_bid = get_average_orderbook_price(orderbook['bids'],
                                                                      money=max_deal_volume,
                                                                      is_ask=False,
                                                                      log=True,
                                                                      exchange=self.exchange_id,
                                                                      symbol=self.symbol)
                        except InsufficientOrderBookVolumeError as e:
                            cprint.warning(f"[SKIP] Недостаточный объём стакана для {self.symbol} биржи {self.exchange_id}: {e}")
                            await asyncio.sleep(0)
                            break

                        if average_ask is not None and average_bid is not None:
                            output_data = {
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

                            # Отправка в очередь без блокировки
                            try:
                                self.orderbook_queue.put_nowait(output_data)
                            except asyncio.QueueFull:
                                try:
                                    _ = self.orderbook_queue.get_nowait()
                                except asyncio.QueueEmpty:
                                    pass
                                await self.orderbook_queue.put(output_data)

                    # --- статистика по тику ---
                    if self.statistic_queue:
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
                        '1000', 'closed by remote server', 'Cannot write to closing transport',
                        'Connection closed', 'WebSocket is already closing', 'Transport closed',
                        'broken pipe', 'reset by peer'
                    ]

                    if any(x in error_str for x in transient_errors):
                        reconnect_attempts += 1
                        if reconnect_attempts > max_reconnect_attempts:
                            raise ReconnectLimitExceededError(exchange_id=self.exchange.id, symbol=self.symbol, attempts=reconnect_attempts)
                        await asyncio.sleep(4 + (2 ** reconnect_attempts))
                        continue
                    raise

        except asyncio.CancelledError:
            cprint.success_w(f"[watch_orderbook] Задача отменена: {self.symbol}")
            raise
        except Exception as e:
            cprint.error_b(f"[watch_orderbook][FATAL] Ошибка для {self.symbol}: {e}")
            raise
        finally:
            self.__class__.orderbook_updating_status_dict.setdefault(self.exchange_id, {})[self.symbol] = False




if __name__ == "__main__":
    asyncio.run(test_watch_orderbook())