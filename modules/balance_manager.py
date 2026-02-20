__version__ = '2.0'
# Класс выполнен по паттерну Multiton
import asyncio
import time
from decimal import Decimal
from typing import Dict, Optional
from contextlib import AsyncExitStack
import ccxt.pro as ccxt
from modules import cprint
from modules.utils import to_decimal
from modules.exchange_instance import ExchangeInstance
from modules.task_manager import TaskManager

class BalanceManager:
    task_manager = None
    exchange_balance_instance_dict = {}
    _lock: asyncio.Lock | None = None

    RETRY_DELAY: float = 3.0
    BALANCE_REQUEST_TIMEOUT: float = 20.0

    min_balance: Optional[Decimal] = None
    exchange_min_balance: Optional[Decimal] = None
    max_deal_volume: Optional[Decimal] = None
    max_deal_slots: Optional[Decimal] = None

    @classmethod
    def create_new_balance_obj(cls, exchange):
        exchange_id = exchange.id
        # Если экземпляр для этой биржи уже есть в словаре
        if exchange_id in cls.exchange_balance_instance_dict:
            msg = f"[BalanceManager][create_new_balance_obj] - биржа '{exchange_id}': Попытка создать уже существующий экземпляр"
            cprint.error_w(msg)
            raise RuntimeError(msg)
        instance = cls(exchange)
        cls.exchange_balance_instance_dict[exchange_id] = instance
        return instance

    @classmethod
    def _get_lock(cls):
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    def get_balance_instance(cls, exchange_id):
        if exchange_id in cls.exchange_balance_instance_dict:
            return cls.exchange_balance_instance_dict.get(exchange_id)
        else:
            msg = f"[BalanceManager][get_balance_instance] Попытка запросить несуществующий объект [{exchange_id}]"
            cprint.error_w(msg)
            raise RuntimeError(msg)

    async def wait_initialized(self):
        await self._initialized_event.wait()

    @classmethod
    def remove(cls, exchange_id):
        cls.exchange_balance_instance_dict.pop(exchange_id, None)


    # Для создания экземпляра менеджера просто передаем в него экземпляр биржи
    def __init__(self, exchange):
        self.exchange = exchange
        self.exchange_id = exchange.id
        self.enable: bool = True

        # Баланс текущей биржи
        self.exchange_balance: Optional[Decimal] = None

        # Минимальный баланс среди всех бирж из экземпляров
        self.min_balance: Optional[Decimal] = self.__class__.min_balance

        # Название биржи с минимальным балансом
        self.exchange_min_balance: Optional[Decimal] = self.__class__.exchange_min_balance

        # Расчетный максимальный объем сделки
        self.max_deal_volume: Optional[Decimal] = self.__class__.max_deal_volume


        self._lock = asyncio.Lock()
        self._initialized_event = asyncio.Event()
        self._last_request_ts: Optional[float] = None

        self.task_name: str = f"_BalanceTask|{self.exchange_id}"
        if self.__class__.task_manager:
            self.__class__.task_manager.add_task(name=self.task_name, coro_func=self._watch_balance)

    # POST-запрос баланса (разовый)
    async def _fetch_balance(self) -> None:
        while self.enable:
            try:
                cprint.info_b(f"[BalanceManager][{self.exchange_id}] Fetching initial balance snapshot...")
                self._last_request_ts = time.monotonic()
                balance_data = await asyncio.wait_for(
                    self.exchange.fetch_balance({"type": "swap"}),
                    timeout=self.BALANCE_REQUEST_TIMEOUT,
                )
                free_dict = balance_data.get("free", {})
                value = free_dict.get("USDT") or free_dict.get("usdt")

                await self._update_balance(value)

                self._initialized_event.set()
                cprint.success_w(f"[BalanceManager][{self.exchange_id}] Initial balance loaded")
                return
            except asyncio.TimeoutError:
                cprint.warning_r(f"[BalanceManager][{self.exchange_id}] Initial fetch timed out")
            except Exception as e:
                cprint.error_b(f"[BalanceManager][{self.exchange_id}] Initial fetch failed: {e}")
            await asyncio.sleep(self.RETRY_DELAY)

    # WebSocket-запрос баланса (подписка)
    async def _watch_balance(self) -> None:
        await self._fetch_balance()
        retry_attempts = 0
        max_backoff = 60
        old_value = to_decimal(0)
        while self.enable:
            try:
                balance_data = await self.exchange.watch_balance({"type": "swap"})
                free_dict = balance_data.get("free", {})
                value = free_dict.get("USDT") or free_dict.get("usdt")
                if value != old_value:
                    old_value = value
                    await self._update_balance(value)
                    await self.__class__.get_min_balance()

                retry_attempts = 0

            except asyncio.CancelledError:
                raise
            except Exception as e:
                retry_attempts += 1
                backoff = min(self.RETRY_DELAY * 2 ** (retry_attempts - 1), max_backoff)
                cprint.error_b(
                    f"[BalanceManager][{self.exchange_id}] WS error: {e}. Retry in {backoff:.1f}s"
                )
                await asyncio.sleep(backoff)

        cprint.warning_r(f"[BalanceManager][{self.exchange_id}] Balance loop stopped")

    async def _update_balance(self, value) -> None:
        dec_value = to_decimal(value) if value is not None else None
        async with self._lock:
            self._last_request_ts = time.monotonic()
            if self.exchange_balance != dec_value:
                self.exchange_balance = dec_value
                cprint.success_w(
                    f"[BalanceManager][{self.exchange_id}] Balance updated: {dec_value}"
                )

    async def get_balance(self) -> Optional[Decimal]:
        async with self._lock:
            return self.exchange_balance

    async def is_balance_valid(self) -> bool:
        async with self._lock:
            if self.exchange_balance is None:
                return False
            if self._last_request_ts is None:
                return False
            return (time.monotonic() - self._last_request_ts) <= self.BALANCE_REQUEST_TIMEOUT

    @classmethod
    async def get_min_balance(cls) -> tuple[Optional[str], Optional[Decimal]]:
        min_balance = Decimal("Infinity")
        min_exchange_id = None
        for exchange_id, obj in cls.exchange_balance_instance_dict.items():
            await obj.wait_initialized()
            if not await obj.is_balance_valid():
                continue
            balance = await obj.get_balance()
            if balance is not None and balance < min_balance:
                min_balance = balance
                min_exchange_id = exchange_id

        cls.min_balance = min_balance
        cls.exchange_min_balance = min_exchange_id
        if min_exchange_id is None:
            return None, None
        if cls.max_deal_slots:
            cls.max_deal_volume = to_decimal('0.9') * cls.min_balance / cls.max_deal_slots
            # Синхронизация с экземплярами
            for obj in cls.exchange_balance_instance_dict.values():
                obj.min_balance = cls.min_balance
                obj.exchange_min_balance = cls.exchange_min_balance
                obj.max_deal_volume = cls.max_deal_volume
        return min_exchange_id, min_balance

async def main():
    exchange_id_list = ["gateio", "okx", "poloniex"]
    BalanceManager.task_manager = TaskManager()
    BalanceManager.max_deal_slots = to_decimal('2')
    # Запускаем менеджер контекста списка бирж - получаем контекст списка экземпляров бирж
    async with AsyncExitStack() as stack:
        exchange_dict = {}
        for exchange_id in exchange_id_list:
            exchange = await stack.enter_async_context(
                ExchangeInstance(ccxt, exchange_id, log=True)
            )
            exchange_dict[exchange_id] = exchange
            BalanceManager.create_new_balance_obj(exchange)

        # Ждём инициализации балансов
        for exchange_id, bm in BalanceManager.exchange_balance_instance_dict.items():
            await bm.wait_initialized()

        # Получаем минимальный баланс
        min_ex_id, min_balance = await BalanceManager.get_min_balance()
        print(f"Минимальный баланс: {min_balance} на бирже {min_ex_id}")
        print(BalanceManager.exchange_min_balance, BalanceManager.min_balance)
        print(BalanceManager.max_deal_volume)
        print(BalanceManager.exchange_balance_instance_dict['okx'].max_deal_volume)


if __name__ == "__main__":
    asyncio.run(main())