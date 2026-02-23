__version__ = '3.1'
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

    # УБИРАЕМ BALANCE_REQUEST_TIMEOUT
    # BALANCE_REQUEST_TIMEOUT: float = 20.0

    min_balance: Optional[Decimal] = None
    exchange_min_balance: Optional[Decimal] = None
    max_deal_volume: Optional[Decimal] = None
    max_deal_slots: Optional[Decimal] = None

    _volume_ready_event: asyncio.Event | None = None

    @classmethod
    def create_new_balance_obj(cls, exchange):
        exchange_id = exchange.id
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
        if self.__class__._volume_ready_event is not None:
            await self.__class__._volume_ready_event.wait()

    async def _compute_volume(self):
        await self.__class__.get_min_balance()
        if self.__class__._volume_ready_event is None:
            self.__class__._volume_ready_event = asyncio.Event()
        self.__class__._volume_ready_event.set()

    @classmethod
    def remove(cls, exchange_id):
        cls.exchange_balance_instance_dict.pop(exchange_id, None)

    def __init__(self, exchange):
        self.exchange = exchange
        self.exchange_id = exchange.id
        self.enable: bool = True

        self.exchange_balance: Optional[Decimal] = None

        self.min_balance: Optional[Decimal] = self.__class__.min_balance
        self.exchange_min_balance: Optional[Decimal] = self.__class__.exchange_min_balance
        self.max_deal_volume: Optional[Decimal] = self.__class__.max_deal_volume

        self._lock = asyncio.Lock()
        self._initialized_event = asyncio.Event()

        self.task_name: str = f"_BalanceTask|{self.exchange_id}"
        if self.__class__.task_manager:
            self.__class__.task_manager.add_task(name=self.task_name, coro_func=self._watch_balance)

    async def _fetch_balance(self) -> None:
        while self.enable:
            try:
                cprint.info_b(f"[BalanceManager][{self.exchange_id}] Fetching initial balance snapshot...")
                balance_data = await self.exchange.fetch_balance({"type": "swap"})
                free_dict = balance_data.get("free", {})
                value = free_dict.get("USDT", free_dict.get("usdt"))

                await self._update_balance(value)

                self._initialized_event.set()
                cprint.success_w(f"[BalanceManager][{self.exchange_id}] Initial balance loaded")
                return
            except Exception as e:
                cprint.error_b(f"[BalanceManager][{self.exchange_id}] Initial fetch failed: {e}")
            await asyncio.sleep(self.RETRY_DELAY)

    async def _watch_balance(self) -> None:
        await self._fetch_balance()
        await self._compute_volume()

        retry_attempts = 0
        max_backoff = 60
        old_value = self.exchange_balance

        while self.enable:
            try:
                balance_data = await self.exchange.watch_balance({"type": "swap"})
                free_dict = balance_data.get("free", {})
                value = free_dict.get("USDT") or free_dict.get("usdt")

                # Обновляем баланс только если изменился
                if value != old_value:
                    old_value = value
                    await self._update_balance(value)
                    await self._compute_volume()

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

        # ❗ Защита: игнорируем None
        if dec_value is None:
            cprint.warning_r(f"[BalanceManager][{self.exchange_id}] Ignoring None balance update")
            return

        async with self._lock:
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
            # Баланс должен быть получен
            if self.exchange_balance is None:
                return False

            # Баланс должен быть > 0
            if self.exchange_balance <= 0:
                return False

            # Баланс валиден
            return True

    @classmethod
    async def get_min_balance(cls) -> tuple[Optional[str], Optional[Decimal]]:
        min_balance = Decimal("Infinity")
        min_exchange_id = None

        for exchange_id, obj in cls.exchange_balance_instance_dict.items():
            balance = await obj.get_balance()
            if balance is None:
                continue
            if balance < min_balance:
                min_balance = balance
                min_exchange_id = exchange_id

        cls.min_balance = min_balance
        cls.exchange_min_balance = min_exchange_id

        if min_exchange_id is None:
            return None, None

        if cls.max_deal_slots:
            cls.max_deal_volume = to_decimal('0.9') * cls.min_balance / cls.max_deal_slots

            for obj in cls.exchange_balance_instance_dict.values():
                obj.min_balance = cls.min_balance
                obj.exchange_min_balance = cls.exchange_min_balance
                obj.max_deal_volume = cls.max_deal_volume

        return min_exchange_id, min_balance

async def main():
    exchange_id_list = ["okx", "htx", "gateio"]
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