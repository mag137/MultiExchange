__version__ = "1.4"

import asyncio
import signal
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
    RETRY_DELAY: float = 3.0
    BALANCE_REQUEST_TIMEOUT: float = 20.0
    _instances: Dict[str, "BalanceManager"] = {}

    def __init__(self, exchange, task_manager: TaskManager):
        self.exchange = exchange
        self.exchange_id: str = exchange.id
        self.task_manager = task_manager

        self._balance: Optional[Decimal] = None
        self._lock = asyncio.Lock()
        self.enable: bool = True
        self._initialized: asyncio.Event = asyncio.Event()
        self._last_request_ts: Optional[float] = None
        self.task_name: str = f"_BalanceTask|{self.exchange_id}"

        self.__class__._instances[self.exchange_id] = self
        self.task_manager.add_task(name=self.task_name, coro_func=self._balance_loop)

    async def _balance_loop(self) -> None:
        await self._initial_fetch()
        retry_attempts = 0
        max_backoff = 60

        while self.enable:
            try:
                balance_data = await self.exchange.watch_balance({"type": "swap"})
                await self._process_balance(balance_data)
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

    async def _initial_fetch(self) -> None:
        while self.enable:
            try:
                cprint.info_b(f"[BalanceManager][{self.exchange_id}] Fetching initial balance snapshot...")
                self._last_request_ts = time.monotonic()
                balance_data = await asyncio.wait_for(
                    self.exchange.fetch_balance({"type": "swap"}),
                    timeout=self.BALANCE_REQUEST_TIMEOUT,
                )
                await self._process_balance(balance_data)
                self._initialized.set()
                cprint.success_w(f"[BalanceManager][{self.exchange_id}] Initial balance loaded")
                return
            except asyncio.TimeoutError:
                cprint.warning_r(f"[BalanceManager][{self.exchange_id}] Initial fetch timed out")
            except Exception as e:
                cprint.error_b(f"[BalanceManager][{self.exchange_id}] Initial fetch failed: {e}")
            await asyncio.sleep(self.RETRY_DELAY)

    async def _process_balance(self, balance_data: dict) -> None:
        free_dict = balance_data.get("free", {})
        value = free_dict.get("USDT") or free_dict.get("usdt")
        await self._update_balance(value)

    async def _update_balance(self, value) -> None:
        dec_value = to_decimal(value) if value is not None else None
        async with self._lock:
            self._last_request_ts = time.monotonic()
            if self._balance != dec_value:
                self._balance = dec_value
                cprint.success_w(
                    f"[BalanceManager][{self.exchange_id}] Balance updated: {dec_value}"
                )

    async def wait_initialized(self) -> None:
        await self._initialized.wait()

    async def get_balance(self) -> Optional[Decimal]:
        async with self._lock:
            return self._balance

    async def is_balance_valid(self) -> bool:
        async with self._lock:
            if self._balance is None:
                return False
            if self._last_request_ts is None:
                return False
            return (
                    time.monotonic() - self._last_request_ts
            ) <= self.BALANCE_REQUEST_TIMEOUT

    @classmethod
    async def get_min_balance(cls) -> tuple[Optional[str], Optional[Decimal]]:
        min_balance = Decimal("Infinity")
        min_exchange_id = None
        for exchange_id, bm in cls._instances.items():
            if not await bm.is_balance_valid():
                continue
            balance = await bm.get_balance()
            if balance is not None and balance < min_balance:
                min_balance = balance
                min_exchange_id = exchange_id
        if min_exchange_id is None:
            return None, None
        return min_exchange_id, min_balance

    async def get_last_update_age(self) -> float:
        if self._last_request_ts is None:
            return float("inf")
        return time.monotonic() - self._last_request_ts

    async def get_deal_balance(self, free_deal_slot = 1):
        if free_deal_slot <= 0:
            return None
        _, min_balance = await self.__class__.get_min_balance()
        if min_balance is None:
            return None
        return min_balance * Decimal("0.9") / Decimal(free_deal_slot)

    async def stop(self) -> None:
        self.enable = False
        await self.task_manager.cancel_task(self.task_name)
        await self.task_manager.await_cancellation(name=self.task_name, timeout=5)

        self.__class__._instances.pop(self.exchange_id, None)


async def main():
    exchange_id_list = ["gateio", "okx", "poloniex"]
    task_manager = TaskManager()

    async with AsyncExitStack() as stack:
        exchange_dict = {}
        for exchange_id in exchange_id_list:
            exchange = await stack.enter_async_context(
                ExchangeInstance(ccxt, exchange_id, log=True)
            )
            exchange_dict[exchange_id] = exchange

        balance_managers = {}
        for exchange_id in exchange_dict:
            bm = BalanceManager(exchange_dict[exchange_id], task_manager)
            balance_managers[exchange_id] = bm

        for bm in balance_managers.values():
            await bm.wait_initialized()

        # Snapshot всех бирж
        cprint.info_b("[MAIN] Balance snapshot:")
        for exchange_id, bm in balance_managers.items():
            balance = await bm.get_balance()
            valid = await bm.is_balance_valid()
            cprint.info_b(f" - {exchange_id}: balance={balance}, valid={valid}")

        min_exchange, min_balance = await BalanceManager.get_min_balance()
        cprint.info_b(f"[MAIN] Minimal balance: {min_balance} at {min_exchange}")

        # Graceful shutdown
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, stop_event.set)
            loop.add_signal_handler(signal.SIGTERM, stop_event.set)
        except NotImplementedError:
            pass

        await stop_event.wait()

        for bm in balance_managers.values():
            await bm.stop()
        await task_manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
