__version__ = "1.0"

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
    """
    Асинхронный менеджер баланса для одной биржи.

    Подписывается на WebSocket и обновляет баланс USDT в реальном времени.
    Первичная синхронизация выполняется через REST API.

    Attributes:
        exchange: Экземпляр биржи ccxt.pro.
        exchange_id: Идентификатор биржи.
        task_manager: TaskManager для управления задачами.
        _balance: Текущий баланс USDT (Decimal) или -1 при ошибке.
        _lock: asyncio.Lock для защиты чтения/записи баланса.
        enable: Флаг работы цикла.
        _initialized: asyncio.Event для ожидания первичной загрузки.
        _last_update_ts: Время последнего обновления баланса (timestamp).
    """

    RETRY_DELAY: float = 3.0  # базовая задержка при ошибках

    def __init__(self, exchange, task_manager: TaskManager):
        self.exchange = exchange
        self.exchange_id: str = exchange.id
        self.task_manager = task_manager

        self._balance: Decimal = Decimal("-1")
        self._lock = asyncio.Lock()
        self.enable: bool = True
        self._initialized: asyncio.Event = asyncio.Event()
        self._last_update_ts: Optional[float] = None
        self.task_name: str = f"_BalanceTask|{self.exchange_id}"

        # Запуск фонового цикла через TaskManager
        self.task_manager.add_task(name=self.task_name, coro_func=self._balance_loop)

    async def _balance_loop(self) -> None:
        """
        Основной асинхронный цикл обновления баланса через WebSocket.
        В случае ошибки выполняется экспоненциальный бэкофф.
        """
        await self._initial_fetch()

        retry_attempts = 0
        max_backoff = 60  # макс задержка между попытками восстановления

        while self.enable:
            try:
                balance_data = await self.exchange.watch_balance({"type": "swap"})
                await self._process_balance(balance_data)
                retry_attempts = 0  # сброс попыток после успешного обновления
            except Exception as e:
                retry_attempts += 1
                backoff = min(self.RETRY_DELAY * 2 ** (retry_attempts - 1), max_backoff)
                cprint.error_b(
                    f"[BalanceManager][{self.exchange_id}] WS error: {e}. Retry in {backoff:.1f}s"
                )
                await asyncio.sleep(backoff)

        cprint.warning_r(f"[BalanceManager][{self.exchange_id}] Balance loop stopped")

    async def _initial_fetch(self) -> None:
        """
        Первичная загрузка баланса через REST перед подпиской на WebSocket.
        Цикл продолжается до успешного получения баланса.
        """
        while self.enable:
            try:
                cprint.info_b(
                    f"[BalanceManager][{self.exchange_id}] Fetching initial balance snapshot..."
                )
                balance_data = await self.exchange.fetch_balance({"type": "swap"})
                await self._process_balance(balance_data)
                self._initialized.set()
                cprint.success_w(f"[BalanceManager][{self.exchange_id}] Initial balance loaded")
                return
            except Exception as e:
                cprint.error_b(
                    f"[BalanceManager][{self.exchange_id}] Initial fetch failed: {e}"
                )
                await asyncio.sleep(self.RETRY_DELAY)

    async def _process_balance(self, balance_data: dict) -> None:
        """
        Обработка данных баланса и обновление внутреннего состояния.

        Args:
            balance_data: словарь баланса, возвращаемый биржей
        """
        free_dict = balance_data.get("free", {})
        value = free_dict.get("USDT") or free_dict.get("usdt")

        if value is None:
            value = Decimal("-1")
            cprint.warning_r(
                f"[BalanceManager][{self.exchange_id}] USDT not found, using {value}"
            )

        await self._update_balance(value)

    async def _update_balance(self, value) -> None:
        """
        Обновление внутреннего баланса с логированием изменений.

        Args:
            value: новое значение баланса
        """
        dec_value = to_decimal(value)
        async with self._lock:
            if self._balance != dec_value:
                self._balance = dec_value
                self._last_update_ts = time.monotonic()
                cprint.success_w(
                    f"[BalanceManager][{self.exchange_id}] Balance updated: {dec_value} USDT"
                )

    async def wait_initialized(self) -> None:
        """Ожидание первичной загрузки баланса."""
        await self._initialized.wait()

    async def get_balance(self) -> Decimal:
        """
        Получение текущего баланса (асинхронно).

        Returns:
            Decimal: баланс USDT, -1 если не получен.
        """
        async with self._lock:
            return self._balance

    async def get_last_update_age(self) -> float:
        """
        Возраст последнего обновления баланса в секундах.

        Returns:
            float: возраст последнего обновления
        """
        if self._last_update_ts is None:
            return float("inf")
        return time.monotonic() - self._last_update_ts

    async def stop(self) -> None:
        """Остановка цикла обновления и отмена задачи в TaskManager."""
        self.enable = False
        self.task_manager.cancel_task(self.task_name)
        await self.task_manager.await_cancellation(name=self.task_name, timeout=5)


async def main():
    """
    Демонстрационный runtime для тестирования BalanceManager.

    Выполняет:
        • Создание подключений к биржам
        • Запуск менеджеров баланса
        • Ожидание первичной синхронизации
        • Вывод текущих балансов
        • Graceful shutdown
    """
    exchange_id_list = ["gateio", "okx"]
    task_manager = TaskManager()

    async with AsyncExitStack() as stack:

        # Создаём объекты бирж
        exchange_dict = {}
        for exchange_id in exchange_id_list:
            exchange = await stack.enter_async_context(
                ExchangeInstance(ccxt, exchange_id, log=True)
            )
            exchange_dict[exchange_id] = exchange

        # Создаём менеджеры баланса
        balance_managers = {}
        for exchange_id in exchange_dict:
            exchange = exchange_dict[exchange_id]
            bm = BalanceManager(exchange, task_manager)
            balance_managers[exchange_id] = bm

        # Ожидаем первичную загрузку всех балансов
        for exchange_id in balance_managers:
            bm = balance_managers[exchange_id]
            await bm.wait_initialized()

        # Вывод текущих балансов
        for exchange_id in balance_managers:
            bm = balance_managers[exchange_id]
            balance = await bm.get_balance()
            cprint.info_b(f"[MAIN] {exchange_id} balance = {balance} USDT")

        # Graceful shutdown
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, stop_event.set)
            loop.add_signal_handler(signal.SIGTERM, stop_event.set)
        except NotImplementedError:
            pass

        await stop_event.wait()

        # Остановка всех менеджеров и TaskManager
        for exchange_id in balance_managers:
            bm = balance_managers[exchange_id]
            await bm.stop()

        await task_manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
