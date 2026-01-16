import time
import asyncio
import ccxt.pro as ccxt
from collections import deque

class TickRateCounter:
    """
    TickRateCounter — измерение частоты поступления событий за скользящее окно времени.

    Класс реализует FIFO-буфер временных меток с ограничением по времени, позволяющий отслеживать,
    сколько раз за последнее фиксированное количество секунд происходили события (например, обновления котировок).
    Полезно для оценки ликвидности, активности торговых пар и других реальных потоков данных.

    Параметры
    ----------
    window_seconds : int, optional
        Размер скользящего временного окна в секундах. По умолчанию 60 секунд.

    Атрибуты
    ----------
    window : int
        Длительность временного окна, в течение которого учитываются события.
    buffer : collections.deque
        Двусторонняя очередь, в которой хранятся временные метки (`float`) каждого события.

    Примеры
    --------
    # >>> counter = TickRateCounter(window_seconds=60)
    # >>> while True:
    # ...     receive_tick()  # здесь можно вызывать обработчик входящего события
    # ...     print(f"Ticks in last 60 seconds: {counter.tick()}")

    Примечания
    ----------
    Класс не использует асинхронных конструкций и потокобезопасность не гарантируется.
    Каждый экземпляр следует использовать из одного контекста (корутины или потока).
    """

    def __init__(self, window_seconds: int = 60):
        """
        Инициализация счетчика частоты событий.

        Параметры
        ----------
        window_seconds : int, optional
            Размер окна (в секундах), в течение которого учитываются события. По умолчанию 60.
        """
        self.window = window_seconds
        self.buffer = deque()

    def tick(self) -> int:
        now = time.time()
        self.buffer.append(now)

        print(f"[DEBUG] before cleanup: {len(self.buffer)} events")

        while self.buffer and now - self.buffer[0] > self.window:
            self.buffer.popleft()

        print(f"[DEBUG] after cleanup: {len(self.buffer)} events")

        return len(self.buffer)


# Пример использования
async def subscribe_and_count(symbol: str):
    exchange = ccxt.binance({'enableRateLimit': True})
    counter = TickRateCounter(window_seconds=60)

    await exchange.load_markets()
    print(f"Подписка на {symbol}...")

    while True:
        try:
            ob = await exchange.watchOrderBook(symbol)

            tick_count = counter.tick()
            print(f"{symbol}: {tick_count} ticks за последние 60 секунд")
        except Exception as e:
            print(f"Ошибка при получении данных: {e}")
            await asyncio.sleep(5)


async def main():
    await subscribe_and_count('BTC/USDT')


if __name__ == "__main__":
    asyncio.run(main())

