import asyncio
from typing import Any, Optional


class DataEventContext:
    def __init__(self, event: "AsyncDataEvent"):
        self.event = event
        self.data = None

    async def __aenter__(self):
        await self.event.wait()
        self.data = self.event.data
        return self.data

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.event.clear()


class AsyncDataEvent(asyncio.Event):
    def __init__(self):
        super().__init__()
        self._data: Optional[Any] = None
        self._other_data: Optional[Any] = None

    def set_data(self, data: Any, other_data: Optional[Any] = None) -> None:
        self._data = data
        self._other_data = other_data
        super().set()

    @property
    def data(self) -> Any:
        return self._data

    @property
    def other_data(self) -> Any:
        return self._other_data

    def clear(self) -> None:
        self._data = None
        self._other_data = None
        super().clear()

    # 🔴 Этого не хватало!
    def context(self):
        return DataEventContext(self)


# Пример использования
async def sender(event: AsyncDataEvent):
    await asyncio.sleep(1)
    event.set_data("Переданные данные", other_data="доп. инфа")


async def receiver(event: AsyncDataEvent):
    async with event.context() as data:
        print("Получено:", data)
        print("Доп. данные:", event.other_data)


async def main():
    event = AsyncDataEvent()
    await asyncio.gather(sender(event), receiver(event))


if __name__ == "__main__":
    asyncio.run(main())