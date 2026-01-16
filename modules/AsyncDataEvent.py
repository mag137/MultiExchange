import asyncio
from datetime import datetime

class AsyncDataEvent:
    def __init__(self):
        self.event = asyncio.Event()
        self.data = None

    def set(self, data):
        self.data = data
        self.event.set()

    def clear(self):
        self.data = None
        self.event.clear()

    async def wait(self):
        await self.event.wait()
        return self.data

    async def start(self):
        while True:
            now = datetime.now().strftime("%H:%M:%S")
            self.set({"time": now})
            await asyncio.sleep(1)
            self.clear()

async def waiter(name, event: AsyncDataEvent):
    while True:
        print(f"{name}: жду данные...")
        data = await event.wait()
        print(f"{name}: получил данные: {data}")
        event.clear()
        await asyncio.sleep(0)  # дать другим шанс обработать

async def main():
    event = AsyncDataEvent()
    await asyncio.gather(
        event.start(),
        waiter("A", event),
    )

if __name__ == "__main__":
    asyncio.run(main())
