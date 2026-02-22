from modules.exchange_instance import ExchangeInstance
import asyncio
from contextlib import AsyncExitStack
from pprint import pprint

import ccxt.pro as ccxt

exchange = ccxt.htx()

async def main():
    exchange = ccxt.htx({
        "enableRateLimit": True,
        "timeout": 30000,
        "options": {"fetchCurrencies": False},
    })
    print(exchange.id)
    try:
        await exchange.load_markets()

        while True:
            orderbook = await exchange.watch_order_book("BTC/USDT:USDT")
            print(orderbook)

    finally:
        await exchange.close()

async def main2():
    exchange_id = "gateio"

    # Запускаем менеджер контекста списка бирж - получаем контекст списка экземпляров бирж
    async with AsyncExitStack() as stack:
        exchange = await stack.enter_async_context(ExchangeInstance(ccxt, exchange_id, log=True))
        print(exchange.id)
        while True:
            orderbook = await exchange.watch_order_book(symbol='BTC/USDT:USDT')
            print(orderbook)
if __name__ == "__main__":
    asyncio.run(main2())