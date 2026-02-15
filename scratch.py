import asyncio
import ccxt.async_support as ccxt
from pprint import pprint
API_KEY = "Y1RWE7J3-ID5L2N9Z-CXUVIV8X-UB12YXZ7"
API_SECRET = "f7458883d513af22a2cbc2ef32121f521c6eb96a1517cd2ed96ee0fd893e4eb814c44f4bd3df3d8a1ccbcd89d884f5855206c3b804ac739895c97bce0ac74b60"

async def main():
    exchange = ccxt.poloniex({
        "apiKey": API_KEY,
        "secret": API_SECRET,
    })

    try:
        await exchange.load_markets()
        print("Markets loaded:", len(exchange.markets))

        balance = await exchange.fetch_balance({ "type": "swap"})
        pprint(balance)
        print("=== TOTAL BALANCE ===")
        for coin, amount in balance["total"].items():
            if amount and amount > 0:
                print(f"{coin}: {amount}")

    finally:
        await exchange.close()

asyncio.run(main())
