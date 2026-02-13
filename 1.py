import asyncio
from contextlib import AsyncExitStack
from pprint import pprint
from typing import Dict, List

# Импортируем твой класс и ExchangeInstance
from modules.exchange_instance import ExchangeInstance

import ccxt.pro as ccxt  # ccxt.pro нужен для ExchangeInstance

class Arbitr:
    arbitrage_obj_dict = {}

    def __init__(self, symbol, swap_pairs_raw_data_dict, swap_pairs_processed_data_dict):
        self.symbol = symbol
        self.swap_pairs_raw_data_dict = swap_pairs_raw_data_dict
        self.swap_pairs_processed_data_dict = swap_pairs_processed_data_dict


    # Инициализация данных списка бирж
    @classmethod
    def create_obj(cls, symbol: str, swap_pairs_raw_data_dict: Dict, swap_pairs_processed_data_dict: Dict):
        return cls(symbol, swap_pairs_raw_data_dict, swap_pairs_processed_data_dict)

    @classmethod
    async def init_exchanges_pairs_data(cls, exchanges_id_list):
        cls.swap_pairs_raw_data_dict = {}                # Словарь сырых с биржи данных для открытия сделок
        cls.swap_pairs_processed_data_dict = {}          # Словарь распарсенных с биржи данных для открытия сделок
        async with AsyncExitStack() as stack:
            async def open_exchange(exchange_id):
                return exchange_id, await stack.enter_async_context(ExchangeInstance(ccxt, exchange_id, log=True))

            results = await asyncio.gather(*(open_exchange(exchange_id) for exchange_id in exchanges_id_list))
            exchanges_instance_dict = dict(results)

            for exchange_id, exchange in exchanges_instance_dict.items():
                print(exchange.id)
                for pair_data in exchange.spot_swap_pair_data_dict.values():
                    if swap_data := pair_data.get("swap"):
                        if not swap_data or swap_data.get("settle") != "USDT":
                            continue
                        symbol = swap_data["symbol"]
                        if swap_data.get('linear') and not swap_data.get('inverse'):
                            cls.swap_pairs_raw_data_dict.setdefault(symbol, {})[exchange_id] = swap_data

            # Парсинг данных для cls.swap_pairs_processed_data_dict
            for symbol, volume in cls.swap_pairs_raw_data_dict.items():
                if len(volume) < 2:
                    continue
                max_contract_size = 0  # Максимальный размер единичного контракта среди бирж символа
                # объем сделки должен быть рассчитан исходя из максимального размера контрактов в группе и кратен максимальному размеру
                for exchange_id, data in volume.items():
                    contract_size = data.get('contractSize')
                    if max_contract_size < contract_size:
                        max_contract_size = contract_size
                    cls.swap_pairs_processed_data_dict.setdefault(symbol, {}).setdefault(exchange_id, {})['contractSize'] = contract_size
                for exchange_id, data in volume.items():
                    cls.swap_pairs_processed_data_dict[symbol][exchange_id]['max_contractSize'] = max_contract_size

            for symbol in cls.swap_pairs_processed_data_dict.keys():
                cls.arbitrage_obj_dict[symbol]=cls.create_obj(symbol=symbol,
                                                              swap_pairs_raw_data_dict=cls.swap_pairs_raw_data_dict,
                                                              swap_pairs_processed_data_dict=cls.swap_pairs_processed_data_dict)

            pprint(cls.swap_pairs_processed_data_dict)


async def main():
    exchanges_id_list = ["phemex", "okx", "gateio"]

    # Инициализация данных с бирж и создание объектов Arbitr
    await Arbitr.init_exchanges_pairs_data(exchanges_id_list)

    # Доступ к объектам для конкретной пары
    for symbol, arbitr_obj in Arbitr.arbitrage_obj_dict.items():
        print(f"Объект для {symbol}:")
        print("Сырые данные:", arbitr_obj.swap_pairs_raw_data_dict.get(symbol))
        print("Обработанные данные:", arbitr_obj.swap_pairs_processed_data_dict.get(symbol))
        print("-" * 60)

if __name__ == "__main__":
    asyncio.run(main())
