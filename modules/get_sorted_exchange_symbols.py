__version__ = "1.4"

import asyncio
import ccxt.pro as ccxt
from modules import get_exchange_instance
from modules.colored_console import cprint
from pprint import pprint


class Get_Sorted_Pairs:
    """
        Класс для классификации торговых пар биржи по типам (спотовые, свопы, фьючерсы, опционы).

        Пример использования:
            async def main():
                # Создаем объект класса для биржи Bybit
                obj = await GetSortedPairs.create('bybit')

                # Выводим список свопов
                pprint(obj.swap)

            if __name__ == "__main__":
                asyncio.run(main())

        Методы:
            - create: Асинхронный метод для создания экземпляра класса и получения данных.
            - get_sorted: Асинхронный метод для загрузки и классификации рынков.
            - print_results: Выводит результаты классификации в консоль.
        """
    def __init__(self, exchange):
        """
                Инициализация экземпляра класса.

                Args:
                    exchange_id (str): Идентификатор биржи (например, 'bybit', 'binance').
                """
        # self.exchange_id = exchange_id
        # self.exchange = get_exchange_instance(ccxt, self.exchange_id)
        self.exchange = exchange
        print(f"Создан экземпляр биржи: {self.exchange.id}")
        self.spot = {}
        self.swap = {}
        self.future = {}
        self.option = {}
        self.future_spot = {}
        self.spot_future = {}
        self.swap_spot = {}
        self.spot_swap = {}
        self.spot_swap_set = set()
        self.spot_swap_list = []

    async def get_sorted(self):
        """
                Загружает данные о рынках через WebSocket API и классифицирует их по типам:
                спотовые, свопы, фьючерсы и опционы.

                Raises:
                    Exception: Если произошла ошибка при загрузке данных.
                """
        try:
            # Загружаем данные о рынках через WebSocket API
            markets = await self.exchange.load_markets()
            for pair, data in markets.items():
                if not data['active']:
                    continue  # Пропускаем неактивные рынки

                spot = data.get('spot', False)
                contract = data.get('contract', False)
                future = data.get('future', False)
                swap = data.get('swap', False)
                option = data.get('option', False)
                type_ = data.get('type', None)

                # Классифицируем инструменты
                if spot:
                    self.spot[pair] = data  # Сохраняем данные в словаре
                if swap:
                    self.swap[pair] = data
                    # pprint(data)
                    # funding_rate = await self.exchange.fetch_funding_rate(pair)
                    # cprint.info_w(f"{pair} {funding_rate}")
                if future:
                    self.future[pair] = data
                if option:
                    self.option[pair] = data

        except Exception as e:
            print(f"Произошла ошибка при получении данных: {e}")
        finally:
            # Закрываем соединение с биржей
            if hasattr(self.exchange, 'close'):
                await self.exchange.close()

    async def print_results(self):
        # Выводим результаты классификации
        print("\n--- Список спотовых рынков ---")
        pprint(self.spot)

        print("\n--- Список свопов ---")
        pprint(self.swap)

        print("\n--- Список фьючерсов ---")
        pprint(self.future)

        print("\n--- Список опционов ---")
        pprint(self.option)

    async def get_cross_coin(self): # получим монеты которые встречаются и на споте, и на свопе, и на фьючерсе
        if self.future and self.spot:
            for spot_symbol, spot_data in self.spot.items():
                for future_symbol, future_data in self.future.items():
                    if spot_data['base'] == future_data['base'] and spot_data['quote'] == future_data['quote']:
                        self.future_spot[future_symbol] = str(spot_symbol)
                        self.spot_future[spot_symbol] = str(future_symbol)
                for swap_symbol, swap_data in self.swap.items():
                    if spot_data['base'] == swap_data['base'] and spot_data['quote'] == swap_data['quote']:
                        self.swap_spot[swap_symbol] = str(spot_symbol)
                        self.spot_swap[spot_symbol] = str(swap_symbol)
                        self.spot_swap_set.add(str(spot_symbol))
                        self.spot_swap_set.add(str(swap_symbol))
        self.spot_swap_list = [[key, value] for key, value in self.spot_swap.items()]
        pass

    @classmethod
    async def create(cls, exchange):
        """
                Создает экземпляр класса и загружает данные о рынках.

                Args:
                    exchange_id (str): Идентификатор биржи (например, 'bybit', 'binance').

                Returns:
                    GetSortedPairs: Экземпляр класса с загруженными данными.
                """
        # Создаем объект класса
        instance = cls(exchange)
        # Вызываем асинхронный метод
        await instance.get_sorted()
        await instance.get_cross_coin()
        return instance


async def main():
    # Создаем объект класса GetSortedPairs для биржи Bybit
    exchange = get_exchange_instance(ccxt, 'bybit')
    obj = await Get_Sorted_Pairs.create(exchange)

    # Выводим результаты
    # await obj.print_results()
    # pprint(obj.future_spot)
    pprint(obj.swap_spot)
    # pprint(obj.spot_swap_set)
    # print(type(obj.spot_swap_set))
    pprint(list(obj.swap_spot.items()))
    pprint(obj.spot_swap_list)


if __name__ == "__main__":
    # Запускаем асинхронную функцию main
    asyncio.run(main())
__version__ = "1.4"

import asyncio
import ccxt.pro as ccxt
from modules import get_exchange_instance
from modules.colored_console import cprint
from pprint import pprint


class Get_Sorted_Pairs:
    """
        Класс для классификации торговых пар биржи по типам (спотовые, свопы, фьючерсы, опционы).

        Пример использования:
            async def main():
                # Создаем объект класса для биржи Bybit
                obj = await GetSortedPairs.create('bybit')

                # Выводим список свопов
                pprint(obj.swap)

            if __name__ == "__main__":
                asyncio.run(main())

        Методы:
            - create: Асинхронный метод для создания экземпляра класса и получения данных.
            - get_sorted: Асинхронный метод для загрузки и классификации рынков.
            - print_results: Выводит результаты классификации в консоль.
        """
    def __init__(self, exchange):
        """
                Инициализация экземпляра класса.

                Args:
                    exchange_id (str): Идентификатор биржи (например, 'bybit', 'binance').
                """
        # self.exchange_id = exchange_id
        # self.exchange = get_exchange_instance(ccxt, self.exchange_id)
        self.exchange = exchange
        print(f"Создан экземпляр биржи: {self.exchange.id}")
        self.spot = {}
        self.swap = {}
        self.future = {}
        self.option = {}
        self.future_spot = {}
        self.spot_future = {}
        self.swap_spot = {}
        self.spot_swap = {}
        self.spot_swap_set = set()
        self.spot_swap_list = []

    async def get_sorted(self):
        """
                Загружает данные о рынках через WebSocket API и классифицирует их по типам:
                спотовые, свопы, фьючерсы и опционы.

                Raises:
                    Exception: Если произошла ошибка при загрузке данных.
                """
        try:
            # Загружаем данные о рынках через WebSocket API
            markets = await self.exchange.load_markets()
            for pair, data in markets.items():
                if not data['active']:
                    continue  # Пропускаем неактивные рынки

                spot = data.get('spot', False)
                contract = data.get('contract', False)
                future = data.get('future', False)
                swap = data.get('swap', False)
                option = data.get('option', False)
                type_ = data.get('type', None)

                # Классифицируем инструменты
                if spot:
                    self.spot[pair] = data  # Сохраняем данные в словаре
                if swap:
                    self.swap[pair] = data
                    # pprint(data)
                    # funding_rate = await self.exchange.fetch_funding_rate(pair)
                    # cprint.info_w(f"{pair} {funding_rate}")
                if future:
                    self.future[pair] = data
                if option:
                    self.option[pair] = data

        except Exception as e:
            print(f"Произошла ошибка при получении данных: {e}")
        finally:
            # Закрываем соединение с биржей
            if hasattr(self.exchange, 'close'):
                await self.exchange.close()

    async def print_results(self):
        # Выводим результаты классификации
        print("\n--- Список спотовых рынков ---")
        pprint(self.spot)

        print("\n--- Список свопов ---")
        pprint(self.swap)

        print("\n--- Список фьючерсов ---")
        pprint(self.future)

        print("\n--- Список опционов ---")
        pprint(self.option)

    async def get_cross_coin(self): # получим монеты которые встречаются и на споте, и на свопе, и на фьючерсе
        if self.future and self.spot:
            for spot_symbol, spot_data in self.spot.items():
                for future_symbol, future_data in self.future.items():
                    if spot_data['base'] == future_data['base'] and spot_data['quote'] == future_data['quote']:
                        self.future_spot[future_symbol] = str(spot_symbol)
                        self.spot_future[spot_symbol] = str(future_symbol)
                for swap_symbol, swap_data in self.swap.items():
                    if spot_data['base'] == swap_data['base'] and spot_data['quote'] == swap_data['quote']:
                        self.swap_spot[swap_symbol] = str(spot_symbol)
                        self.spot_swap[spot_symbol] = str(swap_symbol)
                        self.spot_swap_set.add(str(spot_symbol))
                        self.spot_swap_set.add(str(swap_symbol))
        self.spot_swap_list = [[key, value] for key, value in self.spot_swap.items()]
        pass

    @classmethod
    async def create(cls, exchange):
        """
                Создает экземпляр класса и загружает данные о рынках.

                Args:
                    exchange_id (str): Идентификатор биржи (например, 'bybit', 'binance').

                Returns:
                    GetSortedPairs: Экземпляр класса с загруженными данными.
                """
        # Создаем объект класса
        instance = cls(exchange)
        # Вызываем асинхронный метод
        await instance.get_sorted()
        await instance.get_cross_coin()
        return instance


async def main():
    # Создаем объект класса GetSortedPairs для биржи Bybit
    exchange = get_exchange_instance(ccxt, 'bybit')
    obj = await Get_Sorted_Pairs.create(exchange)

    # Выводим результаты
    # await obj.print_results()
    # pprint(obj.future_spot)
    pprint(obj.swap_spot)
    # pprint(obj.spot_swap_set)
    # print(type(obj.spot_swap_set))
    pprint(list(obj.swap_spot.items()))
    pprint(obj.spot_swap_list)


if __name__ == "__main__":
    # Запускаем асинхронную функцию main
    asyncio.run(main())
