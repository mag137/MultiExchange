__version__ = '6.0'

import ssl
import certifi
import aiohttp
import asyncio
import ccxt.pro as ccxt
import json
import logging
import os
import hashlib
import time

from pathlib import Path
from typing import Optional, Dict, Any
from types import ModuleType
from modules.logger import LoggerFactory
from modules.market_sort_data import MarketsSortData
from modules.time_sync import sync_time_with_exchange
from pprint import pprint


# ===============================================================
# ЛОГГЕР
# ===============================================================
logger = LoggerFactory.get_logger(
    name="exchange_instance",
    log_filename="ex_data.log",
    level=logging.DEBUG,
    split_levels=False,
    use_timed_rotating=True,
    use_dated_folder=True,
    add_date_to_filename=False,
    add_time_to_filename=True,
    base_logs_dir=os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'logs')
    )
)


# ===============================================================
# ОСНОВНОЙ КЛАСС EXCHANGEINSTANCE
# ===============================================================
class ExchangeInstance:
    def __init__(
            self,
            ccxt_module: ModuleType,
            exchange_id: str,
            params: Optional[Dict[str, Any]] = None,
            sandbox: bool = False,
            update_interval: int = 600,
            updating_markets: bool = True,
            log: bool = False
    ) -> None:

        self.ccxt_module = ccxt_module
        self.exchange_id = exchange_id
        self.params = params.copy() if params else {}
        self.sandbox = sandbox
        self.update_interval = update_interval
        self.markets_updating = updating_markets
        self.log = log

        self.upload_counter: int = 0
        self._market_updater_task: Optional[asyncio.Task] = None

        self.session: Optional[aiohttp.ClientSession] = None
        self.exchange: Optional[ccxt.Exchange] = None

        # Данные
        self.api_keys: Dict[str, str] = {}
        self.dict_api: Dict[str, Dict[str, str]] = {}
        self.markets: Optional[Dict[str, Any]] = None
        self.MarketsSortData_instance: Optional[MarketsSortData] = None
        self.spot_swap_pair_data_dict: Dict[str, Any] = {}

        # Для контроля изменений
        self._last_markets_hash: Optional[str] = None
        self._last_pairs_hash: Optional[str] = None

        # Пути
        current_file_path = Path(__file__).resolve()
        self.project_root = current_file_path.parents[1]
        self.source_path = self.project_root / 'source'

        # Загрузка ключей
        self._load_api_keys()


    # ===============================================================
    # CCXT ПРОКСИ МЕТОДЫ
    # ===============================================================
    async def fetch_balance(self, params=None):
        return await self.exchange.fetch_balance(params or {})

    async def fetch_positions(self, params=None):
        return await self.exchange.fetch_positions(params or {})

    async def watch_balance(self, params=None):
        return await self.exchange.watch_balance(params or {})

    async def transfer(self, **kwargs):
        return await self.exchange.transfer(**kwargs)

    async def fetch_my_trades(self, **kwargs):
        return await self.exchange.fetch_my_trades(**kwargs)


    # ===============================================================
    # ЗАГРУЗКА КЛЮЧЕЙ
    # ===============================================================
    def _load_api_keys(self) -> None:
        api_file = self.source_path / ('api_keys_sand.json' if self.sandbox else 'api_keys.json')

        try:
            with open(api_file, 'r', encoding='utf-8') as f:
                self.dict_api = json.load(f)
                self.api_keys = self.dict_api.get(self.exchange_id, {})

            if self.log:
                logger.debug(f"[{self.exchange_id}] API ключи загружены из {api_file}")

        except FileNotFoundError:
            logger.warning(f"[{self.exchange_id}] Файл API ключей не найден: {api_file}")

        except json.JSONDecodeError:
            logger.error(f"[{self.exchange_id}] Ошибка JSON в {api_file}")


    # ===============================================================
    # SSL CONTEXT
    # ===============================================================
    @staticmethod
    def _create_ssl_context() -> ssl.SSLContext:
        context = ssl.create_default_context()
        context.load_verify_locations(cafile=certifi.where())
        return context


    # ===============================================================
    # ENTER / EXIT — КОНТЕКСТНЫЙ МЕНЕДЖЕР
    # ===============================================================
    async def __aenter__(self):
        try:
            ssl_context = self._create_ssl_context()

            self.session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=ssl_context)
            )

            self.params.update({
                'apiKey': self.api_keys.get('API', ''),
                'secret': self.api_keys.get('Secret', ''),
                'password': self.api_keys.get('API_pass', ''),
                'session': self.session,
                'timeout': 30000,
            })

            if self.exchange_id == 'gateio':
                uid = self.api_keys.get('uid')
                if not uid:
                    logger.error("[gateio] отсутствует uid в api_keys.json")
                    raise ValueError("gateio требует ключ uid")
                self.params['uid'] = uid

            exchange_class = getattr(self.ccxt_module, self.exchange_id)
            self.exchange = exchange_class(self.params)

            if self.exchange_id == 'gateio':
                self.exchange.options['createMarketBuyOrderRequiresPrice'] = False

            # Загружаем рынки и обновляем атрибут spot_swap_pair_data_dict
            await self.load_markets_data()

            # Гарантируем, что атрибут есть на объекте биржи, даже если словарь пустой
            if not hasattr(self.exchange, 'spot_swap_pair_data_dict'):
                self.exchange.spot_swap_pair_data_dict = self.spot_swap_pair_data_dict or {}

            self.exchange.deal_amounts_calc_func = self.deal_amounts_calc_func
            self.start_background_market_updater()

            return self.exchange
        except Exception:
            # гарантированная очистка
            if self.exchange:
                await self.exchange.close()
            if self.session:
                await self.session.close()
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._market_updater_task:
            self._market_updater_task.cancel()
            try:
                await self._market_updater_task
            except asyncio.CancelledError:
                logger.debug(f"[{self.exchange_id}] фоновый апдейтер остановлен")

        if self.exchange:
            await self.exchange.close()

        if self.session:
            await self.session.close()


    # ===============================================================
    # ХЕШИРОВАНИЕ ОБЪЕКТОВ
    # ===============================================================
    def _hash_obj(self, obj: Any) -> str:
        try:
            dump = json.dumps(obj, sort_keys=True, ensure_ascii=False)
        except Exception:
            dump = str(obj)
        return hashlib.sha256(dump.encode('utf-8')).hexdigest()


    # ===============================================================
    # ЗАГРУЗКА И ОБНОВЛЕНИЕ MARKETS С РАСШИРЕННЫМ ЛОГОМ
    # ===============================================================
    # ===============================================================
    # ЗАГРУЗКА И ОБНОВЛЕНИЕ MARKETS С РАСШИРЕННЫМ ЛОГОМ
    # ===============================================================
    async def load_markets_data(self, force_reload: bool = False) -> dict:
        if not self.exchange:
            raise RuntimeError("Биржа не инициализирована")

        self.exchange.markets_updating = True
        start_time = asyncio.get_event_loop().time()

        # Загружаем рынки
        markets = await self.exchange.load_markets(reload=force_reload)
        while not markets:
            await asyncio.sleep(0.2)
        latency_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)

        # --- Создаём экземпляр MarketsSortData ---
        new_sort_instance = MarketsSortData(markets, log=False)

        # --- Формируем spot_swap_pair_data_dict по base/quote ---
        new_dict = {}
        spot_markets = {k: v for k, v in markets.items() if v.get('spot')}
        swap_markets = {k: v for k, v in markets.items() if v.get('swap')}

        for spot_sym, spot_data in spot_markets.items():
            base = spot_data['base']
            quote = spot_data['quote']

            # ищем swap с тем же base/quote
            matched_swap = None
            for swap_sym, swap_data in swap_markets.items():
                if swap_data['base'] == base and swap_data['quote'] == quote:
                    matched_swap = swap_data
                    break

            if matched_swap:
                pair_name = f"{spot_sym}_{matched_swap['symbol']}"
                new_dict[pair_name] = {
                    'spot': spot_data,
                    'swap': matched_swap
                }

                # нормализуем комиссии
                for market_type in ('spot', 'swap'):
                    market = new_dict[pair_name][market_type]
                    taker = market.get('taker')
                    maker = market.get('maker')
                    market['taker_fee'] = float(taker) if taker is not None else 0.0
                    market['maker_fee'] = float(maker) if maker is not None else 0.0

        # Проверка на изменения
        changed = new_dict != self.spot_swap_pair_data_dict

        if changed:
            self.markets = markets

            # **Важно:** обновляем экземпляр MarketsSortData, чтобы deal_amounts_calc_func работал
            new_sort_instance.spot_swap_pair_data_dict = new_dict
            self.MarketsSortData_instance = new_sort_instance

            self.spot_swap_pair_data_dict = new_dict
            self.upload_counter += 1
            self.exchange.upload_counter = self.upload_counter
            self.exchange.spot_swap_pair_data_dict = new_dict

            logger.info(
                f"[{self.exchange_id}] Markets обновлены: "
                f"Всего рынков: {len(markets)}, Spot/Swap пар: {len(new_dict)}, Время: {latency_ms} ms"
            )
        else:
            logger.debug(
                f"[{self.exchange_id}] Markets без изменений: "
                f"Spot/Swap пар: {len(new_dict)}, Время: {latency_ms} ms"
            )

        self.exchange.markets_updating = False
        return markets

    # ===============================================================
    # DEAL CALC
    # ===============================================================
    def deal_amounts_calc_func(
            self,
            deal_pair: str,
            spot_deal_usdt: float = None,
            swap_deal_usdt: float = None,
            price_spot: float = None,
            price_swap: float = None
    ) -> dict:
        if self.MarketsSortData_instance:
            return self.MarketsSortData_instance.deal_amounts_calc_func(
                deal_pair, spot_deal_usdt, swap_deal_usdt, price_spot, price_swap
            )
        return {}


    # ===============================================================
    # ФОНОВЫЙ ОБНОВИТЕЛЬ MARKETS
    # ===============================================================
    async def _background_markets_updater(self, interval: int, updating: bool):
        while True:
            try:
                if not updating:
                    return
                await self.load_markets_data()
            except Exception as e:
                logger.error(f"[{self.exchange_id}] ошибка фонового обновления markets: {e}")

            await asyncio.sleep(interval)


    def start_background_market_updater(self) -> None:
        if not self.exchange:
            raise RuntimeError("Биржа не инициализирована")

        if self._market_updater_task is None or self._market_updater_task.done():
            self._market_updater_task = asyncio.create_task(
                self._background_markets_updater(
                    interval=self.update_interval,
                    updating=self.markets_updating
                )
            )
            logger.info(
                f"[{self.exchange_id}] фоновое обновление markets запущено (interval={self.update_interval})"
            )


async def main():
    # Создаём экземпляр ExchangeInstance
    async with ExchangeInstance(ccxt, "bybit", update_interval=120, log=True) as exchange:
        # Синхронизируем время биржи
        await sync_time_with_exchange(exchange)

        # Ждём, пока spot_swap_pair_data_dict появится на объекте биржи
        timeout = 10  # секунд
        start = asyncio.get_event_loop().time()
        while not getattr(exchange, 'spot_swap_pair_data_dict', None):
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start > timeout:
                logger.warning(f"[{exchange.id}] Спот/своп пары не найдены после {timeout}s")
                break

        spot_swap_pairs = getattr(exchange, 'spot_swap_pair_data_dict', {})

        if not spot_swap_pairs:
            print(f"[{exchange.id}] Нет арбитражных пар для обработки")
            return

        # Пример расчёта сделки
        deal_pair = 'BTC/USDT_BTC/USDT:USDT'
        if deal_pair in spot_swap_pairs:
            res2 = exchange.deal_amounts_calc_func(
                deal_pair=deal_pair,
                spot_deal_usdt=100,
                swap_deal_usdt=90,
                price_spot=0.8004,
                price_swap=0.7989
            )
            pprint(res2)
        else:
            print(f"[{exchange.id}] Пара {deal_pair} отсутствует в данных")

        # Список всех собранных арбитражных пар
        print("Всего арбитражных пар собрано:", len(spot_swap_pairs))
        pprint(list(spot_swap_pairs.keys()))



if __name__ == '__main__':
    asyncio.run(main())
