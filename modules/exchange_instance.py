__version__ = '6.1i'
# Р”РѕР±Р°РІР»РµРЅС‹ С„РёР»СЊС‚СЂС‹ РґРѕСЃС‚СѓРїРЅРѕСЃС‚Рё РјРѕРЅРµС‚С‹ РґР»СЏ С‚РѕСЂРіРѕРІР»Рё
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
# Р›РћР“Р“Р•Р 
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
# РћРЎРќРћР’РќРћР™ РљР›РђРЎРЎ EXCHANGEINSTANCE
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

        # Р”Р°РЅРЅС‹Рµ
        self.api_keys: Dict[str, str] = {}
        self.dict_api: Dict[str, Dict[str, str]] = {}
        self.markets: Optional[Dict[str, Any]] = None
        self.MarketsSortData_instance: Optional[MarketsSortData] = None
        self.spot_swap_pair_data_dict: Dict[str, Any] = {}

        # Р”Р»СЏ РєРѕРЅС‚СЂРѕР»СЏ РёР·РјРµРЅРµРЅРёР№
        self._last_markets_hash: Optional[str] = None
        self._last_pairs_hash: Optional[str] = None

        # РџСѓС‚Рё
        current_file_path = Path(__file__).resolve()
        self.project_root = current_file_path.parents[1]
        self.source_path = self.project_root / 'source'

        # Р—Р°РіСЂСѓР·РєР° РєР»СЋС‡РµР№
        self._load_api_keys()


    # ===============================================================
    # CCXT РџР РћРљРЎР РњР•РўРћР”Р«
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
    # Р—РђР“Р РЈР—РљРђ РљР›Р®Р§Р•Р™
    # ===============================================================
    def _load_api_keys(self) -> None:
        api_file = self.source_path / ('api_keys_sand.json' if self.sandbox else 'api_keys.json')

        try:
            with open(api_file, 'r', encoding='utf-8') as f:
                self.dict_api = json.load(f)
                self.api_keys = self.dict_api.get(self.exchange_id, {})

            if self.log:
                logger.debug(f"[{self.exchange_id}] API РєР»СЋС‡Рё Р·Р°РіСЂСѓР¶РµРЅС‹ РёР· {api_file}")

        except FileNotFoundError:
            logger.warning(f"[{self.exchange_id}] Р¤Р°Р№Р» API РєР»СЋС‡РµР№ РЅРµ РЅР°Р№РґРµРЅ: {api_file}")

        except json.JSONDecodeError:
            logger.error(f"[{self.exchange_id}] РћС€РёР±РєР° JSON РІ {api_file}")


    # ===============================================================
    # SSL CONTEXT
    # ===============================================================
    @staticmethod
    def _create_ssl_context() -> ssl.SSLContext:
        context = ssl.create_default_context()
        context.load_verify_locations(cafile=certifi.where())
        return context


    # ===============================================================
    # ENTER / EXIT вЂ” РљРћРќРўР•РљРЎРўРќР«Р™ РњР•РќР•Р”Р–Р•Р 
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
                    logger.error("[gateio] РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚ uid РІ api_keys.json")
                    raise ValueError("gateio С‚СЂРµР±СѓРµС‚ РєР»СЋС‡ uid")
                self.params['uid'] = uid

            exchange_class = getattr(self.ccxt_module, self.exchange_id)
            self.exchange = exchange_class(self.params)

            if self.exchange_id == 'gateio':
                self.exchange.options['createMarketBuyOrderRequiresPrice'] = False

            # Синхронизация времени с сервером биржи (важно для подписи запросов)
            try:
                await sync_time_with_exchange(self.exchange, auto_adjust=True)
            except Exception as e:
                logger.warning(f"[{self.exchange_id}] не удалось синхронизировать время: {e}")
            # Р—Р°РіСЂСѓР¶Р°РµРј СЂС‹РЅРєРё Рё РѕР±РЅРѕРІР»СЏРµРј Р°С‚СЂРёР±СѓС‚ spot_swap_pair_data_dict
            await self.load_markets_data()

            # Р“Р°СЂР°РЅС‚РёСЂСѓРµРј, С‡С‚Рѕ Р°С‚СЂРёР±СѓС‚ РµСЃС‚СЊ РЅР° РѕР±СЉРµРєС‚Рµ Р±РёСЂР¶Рё, РґР°Р¶Рµ РµСЃР»Рё СЃР»РѕРІР°СЂСЊ РїСѓСЃС‚РѕР№
            if not hasattr(self.exchange, 'spot_swap_pair_data_dict'):
                self.exchange.spot_swap_pair_data_dict = self.spot_swap_pair_data_dict or {}

            self.exchange.deal_amounts_calc_func = self.deal_amounts_calc_func
            self.start_background_market_updater()

            return self.exchange
        except Exception:
            # РіР°СЂР°РЅС‚РёСЂРѕРІР°РЅРЅР°СЏ РѕС‡РёСЃС‚РєР°
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
                logger.debug(f"[{self.exchange_id}] С„РѕРЅРѕРІС‹Р№ Р°РїРґРµР№С‚РµСЂ РѕСЃС‚Р°РЅРѕРІР»РµРЅ")

        if self.exchange:
            await self.exchange.close()

        if self.session:
            await self.session.close()


    # ===============================================================
    # РҐР•РЁРР РћР’РђРќРР• РћР‘РЄР•РљРўРћР’
    # ===============================================================
    def _hash_obj(self, obj: Any) -> str:
        try:
            dump = json.dumps(obj, sort_keys=True, ensure_ascii=False)
        except Exception:
            dump = str(obj)
        return hashlib.sha256(dump.encode('utf-8')).hexdigest()


    # ===============================================================
    # Р—РђР“Р РЈР—РљРђ Р РћР‘РќРћР’Р›Р•РќРР• MARKETS РЎ Р РђРЎРЁРР Р•РќРќР«Рњ Р›РћР“РћРњ
    # ===============================================================
    # ===============================================================
    # Р—РђР“Р РЈР—РљРђ Р РћР‘РќРћР’Р›Р•РќРР• MARKETS РЎ Р РђРЎРЁРР Р•РќРќР«Рњ Р›РћР“РћРњ
    # ===============================================================
    async def load_markets_data(self, force_reload: bool = False) -> dict:
        if not self.exchange:
            raise RuntimeError("Р‘РёСЂР¶Р° РЅРµ РёРЅРёС†РёР°Р»РёР·РёСЂРѕРІР°РЅР°")

        self.exchange.markets_updating = True
        start_time = asyncio.get_event_loop().time()

        # Р—Р°РіСЂСѓР¶Р°РµРј СЂС‹РЅРєРё
        markets = await self.exchange.load_markets(reload=force_reload)
        while not markets:
            await asyncio.sleep(0.2)
        latency_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)

        # --- РЎРѕР·РґР°С‘Рј СЌРєР·РµРјРїР»СЏСЂ MarketsSortData ---
        new_sort_instance = MarketsSortData(markets, log=False)

        def is_tradable_market(market: dict) -> bool:
            """
            РЈРЅРёС„РёС†РёСЂРѕРІР°РЅРЅР°СЏ РїСЂРѕРІРµСЂРєР° РґРѕСЃС‚СѓРїРЅРѕСЃС‚Рё СЂС‹РЅРєР°.
            РќСѓР¶РЅР°, С‡С‚РѕР±С‹ РёСЃРєР»СЋС‡РёС‚СЊ РёР· СЃР»РѕРІР°СЂСЏ РїР°СЂС‹ РёРЅСЃС‚СЂСѓРјРµРЅС‚С‹, РєРѕС‚РѕСЂС‹Рµ СѓР¶Рµ РЅРµ С‚РѕСЂРіСѓСЋС‚СЃСЏ
            (РЅР°РїСЂРёРјРµСЂ, halted/suspend/offline), РЅРѕ РµС‰С‘ РїСЂРёСЃСѓС‚СЃС‚РІСѓСЋС‚ РІ РѕС‚РІРµС‚Рµ load_markets().
            """
            if not isinstance(market, dict):
                return False

            if market.get("active") is False:
                return False

            info = market.get("info") or {}
            # Р Р°Р·РЅС‹Рµ Р±РёСЂР¶Рё РёСЃРїРѕР»СЊР·СѓСЋС‚ СЂР°Р·РЅС‹Рµ РєР»СЋС‡Рё СЃРѕСЃС‚РѕСЏРЅРёСЏ.
            raw_states = [
                info.get("state"),
                info.get("status"),
                info.get("contractStatus"),
                info.get("symbolStatus"),
            ]
            states = [
                str(s).strip().lower()
                for s in raw_states
                if s is not None and str(s).strip() != ""
            ]

            # Р•СЃР»Рё СЃС‚Р°С‚СѓСЃС‹ РЅРµ РїСЂРёС€Р»Рё РІРѕРѕР±С‰Рµ вЂ” СЃС‡РёС‚Р°РµРј РёРЅСЃС‚СЂСѓРјРµРЅС‚ РїРѕС‚РµРЅС†РёР°Р»СЊРЅРѕ РїСЂРёРіРѕРґРЅС‹Рј.
            if not states:
                return True

            # Р Р°Р·СЂРµС€С‘РЅРЅС‹Рµ "С‚РѕСЂРіСѓРµС‚СЃСЏ" СЃРѕСЃС‚РѕСЏРЅРёСЏ.
            allowed_states = {
                "live",
                "trading",
                "online",
                "open",
                "listed",
                "normal",
            }

            # РЇРІРЅРѕ РЅРµС‚РѕСЂРіСѓРµРјС‹Рµ СЃРѕСЃС‚РѕСЏРЅРёСЏ.
            blocked_states = {
                "suspend",
                "suspended",
                "halt",
                "halted",
                "pause",
                "paused",
                "offline",
                "delisted",
                "settlement",
                "preopen",
                "close",
                "closed",
                "maintenance",
            }

            if any(s in blocked_states for s in states):
                return False

            # Р•СЃР»Рё РµСЃС‚СЊ СЏРІРЅС‹Р№ РїРѕР·РёС‚РёРІРЅС‹Р№ СЃС‚Р°С‚СѓСЃ вЂ” РёРЅСЃС‚СЂСѓРјРµРЅС‚ С‚РѕСЂРіСѓРµС‚СЃСЏ.
            if any(s in allowed_states for s in states):
                return True

            # РќРµРёР·РІРµСЃС‚РЅС‹Р№ СЃС‚Р°С‚СѓСЃ: РЅРµ Р±Р»РѕРєРёСЂСѓРµРј, РЅРѕ РјРѕР¶РЅРѕ РґРѕР±Р°РІРёС‚СЊ РґРёР°РіРЅРѕСЃС‚РёРєСѓ РїСЂРё РЅРµРѕР±С…РѕРґРёРјРѕСЃС‚Рё.
            return True

        # --- Р¤РѕСЂРјРёСЂСѓРµРј spot_swap_pair_data_dict РїРѕ base/quote ---
        new_dict = {}
        spot_markets = {
            k: v for k, v in markets.items()
            if v.get('spot') and is_tradable_market(v)
        }
        swap_markets = {
            k: v for k, v in markets.items()
            if v.get('swap') and is_tradable_market(v)
        }

        for spot_sym, spot_data in spot_markets.items():
            base = spot_data['base']
            quote = spot_data['quote']

            # РёС‰РµРј swap СЃ С‚РµРј Р¶Рµ base/quote
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

                # РЅРѕСЂРјР°Р»РёР·СѓРµРј РєРѕРјРёСЃСЃРёРё
                for market_type in ('spot', 'swap'):
                    market = new_dict[pair_name][market_type]
                    taker = market.get('taker')
                    maker = market.get('maker')
                    market['taker_fee'] = float(taker) if taker is not None else 0.0
                    market['maker_fee'] = float(maker) if maker is not None else 0.0

        # РџСЂРѕРІРµСЂРєР° РЅР° РёР·РјРµРЅРµРЅРёСЏ
        changed = new_dict != self.spot_swap_pair_data_dict

        if changed:
            self.markets = markets

            # **Р’Р°Р¶РЅРѕ:** РѕР±РЅРѕРІР»СЏРµРј СЌРєР·РµРјРїР»СЏСЂ MarketsSortData, С‡С‚РѕР±С‹ deal_amounts_calc_func СЂР°Р±РѕС‚Р°Р»
            new_sort_instance.spot_swap_pair_data_dict = new_dict
            self.MarketsSortData_instance = new_sort_instance

            self.spot_swap_pair_data_dict = new_dict
            self.upload_counter += 1
            self.exchange.upload_counter = self.upload_counter
            self.exchange.spot_swap_pair_data_dict = new_dict

            logger.info(
                f"[{self.exchange_id}] Markets РѕР±РЅРѕРІР»РµРЅС‹: "
                f"Р’СЃРµРіРѕ СЂС‹РЅРєРѕРІ: {len(markets)}, Spot/Swap РїР°СЂ: {len(new_dict)}, Р’СЂРµРјСЏ: {latency_ms} ms"
            )
        else:
            logger.debug(
                f"[{self.exchange_id}] Markets Р±РµР· РёР·РјРµРЅРµРЅРёР№: "
                f"Spot/Swap РїР°СЂ: {len(new_dict)}, Р’СЂРµРјСЏ: {latency_ms} ms"
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
    # Р¤РћРќРћР’Р«Р™ РћР‘РќРћР’РРўР•Р›Р¬ MARKETS
    # ===============================================================
    async def _background_markets_updater(self, interval: int, updating: bool):
        while True:
            try:
                if not updating:
                    return
                await self.load_markets_data()
            except Exception as e:
                logger.error(f"[{self.exchange_id}] РѕС€РёР±РєР° С„РѕРЅРѕРІРѕРіРѕ РѕР±РЅРѕРІР»РµРЅРёСЏ markets: {e}")

            await asyncio.sleep(interval)


    def start_background_market_updater(self) -> None:
        if not self.exchange:
            raise RuntimeError("Р‘РёСЂР¶Р° РЅРµ РёРЅРёС†РёР°Р»РёР·РёСЂРѕРІР°РЅР°")

        if self._market_updater_task is None or self._market_updater_task.done():
            self._market_updater_task = asyncio.create_task(
                self._background_markets_updater(
                    interval=self.update_interval,
                    updating=self.markets_updating
                )
            )
            logger.info(
                f"[{self.exchange_id}] С„РѕРЅРѕРІРѕРµ РѕР±РЅРѕРІР»РµРЅРёРµ markets Р·Р°РїСѓС‰РµРЅРѕ (interval={self.update_interval})"
            )


async def main():
    # РЎРѕР·РґР°С‘Рј СЌРєР·РµРјРїР»СЏСЂ ExchangeInstance
    async with ExchangeInstance(ccxt, "bybit", update_interval=120, log=True) as exchange:
        # РЎРёРЅС…СЂРѕРЅРёР·РёСЂСѓРµРј РІСЂРµРјСЏ Р±РёСЂР¶Рё
        await sync_time_with_exchange(exchange)

        # Р–РґС‘Рј, РїРѕРєР° spot_swap_pair_data_dict РїРѕСЏРІРёС‚СЃСЏ РЅР° РѕР±СЉРµРєС‚Рµ Р±РёСЂР¶Рё
        timeout = 10  # СЃРµРєСѓРЅРґ
        start = asyncio.get_event_loop().time()
        while not getattr(exchange, 'spot_swap_pair_data_dict', None):
            await asyncio.sleep(0.1)
            if asyncio.get_event_loop().time() - start > timeout:
                logger.warning(f"[{exchange.id}] РЎРїРѕС‚/СЃРІРѕРї РїР°СЂС‹ РЅРµ РЅР°Р№РґРµРЅС‹ РїРѕСЃР»Рµ {timeout}s")
                break

        spot_swap_pairs = getattr(exchange, 'spot_swap_pair_data_dict', {})

        if not spot_swap_pairs:
            print(f"[{exchange.id}] РќРµС‚ Р°СЂР±РёС‚СЂР°Р¶РЅС‹С… РїР°СЂ РґР»СЏ РѕР±СЂР°Р±РѕС‚РєРё")
            return

        # РџСЂРёРјРµСЂ СЂР°СЃС‡С‘С‚Р° СЃРґРµР»РєРё
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
            print(f"[{exchange.id}] РџР°СЂР° {deal_pair} РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚ РІ РґР°РЅРЅС‹С…")

        # РЎРїРёСЃРѕРє РІСЃРµС… СЃРѕР±СЂР°РЅРЅС‹С… Р°СЂР±РёС‚СЂР°Р¶РЅС‹С… РїР°СЂ
        print("Р’СЃРµРіРѕ Р°СЂР±РёС‚СЂР°Р¶РЅС‹С… РїР°СЂ СЃРѕР±СЂР°РЅРѕ:", len(spot_swap_pairs))
        pprint(list(spot_swap_pairs.keys()))



if __name__ == '__main__':
    asyncio.run(main())
