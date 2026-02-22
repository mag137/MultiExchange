__version__ = "7.4"
"""
OKX
"""
import asyncio
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Dict, Any, List
import ccxt.pro as ccxt
from pprint import pprint
import time
import os
import logging

from modules.exception_classes import (
    InsufficientSwapCostFundsError,
    InsufficientSpotCostFundsError,
    InsufficientSpotAmountFundsError,
    InsufficientSwapAmountFundsError
)
from modules.logger import LoggerFactory
from modules.colored_console import cprint

logger = LoggerFactory.get_logger(
    name="market_sort_data",
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


class MarketsSortData:
    """
    Класс для анализа и сортировки рынков биржи с целью расчета объёмов сделок для арбитража спот-своп.
    Все расчеты выполняются в Decimal для точности и с безопасной конверсией.
    """

    def __init__(self, markets: Dict[str, Any], log: bool = False) -> None:
        self.markets: Dict[str, Any] = markets
        self.log: bool = log

        self.spot: Dict[str, Any] = {}
        self.swap: Dict[str, Any] = {}
        self.future: Dict[str, Any] = {}
        self.option: Dict[str, Any] = {}

        self.spot_swap: Dict[str, str] = {}
        self.swap_spot: Dict[str, str] = {}
        self.spot_swap_set: set = set()
        self.spot_swap_list: List[List[str]] = []
        self.spot_swap_pair_data_dict: Dict[str, Dict[str, Any]] = {}

        if markets:
            self.get_sorted()
        else:
            logger.error("[MarketsSortData] Пустые markets при инициализации")

    @staticmethod
    def _safe_decimal(value: Any, default: Decimal = Decimal('0')) -> Decimal:
        """Безопасное преобразование в Decimal."""
        if value is None or value == '':
            return default
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return default

    @staticmethod
    def _round_down(value: Decimal, decimals: int) -> Decimal:
        if not isinstance(value, Decimal):
            value = Decimal(str(value))
        quantize_exp = Decimal("0.1") ** decimals
        return value.quantize(quantize_exp, rounding=ROUND_DOWN)

    @staticmethod
    def _get_decimal_precision(step: Decimal) -> int:
        if step <= 0:
            raise ValueError(f"step должен быть > 0, получено: {step}")
        exponent = Decimal(str(step)).as_tuple().exponent
        return max(0, -exponent)

    def get_sorted(self) -> None:
        if not self.markets:
            logger.error("[MarketsSortData.get_sorted] markets пуст")
            return
        self._clear_data()
        self._sort_markets()
        self._link_spot_swap_pairs()
        if self.log:
            logger.info(f"[MarketsSortData] Найдено {len(self.spot_swap_pair_data_dict)} спот-своп пар")

    def _clear_data(self) -> None:
        self.spot.clear()
        self.swap.clear()
        self.future.clear()
        self.option.clear()
        self.spot_swap.clear()
        self.swap_spot.clear()
        self.spot_swap_set.clear()
        self.spot_swap_list.clear()
        self.spot_swap_pair_data_dict.clear()

    def _sort_markets(self) -> None:
        cprint.success_w("_sort_markets")
        for symbol, data in self.markets.items():
            info = data.get("info", {})
            inst_type = info.get("instType", "").upper()
            raw_state = info.get("state")

            if isinstance(raw_state, str):
                state = raw_state.lower()
            else:
                state = str(raw_state).lower() if raw_state is not None else ""
            if data.get("active") is False or state not in ("live", ""):
                continue
            if data.get("spot") or inst_type == "SPOT":
                self.spot[symbol] = data
                continue
            if data.get("swap") or inst_type == "SWAP":
                self.swap[symbol] = data
                continue
            if data.get("future") or inst_type == "FUTURES":
                self.future[symbol] = data
                continue
            if data.get("option") or inst_type == "OPTION":
                self.option[symbol] = data
                continue

    def _link_spot_swap_pairs(self) -> None:
        print("_link_spot_swap_pairs")
        for spot_symbol, spot_data in self.spot.items():
            spot_base = spot_data.get("base")
            spot_quote = spot_data.get("quote")
            if not spot_base or not spot_quote:
                continue
            for swap_symbol, swap_data in self.swap.items():
                swap_base = swap_data.get("base")
                swap_quote = swap_data.get("quote")
                if not swap_base or not swap_quote:
                    continue
                if swap_data['inverse']:
                    continue
                if swap_data['quote'] != 'USDT':
                    continue

                if spot_base == swap_base and spot_quote == swap_quote:
                    key = f"{spot_symbol}_{swap_symbol}"
                    self.spot_swap_pair_data_dict[key] = {
                        "spot": spot_data,
                        "swap": swap_data,
                    }
                    self.spot_swap[spot_symbol] = swap_symbol
                    self.swap_spot[swap_symbol] = spot_symbol
                    self.spot_swap_set.update([spot_symbol, swap_symbol])
        self.spot_swap_list = [[s, self.spot_swap[s]] for s in self.spot_swap]

    def deal_amounts_calc_func(
            self,
            deal_pair: str,
            spot_deal_usdt: float,
            swap_deal_usdt: float,
            price_spot: float,
            price_swap: float,
    ) -> Dict[str, Any]:
        # cprint.info_w("deal_amounts_calc_func")
        if not self.spot_swap_pair_data_dict:
            raise ValueError("spot_swap_pair_data_dict пуст — вызови get_sorted()")
        pair_data = self.spot_swap_pair_data_dict.get(deal_pair)
        if not pair_data:
            raise ValueError(f"Пара не найдена: {deal_pair}")

        spot_market = pair_data.get("spot", {})
        swap_market = pair_data.get("swap", {})

        contract_size = self._safe_decimal(swap_market.get("contractSize", 1))
        spot_precision_amt = self._safe_decimal(spot_market.get("precision", {}).get("amount", 8))
        swap_precision_amt = self._safe_decimal(swap_market.get("precision", {}).get("amount", 8))
        spot_min_amt = self._safe_decimal(spot_market.get("limits", {}).get("amount", {}).get("min", 1))
        swap_min_amt = self._safe_decimal(swap_market.get("limits", {}).get("amount", {}).get("min", 1))

        price_spot_d = self._safe_decimal(price_spot, Decimal("1"))
        price_swap_d = self._safe_decimal(price_swap, Decimal("1"))

        spot_min_cost = max(
            self._safe_decimal(spot_market.get("limits", {}).get("cost", {}).get("min"), Decimal("1")),
            Decimal("1")
        )
        swap_min_cost = self._safe_decimal(swap_market.get("limits", {}).get("cost", {}).get("min"), Decimal("0"))

        spot_taker_fee = self._safe_decimal(spot_market.get("taker") or 0)
        swap_taker_fee = self._safe_decimal(swap_market.get("taker") or 0)
        fee_summary = spot_taker_fee + swap_taker_fee

        spot_precision_digits = self._get_decimal_precision(spot_precision_amt)
        swap_precision_digits = self._get_decimal_precision(swap_precision_amt)

        spot_amount_coin = Decimal(str(spot_deal_usdt)) / price_spot_d
        swap_amount_coin = Decimal(str(swap_deal_usdt)) / price_swap_d
        min_amount_coin = min(spot_amount_coin, swap_amount_coin)

        raw_contracts = min_amount_coin / contract_size
        swap_contracts = self._round_down(raw_contracts, swap_precision_digits)
        spot_amount = self._round_down(swap_contracts * contract_size, spot_precision_digits)

        spot_cost = spot_amount * price_spot_d
        swap_cost = swap_contracts * contract_size * price_swap_d

        if spot_amount < spot_min_amt:
            raise InsufficientSpotAmountFundsError(exchange_id='', symbol=deal_pair,
                                                   spot_amount=float(spot_amount),
                                                   min_spot_amount=float(spot_min_amt))
        if spot_cost < spot_min_cost:
            raise InsufficientSpotCostFundsError(exchange_id='', symbol=deal_pair,
                                                 balance_spot=float(spot_cost),
                                                 min_spot_cost=float(spot_min_cost))
        if swap_contracts < swap_min_amt:
            raise InsufficientSwapAmountFundsError(exchange_id='', symbol=deal_pair,
                                                   swap_amount=float(swap_contracts),
                                                   min_swap_amount=float(swap_min_amt))
        if swap_cost < swap_min_cost:
            raise InsufficientSwapCostFundsError(exchange_id='', symbol=deal_pair,
                                                 balance_swap=float(swap_cost),
                                                 min_swap_cost=float(swap_min_cost))

        return {
            "spot_amount": spot_amount,
            "swap_contracts": swap_contracts,
            "spot_cost": spot_cost,
            "swap_cost": swap_cost,
            "contract_size": contract_size,
            "price_spot": price_spot_d,
            "price_swap": price_swap_d,
            "fee_summary": fee_summary,
            "spot_min_amt": spot_min_amt,
            "spot_min_cost": spot_min_cost,
            "swap_min_amt": swap_min_amt,
            "swap_min_cost": swap_min_cost,
            "debug": {
                "spot_amount_coin": spot_amount_coin,
                "swap_amount_coin": swap_amount_coin,
                "min_amount_coin": min_amount_coin,
                "raw_contracts": raw_contracts,
                "spot_precision_digits": spot_precision_digits,
                "swap_precision_digits": swap_precision_digits,
            },
        }
