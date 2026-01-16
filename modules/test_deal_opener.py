# deal_opener.py

import time
from typing import Dict, Any
from modules.deal_recorder import DealRecorder
from modules.colored_console import cprint
from modules.exception_classes import (
    OpenSpotOrderError,
    OpenSwapOrderError
)
import asyncio
import os
import sys
import logging
from decimal import Decimal

# Добавляем корень проекта в sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from modules.time_sync import sync_time_with_exchange
from modules.exchange_instance import ExchangeInstance
from modules.telegram_bot_message_sender import TelegramMessageSender
from modules.logger import LoggerFactory
import ccxt.pro as ccxt

class DealOpener:
    def __init__(
        self,
        exchange,
        arb_pair: str,
        spot_symbol: str,
        swap_symbol: str,
        max_order_attempt: int = 2,
        order_attempt_interval: float = 0.5,
        logger_name: str = "deal_opener",
    ):
        self.exchange = exchange
        self.arb_pair = arb_pair
        self.spot_symbol = spot_symbol
        self.swap_symbol = swap_symbol
        self.max_order_attempt = max_order_attempt
        self.order_attempt_interval = order_attempt_interval

        # Настройка логгера
        self.logger = LoggerFactory.get_logger(
            name=logger_name,
            log_filename=f"{logger_name}.log",
            level=logging.DEBUG,
            split_levels=False,
            use_timed_rotating=True,
            use_dated_folder=True,
            add_date_to_filename=False,
            add_time_to_filename=True,
            base_logs_dir=os.path.abspath(
                os.path.join(os.path.dirname(__file__), '..', 'deals_log')
            )
        )

        self.telegram_sender = TelegramMessageSender(
            bot_token_env="DEAL_BOT_TOKEN",
            chat_id_env="DEAL_CHAT_ID"
        )

        self.recorder = DealRecorder()
        self.active_deal_data_dict: Dict[str, Any] = {}
        self.signal_deal_dict: Dict[str, Any] = {}

    async def open_deal(self, signal_deal_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Открытие арбитражной сделки: спот (buy) + своп (short)"""
        self.signal_deal_dict = signal_deal_dict
        # self.recorder.set_signal_deal_dict(signal_deal_dict)
        #
        # self.active_deal_data_dict = {"signal_open_timestamp": time.time()}
        # self.active_deal_data_dict.update(self.signal_deal_dict)

        # self.logger.info("🚀 Начало открытия спот (buy) + своп (short)")

        spot_task = self._open_spot()
        swap_task = self._open_swap()
        spot_result, swap_result = await asyncio.gather(spot_task, swap_task, return_exceptions=True)

        # spot_ok = not isinstance(spot_result, Exception)
        # swap_ok = not isinstance(swap_result, Exception)

        # if spot_ok and swap_ok:
        #     return await self._finalize_successful_open(spot_result, swap_result)
        # else:
        #     await self._handle_failure(spot_result, swap_result)
        #     if not spot_ok and not swap_ok:
        #         raise DealOpenError("Не удалось открыть ни спот, ни своп") from (
        #             spot_result if isinstance(spot_result, Exception) else swap_result
        #         )
        #     elif not spot_ok:
        #         raise spot_result
        #     else:
        #         raise swap_result

    async def _open_spot(self):
        """Покупка на споте по лимитному ордеру с небольшим премиумом к ask"""
        for attempt in range(1, self.max_order_attempt + 1):
            send_time = time.time()
            try:
                # Используем signal_average_spot_ask как базу
                base_price = self.signal_deal_dict["signal_average_spot_ask"]
                if base_price is None:
                    raise ValueError("signal_average_spot_ask is None")

                order_data = await self.exchange.create_order(
                    symbol  = self.spot_symbol,
                    type    = 'limit',
                    side    = 'buy',
                    amount  = self.signal_deal_dict["signal_spot_amount"],  # количество монет
                    price   = float(self.signal_deal_dict["signal_average_spot_ask"] * Decimal("1.01")),  # Лимит выше рынка → сработает как Маркет. Премиум 1% для гарантированного исполнения
                    params  = {}
                )
                recv_time = time.time()
                status = order_data.get('status') or order_data.get('info', {}).get('finish_as')
                if status in ('closed', 'filled', 'finished'):
                    return {
                        'order_data': order_data,
                        'spot_send_time': send_time,
                        'spot_recv_time': recv_time,
                        'duration': recv_time - send_time,
                        'attempts': attempt
                    }
                else:
                    raise Exception(f"Спот-ордер не исполнен, статус: {status}")
            except Exception as e:
                if attempt < self.max_order_attempt:
                    await asyncio.sleep(self.order_attempt_interval)
                else:
                    raise OpenSpotOrderError(
                        self.spot_symbol,
                        f"Не удалось открыть спот-ордер после {self.max_order_attempt} попыток. Ошибка: {e}"
                    ) from e

    async def _open_swap(self):
        """Открытие шорт-позиции на свопе рыночным ордером"""
        try:
            await self.exchange.set_margin_mode(symbol=self.swap_symbol, marginMode='cross')
            await self.exchange.set_leverage(1, self.swap_symbol)
        except Exception as e:
            self.logger.warning(f"Не удалось установить cross-margin или leverage: {e}")

        for attempt in range(1, self.max_order_attempt + 1):
            send_time = time.time()
            try:
                order_data = await self.exchange.create_order(
                    symbol=self.swap_symbol,
                    type='market',
                    side='sell',
                    amount=self.signal_deal_dict["signal_swap_contracts"],
                    params={}
                )
                recv_time = time.time()
                status = order_data.get('status') or order_data.get('info', {}).get('finish_as')
                if status in ('closed', 'filled', 'finished'):
                    return {
                        'order_data': order_data,
                        'swap_send_time': send_time,
                        'swap_recv_time': recv_time,
                        'duration': recv_time - send_time,
                        'attempts': attempt
                    }
                else:
                    raise Exception(f"Своп-ордер не исполнен, статус: {status}")
            except Exception as e:
                if attempt < self.max_order_attempt:
                    await asyncio.sleep(self.order_attempt_interval)
                else:
                    raise OpenSwapOrderError(
                        self.swap_symbol,
                        f"Не удалось открыть своп-ордер после {self.max_order_attempt} попыток. Ошибка: {e}"
                    ) from e

async def main():
    async with ExchangeInstance(ccxt, 'gateio', update_interval=10, log=True) as exchange:
        await sync_time_with_exchange(exchange)

        # Пример сигнала — как у тебя
        limit_price = 0.023456  # ← сюда подставляется реальный ask
        signal_deal_dict = {
            "arb_pair": 'SOMI/USDT_SOMI/USDT:USDT',
            "spot_symbol": 'SOMI/USDT',
            "swap_symbol": 'SOMI/USDT:USDT',
            "signal_spot_amount": 13,
            "signal_swap_contracts": 13,
            "signal_average_spot_ask": limit_price,
            "signal_average_spot_bid": None,
            "signal_average_swap_ask": None,
            "signal_average_swap_bid": None,
            "signal_open_ratio": 1.1,
            "signal_open_threshold_ratio": 0.5,
            "signal_close_ratio": 1.1,
            "signal_close_threshold_ratio": 0.3,
            "signal_max_open_ratio": 1,
            "signal_max_close_ratio": 1,
            "signal_min_open_ratio": -1,
            "signal_min_close_ratio": -1,
            "signal_delta_ratios": 2
        }

        opener = DealOpener(
            exchange=exchange,
            arb_pair=signal_deal_dict["arb_pair"],
            spot_symbol=signal_deal_dict["spot_symbol"],
            swap_symbol=signal_deal_dict["swap_symbol"],
            max_order_attempt=2,
            order_attempt_interval=1.0,
            logger_name="deal_opener_instance",
        )

        try:
            cprint.info("🚀 Запуск открытия арбитражной сделки...")
            deal_data = await opener.open_deal(signal_deal_dict)
            # cprint.info(f"✅ Сделка открыта. Спред: {deal_data.get('deal_open_ratio', 0):.4f}%")
        except Exception as e:
            cprint.error_b(f"🔥 Ошибка открытия сделки: {e}")


if __name__ == "__main__":
    asyncio.run(main())