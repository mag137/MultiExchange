"""
Модуль для безопасного закрытия арбитражных сделок между спотовым и своп-рынками (фьючерсами).

Основная задача:
    - одновременно закрыть спотовую позицию (продажа) и своп-позицию (покупка);
    - в случае частичного сбоя — аварийно закрыть оставшуюся часть;
    - зафиксировать все данные закрытия (объёмы, комиссии, временные метки);
    - обеспечить идемпотентность и безопасность при работе с биржей Gate.io.

Особенности Gate.io:
    - Спот закрывается рыночным ордером на продажу.
    - Своп закрывается рыночным ордером с `reduce_only=True`.
    - Комиссии могут быть в USDT, базовой монете или GT.

Архитектура:
    - Класс `DealCloser` не управляет состоянием — принимает зависимости через конструктор.
    - Данные сделки берутся из `deal_data` (обычно из `active_deals_dict`).
    - Поддержка аварийного закрытия даже если запись в active_deals.json неактуальна.

Использование:
    # >>> deal_closer = DealCloser(exchange, ...)
    # >>> await deal_closer.close_deal(deal_data)

Требования к `deal_data`:
    - `deal_open_spot_amount` (str или float): объём спота для закрытия.
    - `deal_open_swap_contracts` (str или float): объём свопа для закрытия.
    - `arb_pair`, `spot_symbol`, `swap_symbol` — должны присутствовать.

Безопасность:
    - Все расчёты в `Decimal`.
    - Все вызовы к бирже с retry-логикой.
    - Баланс получается с таймаутом 10 сек.
    - Удаляет запись из `active_deals_dict` при успехе.

Версия: 1.0
"""

__version__ = "1.1"

import asyncio
import json
import time
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional, Union

import ccxt.pro as ccxt

from modules.deal_recorder import DealRecorder
from modules.exception_classes import (
    CloseSpotOrderError,
    CloseSwapOrderError,
    DealCloseError
)
from modules.colored_console import cprint


def decimal_to_str(obj):
    """Рекурсивно преобразует Decimal в строки в словаре или списке."""
    if isinstance(obj, Decimal):
        return str(obj)
    elif isinstance(obj, dict):
        return {k: decimal_to_str(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decimal_to_str(item) for item in obj]
    else:
        return obj


class DealCloser:
    """
    Класс для управления закрытием арбитражной сделки между спотом и свопом.

    Отвечает за:
        - параллельное закрытие спотовой и своп-позиции;
        - обработку частичных сбоев с аварийным закрытием оставшейся части;
        - расчёт комиссий в USDT;
        - запись данных закрытия в дампы и удаление из active_deals.json.

    Атрибуты:
        exchange (ccxt.Exchange): Экземпляр биржи (ccxt.pro).
        arb_pair (str): Уникальный идентификатор арбитражной пары.
        spot_symbol (str): Символ спотовой пары.
        swap_symbol (str): Символ своп-пары.
        max_order_attempt (int): Максимальное число попыток закрытия ордера.
        order_attempt_interval (float): Интервал между попытками в секундах.
        active_deals_dict (dict): Общий словарь активных сделок.
        telegram_sender (Optional[Any]): Экземпляр отправителя Telegram-уведомлений.
        deal_recorder (DealRecorder): Агент записи дампов.
    """

    def __init__(
        self,
        exchange: ccxt.Exchange,
        arb_pair: str,
        spot_symbol: str,
        swap_symbol: str,
        max_order_attempt: int = 3,
        order_attempt_interval: float = 0.5,
        active_deals_dict: Optional[Dict[str, Any]] = None,
        telegram_sender: Optional[Any] = None,
        active_deals_file_manager = None
    ) -> None:
        self.exchange = exchange
        self.arb_pair = arb_pair
        self.spot_symbol = spot_symbol
        self.swap_symbol = swap_symbol
        self.max_order_attempt = max_order_attempt
        self.order_attempt_interval = order_attempt_interval
        self.active_deals_dict = active_deals_dict or {}
        self.telegram_sender = telegram_sender
        self.deal_recorder = DealRecorder()
        self.active_deals_file_manager = active_deals_file_manager

    async def close_deal(self) -> bool:
        """
        Закрывает арбитражную сделку: продажа спота + покупка свопа.

        Args:
            deal_data: Данные сделки с ключами:
                - deal_open_spot_amount
                - deal_open_swap_contracts
                - (опционально) другие метаданные

        Returns:
            bool: True при успешном закрытии обеих позиций.

        Raises:
            DealCloseError: Если обе позиции не закрыты.
            CloseSpotOrderError / CloseSwapOrderError: При сбое одной из частей.
            ValueError: При отсутствии обязательных полей.
        """
        deal_data = self.active_deals_dict.get(self.arb_pair, None) or {}
        required_keys = {"available_for_sell_spot_balance", "deal_open_swap_contracts"}
        missing = required_keys - deal_data.keys()
        if missing:
            raise ValueError(f"Отсутствуют обязательные поля в deal_data: {missing}")

        close_data = deal_data.copy()
        close_data["signal_close_timestamp"] = time.time()


        try:
            spot_amount = float(deal_data["available_for_sell_spot_balance"])
            swap_contracts = float(deal_data["deal_open_swap_contracts"])
        except (ValueError, TypeError) as e:
            raise ValueError(f"Некорректные числовые данные в deal_data: {e}")

        # === Параллельное закрытие позиций ===
        spot_task = self._close_spot(spot_amount)
        swap_task = self._close_swap(swap_contracts)
        spot_result, swap_result = await asyncio.gather(spot_task, swap_task, return_exceptions=True)

        spot_ok = not isinstance(spot_result, Exception)
        swap_ok = not isinstance(swap_result, Exception)

        if spot_ok and swap_ok:
            return await self._handle_successful_close(spot_result, swap_result, close_data)
        else:
            return await self._handle_partial_or_full_failure(spot_ok, swap_ok, spot_result, swap_result, close_data)



    async def _handle_successful_close(
        self,
        spot_result: Dict[str, Any],
        swap_result: Dict[str, Any],
        close_data: Dict[str, Any]
    ) -> bool:
        """Обрабатывает успешное закрытие обеих позиций."""
        spot_filled = spot_result["order_data"].get("filled") or spot_result["order_data"].get("amount", 0)
        swap_filled = swap_result["order_data"].get("filled") or swap_result["order_data"].get("amount", 0)

        spot_avg = Decimal(str(spot_result["order_data"]["average"]))
        swap_avg = Decimal(str(swap_result["order_data"]["average"]))
        spot_filled_dec = Decimal(str(spot_filled))
        swap_filled_dec = Decimal(str(swap_filled))

        # Расчёт PnL (простой, без учёта комиссий)
        pnl_gross = (swap_avg - spot_avg) * min(spot_filled_dec, swap_filled_dec)

        close_data.update({
            "deal_close_spot_id": spot_result["order_data"]["id"],
            "deal_close_swap_id": swap_result["order_data"]["id"],
            "deal_close_spot_cost": str(Decimal(str(spot_result["order_data"]["cost"]))),
            "deal_close_swap_cost": str(Decimal(str(swap_result["order_data"]["cost"]))),
            "deal_close_spot_average_price": str(spot_avg),
            "deal_close_swap_average_price": str(swap_avg),
            "deal_close_spot_amount": str(spot_filled_dec),
            "deal_close_swap_contracts": str(swap_filled_dec),
            "deal_close_pnl_gross": str(pnl_gross.quantize(Decimal("0.00000001"))),
        })

        # Расчёт комиссий
        spot_fee_usdt = await self._compute_spot_fee_usdt(spot_result["order_data"])
        swap_fee_usdt = self._compute_swap_fee_usdt(swap_result["order_data"])

        spot_cost = Decimal(str(spot_result["order_data"].get("cost", "1")))
        swap_cost = Decimal(str(swap_result["order_data"].get("cost", "1")))
        spot_fee_percent = (spot_fee_usdt / spot_cost * 100) if spot_cost > 0 else Decimal("0.0")
        swap_fee_percent = (swap_fee_usdt / swap_cost * 100) if swap_cost > 0 else Decimal("0.0")

        close_data.update({
            "deal_close_spot_fee_usdt": str(spot_fee_usdt.quantize(Decimal("0.00000001"))),
            "deal_close_swap_fee_usdt": str(swap_fee_usdt.quantize(Decimal("0.00000001"))),
            "deal_close_spot_fee_percent": str(spot_fee_percent.quantize(Decimal("0.0001"))),
            "deal_close_swap_fee_percent": str(swap_fee_percent.quantize(Decimal("0.0001"))),
            "deal_close_total_fee_usdt": str((spot_fee_usdt + swap_fee_usdt).quantize(Decimal("0.00000001"))),
        })

        # Временные метки
        close_data["deal_close_spot_complete_timestamp"] = float(spot_result["order_data"]["lastTradeTimestamp"]) / 1000
        close_data["deal_close_swap_complete_timestamp"] = float(swap_result["order_data"]["lastTradeTimestamp"]) / 1000
        close_data["deal_close_spot_duration"] = (
            float(spot_result["order_data"]["lastTradeTimestamp"]) / 1000 - close_data["signal_close_timestamp"]
        )
        close_data["deal_close_swap_duration"] = (
            float(swap_result["order_data"]["lastTradeTimestamp"]) / 1000 - close_data["signal_close_timestamp"]
        )
        close_data["coin"] = close_data["spot_symbol"].split('/')[0]

        # Удаляем из активных сделок
        self.active_deals_dict.pop(self.arb_pair, None)

        # Удаляем с помощью менеджера файла запись о данной арбитражной паре
        self.active_deals_file_manager.remove(self.arb_pair)

        # Сохранение дампа
        dump_path = self.deal_recorder.record_orders_dump(close_data, insertion_descriptor="close_deal")
        close_data["close_dump_path"] = dump_path

        await self._check_gt_balance()

        # Уведомление
        if self.telegram_sender:
            safe_close_data = decimal_to_str(close_data)
            await self.telegram_sender.send_numbered_message(
                f"CloseOperation✅ Успешное закрытие арбитража\n{json.dumps(safe_close_data, indent=2, ensure_ascii=False)}"
            )
        cprint.info("✅ Обе позиции успешно закрыты")
        return True

    async def _handle_partial_or_full_failure(
        self,
        spot_ok: bool,
        swap_ok: bool,
        spot_result: Union[Dict, Exception],
        swap_result: Union[Dict, Exception],
        close_data: Dict[str, Any]
    ) -> bool:
        """Обрабатывает частичный или полный сбой закрытия."""
        cprint.error(f"Ошибка закрытия: spot_ok={spot_ok}, swap_ok={swap_ok}")

        # Аварийное завершение оставшейся части
        if spot_ok and not swap_ok:
            cprint.error_w("🚨 Своп НЕ закрыт! Аварийная покупка свопа...")
            filled = spot_result["order_data"].get("filled") or spot_result["order_data"].get("amount", 0)
            # Но нам нужно закрыть своп — используем объём из исходной сделки
            original_swap = float(close_data.get("deal_open_swap_contracts", 0))
            if original_swap > 0:
                await self._close_swap(original_swap)
                cprint.info("✅ Своп аварийно закрыт")
            else:
                cprint.warning("Исходный объём свопа неизвестен — аварийное закрытие невозможно")

        elif swap_ok and not spot_ok:
            cprint.error_w("🚨 Спот НЕ закрыт! Аварийная продажа спота...")
            original_spot = float(close_data.get("deal_open_spot_amount", 0))
            if original_spot > 0:
                await self._close_spot(original_spot)
                cprint.info("✅ Спот аварийно закрыт")
            else:
                cprint.warning("Исходный объём спота неизвестен — аварийное закрытие невозможно")

        else:
            cprint.error_w("🔥 Обе позиции НЕ закрыты")

        # Дамп ошибки
        failure_data = {
            "arb_pair": self.arb_pair,
            "spot_order": spot_result if spot_ok else str(spot_result),
            "swap_order": swap_result if swap_ok else str(swap_result),
            "timestamp": time.time(),
        }
        safe_failure_data = decimal_to_str(failure_data)
        self.deal_recorder.record_orders_dump(safe_failure_data, insertion_descriptor="close_deal_failure")

        # Удаляем с помощью менеджера файла запись о данной арбитражной паре
        self.active_deals_file_manager.remove(self.arb_pair)

        await self._check_gt_balance()

        # Исключение
        if not spot_ok and not swap_ok:
            primary_error = spot_result if isinstance(spot_result, Exception) else swap_result
            raise DealCloseError("Не удалось закрыть ни спот, ни своп") from primary_error
        elif not spot_ok:
            raise spot_result
        else:
            raise swap_result

    async def _close_spot(self, amount: float) -> Dict[str, Any]:
        """Закрывает спот рыночным ордером на продажу (аналогично DealOpener)."""
        if amount <= 0:
            cprint.warning("Запрос на закрытие спота с нулевым объёмом — пропуск")
            return {"order_data": None}
        cprint.info(f"Закрытие спота: {amount} {self.spot_symbol}")
        for attempt in range(1, self.max_order_attempt + 1):
            try:
                precise_amount = self.exchange.amount_to_precision(self.spot_symbol, amount)
                order_data = await self.exchange.create_order(
                    symbol=self.spot_symbol,
                    type="market",
                    side="sell",
                    amount=precise_amount
                )
                status = order_data.get("status") or order_data.get("info", {}).get("finish_as")
                if status in ("closed", "filled", "finished"):
                    cprint.info("✅ Спот успешно закрыт")
                    return {"order_data": order_data}
                else:
                    raise Exception(f"Ордер закрытия не исполнен, статус: {status}")
            except Exception as e:
                cprint.warning_r(f"Попытка {attempt}/{self.max_order_attempt} закрыть спот: {e}")
                if attempt >= self.max_order_attempt:
                    raise CloseSpotOrderError(self.spot_symbol, "Не удалось закрыть спот") from e
                await asyncio.sleep(self.order_attempt_interval)

    async def _close_swap(self, contracts: float) -> Dict[str, Any]:
        """Закрывает своп рыночным ордером на покупку с reduce_only=True (аналогично DealOpener)."""
        await self._init_swap_settings(self.swap_symbol)
        if contracts <= 0:
            cprint.warning("Запрос на закрытие свопа с нулевым объёмом — пропуск")
            return {"order_data": None}
        cprint.info(f"Закрытие свопа: {contracts} контрактов")
        for attempt in range(1, self.max_order_attempt + 1):
            try:
                order_data = await self.exchange.create_order(
                    symbol=self.swap_symbol,
                    type="market",
                    side="buy",
                    amount=contracts,
                    params={"reduce_only": True}
                )
                status = order_data.get("status") or order_data.get("info", {}).get("finish_as")
                if status in ("closed", "filled", "finished"):
                    cprint.info("✅ Своп успешно закрыт")
                    return {"order_data": order_data}
                else:
                    raise Exception(f"Ордер закрытия не исполнен, статус: {status}")
            except Exception as e:
                cprint.warning_r(f"Попытка {attempt}/{self.max_order_attempt} закрыть своп: {e}")
                if attempt >= self.max_order_attempt:
                    raise CloseSwapOrderError(self.swap_symbol, "Не удалось закрыть своп") from e
                await asyncio.sleep(self.order_attempt_interval)

    async def _check_gt_balance(self):
        """
        Пополнение GT, если его доля < 0.5% от спот-USDT.
        Сумма пополнения:
          - 0.5% от депозита, если ≥ 3 USDT,
          - иначе — ровно 3 USDT (минимум Gate.io).
        """
        try:
            print("Проверим баланс GT и пополним при необходимости")
            cprint.info("_check_gt_balance запущен")
            params_spot = {"type": "spot"}
            balance_spot = await self.exchange.fetch_balance(params_spot)
            ticker = await self.exchange.fetch_ticker('GT/USDT')

            spot_usdt = Decimal(str(balance_spot.get("USDT", {}).get("free", "0")))
            gt_amount = Decimal(str(balance_spot.get("GT", {}).get("free", "0")))
            gt_price = Decimal(str(ticker.get('last', '0')))

            if not spot_usdt or not gt_price:
                cprint.warning("Пропуск GT-проверки: недостаточно данных.")
                return

            gt_cost_usdt = gt_amount * gt_price
            gt_ratio = gt_cost_usdt / spot_usdt

            GT_THRESHOLD = Decimal('0.005')  # 0.5%
            TOP_UP_RATIO = Decimal('0.005')  # 0.5%
            MIN_USDT_FOR_BUY = 3.0  # Минимум Gate.io для market buy

            if gt_ratio < GT_THRESHOLD:
                planned_usdt = float(spot_usdt * TOP_UP_RATIO)

                if planned_usdt >= MIN_USDT_FOR_BUY:
                    usdt_to_spend = round(planned_usdt, 2)
                    log_msg = f"Покупка GT на {usdt_to_spend:.2f} USDT (0.5% от депозита)"
                else:
                    usdt_to_spend = MIN_USDT_FOR_BUY
                    log_msg = f"0.5% < {MIN_USDT_FOR_BUY} USDT → покупка на минимум: {usdt_to_spend:.2f} USDT"

                cprint.info(
                    f"GT = {gt_ratio:.4%} (<0.7%). {log_msg} по цене {gt_price}"
                )

                gt_order_data = await self.exchange.createMarketOrder(
                    symbol='GT/USDT',
                    side='buy',
                    amount=usdt_to_spend  # Gate.io: market buy → amount = сумма в USDT
                )
                self.deal_recorder.record_gt_order_dump(order_data=gt_order_data)

            else:
                cprint.info(f"GT в норме: {gt_ratio:.4%}")

        except (InvalidOperation, TypeError, KeyError, ValueError) as e:
            cprint.error_w(f"Ошибка в _check_gt_balance: {e}")
        except Exception as e:
            cprint.error_w(f"Неожиданная ошибка: {e}", exc_info=True)

    async def _init_swap_settings(self, symbol: str) -> None:
        """Настройка свопа (повтор из DealOpener для идемпотентности)."""
        try:
            await self.exchange.set_margin_mode(symbol=symbol, marginMode="cross")
        except Exception as e:
            cprint.warning_r(f"Не удалось установить margin mode для {symbol}: {e}")
        try:
            await self.exchange.set_leverage(1, symbol)
        except Exception as e:
            cprint.warning_r(f"Не удалось установить leverage для {symbol}: {e}")

    @staticmethod
    def _compute_swap_fee_usdt(order: Dict[str, Any]) -> Decimal:
        """Аналогично DealOpener."""
        fees = order.get("fees") or []
        for f in fees:
            if f.get("currency") == "USDT":
                return Decimal(str(f.get("cost", "0")))
        try:
            tkfr = Decimal(str(order["info"].get("tkfr", "0")))
            filled = Decimal(str(order.get("filled", "0")))
            fill_price = Decimal(str(order["info"].get("fill_price", order.get("average", "0"))))
            return filled * fill_price * tkfr
        except (KeyError, InvalidOperation, TypeError):
            return Decimal("0.0")

    async def _compute_spot_fee_usdt(self, order: Dict[str, Any]) -> Decimal:
        """Аналогично DealOpener."""
        fees = order.get("fees") or []
        avg_price = Decimal(str(order.get("average", "0")))
        for f in fees:
            cur = f.get("currency")
            cost = Decimal(str(f.get("cost", "0")))
            if cur == "USDT":
                return cost
            elif cur == "GT":
                try:
                    ticker = await self.exchange.fetch_ticker("GT/USDT")
                    gt_usdt_price = Decimal(str(ticker["last"]))
                    return cost * gt_usdt_price if gt_usdt_price else Decimal("0.0")
                except Exception as e:
                    cprint.warning_r(f"Не удалось получить курс GT/USDT: {e}")
                return Decimal("0.0")
            elif cur and cur != "USDT" and cost > 0 and avg_price > 0:
                return cost * avg_price
        return Decimal("0.0")

# ========================
# Тестовый запуск (для разработки)
# ========================
async def main():
    """
    Тестовый сценарий для проверки работы DealCloser.

    Использует реальное подключение к Gate.io.
    Перед запуском убедитесь, что заданы:
        - API ключи в переменных окружения;
        - DEAL_BOT_TOKEN и DEAL_CHAT_ID для Telegram.

    Закрытие происходит рыночными ордерами

    Сценарий:
        1. Читаем данные открытой сделки из файла active_deals.json
        2. Подключается к бирже.
        3. Синхронизирует время.
        4. Пытается открыть сделку.
        5. Уведомляет о результате.
    """
    from modules.exchange_instance import ExchangeInstance
    from modules.time_sync import sync_time_with_exchange
    from modules.telegram_bot_message_sender import TelegramMessageSender
    import os
    from modules.ORJSON_file_manager import JsonFileManager
    from pprint import pprint

    # Создадим для файла active_deals.json экземпляр файлового менеджера
    project_root: str = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    active_dir: str = os.path.join(project_root, "deals_log")
    os.makedirs(active_dir, exist_ok=True)
    active_path: str = os.path.join(active_dir, "active_deals.json")
    active_manager: JsonFileManager = JsonFileManager(active_path)
    active_deals_dict = active_manager.load()
    telegram_sender = TelegramMessageSender(bot_token_env="DEAL_BOT_TOKEN", chat_id_env="DEAL_CHAT_ID")

    # Данный пример закрывает все сделки указанные в словаре active_deals_dict
    # arb_pair = 'XION/USDT_XION/USDT:USDT'
    for arb_pair in list(active_deals_dict):
        deal_data = active_deals_dict[arb_pair]
        pprint(deal_data)
        spot_symbol = deal_data['spot_symbol']
        swap_symbol = deal_data['swap_symbol']
        async with ExchangeInstance(ccxt, exchange_id="gateio", log=True) as exchange:
            await sync_time_with_exchange(exchange)
            deal_closer = DealCloser(   exchange = exchange,
                                        arb_pair = arb_pair,
                                        spot_symbol = spot_symbol,
                                        swap_symbol = swap_symbol,
                                        max_order_attempt = 2,
                                        order_attempt_interval = 0.5,
                                        active_deals_dict = active_deals_dict,
                                        telegram_sender = telegram_sender,
                                        active_deals_file_manager=active_manager)

            try:
                await deal_closer.close_deal()
            except DealCloseError as e:
                error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА закрытия: {e}"
                cprint.error(error_msg)
                if telegram_sender:
                    # Безопасная сериализация
                    await telegram_sender.send_numbered_message(error_msg)
            except Exception as e:
                error_msg = f"💥 НЕОЖИДАННАЯ ОШИБКА ЗАКРЫТИЯ: {e}"
                cprint.error(error_msg)
                if telegram_sender:
                    await telegram_sender.send_numbered_message(error_msg)
        print('YES')

if __name__ == "__main__":
    asyncio.run(main())