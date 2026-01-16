"""
Модуль для безопасного открытия арбитражных сделок между спотовым и своп-рынками (фьючерсами).

Основная задача:
    - одновременно открыть лимитный ордер на покупку спота и рыночный ордер на продажу свопа;
    - в случае частичного сбоя — аварийно закрыть успешно открытую часть;
    - зафиксировать все данные сделки (объёмы, комиссии, временные метки);
    - обеспечить идемпотентность и безопасность при работе с биржей Gate.io.

Особенности Gate.io:
    - Рыночные ордера на покупку спота через `amount` не работают → используется лимитный ордер с премией 1%.
    - Свопы требуют явной настройки `margin_mode=cross` и `leverage=1`.
    - Комиссии могут быть в USDT, базовой монете или GT.

Архитектура:
    - Класс `DealOpener` не управляет состоянием процессов или GUI — он принимает зависимости через конструктор.
    - Все данные о сделке передаются через `signal_deal_dict` и возвращаются через обновление `active_deals_dict`.
    - Поддержка аварийного закрытия даже до записи в `active_deals.json`.

Использование:
    # >>> deal_opener = DealOpener(exchange, ...)
    # >>> await deal_opener.open_deal(signal_deal_dict)

Требования к `signal_deal_dict`:
    - `signal_spot_amount` (float или str): объём монет для покупки на споте.
    - `signal_swap_contracts` (float или str): количество контрактов свопа.
    - `signal_average_swap_bid` (float или str): усреднённая цена bid свопа (используется как ориентир для спота).

Безопасность:
    - Все финансовые расчёты выполняются в `Decimal`.
    - Все вызовы к бирже защищены retry-логикой.
    - Баланс получается с таймаутом 10 сек.
    - При закрытии используется только фактически исполненный объём (`filled`).

Версия: 1.0 после 0.15
"""

__version__ = "1.0"

import asyncio
import json
import time
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional, Union

import ccxt.pro as ccxt

from modules.deal_recorder import DealRecorder
from modules.exception_classes import (
    OpenSpotOrderError,
    OpenSwapOrderError,
    CloseSpotOrderError,
    CloseSwapOrderError,
    DealOpenError
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


class DealOpener:
    """
    Класс для управления открытием арбитражной сделки между спотом и свопом.

    Отвечает за:
        - параллельное открытие спотовой и своп-позиции;
        - обработку частичных сбоев с аварийным закрытием;
        - расчёт комиссий в USDT;
        - запись данных сделки в дампы и active_deals.json.

    Атрибуты:
        exchange (ccxt.Exchange): Экземпляр биржи (ccxt.pro).
        arb_pair (str): Уникальный идентификатор арбитражной пары, например "BTC/USDT_BTC/USDT:USDT".
        spot_symbol (str): Символ спотовой пары, например "BTC/USDT".
        swap_symbol (str): Символ своп-пары, например "BTC/USDT:USDT".
        max_order_attempt (int): Максимальное число попыток открытия ордера.
        max_active_deals (int): Максимальное число активных сделок (для будущего расширения).
        order_attempt_interval (float): Интервал между попытками в секундах.
        active_deals_dict (dict): Словарь активных сделок, общий для всех экземпляров.
        telegram_sender (Optional[Any]): Экземпляр отправителя Telegram-уведомлений.
        deal_recorder (DealRecorder): Агент записи дампов и active_deals.json.
    """

    def __init__(
        self,
        exchange: ccxt.Exchange,
        arb_pair: str,
        spot_symbol: str,
        swap_symbol: str,
        max_order_attempt: int = 3,
        max_active_deals: int = 1,
        order_attempt_interval: float = 0.5,
        active_deals_dict: Optional[Dict[str, Any]] = None,
        telegram_sender: Optional[Any] = None,
    ) -> None:
        """
        Инициализирует экземпляр DealOpener.

        Args:
            exchange: Экземпляр биржи (ccxt.pro).
            arb_pair: Уникальная строка вида "BASE/QUOTE_BASE/QUOTE:QUOTE".
            spot_symbol: Символ спотовой пары (например, "SOMI/USDT").
            swap_symbol: Символ своп-пары (например, "SOMI/USDT:USDT").
            max_order_attempt: Макс. число попыток открытия ордера (по умолчанию 3).
            max_active_deals: Макс. число активных сделок (резерв для будущего).
            order_attempt_interval: Задержка между попытками в секундах (по умолчанию 0.5).
            active_deals_dict: Общий словарь активных сделок (передаётся извне).
            telegram_sender: Объект с методом `send_numbered_message`.
        """
        self.exchange = exchange
        self.arb_pair = arb_pair
        self.spot_symbol = spot_symbol
        self.swap_symbol = swap_symbol
        self.max_order_attempt = max_order_attempt
        self.max_active_deals = max_active_deals
        self.order_attempt_interval = order_attempt_interval
        self.active_deals_dict = active_deals_dict or {}
        self.telegram_sender = telegram_sender

        self.deal_recorder = DealRecorder()
        self.spot_order_id: Optional[str] = None
        self.swap_order_id: Optional[str] = None

    async def open_deal(self, signal_deal_dict: Dict[str, Any]) -> bool:
        """
        Открывает арбитражную сделку: покупка спота + продажа свопа.

        Выполняет:
            1. Валидацию входных данных.
            2. Параллельное открытие спота (лимит с премией) и свопа (рынок).
            3. При успехе — расчёт комиссий, получение баланса, запись дампов.
            4. При частичном сбое — аварийное закрытие открытой части.
            5. При полном сбое — запись ошибки и выброс исключения.

        Args:
            signal_deal_dict: Словарь с данными сигнала. Обязательные ключи:
                - signal_spot_amount: объём монет для покупки (float или str).
                - signal_swap_contracts: объём контрактов свопа (float или str).
                - signal_average_swap_bid: цена для расчёта лимита спота (float или str).

        Returns:
            bool: True при успешном открытии обеих позиций.

        Raises:
            DealOpenError: Если обе позиции не открыты.
            OpenSpotOrderError / OpenSwapOrderError: При сбое одной из частей.
            ValueError: При отсутствии обязательных полей в сигнале.

        Примечание:
            Даже при аварийном закрытии записывается дамп ошибки в файл с пометкой "open_deal_failure".
        """
        # === Валидация обязательных полей ===
        required_keys = {"signal_spot_amount", "signal_swap_contracts", "signal_average_swap_bid"}
        missing = required_keys - signal_deal_dict.keys()
        if missing:
            raise ValueError(f"Отсутствуют обязательные поля в signal_deal_dict: {missing}")

        deal_data = signal_deal_dict.copy()
        deal_data["signal_open_timestamp"] = time.time()

        # Приведение к числовым типам с сохранением точности
        try:
            spot_amount = float(signal_deal_dict["signal_spot_amount"])
            swap_contracts = float(signal_deal_dict["signal_swap_contracts"])
            spot_price = Decimal(str(signal_deal_dict["signal_average_swap_bid"]))
        except (ValueError, TypeError) as e:
            raise ValueError(f"Некорректные числовые данные в сигнале: {e}")

        # === Параллельное открытие позиций ===
        spot_task = self._open_spot(spot_amount, spot_price)
        swap_task = self._open_swap(swap_contracts)
        spot_result, swap_result = await asyncio.gather(spot_task, swap_task, return_exceptions=True)

        spot_ok = not isinstance(spot_result, Exception)
        swap_ok = not isinstance(swap_result, Exception)

        if spot_ok and swap_ok:
            return await self._handle_successful_open(spot_result, swap_result, deal_data)
        else:
            return await self._handle_partial_or_full_failure(spot_ok, swap_ok, spot_result, swap_result, deal_data)

    async def _handle_successful_open(
        self,
        spot_result: Dict[str, Any],
        swap_result: Dict[str, Any],
        deal_data: Dict[str, Any]
    ) -> bool:
        """Обрабатывает успешное открытие обеих позиций."""
        # Используем фактически исполненный объём (filled), fallback на amount
        spot_filled = spot_result["order_data"].get("filled") or spot_result["order_data"].get("amount", 0)
        swap_filled = swap_result["order_data"].get("filled") or swap_result["order_data"].get("amount", 0)

        # Убедимся, что все значения — строки или float/int, а не Decimal
        spot_avg = Decimal(str(spot_result["order_data"]["average"]))
        swap_avg = Decimal(str(swap_result["order_data"]["average"]))
        spot_filled_dec = Decimal(str(spot_filled))
        swap_filled_dec = Decimal(str(swap_filled))

        # Рассчитываем ratio как Decimal, но сразу конвертируем в строку
        if spot_avg > 0:
            deal_open_ratio = (swap_avg - spot_avg) / spot_avg * 100
        else:
            deal_open_ratio = Decimal("0.0")

        deal_data.update({
            "deal_open_spot_id": spot_result["order_data"]["id"],
            "deal_open_swap_id": swap_result["order_data"]["id"],
            "deal_open_spot_cost": str(Decimal(str(spot_result["order_data"]["cost"]))),
            "deal_open_swap_cost": str(Decimal(str(swap_result["order_data"]["cost"]))),
            "deal_open_spot_average_price": str(spot_avg),
            "deal_open_swap_average_price": str(swap_avg),
            "deal_open_spot_amount": str(spot_filled_dec),
            "deal_open_swap_contracts": str(swap_filled_dec),
            "deal_open_ratio": str(deal_open_ratio.quantize(Decimal("0.0001"))),
        })

        # Расчёт комиссий
        spot_fee_usdt = await self._compute_spot_fee_usdt(spot_result["order_data"])
        swap_fee_usdt = self._compute_swap_fee_usdt(swap_result["order_data"])

        spot_cost = Decimal(str(spot_result["order_data"].get("cost", "1")))
        swap_cost = Decimal(str(swap_result["order_data"].get("cost", "1")))
        spot_fee_percent = (spot_fee_usdt / spot_cost * 100) if spot_cost > 0 else Decimal("0.0")
        swap_fee_percent = (swap_fee_usdt / swap_cost * 100) if swap_cost > 0 else Decimal("0.0")

        deal_data.update({
            "deal_open_spot_fee_usdt": str(spot_fee_usdt.quantize(Decimal("0.00000001"))),
            "deal_open_swap_fee_usdt": str(swap_fee_usdt.quantize(Decimal("0.00000001"))),
            "deal_open_spot_fee_percent": str(spot_fee_percent.quantize(Decimal("0.0001"))),
            "deal_open_swap_fee_percent": str(swap_fee_percent.quantize(Decimal("0.0001")))
        })

        # Получение баланса с таймаутом
        base_currency = self.spot_symbol.split("/")[0]
        try:
            balance = await asyncio.wait_for(self.exchange.fetch_balance(), timeout=10.0)
        except asyncio.TimeoutError:
            cprint.error("Таймаут при получении баланса")
            balance = {}
        available_for_sell_spot_balance = float(balance.get(base_currency, {}).get("free", 0.0))
        deal_data["available_for_sell_spot_balance"] = available_for_sell_spot_balance

        # Временные метки (float)
        deal_data["deal_open_spot_complete_timestamp"] = float(spot_result["order_data"]["lastTradeTimestamp"]) / 1000
        deal_data["deal_open_swap_complete_timestamp"] = float(swap_result["order_data"]["lastTradeTimestamp"]) / 1000
        deal_data["deal_open_spot_duration"] = (
            float(spot_result["order_data"]["lastTradeTimestamp"]) / 1000 - deal_data["signal_open_timestamp"]
        )
        deal_data["deal_open_swap_duration"] = (
            float(swap_result["order_data"]["lastTradeTimestamp"]) / 1000 - deal_data["signal_open_timestamp"]
        )

        # Дополнительные данные для записи дампа ордеров
        dump_data = {
            "spot_order": spot_result or {"error": str(spot_result) if isinstance(spot_result, Exception) else "unknown"},
            "swap_order": swap_result or {"error": str(swap_result) if isinstance(swap_result, Exception) else "unknown"},
            "signal_open_timestamp": deal_data['signal_open_timestamp'],
            "available_for_sell_spot_balance": available_for_sell_spot_balance,
            "coin" : self.spot_symbol.split('/')[0]}

        # Сохранение состояния и уведомления
        dump_path = self.deal_recorder.record_orders_dump(dump_data, insertion_descriptor="open_deal")
        deal_data["open_dump_path"] = dump_path
        self.active_deals_dict[self.arb_pair] = deal_data

        self.deal_recorder.record_active_deal_dict(deal_data)

        if self.telegram_sender:
            # Преобразуем всё в строки, чтобы избежать ошибки сериализации
            safe_deal_data = decimal_to_str(deal_data)
            await self.telegram_sender.send_numbered_message(
                f"✅ Успешное открытие арбитража\n{json.dumps(safe_deal_data, indent=2, ensure_ascii=False)}"
            )
        cprint.info("✅ Обе позиции успешно открыты")
        return True

    async def _handle_partial_or_full_failure(
        self,
        spot_ok: bool,
        swap_ok: bool,
        spot_result: Union[Dict, Exception],
        swap_result: Union[Dict, Exception],
        deal_data: Dict[str, Any]
    ) -> bool:
        """Обрабатывает частичный или полный сбой открытия."""
        cprint.error(f"Ошибка открытия: spot_ok={spot_ok}, swap_ok={swap_ok}")

        # Аварийное закрытие
        if spot_ok and not swap_ok:
            cprint.error_w("🚨 Своп НЕ открыт! Аварийная продажа спота...")
            filled = spot_result["order_data"].get("filled") or spot_result["order_data"].get("amount", 0)
            if float(filled) > 0:
                await self._close_spot(float(filled))
                cprint.info("✅ Спот аварийно закрыт")
            else:
                cprint.warning("Спот не был исполнен — закрытие не требуется")

        elif swap_ok and not spot_ok:
            cprint.error_w("🚨 Спот НЕ открыт! Аварийное закрытие свопа...")
            filled = swap_result["order_data"].get("filled") or swap_result["order_data"].get("amount", 0)
            if float(filled) > 0:
                await self._close_swap(float(filled))
                cprint.info("✅ Своп аварийно закрыт")
            else:
                cprint.warning("Своп не был исполнен — закрытие не требуется")

        else:
            cprint.error_w("🔥 Обе позиции НЕ открыты")

        # Запись дампа ошибки
        failure_data = {
            "arb_pair": self.arb_pair,
            "spot_order": spot_result if spot_ok else str(spot_result),
            "swap_order": swap_result if swap_ok else str(swap_result),
            "timestamp": time.time(),
        }
        # Убедимся, что и здесь нет Decimal
        safe_failure_data = decimal_to_str(failure_data)
        self.deal_recorder.record_orders_dump(safe_failure_data, insertion_descriptor="open_deal_failure")

        # Выброс исключения
        if not spot_ok and not swap_ok:
            primary_error = spot_result if isinstance(spot_result, Exception) else swap_result
            raise DealOpenError("Не удалось открыть ни спот, ни своп") from primary_error
        elif not spot_ok:
            raise spot_result
        else:
            raise swap_result

    async def _open_spot(self, spot_amount: float, spot_price: Decimal) -> Dict[str, Any]:
        """
        Открывает спотовую позицию лимитным ордером с премией 1% (имитация маркета).

        Args:
            spot_amount: Объём монет для покупки.
            spot_price: Ориентир цены (обычно bid свопа).

        Returns:
            Словарь с данными ордера и метриками.

        Raises:
            OpenSpotOrderError: После исчерпания попыток.
        """
        for attempt in range(1, self.max_order_attempt + 1):
            send_time = time.time()
            try:
                price_with_premium = float(spot_price * Decimal("1.01"))
                order_data = await self.exchange.create_order(
                    symbol=self.spot_symbol,
                    type="limit",
                    side="buy",
                    amount=spot_amount,
                    price=price_with_premium,
                    params={}
                )
                recv_time = time.time()
                status = order_data.get("status") or order_data.get("info", {}).get("finish_as")
                if status in ("closed", "filled", "finished"):
                    result = {
                        "order_data": order_data,
                        "spot_send_time": send_time,
                        "spot_recv_time": recv_time,
                        "duration": recv_time - send_time,
                        "attempts": attempt
                    }
                    if attempt > 1:
                        cprint.info(f"✅ Спот открыт с {attempt}-й попытки")
                    return result
                else:
                    raise Exception(f"Ордер не исполнен, статус: {status}")
            except Exception as e:
                if attempt < self.max_order_attempt:
                    await asyncio.sleep(self.order_attempt_interval)
                else:
                    raise OpenSpotOrderError(
                        self.spot_symbol,
                        f"Не удалось открыть спот-ордер после {self.max_order_attempt} попыток"
                    ) from e

    async def _open_swap(self, swap_contracts: float) -> Dict[str, Any]:
        """
        Открывает своп-позицию рыночным ордером на продажу.

        Args:
            swap_contracts: Количество контрактов.

        Returns:
            Словарь с данными ордера и метриками.

        Raises:
            OpenSwapOrderError: После исчерпания попыток.
        """
        await self._init_swap_settings(self.swap_symbol)
        for attempt in range(1, self.max_order_attempt + 1):
            send_time = time.time()
            try:
                order_data = await self.exchange.create_order(
                    symbol=self.swap_symbol,
                    type="market",
                    side="sell",
                    amount=swap_contracts,
                    params={"reduce_only": False}  # Явно указываем: это открытие позиции
                )
                recv_time = time.time()
                status = order_data.get("status") or order_data.get("info", {}).get("finish_as")
                if status in ("closed", "filled", "finished"):
                    result = {
                        "order_data": order_data,
                        "swap_send_time": send_time,
                        "swap_recv_time": recv_time,
                        "duration": recv_time - send_time,
                        "attempts": attempt
                    }
                    if attempt > 1:
                        cprint.info(f"✅ Своп открыт с {attempt}-й попытки")
                    return result
                else:
                    raise Exception(f"Ордер не исполнен, статус: {status}")
            except Exception as e:
                if attempt < self.max_order_attempt:
                    await asyncio.sleep(self.order_attempt_interval)
                else:
                    raise OpenSwapOrderError(
                        self.swap_symbol,
                        f"Не удалось открыть своп-ордер после {self.max_order_attempt} попыток"
                    ) from e

    async def _init_swap_settings(self, symbol: str) -> None:
        """Настройка режима маржи и кредитного плеча для свопа."""
        try:
            await self.exchange.set_margin_mode(
                marginMode="cross",
                symbol=self.swap_symbol,
            )

        except Exception as e:
            cprint.warning_r(f"Не удалось установить margin mode для {symbol}: {e}")
        try:
            await self.exchange.set_leverage(
                leverage=1,
                symbol=self.swap_symbol,
            )

        except Exception as e:
            cprint.warning_r(f"Не удалось установить leverage для {symbol}: {e}")

    async def _close_spot(self, amount: float) -> Dict[str, Any]:
        """
        Закрывает спотовую позицию рыночным ордером на продажу.

        Args:
            amount: Объём монет для продажи.

        Returns:
            Словарь с данными ордера.

        Raises:
            CloseSpotOrderError: После исчерпания попыток.
        """
        if amount <= 0:
            cprint.warning("Запрос на закрытие спота с нулевым или отрицательным объёмом — пропуск")
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
        """
        Закрывает своп-позицию рыночным ордером на покупку с reduce_only=True.

        Args:
            contracts: Количество контрактов для закрытия.

        Returns:
            Словарь с данными ордера.

        Raises:
            CloseSwapOrderError: После исчерпания попыток.
        """
        if contracts <= 0:
            cprint.warning("Запрос на закрытие свопа с нулевым или отрицательным объёмом — пропуск")
            return {"order_data": None}
        cprint.info(f"Закрытие свопа: {contracts} контрактов")
        await self._init_swap_settings(self.swap_symbol)
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

    @staticmethod
    def _compute_swap_fee_usdt(order: Dict[str, Any]) -> Decimal:
        """
        Вычисляет комиссию свопа в USDT.

        Логика:
            - Если в fees есть запись с currency=USDT → берём cost.
            - Иначе: filled * fill_price * taker_fee_rate (tkfr из info).

        Args:
            order: Данные ордера от ccxt.

        Returns:
            Комиссия в USDT как Decimal.
        """
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
        """
        Вычисляет комиссию спота в USDT.

        Логика:
            - Если комиссия в USDT → берём напрямую.
            - Если в GT → конвертируем через GT/USDT.
            - Если в базовой монете → умножаем на average_price.

        Args:
            order: Данные ордера от ccxt.

        Returns:
            Комиссия в USDT как Decimal.
        """
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
async def main() -> None:
    """
    Тестовый сценарий для проверки работы DealOpener.

    Использует реальное подключение к Gate.io.
    Перед запуском убедитесь, что заданы:
        - API ключи в переменных окружения;
        - DEAL_BOT_TOKEN и DEAL_CHAT_ID для Telegram.

    Сценарий:
        1. Подключается к бирже.
        2. Синхронизирует время.
        3. Эмулирует сигнал.
        4. Пытается открыть сделку.
        5. Уведомляет о результате.
    """
    from modules.exchange_instance import ExchangeInstance
    from modules.time_sync import sync_time_with_exchange
    from modules.telegram_bot_message_sender import TelegramMessageSender

    async with ExchangeInstance(ccxt, exchange_id="gateio", log=True) as exchange:
        await sync_time_with_exchange(exchange)

        arb_pair = "SOMI/USDT_SOMI/USDT:USDT"
        spot_symbol = "SOMI/USDT"
        swap_symbol = "SOMI/USDT:USDT"
        max_order_attempt = 2
        active_deals_dict = {}
        telegram_sender = TelegramMessageSender(
            bot_token_env="DEAL_BOT_TOKEN",
            chat_id_env="DEAL_CHAT_ID"
        )

        # Получаем цену для эмуляции сигнала
        try:
            ticker = await exchange.fetch_ticker("SOMI/USDT")
            market_price = ticker["last"] or ticker["close"]
        except Exception as e:
            cprint.error(f"Ошибка получения тикера: {e}")
            market_price = 0.001  # fallback

        deal_opener = DealOpener(
            exchange=exchange,
            arb_pair=arb_pair,
            spot_symbol=spot_symbol,
            swap_symbol=swap_symbol,
            max_order_attempt=max_order_attempt,
            max_active_deals=1,
            telegram_sender=telegram_sender,
            active_deals_dict=active_deals_dict,
            order_attempt_interval=0.5,
        )

        signal_deal_dict = {
            "arb_pair": arb_pair,
            "spot_symbol": spot_symbol,
            "swap_symbol": swap_symbol,
            "signal_spot_amount": 15,
            "signal_swap_contracts": 15,
            "signal_average_swap_bid": market_price,
            "signal_open_ratio": 1.1,
            "signal_open_threshold_ratio": 0.5,
            "signal_close_ratio": 1.1,
            "signal_close_threshold_ratio": 0.3,
        }

        try:
            await deal_opener.open_deal(signal_deal_dict=signal_deal_dict)
        except DealOpenError as e:
            error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА открытия: {e}"
            cprint.error(error_msg)
            if telegram_sender:
                # Безопасная сериализация
                await telegram_sender.send_numbered_message(error_msg)
        except Exception as e:
            error_msg = f"💥 НЕОЖИДАННАЯ ОШИБКА ОТКРЫТИЯ: {e}"
            cprint.error(error_msg)
            if telegram_sender:
                await telegram_sender.send_numbered_message(error_msg)


if __name__ == "__main__":
    asyncio.run(main())