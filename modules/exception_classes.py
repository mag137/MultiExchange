__version__ = "2.1"
"""
Библиотека классов исключений.
"""

import os
import logging
from typing import Any

from modules.logger import LoggerFactory


class BaseArbitrageException(Exception):
    """Базовый класс для всех арбитражных исключений."""
    _log_filename = "exceptions.log"
    _logger = None
    _name = None

    @classmethod
    def get_logger(cls):
        if cls._logger is None:
            cls._logger = LoggerFactory.get_logger(
                name=cls._name,
                log_filename=cls._log_filename,
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
        return cls._logger

    @property
    def logger(self):
        return self.get_logger()

    def __init__(self, message: str, level: int = logging.ERROR):
        """
        Args:
            message (str): Текст ошибки
            level (int): Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.class_name = self.__class__.__name__
        full_message = f"[{self.class_name}] {message}"
        super().__init__(full_message)
        self.logged_file = self._log_message(full_message, level)

    def _log_message(self, message: str, level: int = logging.WARNING) -> str:
        """
        Универсальный метод логирования. Возвращает путь к лог-файлу
        (предполагается, что LoggerFactory возвращает путь при вызове методов логгера).
        """
        if level >= logging.CRITICAL:
            return self.logger.critical(message)
        elif level >= logging.ERROR:
            return self.logger.error(message)
        elif level >= logging.WARNING:
            return self.logger.warning(message)
        elif level >= logging.INFO:
            return self.logger.info(message)
        else:
            return self.logger.debug(message)

class BaseArbitrageCalcException(BaseArbitrageException):
    """Базовый класс для исключений, связанных с расчётом объёмов и условий сделок."""
    _log_filename = "exceptions.log"
    _logger = None
    _name = "exceptions"

class BaseArbitrageOrderExecutionException(BaseArbitrageException):
    """Базовый класс для исключений, связанных с исполнением ордеров."""
    _log_filename = "orders_errors.log"
    _logger = None
    _name = "orders_errors"


# === Исключения расчёта ===

class ReconnectLimitExceededError(BaseArbitrageCalcException):
    """Превышено максимальное количество попыток подключения."""
    def __init__(self, exchange_id: str = '', symbol: str = '', attempts: int | None = None):
        message = f"[{exchange_id}][{symbol}]: Превышено количество попыток подключения: {attempts}"
        self.symbol = symbol
        self.attempts = attempts
        super().__init__(message, level=logging.WARNING)

# Данное исключение выбрасывает get_average_orderbook_price - если при вычислении средней цены не хватает объема стакана
class InvalidOrEmptyOrderBookError(BaseArbitrageCalcException):
    """Некорректные или пустые данные стакана."""
    def __init__(self, exchange_id: str = '', symbol: str = '', orderbook_data: Any = None):
        self.exchange_id = exchange_id
        self.symbol = symbol
        self.orderbook = orderbook_data

        if not isinstance(self.orderbook, dict):
            message = f"Неправильный тип данных стакана: {type(self.orderbook).__name__}"
        elif len(self.orderbook) == 0:
            message = "Стакан пуст (len == 0)"
        elif 'asks' not in self.orderbook or 'bids' not in self.orderbook:
            message = "Отсутствуют ключи 'asks' и/или 'bids' в данных стакана"
        else:
            message = "Неизвестная ошибка структуры стакана"

        self.result_message = f"[{exchange_id}][{symbol}]: {message}. Orderbook: {self.orderbook}"
        super().__init__(self.result_message, level=logging.WARNING)

# Данное исключение выбрасывает get_average_orderbook_price - если при вычислении средней цены не хватает объема стакана
class InsufficientOrderBookVolumeError(BaseArbitrageCalcException):
    """Недостаточный объём в стакане для выполнения ордера.
    Данное исключение выбрасывает get_average_orderbook_price - если при вычислении средней цены не хватает объема стакана
    """
    def __init__(
        self,
        exchange_id: str,
        symbol: str,
        orderbook_remains: str = "",
        money_usdt: str = ""
    ):
        self.exchange_id = exchange_id
        self.symbol = symbol
        self.orderbook_remains = orderbook_remains
        self.money_usdt = money_usdt
        message = (
            f"[{exchange_id}][{symbol}]: Недостаточный объём стакана. "
            f"Не хватает монет на {orderbook_remains} USDT для сделки объёмом {money_usdt} USDT"
        )
        super().__init__(message, level=logging.WARNING)

# Данное исключение выбрасывает MarketSortData.deal_amount_calc_func - если вычисленные объемы меньше лимитов биржи по инструменту
class InsufficientSpotCostFundsError(BaseArbitrageCalcException):
    """Недостаточно USDT на спотовом балансе для покрытия минимальной стоимости."""
    def __init__(self, exchange_id: str, symbol: str, balance_spot: float, min_spot_cost: float):
        self.symbol = symbol
        self.balance = balance_spot
        self.required_cost = min_spot_cost
        message = (
            f"[{exchange_id}][{symbol}]: Недостаточно средств на покупку. "
            f"Баланс спот USDT: {balance_spot} < min_cost={min_spot_cost}"
        )
        super().__init__(message, level=logging.WARNING)

# Данное исключение выбрасывает MarketSortData.deal_amount_calc_func - если вычисленные объемы меньше лимитов биржи по инструменту
class InsufficientSwapCostFundsError(BaseArbitrageCalcException):
    """Недостаточно USDT на своп-балансе для покрытия минимальной стоимости."""
    def __init__(self, exchange_id: str, symbol: str, balance_swap: float, min_swap_cost: float):
        self.symbol = symbol
        self.balance = balance_swap
        self.required_cost = min_swap_cost
        message = (
            f"[{exchange_id}][{symbol}]: Недостаточно средств на покупку. "
            f"Баланс своп USDT: {balance_swap} < min_cost={min_swap_cost}"
        )
        super().__init__(message, level=logging.WARNING)

# Данное исключение выбрасывает MarketSortData.deal_amount_calc_func - если вычисленные объемы меньше лимитов биржи по инструменту
class InsufficientSpotAmountFundsError(BaseArbitrageCalcException):
    """Рассчитанный объём (amount) для спота ниже минимального лота."""
    def __init__(self, exchange_id: str, symbol: str, spot_amount: float, min_spot_amount: float):
        self.symbol = symbol
        self.amount = spot_amount
        self.required_amount = min_spot_amount
        message = (
            f"[{exchange_id}][{symbol}]: Недостаточный объём для спот-сделки. "
            f"Расчётный amount: {spot_amount} < min_amount={min_spot_amount}"
        )
        super().__init__(message, level=logging.WARNING)

# Данное исключение выбрасывает MarketSortData.deal_amount_calc_func - если вычисленные объемы меньше лимитов биржи по инструменту
class InsufficientSwapAmountFundsError(BaseArbitrageCalcException):
    """Рассчитанный объём (amount) для свопа ниже минимального лота."""
    def __init__(self, exchange_id: str, symbol: str, swap_amount: float, min_swap_amount: float):
        self.symbol = symbol
        self.amount = swap_amount
        self.required_amount = min_swap_amount
        message = (
            f"[{exchange_id}][{symbol}]: Недостаточный объём для своп-сделки. "
            f"Расчётный amount: {swap_amount} < min_amount={min_swap_amount}"
        )
        super().__init__(message, level=logging.WARNING)


# === Исключения исполнения ордеров (только symbol и message) ===
class OrderExecutionError(BaseArbitrageOrderExecutionException):
    def __init__(self, order_type: str, symbol: str, message: str):
        self.symbol = symbol
        full_msg = f"[{order_type}][{symbol}] {message}"
        super().__init__(full_msg, level=logging.ERROR)

class OpenSpotOrderError(OrderExecutionError):
    def __init__(self, symbol: str, message: str):
        super().__init__("SPOT_OPEN", symbol, message)

class OpenSwapOrderError(OrderExecutionError):
    def __init__(self, symbol: str, message: str):
        super().__init__("SWAP_OPEN", symbol, message)

class CloseSpotOrderError(OrderExecutionError):
    def __init__(self, symbol: str, message: str):
        super().__init__("SPOT_CLOSE", symbol, message)

class CloseSwapOrderError(OrderExecutionError):
    def __init__(self, symbol: str, message: str):
        super().__init__("SWAP_CLOSE", symbol, message)

class DealOpenError(BaseArbitrageOrderExecutionException):
    def __init__(self, message: str):
        super().__init__(message, level=logging.CRITICAL)

class DealCloseError(BaseArbitrageOrderExecutionException):
    def __init__(self, message: str):
        super().__init__(message, level=logging.CRITICAL)

# === Вспомогательные классы для тестов ===

class MockExchange:
    def __init__(self, exchange_id: str):
        self.id = exchange_id
        self.exchange_id = exchange_id


# === Тесты (без InsufficientFundsError и без тестов на OrderExecutionError) ===

def test_exceptions():
    exchange = MockExchange("HTX")
    pair = "BTC/USDT"

    # 1. ReconnectLimitExceededError
    try:
        attempts = 6
        if attempts > 5:
            raise ReconnectLimitExceededError(
                exchange_id=exchange.exchange_id,
                symbol=pair,
                attempts=attempts
            )
    except ReconnectLimitExceededError as e:
        print(f"✅ Caught: {e}")

    # 2. InvalidOrEmptyOrderBookError — пустой стакан
    try:
        orderbook_data = {}
        if not orderbook_data:
            raise InvalidOrEmptyOrderBookError(
                exchange.exchange_id,
                pair,
                orderbook_data
            )
    except InvalidOrEmptyOrderBookError as e:
        print(f"✅ Caught: {e}")

    # 3. InsufficientOrderBookVolumeError — мало объёма
    try:
        requested = 1000
        available = 50
        if available < requested:
            raise InsufficientOrderBookVolumeError(
                exchange.exchange_id,
                pair,
                orderbook_remains=str(available),
                money_usdt=str(requested)
            )
    except InsufficientOrderBookVolumeError as e:
        print(f"✅ Caught: {e}")

    # 4. OrderExecutionError — ошибка исполнения ордера (например, при открытии спота)
    try:
        raise OpenSpotOrderError(
            symbol=pair,
            message="API вернул ошибку: insufficient balance"
        )
    except OpenSpotOrderError as e:
        print(f"✅ Caught OrderExecutionError: {e}")
        print(f"   Symbol: {e.symbol}")
        # Проверим, что сообщение содержит нужные части
        assert "[SPOT_OPEN]" in str(e)
        assert pair in str(e)
        assert "insufficient balance" in str(e)


if __name__ == "__main__":
    test_exceptions()