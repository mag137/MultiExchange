__version__ = "1.13"

from decimal import Decimal,  ROUND_UP, ROUND_DOWN, InvalidOperation,  ConversionSyntax
from modules.colored_console import cprint
from modules.exception_classes import InsufficientOrderBookVolumeError, InvalidOrEmptyOrderBookError

import math
from typing import  Optional, Union, List, Tuple
from modules.logger import LoggerFactory
from datetime import datetime, UTC, timezone
import asyncio
import functools
import logging
from typing import Any, Callable, Coroutine

logger = logging.getLogger(__name__)

datetime.now(UTC)

# Настройка логгера
logger = LoggerFactory.get_logger("app." + __name__)

bids = [(4.001, 100), (3.002, 100), (2.01111111, 1000)]
asks = [(2, 100), (3, 100), (4, 100), (5, 100), (6, 100), (7, 100), (8, 100), (9, 100), (10, 1000)]
target_sell = 800

# exmo:
asks = [[0.9358, 10000.0, 9358], [0.93884178, 10550.1493838, 9904], [0.93999999, 6174.23585146, 5803], [0.94, 250.0, 235]]
bids = [[0.93153111, 321.1131314, 299], [0.93096845, 6638.90651867, 6180], [0.92995443, 321.49172159, 298], [0.92896514, 5427.17311525, 5041]]


def get_current_iso_and_timestamp() -> dict:
    """
    Возвращает словарь с текущим временем в формате биржи.
    """
    now_utc = datetime.now(timezone.utc)
    dt_string = now_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    timestamp_ms = int(now_utc.timestamp() * 1000)
    return {
        'datetime' : dt_string,
        'timestamp': timestamp_ms
    }

def timestamp_to_print(timestamp: Union[int, float, str]) -> str:
    """
    Преобразует timestamp в формате миллисекунд в строку: "чч:мм:сс.мс дд.мм.гггг"
    """
    try:
        if timestamp:
            ts = float(timestamp) / 1000  # перевод в секунды
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return f"{dt.strftime('%H:%M:%S.%f')[:-3]} {dt.strftime('%d.%m.%Y')}"
        else:
            return f"Неверный timestamp: {timestamp}"
    except Exception as error:
        print(f'Error: function "timestamp_to_print": {error}')
        raise

def is_valid_price(price):
    '''Проверяет цену на возможность математических операций
    :param price:
    :return:
    '''
    return isinstance(price, (int, float)) and not math.isnan(price) and math.isfinite(price)

def count_decimal_places(number):
    """
    Определяет количество знаков после запятой у числа считая также нули после запятой.
    Отличие от get_decimal_precision():
    count_decimal_places(1.0) вернет 1,
    count_decimal_places(1) вернет 0,
    Параметры:
    number (float или int): Число, для которого нужно вычислить количество знаков после запятой.

    Возвращает:
    int: Количество знаков после запятой. Если число целое, возвращает 0.

    Примечание:
    Используется Decimal для точности работы с числами с плавающей точкой,
    так как встроенный тип float может приводить к неточным результатам.
    """
    # Преобразуем число в строку и передаем в Decimal для точных вычислений
    decimal_number = Decimal(str(number))
    # Возвращаем максимальное из 0 и отрицательной степени числа (количество знаков после запятой)
    return max(0, -decimal_number.as_tuple().exponent)

def get_decimal_precision(number) -> int:
    """
    Возвращает количество знаков после запятой (десятичную степень)
    для числа вида 0.001 → 3
    В числе вида 1.0 - ноль за разряд не считается!
    """
    num_str = format(number, 'f').rstrip('0')
    if '.' in num_str:
        return len(num_str.split('.')[1])
    return 0

def _count_decimal_places(value: Union[int, float, str, Decimal]) -> int:
    """
    Возвращает количество знаков после запятой в числе.
    Работает с int, float, str, Decimal.
    """
    if isinstance(value, Decimal):
        d = value
    else:
        d = Decimal(str(value))
    exponent = d.as_tuple().exponent
    return max(0, -exponent)

def safe_decimal(val, field_name: str = ""):
    """Безопасная конвертация в Decimal с логгированием."""
    if val is None or val == "" or val == "null" or isinstance(val, type(None)):
        cprint.warning_r(f"[_get_PnL] Поле '{field_name}' пустое: {val}. Используем 0.")
        return Decimal('0')
    try:
        return Decimal(str(val))
    except (InvalidOperation, ConversionSyntax) as e:
        cprint.error_w(f"[_get_PnL] Ошибка конвертации '{field_name}'='{val}': {e}")
        return Decimal('0')

def round_up(
        value: Union[int, float, str, Decimal],
        decimals: int
) -> Union[float, Decimal]:
    """
    Универсальное округление вверх до заданного количества знаков.

    Поддерживает: int, float, str, Decimal.
    Возвращает:
        - Decimal → Decimal
        - float/int/str → float

    Args:
        value: Число для округления
        decimals: Количество знаков после запятой

    Returns:
        Округлённое число. Тип зависит от входа: Decimal → Decimal, остальное → float

    Raises:
        ValueError: Если значение не может быть преобразовано в число
    """
    if isinstance(value, Decimal):
        # Если на входе Decimal — возвращаем Decimal
        return value.quantize(Decimal('0.1') ** decimals, rounding=ROUND_UP)
    else:
        # Для float, int, str — конвертируем через Decimal, возвращаем float
        try:
            d = Decimal(str(value))
            rounded = d.quantize(Decimal('0.1') ** decimals, rounding=ROUND_UP)
            return float(rounded)
        except Exception as e:
            raise ValueError(f"Не удалось округлить значение: {value}, ошибка: {e}")


def round_down(
        value: Union[int, float, str, Decimal],
        decimals: int
) -> Union[float, Decimal]:
    """
    Универсальное округление вниз.
    Аналогично round_up.
    """
    if isinstance(value, Decimal):
        return value.quantize(Decimal('0.1') ** decimals, rounding=ROUND_DOWN)
    else:
        try:
            d = Decimal(str(value))
            rounded = d.quantize(Decimal('0.1') ** decimals, rounding=ROUND_DOWN)
            return float(rounded)
        except Exception as e:
            raise ValueError(f"Не удалось округлить значение: {value}, ошибка: {e}")


def get_average_orderbook_price(
        data: List[Tuple[Union[int, float, str, Decimal], Union[int, float, str, Decimal], ...]],
        money: Union[float, str, Decimal],
        is_ask: bool,
        log: bool = False,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None
) -> Decimal:
    """
    Рассчитывает среднюю цену исполнения рыночного ордера по стакану (order book).

    Алгоритм:
    - Имитирует исполнение рыночного ордера на сумму `money`
    - Проходит по ордерам сверху вниз (от лучшей цены)
    - Накапливает количество купленных/проданных монет
    - Возвращает среднюю цену: total_money / total_coins

    Args:
        data: Список ордеров в формате [(price, volume, ...), ...]
              price и volume могут быть int, float, str, Decimal
        money: Сумма в валюте котировки (напр. USDT), которую нужно потратить (при покупке) или получить (при продаже)
               Может быть float, str, Decimal
        is_ask: True — покупка (берём ask), округление средней цены вверх; False — продажа (bid), округление вниз
        log: Если True — выводит подробные логи
        exchange: Название биржи (для логов и ошибок)
        symbol: Торговая пара (для логов и ошибок)

    Returns:
        Средняя цена исполнения как Decimal, округлённая с учётом точности стакана и направления сделки.

    Raises:
        ValueError: Если данные некорректны (не числа, отрицательные объёмы, money <= 0)
        InsufficientOrderBookVolumeError: Если в стакане недостаточно объёма для покрытия `money`
    """
    # Обработка ваианта получения некорректного стакана
    if not isinstance(data, list) or not data:
        msg = f"Пустой или некорректный стакан: exchange={exchange}, symbol={symbol}"
        cprint.warning_r(f"{symbol}: {msg}, type: {type(data)}: {data}")
        raise InvalidOrEmptyOrderBookError(exchange_id=exchange, symbol=symbol, orderbook_data=data)

    try:
        money_dec = Decimal(str(money))
    except Exception as e:
        raise ValueError(f"Не удалось преобразовать money в Decimal: {money}, ошибка: {e}")

    if money_dec <= 0:
        raise ValueError(f"Параметр 'money' должен быть > 0, получено: {money}")

    remains: Decimal = money_dec
    coins: Decimal = Decimal('0')
    max_decimal_places: int = 0  # Максимальная точность цен в стакане

    for i, item in enumerate(data):
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            raise ValueError(f"Ордер должен быть списком/кортежем из минимум двух элементов: {item}")

        price_raw, volume_raw = item[:2]

        # Конвертируем в Decimal
        try:
            price = Decimal(str(price_raw))
            volume = Decimal(str(volume_raw))
        except Exception as e:
            raise ValueError(f"Не удалось преобразовать цену или объём: price={price_raw}, volume={volume_raw}, ошибка: {e}")

        if price <= 0:
            raise ValueError(f"Цена должна быть > 0, получено: {price}")
        if volume < 0:
            raise ValueError(f"Объём не может быть отрицательным: {volume}")

        order_cost: Decimal = price * volume

        # Обновляем максимальную точность
        precision = _count_decimal_places(price)
        max_decimal_places = max(max_decimal_places, precision)

        if order_cost <= remains:
            # Полностью забираем ордер
            coins += volume
            remains -= order_cost
        else:
            # Частичное исполнение
            partial_coins = remains / price
            coins += partial_coins
            remains = Decimal('0')
            break  # Больше не нужно

    # Проверяем, хватило ли ликвидности
    if remains > 0:
        msg = (
            f"Недостаточно ликвидности в стакане: биржа={exchange}, пара={symbol}, "
            f"запрошено={money_dec}, не хватает={remains:.10f} USDT"
        )
        logger.warning(msg)
        raise InsufficientOrderBookVolumeError(exchange_id=exchange, symbol=symbol, orderbook_remains=remains, money_usdt=money)

    if coins == 0:
        msg = f"Не удалось получить ни одной монеты: exchange={exchange}, symbol={symbol}"
        logger.error(msg)
        raise ValueError(msg)

    average_price = money_dec / coins

    # Округление: при покупке — вверх, при продаже — вниз
    if is_ask:
        final_price = round_up(average_price, max_decimal_places)
    else:
        final_price = round_down(average_price, max_decimal_places)

    if log:
        logger.info(
                f"[get_average_orderbook_price] "
                f"exchange={exchange}, symbol={symbol}, direction={'BUY' if is_ask else 'SELL'}, "
                f"spend={money_dec:.8f}, coins={coins:.8f}, avg_price={average_price:.8f}, "
                f"final_price={final_price:.8f}"
        )

    return final_price





def sync_async_runner(f: Callable[..., Coroutine[Any, Any, Any]]) -> Callable[..., Any]:
    """
    Декоратор для синхронного вызова асинхронных функций.

    Поведение:
    - Если нет активного event loop → создаёт новый, использует его, закрывает.
    - Если есть активный loop → вызывает ошибку (т.к. нельзя запускать sync из async).
    """

    @functools.wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            # Проверяем, есть ли уже запущенный loop
            loop = asyncio.get_running_loop()
            # Если мы здесь — значит, уже внутри async-контекста
            raise RuntimeError(
                f"Нельзя вызывать {f.__name__} синхронно изнутри асинхронного контекста. "
                "Используйте 'await' вместо синхронного вызова."
            )
        except RuntimeError:
            # Нет активного loop — создаём свой
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(f(*args, **kwargs))
                logger.debug(f"Функция {f.__name__} успешно завершена.")
                return result
            except Exception as e:
                logger.error(
                    f"Ошибка при выполнении асинхронной функции {f.__name__}: {e}",
                    exc_info=True
                )
                raise
            finally:
                try:
                    loop.run_until_complete(loop.shutdown_asyncgens())
                except Exception:
                    pass  # Игнорируем ошибки при shutdown
                loop.close()
                asyncio.set_event_loop(None)

    return wrapper

if __name__ == '__main__':
    b = get_average_orderbook_price(bids, 1000, False)
    a = get_average_orderbook_price(asks, 5400, True, log=True)
    print(a, b)


    @sync_async_runner
    async def faulty_coroutine():
        await asyncio.sleep(0.1)
        raise ValueError("Что-то пошло не так!")


    try:
        faulty_coroutine()
    except Exception as e:
        logger.warning(f"Поймано исключение: {e}")  # через f-строку
        logger.warning("Поймано исключение: %s", e, exc_info=True)
