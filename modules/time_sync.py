__version__ = "1.1"

import ccxt.async_support as ccxt
from modules.colored_console import cprint
from modules.logger import LoggerFactory
import os
import logging

# ===============================================================
# ЛОГГЕР
# ===============================================================
logger = LoggerFactory.get_logger(
    name="time_sync",
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

async def fetch_exchange_time(exchange: ccxt.Exchange):
    try:
        if hasattr(exchange, 'fetch_time'):
            response = await exchange.fetch_time()
            if isinstance(response, int):
                return response
            elif isinstance(response, dict) and 'time' in response:
                return response['time']
            else:
                raise ValueError(f"Неожиданный формат ответа от {exchange.id}: {response}")
        else:
            raise NotImplementedError(f"Метод fetch_time() не поддерживается биржей {exchange.id}")
    except Exception as e:
        cprint.error(f"[{exchange.id}] Не удалось получить время сервера: {e}")
        raise


def calculate_time_difference(exchange_time: int, local_time: int) -> int:
    return exchange_time - local_time


async def sync_time_with_exchange(exchange: ccxt.Exchange, auto_adjust=True):
    try:
        local_time_before = exchange.milliseconds()
        exchange_time = await fetch_exchange_time(exchange)
        local_time_after = exchange.milliseconds()

        # Усредняем локальное время для точности
        local_time = (local_time_before + local_time_after) // 2

        time_diff = calculate_time_difference(exchange_time, local_time)

        if auto_adjust:
            exchange.options['adjustForTimeDifference'] = True
            exchange.options['timeDifference'] = time_diff

        cprint.success(f"[{exchange.id}] Время синхронизировано. Разница: {time_diff} мс")
        logger.debug(f"Биржа [{exchange.id}]: Время синхронизировано. Разница: {time_diff} мс")
        return time_diff
    except Exception as e:
        # cprint.error_b(f"[{exchange.id}] Ошибка при синхронизации времени: {e}")
        logger.error(f"Биржа [{exchange.id}]: Ошибка при синхронизации времени: {e}")
        return None