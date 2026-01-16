__version__ = "3.0 OKX"

import asyncio
import signal
import logging
from modules.task_manager import TaskManager
from modules.logger import LoggerFactory
from modules.process_manager import ProcessManager
from modules.arbitrage_pairs import run_analytic_process_wrapper

from modules.test_process_value_getter import shared_values_receiver
from modules.test_process_value_setter import string_writer_process
from modules.TkGrid3 import run_gui_grid_process
from functools import partial
import itertools
import multiprocessing
from pprint import pprint
import random

# === Инициализация ===
shutdown_event = asyncio.Event()
task_manager = TaskManager()
p_manager = ProcessManager()
shared_values = {}
logger = LoggerFactory.get_logger(
        name="app",
        level=logging.DEBUG,
        split_levels=True,
        use_timed_rotating=True,
        use_dated_folder=True,
)
exchange_id = 'okx'  # Название биржи
max_active_deals = 1  # Максимальное количество сделок


def handle_shutdown_signal(signum=None, frame=None):
    logger.warning("[Main] Получен сигнал завершения (SIGINT/SIGTERM/KeyboardInterrupt)")
    try:
        shared_values["shutdown"].value = True
    except Exception as e:
        logger.error(f"[Main] Ошибка при установке shutdown=True: {e}")
    shutdown_event.set()


async def shutdown():
    """Безопасное завершение всех компонентов"""
    logger.info("[Main] Завершаем приложение...")

    if p_manager:
        logger.info("[Main] Останавливаем все процессы...")
        p_manager.stop_all_process()

    logger.info("[Main] Программа завершена.")
    logger.info(
            "------------------------------------------------------------Конец сессии------------------------------------------------------------")


async def main():
    global shared_values
    # Установка обработчиков сигналов
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_shutdown_signal)
        except NotImplementedError:
            # Для Windows: fallback
            signal.signal(sig, handle_shutdown_signal)

    try:
        logger.info(f"[Main] Запуск приложения с биржей {exchange_id}")

        # Shared values словарь
        shared_values = {
            "shutdown"                          : multiprocessing.Value('b', False),                # булева shared переменная
            "balance_aligned"                   : multiprocessing.Value('b', False),                # Флаг выровненных балансов
            "balance_changed"                   : multiprocessing.Value('b', False),                # Флаг изменившихся балансов
            "alignment lock"                    : multiprocessing.Value('b', False),                # Флаг блокирования выравнивания на время торговой операции
            "exchange_id"                       : ProcessManager.create_shared_str(str(exchange_id), max_bytes=32),     # Название биржи
            "deal_ready_flag"                   : multiprocessing.Value('b', False),                # Флаг нового сигнала на сделку - используется после выравнивания балансов типа балансы готовы к торгам

            # Переменные обработки открытых арбитражных позиций
            "deal_spot_coin"                    : ProcessManager.create_shared_str(str(""), max_bytes=32),              # Спот символ сделки - строка
            "deal_swap_pair"                    : ProcessManager.create_shared_str(str(""), max_bytes=32),              # Своп символ сделки - строка
            "deal_spot_amount"                  : ProcessManager.create_shared_decimal("0", max_bytes=32),    # Объем монет спот-ордера - decimal
            "deal_swap_contracts_amount"        : ProcessManager.create_shared_decimal("0", max_bytes=32),    # Объем контрактов своп-ордера - decimal

            # Переменные возвращаемые оркестратору после подтверждения открытия арбитражной сделки (арбитражных позиций)
            "deal_real_spot_value"              : ProcessManager.create_shared_decimal("0", max_bytes=32),    # Реальное количество монет на спот счету - decimal
            "deal_real_swap_value"              : ProcessManager.create_shared_decimal("0", max_bytes=32),    # Реальное количество контрактов своп-ордера - decimal
            "deal_real_swap_entry_price"        : ProcessManager.create_shared_decimal("0", max_bytes=32),    # Реальная цена открытия своп-ордера - decimal
            "deal_swap_realised_pnl"            : ProcessManager.create_shared_decimal("0", max_bytes=32),    # Снятая комиссия при открытии своп-ордера - decimal
            "timestamp"                         : multiprocessing.Value("i", 0),                    # Timestamp своп - ордера
            "deal_manage_pnl_ready_signal_event": multiprocessing.Event(),                                              # Арбитражная позиция открыта и готова к сопровождению pnl

            # Переменные обработки балансов usdt
            "spot_usdt"                         : ProcessManager.create_shared_decimal("0", max_bytes=32),
            # Строковая переменная для последующего преобразования в decimal
            "swap_usdt"                         : ProcessManager.create_shared_decimal("0", max_bytes=32),
            # Строковая переменная для последующего преобразования в decimal
            "deal_spot_usdt"                    : ProcessManager.create_shared_decimal("0", max_bytes=32),
            # Строковая переменная для последующего преобразования в decimal
            "deal_swap_usdt"                    : ProcessManager.create_shared_decimal("0", max_bytes=32),
            # Строковая переменная для последующего преобразования в decimal

            # Переменные контроля сделок
            "active_deals_count"                : multiprocessing.Value('i', 0),  # целочисленная shared переменная
            "max_active_deals"                  : multiprocessing.Value('i', max_active_deals),  # целочисленная shared переменная
            "trade_signal_event"                : multiprocessing.Event(),  # ⬅️ Событие!
        }

        """
        shutdown            : Флаг закрытия приложения для всех процессов
        balance_aligned     : Флаг выровненных переданных балансов
        balance_changed     : Флаг изменившихся балансов - еще не выровненных
        alignment lock      : Флаг процесса выравнивания балансов, пока взведен - балансы не готовы к работе
        exchange_id         : Название биржи
        deal_ready_flag     : Флаг готовности к новой сделке
        
        deal_spot_coin      : Название символа спот сделки
        deal_swap_pair      : Название символа своп сделки
        deal_spot_amount    : Заявленное купленное количество монет
        deal_swap_contracts_amount : Заявленный объем своп-шорта в монетах
        
        deal_real_spot_value: реальное количество спот-монет в кошельке
        deal_real_swap_value: реальный объем выставленного ордера      
        timestamp           : Время обновления данных
        deal_manage_pnl_ready_signal_event: Событие - Арбитражная позиция открыта и готова к сопровождению pnl

        spot_usdt           : Баланс спот-счета
        active_deals_count  : Текущее количество открытых сделок
        max_active_deals    : Максимально разрешенное одновременное количество открытых сделок
        trade_signal_event  : Событие открытия сделки - возможно не нужно???
        
        
        Расчет deal_spot_usdt и deal_swap_usdt производится в классе BalanceWatcher при выравнивании балансов - разбиваем выровненный баланс на оставшееся количество разрешенных, еще не открытых сделок
        """

        # Очередь для обмена между процессами
        spread_table_queue_data     = multiprocessing.Queue()  # Очередь с данными для спред-таблицы
        deal_table_queue_data       = multiprocessing.Queue()  # Очередь с данными для спред-таблицы
        balance_queue_data          = multiprocessing.Queue()  # Очередь с данными для спред-таблицы
        get_deal_data_request_event = multiprocessing.Event()  # Событие получения данных сделки
        get_deal_data_event         = multiprocessing.Event()  # Событие получения данных сделки

        # Запуск процессов
        # p_manager.start_process("spread_table",     run_gui_grid_process,           kwargs={"table_queue_data"              : spread_table_queue_data,      "shared_values"   : shared_values})
        # p_manager.start_process("deal_table",       run_gui_grid_process,           kwargs={"table_queue_data"              : deal_table_queue_data,        "shared_values"   : shared_values})
        # p_manager.start_process ("test_process_value_getter", shared_values_receiver, kwargs = {"shared_values": shared_values})
        # p_manager.start_process("shared_time_writer", string_writer_process, kwargs = {"shared_values": shared_values})

        p_manager.start_process("analytic_process", run_analytic_process_wrapper,   kwargs={"spread_table_queue_data"       : spread_table_queue_data,      "shared_values"   : shared_values, "deal_table_queue_data"   : deal_table_queue_data})

        logger.info("[Main] Приложение запущено. Ожидание сигнала завершения...")
        await shutdown_event.wait()

    except KeyboardInterrupt:
        logger.warning("[Main] KeyboardInterrupt — завершение")
        handle_shutdown_signal()
        shutdown_event.set()
    except Exception as e:
        logger.exception(f"[Main] Необработанное исключение: {e}")
        handle_shutdown_signal()
        shutdown_event.set()
    finally:
        handle_shutdown_signal()
        if p_manager:
            p_manager.stop_all_process()
        await shutdown()


# ... (остальной код без изменений: data_20x5, data_20x6, data_30x5, headers, data, grid_data, data_variants, send_grid_data)

if __name__ == "__main__":
    asyncio.run(main())

# Тестовый код для tkgrid
data_20x5 = {
    'header': {i: {'text': f'Заголовок {i + 1}'} for i in range(5)},
    **{row: {col: {'text': f'Слово_{row}{col}'} for col in range(5)} for row in range(1, 21)}
}
data_20x6 = {
    'header': {i: {'text': f'Колонка_{i + 1}'} for i in range(6)},
    **{row: {col: {'text': f'Данные_{row}-{col}'} for col in range(6)} for row in range(1, 21)}
}
data_30x5 = {
    'header': {i: {'text': f'Поле_{i + 1}'} for i in range(5)},
    **{row: {col: {'text': f'Число_{row * col}'} for col in range(5)} for row in range(1, 31)}
}
headers = ['Header 1', 'Header 2', 'Header 3']
data = {
    'header': {i: {'text': headers[i]} for i in range(len(headers))},
    1       : {0: {'text': f'Row 1, Col 1: {random.randint(1, 100)}'},
               1: {'text': f'Row 1, Col 2: {random.randint(1, 100)}'},
               2: {'text': f'Row 1, Col 3: {random.randint(1, 100)}'}},
    2       : {0: {'text': f'Row 2, Col 1: {random.randint(1, 100)}'},
               1: {'text': f'Row 2, Col 2: {random.randint(1, 100)}'},
               2: {'text': f'Row 2, Col 3: {random.randint(1, 100)}'}}
}
grid_data = {
    'header': {
        0: {'text': 'Column 1', 'align': 'left'},
        1: {'text': 'Column 2', 'align': 'center'},
        2: {'text': 'Column 3', 'align': 'right'}
    },
    1       : {
        0: {'text': 'Data 1', 'align': 'left'},
        1: {'text': 'Data 2', 'align': 'center'},
        2: {'text': 'Data 3', 'align': 'right'}
    }
}
data_variants = itertools.cycle([data_20x5, data_20x6, data_30x5, data, grid_data])


async def source_data(queue):
    while True:
        queue.put(next(data_variants))  # Отправляем данные в очередь
        await asyncio.sleep(1)  # Обновляем данные каждую секунду
        queue.put({'config': {'title': 'ЕЩЕ Новый заголовок окна'}})
