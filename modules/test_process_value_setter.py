import time
import multiprocessing
from datetime import datetime
from modules.process_manager import ProcessManager


def string_writer_process (shared_values: dict):
    """
    Процесс для тестирования записи строковых переменных
    """
    print ("[string_writer] Запущен процесс записи строк")

    counter = 0
    while not shared_values [ "shutdown" ].value:
        try:
            current_time = datetime.now ( ).strftime ("%H:%M:%S.%f") [ :-3 ]

            # Тестируем запись в разные строковые переменные
            ProcessManager.update_shared_str (
                shared_values [ "deal_swap_usdt" ],
                f"SWAP_TIME_{current_time}_{counter}",
                max_bytes = 32
            )

            ProcessManager.update_shared_str (
                shared_values [ "deal_spot_usdt" ],
                f"SPOT_TIME_{current_time}_{counter}",
                max_bytes = 32
            )

            ProcessManager.update_shared_str (
                shared_values [ "timestamp" ],
                f"SPOT_TIME_{current_time}_{counter}",
                max_bytes = 32
            )

            ProcessManager.update_shared_str (
                shared_values [ "deal_pair" ],
                f"TEST_PAIR_{counter:03d}",
                max_bytes = 32
            )

            # Меняем флаги для визуализации
            shared_values [ "deal_ready_flag" ].value = counter % 2 == 0
            shared_values [ "active_deals_count" ].value = counter % 5

            print (f"[string_writer] Записаны значения (итерация {counter})")
            counter += 1

        except Exception as e:
            print (f"[string_writer] Ошибка: {e}")

        time.sleep (1)

    print ("[string_writer] Завершение")


def string_reader_process (shared_values: dict):
    """
    Процесс для тестирования чтения строковых переменных
    """
    print ("[string_reader] Запущен процесс чтения строк")

    while not shared_values [ "shutdown" ].value:
        try:
            # Читаем все строковые переменные
            values = {}
            for key in [ "deal_swap_usdt", "deal_spot_usdt", "deal_pair", "exchange_id" ]:
                if key in shared_values:
                    values [ key ] = ProcessManager.read_str (shared_values [ key ])

            # Читаем числовые переменные и флаги
            values [ "deal_ready_flag" ] = shared_values [ "deal_ready_flag" ].value
            values [ "active_deals_count" ] = shared_values [ "active_deals_count" ].value
            values [ "shutdown" ] = shared_values [ "shutdown" ].value

            print ("=== ТЕКУЩИЕ ЗНАЧЕНИЯ ===")
            for k, v in values.items ( ):
                print (f"{k}: {v}")
            print ("=======================")

        except Exception as e:
            print (f"[string_reader] Ошибка чтения: {e}")

        time.sleep (0.5)  # Читаем чаще чем пишем

    print ("[string_reader] Завершение")

import time
import multiprocessing
from datetime import datetime
from modules.process_manager import ProcessManager


def string_writer_process (shared_values: dict):
    """
    Процесс для тестирования записи строковых переменных
    """
    print ("[string_writer] Запущен процесс записи строк")

    counter = 0
    while not shared_values [ "shutdown" ].value:
        try:
            current_time = datetime.now ( ).strftime ("%H:%M:%S.%f") [ :-3 ]

            # Тестируем запись в разные строковые переменные
            ProcessManager.update_shared_str (
                shared_values [ "deal_swap_usdt" ],
                f"SWAP_TIME_{current_time}_{counter}",
                max_bytes = 32
            )

            ProcessManager.update_shared_str (
                shared_values [ "deal_spot_usdt" ],
                f"SPOT_TIME_{current_time}_{counter}",
                max_bytes = 32
            )

            ProcessManager.update_shared_str (
                shared_values [ "timestamp" ],
                f"SPOT_TIME_{current_time}_{counter}",
                max_bytes = 32
            )

            ProcessManager.update_shared_str (
                shared_values [ "deal_pair" ],
                f"TEST_PAIR_{counter:03d}",
                max_bytes = 32
            )

            # Меняем флаги для визуализации
            shared_values [ "deal_ready_flag" ].value = counter % 2 == 0
            shared_values [ "active_deals_count" ].value = counter % 5

            print (f"[string_writer] Записаны значения (итерация {counter})")
            counter += 1

        except Exception as e:
            print (f"[string_writer] Ошибка: {e}")

        time.sleep (1)

    print ("[string_writer] Завершение")


def string_reader_process (shared_values: dict):
    """
    Процесс для тестирования чтения строковых переменных
    """
    print ("[string_reader] Запущен процесс чтения строк")

    while not shared_values [ "shutdown" ].value:
        try:
            # Читаем все строковые переменные
            values = {}
            for key in [ "deal_swap_usdt", "deal_spot_usdt", "deal_pair", "exchange_id" ]:
                if key in shared_values:
                    values [ key ] = ProcessManager.read_str (shared_values [ key ])

            # Читаем числовые переменные и флаги
            values [ "deal_ready_flag" ] = shared_values [ "deal_ready_flag" ].value
            values [ "active_deals_count" ] = shared_values [ "active_deals_count" ].value
            values [ "shutdown" ] = shared_values [ "shutdown" ].value

            print ("=== ТЕКУЩИЕ ЗНАЧЕНИЯ ===")
            for k, v in values.items ( ):
                print (f"{k}: {v}")
            print ("=======================")

        except Exception as e:
            print (f"[string_reader] Ошибка чтения: {e}")

        time.sleep (0.5)  # Читаем чаще чем пишем

    print ("[string_reader] Завершение")