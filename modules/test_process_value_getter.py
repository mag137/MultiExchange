import time
import multiprocessing
from pprint import pprint


def shared_values_receiver (shared_values: dict):
    """
    Процесс для чтения и отображения всех shared значений
    """
    print ("[receiver] Запущен процесс получателя")

    while not shared_values [ "shutdown" ].value:
        try:
            # Читаем все значения из shared_values
            values = {}
            for k, v in shared_values.items ( ):
                if hasattr (v, "value"):
                    # Для строковых значений
                    if hasattr (v, 'get_lock') and hasattr (v, 'value'):
                        with v.get_lock ( ):
                            try:
                                values [ k ] = v.value.decode ('utf-8').strip ('\x00')
                            except (UnicodeDecodeError, AttributeError):
                                values [ k ] = v.value
                    else:
                        values [ k ] = v.value
                else:
                    values [ k ] = v

            print()
            print ("ТЕКУЩИЕ SHARED ЗНАЧЕНИЯ:")
            print ("=" * 50)
            pprint (values)
            print ("=" * 50)

        except Exception as e:
            print (f"[receiver] Ошибка чтения: {e}")

        time.sleep (1)

    print ("[receiver] Завершение, shutdown=True")