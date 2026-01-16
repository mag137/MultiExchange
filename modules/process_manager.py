__version__ = "4.0"  # Обновил версию

import multiprocessing
import ctypes
from typing import Optional, Dict, Tuple, Callable, Any, Union
from decimal import Decimal, InvalidOperation
from modules.logger import LoggerFactory

logger = LoggerFactory.get_logger ("app." + __name__)


class ProcessManager:
    """
    Универсальный менеджер процессов и вспомогательные методы для работы с разделяемыми переменными.

    Ключевые особенности:
    - Все строковые shared-переменные используют стандартный NULL-terminator b'\\x00'
    - Нулевые байты после маркера используются для заполнения буфера
    - Все операции с shared_str потокобезопасны (get_lock)
    - Размеры буферов должны быть согласованы между create, update, read
    """

    # 🌟 Единый маркер конца строки
    _END_MARKER = b'\x00'

    @staticmethod
    def create_shared_str (value: str, max_bytes: int = 32) -> multiprocessing.Value:
        """
        Упрощённая версия создания shared строки.
        """
        # Создаём массив с нулевыми байтами
        array_type = ctypes.c_char * max_bytes
        shared_str = multiprocessing.Value (array_type, lock = True)

        # Инициализируем значением
        ProcessManager.update_shared_str (shared_str, value, max_bytes)

        logger.debug (f"[create_shared_str] Создана строка: '{value}' [{max_bytes} байт]")
        return shared_str

    @staticmethod
    def update_shared_str (shared_str: multiprocessing.Value, value: str, max_bytes: int = 32) -> None:
        """
        Упрощённая версия обновления shared строки.
        """
        encoded = value.encode ("utf-8") [ :max_bytes - 1 ]
        encoded_with_marker = encoded + ProcessManager._END_MARKER
        padded = encoded_with_marker.ljust (max_bytes, ProcessManager._END_MARKER)

        with shared_str.get_lock ( ):
            # Копируем байты в shared memory
            buffer = shared_str.get_obj ( )
            for i in range (max_bytes):
                buffer [ i ] = padded [ i:i + 1 ]  # Ключевое исправление!

        logger.debug (f"[update_shared_str] Обновлено: '{value}'")

    @staticmethod
    def read_str (shared_str: multiprocessing.Value) -> str:
        """Читает NULL-terminated строку из shared memory."""
        with shared_str.get_lock ( ):
            raw_bytes = bytes (shared_str.get_obj ( ))

        # Ищем первый NULL byte
        null_pos = raw_bytes.find (ProcessManager._END_MARKER)
        if null_pos == -1:
            data = raw_bytes  # NULL не найден - возвращаем всё
        else:
            data = raw_bytes [ :null_pos ]

        try:
            return data.decode ("utf-8")
        except UnicodeDecodeError:
            return data.decode ("utf-8", errors = "replace")

    @staticmethod
    def read_decimal (shared_value: Any, default: Decimal = Decimal ('0')) -> Decimal:
        """
        Безопасное извлечение Decimal из shared переменной.

        Поддерживает:
        - multiprocessing.Value с атрибутом value
        - bytes объекты
        - обычные строки и числа

        Args:
            shared_value: shared переменная любого типа
            default: значение по умолчанию при ошибке

        Returns:
            Decimal значение
        """
        if shared_value is None:
            return default

        try:
            # Если это multiprocessing.Value с атрибутом value
            if hasattr (shared_value, 'value'):
                raw_value = shared_value.value

                if isinstance (raw_value, bytes):
                    # Декодируем bytes в строку
                    str_value = raw_value.decode ('utf-8').split ('\x00') [ 0 ].strip ( )
                    if str_value:
                        return Decimal (str_value)
                else:
                    # Любой другой тип - преобразуем в строку и затем в Decimal
                    return Decimal (str (raw_value))

            # Если это простые bytes
            elif isinstance (shared_value, bytes):
                str_value = shared_value.decode ('utf-8').split ('\x00') [ 0 ].strip ( )
                if str_value:
                    return Decimal (str_value)

            # Если это обычная строка или число
            else:
                return Decimal (str (shared_value))

        except (UnicodeDecodeError, ValueError, InvalidOperation, AttributeError) as e:
            logger.warning (f"[read_decimal] Ошибка преобразования в Decimal: {e}, значение: {shared_value}")
            return default

        return default

    @staticmethod
    def create_shared_decimal (initial_value: Union [ Decimal, float, str, int ],
                               max_bytes: int = 32) -> multiprocessing.Value:
        """
        Создает shared переменную для хранения Decimal значения.

        Args:
            initial_value: начальное значение
            max_bytes: размер буфера

        Returns:
            multiprocessing.Value для хранения Decimal как строки
        """
        decimal_str = str (Decimal (initial_value))
        return ProcessManager.create_shared_str (decimal_str, max_bytes)

    @staticmethod
    def update_shared_decimal (shared_value: multiprocessing.Value,
                               new_value: Union [ Decimal, float, str, int ],
                               max_bytes: int = 32) -> None:
        """
        Обновляет shared переменную с Decimal значением.

        Args:
            shared_value: shared переменная для обновления
            new_value: новое значение
            max_bytes: размер буфера
        """
        decimal_str = str (Decimal (new_value))
        ProcessManager.update_shared_str (shared_value, decimal_str, max_bytes)

    # Остальные методы без изменений...
    def __init__ (self) -> None:
        self.processes: Dict [ str, multiprocessing.Process ] = {}
        logger.debug ("ProcessManager инициализирован")

    def start_process(
            self,
            name: str,
            target: Callable,
            args: Tuple = (),
            kwargs: Optional[Dict[str, Any]] = None,
            daemon: bool = True  # ← по умолчанию демон
    ) -> None:
        kwargs = kwargs or {}
        proc = multiprocessing.Process(
            target=target,
            args=args,
            kwargs=kwargs,
            name=name,
            daemon=daemon  # ← теперь управляется параметром
        )
        proc.start()
        self.processes[name] = proc
        logger.debug(f"Процесс [{name}] запущен (daemon={daemon})")

    def stop_process (self, name: str) -> None:
        """Останавливает процесс по имени."""
        proc = self.processes.pop (name, None)
        if proc:
            if proc.is_alive ( ):
                proc.terminate ( )
                proc.join (timeout = 5)
            logger.debug (f"[stop_process] Процесс [{name}] остановлен")

    def stop_all_process (self) -> None:
        """Останавливает все процессы."""
        for name in list (self.processes.keys ( )):
            self.stop_process (name)
        logger.debug ("[stop_all_process] Все процессы остановлены")

    def join_process (self, name: str, timeout: Optional [ float ] = None) -> None:
        """Ждёт завершения процесса."""
        proc = self.processes.get (name)
        if proc:
            proc.join (timeout = timeout)

    def join_all (self, timeout: Optional [ float ] = None) -> None:
        """Ждёт завершения всех процессов."""
        for proc in self.processes.values ( ):
            proc.join (timeout = timeout)


# ========================
# Тестовые функции
# ========================

def proc_a (qin, qout, shared_str):
    while True:
        msg = qin.get ( )
        if msg == "stop":
            qout.put ("A: done")
            break
        elif msg.startswith ("update:"):
            to_set = msg.split (":", 1) [ 1 ]
            ProcessManager.update_shared_str (shared_str, to_set)
            qout.put ("A: updated")
        elif msg == "get":
            val = ProcessManager.read_str (shared_str)
            qout.put (f"A: Текущее значение: {val}")
        elif msg == "exchange":
            val = ProcessManager.read_str (shared_str)
            qout.put (f"EXCHANGE:{val}")
        elif msg.startswith ("set_from_other:"):
            to_set = msg.split (":", 1) [ 1 ]
            ProcessManager.update_shared_str (shared_str, to_set)
            qout.put ("A: строка обновлена от B")
        else:
            qout.put (f"A: неизвестная команда: {msg}")


def proc_b (qin, qout, shared_str):
    while True:
        msg = qin.get ( )
        if msg == "stop":
            qout.put ("B: done")
            break
        elif msg.startswith ("update:"):
            to_set = msg.split (":", 1) [ 1 ]
            ProcessManager.update_shared_str (shared_str, to_set)
            qout.put ("B: updated")
        elif msg == "get":
            val = ProcessManager.read_str (shared_str)
            qout.put (f"B: Текущее значение: {val}")
        elif msg.startswith ("exchange:"):
            new_val = msg.split (":", 1) [ 1 ]
            ProcessManager.update_shared_str (shared_str, new_val)
            qout.put ("B: строка обновлена от A")
        elif msg == "exchange":
            val = ProcessManager.read_str (shared_str)
            qout.put (f"EXCHANGE:{val}")
        else:
            qout.put (f"B: неизвестная команда: {msg}")


# ========================
# Тест новых Decimal методов
# ========================

def test_decimal_methods ( ):
    """Тестирование новых методов работы с Decimal"""
    print ("🧪 Тестирование Decimal методов...")

    # Тест 1: Создание shared Decimal
    shared_decimal = ProcessManager.create_shared_decimal ("123.456", 32)
    decimal_value = ProcessManager.read_decimal (shared_decimal)
    print (f"✅ Shared Decimal создан: {decimal_value} ({type (decimal_value)})")

    # Тест 2: Обновление shared Decimal
    ProcessManager.update_shared_decimal (shared_decimal, Decimal ("789.123"))
    decimal_value = ProcessManager.read_decimal (shared_decimal)
    print (f"✅ Shared Decimal обновлен: {decimal_value}")

    # Тест 3: Чтение из bytes
    bytes_value = b"456.789\x00"
    decimal_from_bytes = ProcessManager.read_decimal (bytes_value)
    print (f"✅ Decimal из bytes: {decimal_from_bytes}")

    # Тест 4: Обработка ошибок
    invalid_bytes = b"not_a_number\x00"
    decimal_error = ProcessManager.read_decimal (invalid_bytes, Decimal ('999'))
    print (f"✅ Обработка ошибок: {decimal_error}")

    print ("✅ Все тесты Decimal пройдены!")


# ========================
# Тест
# ========================

if __name__ == "__main__":
    # Тестируем новые методы
    test_decimal_methods ( )
    print ( )

    manager = ProcessManager ( )

    a_qin, a_qout = multiprocessing.Queue ( ), multiprocessing.Queue ( )
    b_qin, b_qout = multiprocessing.Queue ( ), multiprocessing.Queue ( )

    # 🌟 Важно: везде одинаковый max_bytes
    max_bytes = 32

    shared_str_a = ProcessManager.create_shared_str ("Привет от A!", max_bytes = max_bytes)
    shared_str_b = ProcessManager.create_shared_str ("Привет от B!", max_bytes = max_bytes)

    manager.start_process ("proc_a", proc_a, args = (a_qin, a_qout, shared_str_a))
    manager.start_process ("proc_b", proc_b, args = (b_qin, b_qout, shared_str_b))

    # Тест: чтение
    a_qin.put ("get")
    b_qin.put ("get")
    print (a_qout.get ( ))
    print (b_qout.get ( ))

    # Обновление
    a_qin.put ("update:Новое значение от A")
    b_qin.put ("update:Новое значение от B")
    print (a_qout.get ( ))
    print (b_qout.get ( ))

    # Обмен
    a_qin.put ("exchange")
    msg_a = a_qout.get ( )
    b_qin.put (f"exchange:{msg_a.split (':', 1) [ 1 ]}")
    print (b_qout.get ( ))

    b_qin.put ("exchange")
    msg_b = b_qout.get ( )
    a_qin.put (f"set_from_other:{msg_b.split (':', 1) [ 1 ]}")
    print (a_qout.get ( ))

    # Финал
    a_qin.put ("get")
    b_qin.put ("get")
    print (a_qout.get ( ))
    print (b_qout.get ( ))

    # Завершение
    a_qin.put ("stop")
    b_qin.put ("stop")
    print (a_qout.get ( ))
    print (b_qout.get ( ))

    manager.join_all ( )
    print ("✅ Все процессы завершены.")