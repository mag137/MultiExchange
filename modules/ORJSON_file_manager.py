# JSON_file_manager.py


"""
Модуль: JSON_file_manager.py

Управление JSON-файлами с поддержкой Decimal, атомарной записью и восстановлением типов.
Использует orjson для высокой производительности и надёжной сериализации.
"""

__version__ = "1.1"
__author__ = "Max Go"

import orjson
import os
import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Union
from pprint import pprint

# Настройка логгера для внутренних ошибок
logger = logging.getLogger(__name__)


class JsonFileManager:
    """
    Класс для безопасного создания, чтения, записи и удаления JSON-файлов.

    Поддерживает:
        - Атомарную запись (через временный файл .tmp)
        - Автоматическое создание директорий
        - Сериализацию `Decimal` через `orjson` + `default=str`
        - Восстановление `Decimal` при загрузке
        - Работу с большими файлами и частыми операциями

    Attributes:
        filename (str): Полный путь к JSON-файлу.
        name (str): Имя файла без пути.
        file_data_dict (dict): Кэш последних загруженных данных.
    """

    def __init__(self, filename: str) -> None:
        """
        Инициализирует менеджер для указанного JSON-файла.

        Args:
            filename: Полный путь к файлу (например, '/path/to/data.json').
        """
        self.filename: str = filename
        self.name: str = os.path.basename(filename)
        self.file_data_dict: Dict = {}

    def _deserialize_decimal(self, obj: Any) -> Any:
        """
        Рекурсивно конвертирует строки в Decimal, если они выглядят как числа.

        Args:
            obj: Объект любого типа (dict, list, str, и т.д.).

        Returns:
            Объект с заменёнными строками-числами на Decimal.
        """
        if isinstance(obj, dict):
            return {k: self._deserialize_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._deserialize_decimal(i) for i in obj]
        elif isinstance(obj, str):
            try:
                # Проверяем, похожа ли строка на число
                if '.' in obj or 'e' in obj.lower() or obj.replace('-', '').replace('.', '').isdigit():
                    return Decimal(obj)
            except (ValueError, InvalidOperation):
                pass
        return obj

    def ensure_file_exists(self) -> None:
        """
        Создаёт файл с пустым словарём, если он не существует.

        Raises:
            OSError: Если невозможно создать директорию или файл.
        """
        if not os.path.exists(self.filename):
            self._atomic_write({})

    def _atomic_write(self, data: Dict[str, Any]) -> None:
        """
        Атомарно записывает данные в файл через временный файл.

        Args:
            data: Словарь для записи.

        Raises:
            OSError: Если запись не удалась.
            TypeError: Если объект несериализуем.
        """
        dir_name = os.path.dirname(self.filename)
        os.makedirs(dir_name, exist_ok=True)
        temp_name = f"{self.filename}.tmp"

        try:
            # Сериализуем с отступами и UTC
            serialized = orjson.dumps(
                data,
                option=orjson.OPT_INDENT_2 | orjson.OPT_NAIVE_UTC,
                default=str  # Обработка Decimal, datetime и др.
            )
            with open(temp_name, 'wb') as f:
                f.write(serialized)
            os.replace(temp_name, self.filename)  # Атомарная замена
        except Exception as e:
            if os.path.exists(temp_name):
                try:
                    os.remove(temp_name)
                except:
                    pass
            logger.error(f"Ошибка при атомарной записи в {self.filename}: {e}")
            raise

    def load(self) -> Dict[str, Any]:
        """
        Загружает словарь из JSON-файла, восстанавливая Decimal.

        Создаёт пустой файл, если он не существует.

        Returns:
            Словарь с данными из файла. При ошибке — пустой словарь.
        """
        self.ensure_file_exists()
        try:
            with open(self.filename, 'rb') as f:
                data = orjson.loads(f.read())
            self.file_data_dict = self._deserialize_decimal(data)
            return self.file_data_dict
        except Exception as e:
            logger.error(f"[JsonFileManager] Ошибка при загрузке {self.filename}: {e}")
            return {}

    def save(self, deals: Dict[str, Any]) -> None:
        """
        Сохраняет словарь в JSON-файл атомарно.

        Args:
            deals: Словарь для сохранения.
        """
        self._atomic_write(deals)
        self.file_data_dict = deals

    def add(self, key: str, data: Dict[str, Any], merge: bool = False) -> None:
        """
        Добавляет или обновляет запись по ключу.

        Args:
            key: Ключ (например, 'BTC/USDT:USDT').
            data: Данные для сохранения.
            merge: Если True — обновляет существующую запись, не перезаписывая полностью.
                   Если False — заменяет запись целиком (по умолчанию).
        """
        deals = self.load()

        if merge and key in deals and isinstance(deals[key], dict):
            # Обновляем существующий словарь новыми полями
            deals[key].update(data)
        else:
            # Полностью заменяем значение по ключу
            deals[key] = data

        self.save(deals)

    def remove(self, key: str) -> bool:
        """
        Удаляет запись по ключу, если она существует.

        Args:
            key: Ключ для удаления.

        Returns:
            True, если запись была удалена, иначе False.
        """
        deals = self.load()
        if key in deals:
            del deals[key]
            self.save(deals)
            return True
        return False

    def get(self, key: str, default: Any = None) -> Any:
        """
        Получает значение по ключу, как в словаре.

        Args:
            key: Ключ.
            default: Значение по умолчанию.

        Returns:
            Значение или default.
        """
        return self.load().get(key, default)

    def __contains__(self, key: str) -> bool:
        """
        Позволяет проверять наличие ключа через `in`.

        Args:
            key: Ключ для проверки.

        Returns:
            True, если ключ существует.
        """
        return key in self.load()

    def __getitem__(self, key: str) -> Any:
        """
        Позволяет использовать `manager[key]`.

        Args:
            key: Ключ.

        Returns:
            Значение по ключу.
        """
        return self.load()[key]

    def __setitem__(self, key: str, data: Dict[str, Any]) -> None:
        """
        Позволяет использовать `manager[key] = data`.

        Args:
            key: Ключ.
            data: Данные.
        """
        self.add(key, data)

    def keys(self) -> list:
        """Возвращает список ключей."""
        return list(self.load().keys())

    def values(self) -> list:
        """Возвращает список значений."""
        return list(self.load().values())

    def items(self) -> list:
        """Возвращает пары (ключ, значение)."""
        return list(self.load().items())

    def clear(self) -> None:
        """Очищает файл (записывает пустой словарь)."""
        self.save({})

    def pretty_print(self) -> None:
        """Красиво выводит содержимое файла."""
        pprint(self.load())


# Пример использования
if __name__ == '__main__':
    from decimal import Decimal

    # Путь к тестовому файлу
    test_file = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'source', 'active_deals.json')
    )
    print(f"📁 Файл: {test_file}")

    # Создаём менеджер
    manager = JsonFileManager(test_file)

    # Тестовые данные с Decimal
    test_data = {
        "pair": "BTC/USDT:USDT",
        "amount": Decimal("0.001"),
        "price": Decimal("113000.50"),
        "ratio": Decimal("1.2345"),
        "fees": Decimal("0.0008"),
        "timestamp": "2025-04-05T12:34:56.789Z"
    }
    new_dict = {'new':123,
                "fees": Decimal("0.0001"),
                "timestamp": ''}
    # Добавляем запись
    manager.add("BTC/USDT:USDT", test_data)
    print("\n✅ Запись добавлена:")

    manager.add("BTC/USDT:USDT", new_dict, merge=True)
    manager.pretty_print()
    # Проверка наличия
    if "BTC/USDT:USDT" in manager:
        print("✅ Ключ найден!")

    # Получение данных
    data = manager.get("BTC/USDT:USDT")
    print(f"\n🔍 Получено: amount = {data['amount']} ({type(data['amount'])})")

    # Удаление
    manager.remove("BTC/USDT:USDT")
    print("\n🗑️ После удаления:")
    manager.pretty_print()

    print("\n✅ Тест завершён успешно.")