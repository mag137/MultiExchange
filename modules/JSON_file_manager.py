__version__ = "1.0"

import json
import os
from pprint import pprint

class JsonFileManager:
    """
    Класс безопасного создания, чтения, записи и сопровождения JSON-файла.
    Используется атомарная запись через временный файл с расширением .tmp.
    """
    def __init__(self, filename):
        self.filename = filename
        self.name = os.path.basename(filename)
        self.file_data_dict = {}

    def ensure_file_exists(self):
        """Создает файл с пустым словарем, если его нет."""
        if not os.path.exists(self.filename):
            self._atomic_write({})

    def _atomic_write(self, data: dict):
        """Записывает данные атомарно через временный файл с расширением .tmp."""
        dir_name = os.path.dirname(self.filename)
        os.makedirs(dir_name, exist_ok=True)  # создаём папку, если нет
        temp_name = f"{self.filename}.tmp"
        with open(temp_name, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        os.replace(temp_name, self.filename)  # атомарная замена

    def load(self) -> dict:
        """Загружает словарь из JSON, создавая файл при необходимости."""
        self.ensure_file_exists()
        with open(self.filename, 'r', encoding='utf-8') as f:
            self.file_data_dict = json.load(f)
        return self.file_data_dict

    def save(self, deals: dict):
        """Сохраняет словарь атомарно в JSON-файл."""
        self._atomic_write(deals)
        self.file_data_dict = deals

    def add(self, key: str, data: dict):
        """Добавляет или обновляет запись по ключу."""
        deals = self.load()
        deals[key] = data
        self.save(deals)

    def remove(self, key: str):
        """Удаляет запись по ключу, если она есть."""
        deals = self.load()
        if key in deals:
            del deals[key]
            self.save(deals)

    def __contains__(self, key):
        """Позволяет проверять наличие ключа через `in`."""
        return key in self.load()

    def pretty_print(self):
        """Красиво печатает текущее содержимое файла."""
        pprint(self.load())

if __name__ == '__main__':
    # Пример использования с полным путем
    active_deals_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','source', 'active_deals_test.json'))
    print(active_deals_file)
    active_file = JsonFileManager(active_deals_file)

    active_file.add("BTC/USDT:USDT", {"test": "Это тестовая запись из отладочного кода класса 'JsonFileManager' из файла 'JSON_file_manager'","amount": 0.001, "price": 113000})
    active_file.pretty_print()

    if "BTC/USDT:USDT" in active_file:
        print("Запись найдена!")

    # active_file.remove("BTC/USDT:USDT")
    active_file.pretty_print()
