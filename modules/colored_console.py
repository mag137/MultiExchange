__version__ = "2.1"
'''
    Таблица цветов
Стандартные цвета текста (30–37):   Яркие цвета текста (90–97):
30 Черный                           90 Серый
31 Красный                          91 Ярко-красный
32 Зеленый                          92 Ярко-зеленый
33 Желтый                           93 Ярко-желтый
34 Синий                            94 Ярко-синий
35 Пурпурный                        95 Ярко-пурпурный
36 Голубой                          96 Ярко-голубой
37 Белый                            97 Ярко-белый

Стандартные цвета фона (40–47):     Яркие цвета фона (100–107):
40 Черный                           100 Серый
41 Красный                          101 Ярко-красный
42 Зеленый                          102 Ярко-зеленый
43 Желтый                           103 Ярко-желтый
44 Синий                            104 Ярко-синий
45 Пурпурный                        105 Ярко-пурпурный
46 Голубой                          106 Ярко-голубой
47 Белый                            107 Ярко-белый
'''

class cprint:
    COLORS = \
        {
        'RESET'         : '\033[0m',        # Сброс всех стилей
        'INFO'          : '\033[94m',       # Blue
        'INFO_B'        : '\033[30;106m',   # Белый текст, синий фон
        'INFO_W'        : '\033[97;44m',    # Белый текст, синий фон
        'WARNING'       : '\033[93m',       # Yellow
        'WARNING_B'     : '\033[30;43m',    # Черный текст, желтый фон
        'WARNING_R'     : '\033[91;103m',   # Красный текст, желтый фон
        'ERROR'         : '\033[91m',       # Red
        'ERROR_B'       : '\033[30;101m',   # Черный текст, красный фон
        'ERROR_W'       : '\033[97;41m',    # Белый текст, красный фон
        'SUCCESS'       : '\033[92m',       # Green
        'SUCCESS_B'     : '\033[30;102m',   # Черный текст, зеленый фон
        'SUCCESS_W'     : '\033[97;42m'     # Белый текст, зеленый фон
        }

    @staticmethod
    def _print(color, message):
        """Внутренний метод для вывода сообщения с указанным цветом."""
        print(f"{color}{message}{cprint.COLORS['RESET']}")

    @staticmethod
    def info(message):
        """Выводит сообщение с синим текстом."""
        cprint._print(cprint.COLORS['INFO'], message)

    @staticmethod
    def info_w(message):
        """Выводит сообщение с белым текстом на синем фоне."""
        cprint._print(cprint.COLORS['INFO_W'], message)

    @staticmethod
    def info_b(message):
        """Выводит сообщение с черным текстом на синем фоне."""
        cprint._print(cprint.COLORS['INFO_B'], message)

    @staticmethod
    def warning(message):
        """Выводит сообщение с желтым текстом (предупреждение)."""
        cprint._print(cprint.COLORS['WARNING'], message)

    @staticmethod
    def warning_r(message):
        """Выводит сообщение с белым текстом на желтом фоне (предупреждение)."""
        cprint._print(cprint.COLORS['WARNING_R'], message)

    @staticmethod
    def warning_b(message):
        """Выводит сообщение с черным текстом на желтом фоне (предупреждение)."""
        cprint._print(cprint.COLORS['WARNING_B'], message)

    @staticmethod
    def error(message):
        """Выводит сообщение с красным текстом (ошибка)."""
        cprint._print(cprint.COLORS['ERROR'], message)

    @staticmethod
    def error_w(message):
        """Выводит сообщение с белым текстом на красном фоне (ошибка)."""
        cprint._print(cprint.COLORS['ERROR_W'], message)

    @staticmethod
    def error_b(message):
        """Выводит сообщение с черным текстом на красном фоне (ошибка)."""
        cprint._print(cprint.COLORS['ERROR_B'], message)

    @staticmethod
    def success(message):
        """Выводит сообщение с черным текстом на зеленом фоне (успешное выполнение)."""
        cprint._print(cprint.COLORS['SUCCESS'], message)

    @staticmethod
    def success_w(message):
        """Выводит сообщение с белым текстом на зеленом фоне (успешное выполнение)."""
        cprint._print(cprint.COLORS['SUCCESS_W'], message)

    @staticmethod
    def success_b(message):
        """Выводит сообщение с черным текстом на зеленом фоне (успешное выполнение)."""
        cprint._print(cprint.COLORS['SUCCESS_B'], message)


# Пример использования
if __name__ == "__main__":
    cprint.info     ("This is an informational message.")
    cprint.info_w   ("This is an informational message w.")
    cprint.info_b   ("This is an informational message b.")
    cprint.warning  ("This is a warning message.")
    cprint.warning_r("This is a warning message w.")
    cprint.warning_b("This is a warning message b.")
    cprint.error    ("This is an error message.")
    cprint.error_w  ("This is an error message w.")
    cprint.error_b  ("This is an error message b.")
    cprint.success  ("This operation was successful.")
    cprint.success_w("This operation was successful w.")
    cprint.success_b("This operation was successful b.")