import tkinter as tk
import itertools
import time
import threading
from typing import Optional, Dict

"""
TkDataScrollGrid - это класс для отображения и динамического обновления таблицы в Tkinter. Он автоматически обновляет данные без удаления и создания виджетов, что увеличивает скорость перерисовки.

Основные особенности:

✅ Динамическое обновление таблицы:
Использует готовые Label-виджеты повторно.
При необходимости скрывает лишние ячейки вместо удаления.
Добавляет новые ячейки, если данных становится больше.

✅ Оптимизированная работа с grid():

Вызывает grid() в одном цикле, сокращая число перерисовок.
Обновляет scrollregion Canvas только при изменении размеров содержимого.
✅ Поддержка вертикальной прокрутки:

Если содержимое превышает max_height, автоматически активируется Scrollbar.
✅ Гибкая настройка стилей:

Возможность задать цвет текста (fg), фона (bg), ширину ячеек (width).
Параметры borderwidth и relief позволяют настроить границы ячеек.
Ожидаемый формат данных:
Метод update_grid_data() принимает словарь формата:

{
    'header': {0: {'text': 'Заголовок 1'}, 1: {'text': 'Заголовок 2'}},
    1: {0: {'text': 'Ячейка 1-1'}, 1: {'text': 'Ячейка 1-2'}},
    2: {0: {'text': 'Ячейка 2-1'}, 1: {'text': 'Ячейка 2-2'}}
}
header — строка заголовков.
Ключи — индексы строк (0 для заголовка, остальные — номера строк).
Внутренние словари содержат ячейки (col: {атрибуты}).
Когда использовать этот класс?
✔ Если нужна быстрая отрисовка таблицы без удаления и создания Label.
✔ Если таблица изменяет размеры, и важно минимизировать grid()-операции.
✔ Если данные обновляются динамически (например, приходят с сервера).

Этот класс — альтернатива Treeview из ttk, но с большей гибкостью по стилям и оптимизированным обновлением.
"""


class TkDataScrollGrid:
    """
    Класс для отображения данных в виде прокручиваемой таблицы на Tkinter.

    Атрибуты:
        root (tk.Tk): Главное окно.
        canvas (tk.Canvas): Холст для размещения содержимого.
        scrollbar (tk.Scrollbar): Вертикальная полоса прокрутки.
        scrollable_frame (tk.Frame): Фрейм внутри Canvas, содержащий данные.
        max_height (int): Максимальная высота фрейма перед включением прокрутки.
        cells (Dict[tuple, tk.Label]): Словарь с ячейками (метками) таблицы.
        font (tuple): Шрифт ячеек.
        borderwidth (int): Ширина границы ячеек.
        relief (str): Вид границы ячеек.
        last_scrollregion (tuple): Последнее зарегистрированное значение scrollregion.
        row_header (bool): Флаг включения нумерации строк.
        header_bg (str): Цвет фона заголовка.
        header_fg (str): Цвет текста заголовка.
    """

    def __init__(self) -> None:
        """
        Создаёт главное окно с таблицей, поддерживающей прокрутку.
        """
        self.root = tk.Tk()
        self.root.title("Display title3")

        # Устанавливаем флаг "always on top"
        self.root.attributes('-topmost', True)

        # Создаём Canvas + прокручиваемый Frame
        self.canvas = tk.Canvas(self.root)
        self.scrollbar = tk.Scrollbar(self.root, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = tk.Frame(self.canvas)

        # Настройка прокрутки
        self.max_height = 300  # Максимальная высота фрейма, после которой включается скролл
        self.scrollable_frame.bind("<Configure>", self.update_scrollregion)
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")

        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        self.cells: Dict[tuple, tk.Label] = {}  # Словарь для хранения меток ячеек

        # Параметры ячейки
        self.font = ("Arial", 10)
        self.borderwidth = 1
        self.relief = "solid"

        self.last_scrollregion = (0, 0, 0, 0)  # Запоминаем предыдущее scrollregion
        self.row_header = False  # Флаг номерации строк
        self.header_bg = '#323030'  # Цвет фона
        self.header_fg = 'white'  # Цвет шрифта
        self.ident = 2 # Отступ края границы ячейки от текста ячейки

    def update_scrollregion(self, event: Optional[tk.Event] = None) -> None:
        """
        Обновляет область прокрутки, если размеры содержимого изменились.

        Аргументы:
            event (Optional[tk.Event]): Событие изменения размера (по умолчанию None).
        """
        new_region = self.canvas.bbox("all")
        if new_region != self.last_scrollregion:
            self.canvas.configure(scrollregion=new_region)
            self.last_scrollregion = new_region

    def create_cell_label(self, row: int, column: int, text: str, width: int, fg: str, bg: str) -> tk.Label:
        """
        Создаёт Label (ячейку) с заданными параметрами.

        Аргументы:
            row (int): Номер строки.
            column (int): Номер столбца.
            text (str): Текст ячейки.
            width (int): Ширина ячейки (в символах).
            fg (str): Цвет текста.
            bg (str): Цвет фона.

        Возвращает:
            tk.Label: Созданный объект метки.
        """
        label = tk.Label(self.scrollable_frame, text=text, font=self.font, fg=fg, bg=bg,
                         borderwidth=self.borderwidth, relief=self.relief, width=width)
        self.cells[(row, column)] = label
        return label

    def update_grid_data(self, grid_data: Optional[dict] = None, title: str = "Display title3") -> None:
        """
        Обновляет данные в таблице, скрывая неиспользуемые ячейки.

        Аргументы:
            grid_data (Optional[dict]): Данные для отображения.
                Формат: {row: {col: {'text': str, 'fg': str, 'bg': str}}}
            title (str): Заголовок окна.
        """
        if grid_data is None:
            grid_data = {'header': {0: {'text': 'test'}}}

        self.root.title(title)

        # Определяем используемые ячейки
        used_keys = set()
        for row, row_data in grid_data.items():
            row = 0 if row == 'header' else row
            col_offset = 1 if self.row_header else 0
            for col, cell_data in row_data.items():
                used_keys.add((row, col + col_offset))

        # Создаём и обновляем ячейки
        for row, row_data in grid_data.items():
            row = 0 if row == 'header' else row
            col_offset = 1 if self.row_header else 0

            if self.row_header:
                row_number = str(row)
                if (row, 0) in self.cells:
                    label = self.cells[(row, 0)]
                    label.config(text=row_number, width=len(row_number) + self.ident) if row != 0 else label.config(text=' № ', width=len(row_number) + self.ident)
                else:
                    self.create_cell_label(row, 0, row_number, len(row_number) + self.ident, self.header_fg, self.header_bg)
                used_keys.add((row, 0))

            for col, cell_data in row_data.items():
                text = cell_data.get('text', '')
                fg = self.header_fg if col == -1 or row == 0 else cell_data.get('fg', 'black')
                bg = self.header_bg if col == -1 or row == 0 else cell_data.get('bg', '#D9D9D9')
                width = max(len(text), 10) + self.ident

                self.scrollable_frame.grid_rowconfigure(row, weight=1)
                self.scrollable_frame.grid_columnconfigure(col + col_offset, weight=1)

                if (row, col + col_offset) in self.cells:
                    label = self.cells[(row, col + col_offset)]
                    label.config(text=text, fg=fg, bg=bg, width=width)
                else:
                    self.create_cell_label(row, col + col_offset, text, width, fg, bg)

        # Показываем только используемые ячейки
        for key, label in self.cells.items():
            if key in used_keys:
                label.grid(row=key[0], column=key[1], sticky="nsew")
            else:
                label.grid_remove()

        # Обновляем размеры Canvas
        self.scrollable_frame.update_idletasks()
        frame_width = self.scrollable_frame.winfo_reqwidth()
        frame_height = self.scrollable_frame.winfo_reqheight()
        height = min(frame_height, self.max_height)
        self.canvas.config(width=frame_width, height=height)


# Тестовые данные

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

data_variants = itertools.cycle([data_20x5, data_20x6, data_30x5])

grid_display = TkDataScrollGrid()
grid_display.max_height = 1000
grid_display.row_header = True  # Включаем нумерацию строк

def update_loop():
    while True:
        new_data = next(data_variants)
        grid_display.root.after(0, grid_display.update_grid_data, new_data)
        time.sleep(1)

threading.Thread(target=update_loop, daemon=True).start()

grid_display.root.mainloop()
