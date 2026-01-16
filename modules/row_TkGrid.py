__version__ = '1.0'
import tkinter as tk
import multiprocessing
import time
import random
from typing import Dict


# ---------------- TkGrid ----------------
class Row_TkGrid:
    """
    Row_TkGrid: отображение таблицы с динамическим обновлением через очередь.
    Особенность версии row - в очередь данные таблицы кидаются построчно.
    Обновление таблицы происходит при поступлении данных строки в очередь.

    Особенности:
    - Колонка 0 всегда для нумерации строк.
    - Данные начинаются с колонки 1.
    - Поддержка динамического обновления нескольких сообщений из очереди.
    - Авторазмер окна по содержимому таблицы.
    - Поддержка font_size в данных ячейки (включая заголовок).
    """

    def __init__(self, data_queue: multiprocessing.Queue, row_header=True):
        self.data_queue = data_queue
        self.row_header = row_header
        self.cells: Dict[tuple, tk.Label] = {}
        self.table_state: Dict[int, dict] = {}
        self.ident = 2  # уменьшена добавка к ширине ячеек
        self.align = 'center'

        # Стили
        self.header_bg = '#323030'
        self.header_fg = 'white'
        self.rownum_bg = 'black'
        self.rownum_fg = 'white'
        self.cell_bg = '#D9D9D9'
        self.cell_fg = 'black'

        # Шрифты
        self.font_family = "Ubuntu"
        self.font_size = 14
        self.header_font_size = 11  # <-- заголовок меньше
        self.font = (self.font_family, self.font_size)

        self.header_columns = 0  # количество колонок данных (не считая колонки нумерации)

    @property
    def queue(self):
        if self.data_queue is None:
            raise RuntimeError("data_queue не инициализирована")
        return self.data_queue

    @staticmethod
    def align_to_anchor(align):
        if align in ('left', 'w'):
            return 'w'
        elif align in ('right', 'e'):
            return 'e'
        return 'center'

    def _resolve_font(self, row: int, cell_data: dict | None):
        """
        Определяет шрифт ячейки:
        - font_size из данных имеет приоритет
        - иначе заголовок / данные
        """
        if cell_data and 'font_size' in cell_data:
            return (self.font_family, int(cell_data['font_size']))

        if row == 0:
            return (self.font_family, self.header_font_size)

        return self.font

    def create_cell_label(self, row, col, text, width, fg, bg, align, font):
        lbl = tk.Label(
            self.scrollable_frame,
            text=text,
            font=font,
            fg=fg,
            bg=bg,
            borderwidth=1,
            relief="solid",
            width=width,
            anchor=self.align_to_anchor(align)
        )
        lbl.grid(row=row, column=col, sticky="nsew")
        self.cells[(row, col)] = lbl
        return lbl

    def update_header(self, header_data: dict):
        """Обновление заголовка (ключи с 1 и выше для данных)"""
        if 'header' in header_data:
            header_data = header_data['header']
        elif 0 in header_data:
            header_data = header_data[0]

        self.header_columns = max(header_data.keys())

        for col, cell in header_data.items():
            text = str(cell.get("text", ""))
            align = cell.get("align", "center")
            font = self._resolve_font(0, cell)

            # минимальная ширина уменьшена
            width = max(len(text), 3) + self.ident

            if (0, col) in self.cells:
                self.cells[(0, col)].config(
                    text=text,
                    fg=self.header_fg,
                    bg=self.header_bg,
                    width=width,
                    font=font,
                    anchor=self.align_to_anchor(align)
                )
            else:
                self.create_cell_label(
                    0, col, text, width,
                    self.header_fg, self.header_bg, align, font
                )

    def resize_window_to_table(self):
        """Подгонка окна: ширина = таблица, высота ≤ таблицы"""

        self.scrollable_frame.update_idletasks()

        table_width = self.scrollable_frame.winfo_reqwidth()
        table_height = self.scrollable_frame.winfo_reqheight()

        scrollbar_width = self.scrollbar.winfo_reqwidth()
        window_width = table_width + scrollbar_width

        current_height = self.root.winfo_height()

        self.root.geometry(f"{window_width}x{current_height}")
        self.root.resizable(False, True)

        if current_height > table_height:
            self.root.geometry(f"{window_width}x{table_height}")

        self.root.minsize(window_width, 200)
        self.root.maxsize(window_width, table_height)

    def update_row(self, row: int, row_data: dict):
        """Обновление одной строки"""
        self.table_state[row] = row_data

        # Номер строки
        if self.row_header:
            text = str(row)
            font = (self.font_family, self.font_size - 1)

            if (row, 0) in self.cells:
                self.cells[(row, 0)].config(text=text, fg=self.rownum_fg, bg=self.rownum_bg, font=font)
            else:
                self.create_cell_label(
                    row, 0, text,
                    len(text) + self.ident,
                    self.rownum_fg, self.rownum_bg,
                    'center', font
                )

            if (0, 0) not in self.cells:
                self.create_cell_label(
                    0, 0, "",
                    3,
                    self.header_fg, self.header_bg,
                    'center',
                    (self.font_family, self.header_font_size)
                )

        # Остальные колонки
        for col in range(1, self.header_columns + 1):
            cell_data = row_data.get(col, {})
            text = str(cell_data.get("text", ""))
            fg = cell_data.get("fg", self.cell_fg)
            bg = cell_data.get("bg", self.cell_bg)
            align = cell_data.get("align", self.align)
            font = self._resolve_font(row, cell_data)

            width = max(len(text), 4) + self.ident

            if (row, col) in self.cells:
                self.cells[(row, col)].config(
                    text=text,
                    fg=fg,
                    bg=bg,
                    width=width,
                    font=font,
                    anchor=self.align_to_anchor(align)
                )
            else:
                self.create_cell_label(
                    row, col, text, width,
                    fg, bg, align, font
                )

    def process_queue(self):
        """Обработка всех сообщений из очереди и авторазмер окна"""
        updated = False
        while not self.data_queue.empty():
            msg = self.data_queue.get_nowait()
            updated = True

            if 'header' in msg or 0 in msg:
                self.update_header(msg)

            elif 'title' in msg:
                self.root.title(msg['title'])

            else:
                for row, row_data in msg.items():
                    if row in ('header', 0):
                        continue
                    self.update_row(row, row_data)

        if updated:
            self.resize_window_to_table()

        self.root.after(50, self.process_queue)

    def run_gui(self):
        """Запуск GUI"""
        self.root = tk.Tk()
        self.root.title("TkGrid 2.6")

        self.canvas = tk.Canvas(self.root)
        self.scrollbar = tk.Scrollbar(self.root, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = tk.Frame(self.canvas)

        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )

        self.root.after(50, self.process_queue)
        self.root.mainloop()


# ---------------- Динамический источник данных ----------------
def dynamic_data_sender(queue: multiprocessing.Queue):
    header = {'header': {
        1: {"text": "Col 1", "align": "left", "font_size": 11},
        2: {"text": "Col 2", "align": "center", "font_size": 11},
        3: {"text": "Col 3", "align": "right", "font_size": 11},
    }}
    queue.put(header)

    row = 1
    while True:
        queue.put({"title": "TkGrid demo"})

        for _ in range(10):
            row_data = {
                1: {"text": str(random.randint(10, 99))},
                2: {"text": str(random.randint(100, 999))},
                3: {"text": str(random.randint(1000, 9999))}
            }
            queue.put({row: row_data})
            row += 1
            time.sleep(0.02)

        row = 1


# ---------------- Основной процесс ----------------
if __name__ == "__main__":
    q = multiprocessing.Queue()
    gui_process = multiprocessing.Process(
        target=lambda q: Row_TkGrid(q).run_gui(),
        args=(q,)
    )
    gui_process.start()

    time.sleep(0.5)
    dynamic_data_sender(q)
