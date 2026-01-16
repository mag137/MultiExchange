__version__ = "2.5"

import tkinter as tk
import multiprocessing
from typing import Optional, Dict
import itertools
from modules import cprint
from modules.logger import LoggerFactory

logger = LoggerFactory.get_logger ("app." + __name__)

class TkGrid:

    def __init__(self, max_height: int = 300, row_header: bool = True, fix_max_width_column: bool = True, allow_vertical_resize: bool = True):
        """
        Инициализирует объект TkDataScrollGrid.
        Создаёт очередь для передачи данных и запускает процесс Tkinter.
        Здесь только происходит инициализация нового процесса графического интерфейса.
        Все настройки окна и сетки происходят уже в отдельном процессе метода run_gui
        Возможно менять параметры таблицы динамически через очередь
        """
        logger.debug(f"Инициализация TkGrid: {__name__}")
        self.data_queue = multiprocessing.Queue()  # Очередь для получения данных
        self.queue_datadict_wrapper_key = None # Наличие вложенности словаря с данными - если есть внешний словарь - то указывается ключ
        self.fix_max_width_column = fix_max_width_column # При включённой опции fix_max_width_column запоминает максимальные ширины ячеек по колонкам.
        self.max_column_widths: Dict[int, int] = {}  # Хранит максимальные ширины по столбцам
        self.allow_vertical_resize = allow_vertical_resize # При включённой опции allow_vertical_resize высота подгоняется под содержимое до конца экрана, при фалсе - фиксируется размерами
        self.align = 'center'  # Значение по умолчанию для выравнивания, возможные значения: 'e, w, center, left, right'

        # Настройки
        self.max_height = max_height
        self.row_header = row_header

        # Запускаем процесс Tkinter - варинт автономного запуска приложения в отдельном процессе
        # self.tk_process = multiprocessing.Process(target=self.run_gui, daemon=True)
        # self.tk_process.start()

    @property
    def queue(self):
        """
        Возвращает очередь для передачи данных.
        """
        return self.data_queue

    def run_gui(self, shared_values=None):
        """
        Запускает графический интерфейс Tkinter.
        """
        self.root = tk.Tk()
        self.root.title("Default Display title")
        self.root.attributes('-topmost', True)

        # Создание Canvas, Scrollbar и других элементов...
        self.canvas = tk.Canvas(self.root)
        self.scrollbar = tk.Scrollbar(self.root, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = tk.Frame(self.canvas)

        # Настройка прокрутки и других параметров...
        self.max_height = 300
        self.scrollable_frame.bind("<Configure>", self.update_scrollregion)
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")

        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        # Упаковка Canvas и Scrollbar
        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        # Остальная логика...
        self.cells: Dict[tuple, tk.Label] = {}
        self.font = ("Ubuntu", 14)
        self.borderwidth = 1
        self.relief = "solid"
        self.last_scrollregion = (0, 0, 0, 0)
        self.header_bg = '#323030'
        self.header_fg = 'white'
        self.ident = 2

        # Определяем размеры экрана
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()

        # Определяем размеры таблицы
        self.table_width = 800  # Примерная ширина таблицы (может быть динамической)
        self.table_height = 600  # Примерная высота таблицы (может быть динамической)

        # Устанавливаем минимальные и максимальные размеры окна
        scrollbar_width = 20  # Ширина скроллбара
        self.root.minsize(width=400, height=300)  # Минимальный размер окна
        # self.root.maxsize(width=min(self.table_width + scrollbar_width, screen_width),
        #                   height=min(self.table_height, screen_height))
        self.root.resizable(True, True)

        # Центрируем окно на экране
        x_position = (screen_width - min(self.table_width + scrollbar_width, screen_width)) // 2
        y_position = (screen_height - min(self.table_height, screen_height)) // 2
        self.root.geometry(f"{min(self.table_width + scrollbar_width, screen_width)}x"
                           f"{min(self.table_height, screen_height)}+{x_position}+{y_position}")

        # Запуск GUI
        self.process_data_queue()  # Обработка очереди

        # Проверка shared_values["shutdown"]
        def check_shutdown():
            try:
                if shared_values and shared_values["shutdown"].value:
                    logger.info("Получен сигнал завершения от shared_values['shutdown']")
                    self.root.destroy()
                    return
            except Exception as e:
                logger.error(f"Ошибка при проверке shutdown: {e}")
            self.root.after(500, check_shutdown)

        check_shutdown()  # запускаем первый вызов
        self.root.mainloop()

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

    @staticmethod
    def align_to_anchor(align):
        """
        Поддерживает значения align: 'left', 'right', 'center'.
        Преобразует их в соответствующие значения anchor Tkinter: 'w', 'e', 'center'.
        """
        if align == 'left' or align =='w':
            anchor = 'w'
        elif align == 'right' or align =='e':
            anchor = 'e'
        else:
            anchor = 'center'
        return anchor

    def create_cell_label(self, row: int, column: int, text: str, width: int, fg: str, bg: str,
                          align: str = 'center') -> tk.Label:

        """
        Создаёт Label (ячейку) с заданными параметрами.

        Аргументы:
            row (int): Номер строки.
            column (int): Номер столбца.
            text (str): Текст ячейки.
            width (int): Ширина ячейки (в символах).
            fg (str): Цвет текста.
            bg (str): Цвет фона.
            anchor(str): выравнивание текста

        Возвращает:
            tk.Label: Созданный объект метки.

        Создаёт Label (ячейку) с заданными параметрами.

        """
        label = tk.Label(
            self.scrollable_frame,
            text=text,
            font=self.font,
            fg=fg,
            bg=bg,
            borderwidth=self.borderwidth,
            relief=self.relief,
            width=width,
            anchor=TkGrid.align_to_anchor(align)  # <-- здесь передаем anchor
        )
        self.cells[(row, column)] = label
        return label

    # Патч для класса TkGrid: исправление ошибки len(float)
    # и правильное преобразование данных в строки для корректного отображения.

    def update_grid_data(self, grid_data: Optional[dict] = None) -> None:
        """
        Обновляет данные в таблице, скрывая неиспользуемые ячейки.
        При включённой опции fix_max_width_column запоминает максимальные ширины ячеек по колонкам.
        """
        if grid_data is None:
            grid_data = {'header': {0: {'text': 'test'}}}
        if not isinstance(grid_data, dict):
            raise ValueError("grid_data must be a dictionary")

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

            # Если у нас ряд - заголовок
            if self.row_header:
                row_number = str(row)
                if (row, 0) in self.cells:
                    label = self.cells[(row, 0)]
                    label.config(text=row_number, width=len(row_number) + self.ident) if row != 0 else label.config(
                        text=' № ', width=len(row_number) + self.ident)
                else:
                    self.create_cell_label(row, 0, row_number, len(row_number) + self.ident,
                                           self.header_fg, self.header_bg, self.align)
                used_keys.add((row, 0))

            for col, cell_data in row_data.items():
                text = cell_data.get('text', '')
                # Приводим к строке для безопасного вычисления длины и передачи в Label
                text_str = str(text)
                fg = self.header_fg if col == -1 or row == 0 else cell_data.get('fg', 'black')
                bg = self.header_bg if col == -1 or row == 0 else cell_data.get('bg', '#D9D9D9')
                align = cell_data.get('align', self.align)  # Получаем выравнивание из данных или по умолчанию

                base_width = max(len(text_str), 10) + self.ident

                col_key = col + col_offset
                if self.fix_max_width_column:
                    prev_max = self.max_column_widths.get(col_key, 0)
                    width = max(prev_max, base_width)
                    self.max_column_widths[col_key] = width
                else:
                    width = base_width

                self.scrollable_frame.grid_rowconfigure(row, weight=1)
                self.scrollable_frame.grid_columnconfigure(col_key, weight=1)

                if (row, col_key) in self.cells:
                    label = self.cells[(row, col_key)]
                    label.config(text=text_str, fg=fg, bg=bg, width=width,
                                 anchor=TkGrid.align_to_anchor(align))  # <-- anchor из данных
                else:
                    self.create_cell_label(row, col_key, text_str, width, fg, bg, align)  # <-- anchor из данных

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

        # Динамическое вычисление размеров таблицы
        scrollbar_width = 15  # Ширина скроллбара
        self.table_width = frame_width + scrollbar_width
        if self.allow_vertical_resize:
            self.table_height = frame_height
        else:
            self.table_height = self.max_height

        # self.table_height = frame_height

        # Обновляем размеры окна
        self.root.maxsize(width=min(self.table_width, self.root.winfo_screenwidth()),
                          height=min(self.table_height, self.root.winfo_screenheight()))
        self.root.geometry(f"{min(self.table_width, self.root.winfo_screenwidth())}x"
                           f"{min(self.table_height, self.root.winfo_screenheight())}")

    def process_data_queue(self):
        """
        Обрабатывает данные из очереди.

        Проверяет очередь на наличие новых данных и обновляет интерфейс.
        Вызывается каждые 100 мс через `root.after`.
        """
        try:
            while not self.data_queue.empty ( ):
                if self.queue_datadict_wrapper_key:
                    item = self.data_queue.get_nowait ( )[self.queue_datadict_wrapper_key]
                else:
                    item = self.data_queue.get_nowait()
                # Диагностическое сообщение получаемой очереди
                # cprint.success_w (f"Получаем в TKGRID: {item}")

                if isinstance (item, dict):
                    if item.get ("command") == "close":
                        self.root.destroy ( )
                        return

                    # Обработка config
                    if 'config' in item:
                        config = item [ 'config' ]
                        for key, value in config.items ( ):
                            if key == "title":
                                self.root.title (value)
                            elif hasattr (self, key):
                                setattr (self, key, value)
                                print (f"Настройка {key} изменена на {value}")

                    # Обработка отдельного title
                    elif 'title' in item:
                        self.root.title (item [ 'title' ])

                    else:
                        # Это данные для таблицы
                        self.update_grid_data (item)
        except Exception as e:
            cprint.error_b (f"Error: TkGrid: Ошибка при обработке данных из очереди источника: {e}")
        self.root.after (100, self.process_data_queue)

def run_gui_grid_process(*, table_queue_data: multiprocessing.Queue, shared_values: multiprocessing.Value, queue_datadict_wrapper_key: Optional[str] = None, **kwargs):
    try:
        logger.debug("'run_gui_grid_process': запуск")
        TkGrid3_obj = TkGrid(**kwargs)
        # TkGrid3_obj.queue_datadict_wrapper_key = 'spread_table'
        TkGrid3_obj.queue_datadict_wrapper_key = queue_datadict_wrapper_key
        TkGrid3_obj.data_queue = table_queue_data
        print("Получена переменная:  ",str(shared_values["shutdown"].value))
        print(shared_values["shutdown"])
        logger.debug("shared_values")
        logger.debug(shared_values["shutdown"])

        TkGrid3_obj.run_gui(shared_values)

    except Exception as e:
        logger.exception(f"Ошибка в run_gui_grid_process: {e}")

import random
import time

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
            1: {0: {'text': f'Row 1, Col 1: {random.randint(1, 100)}'},
                1: {'text': f'Row 1, Col 2: {random.randint(1, 100)}'},
                2: {'text': f'Row 1, Col 3: {random.randint(1, 100)}'}},
            2: {0: {'text': f'Row 2, Col 1: {random.randint(1, 100)}'},
                1: {'text': f'Row 2, Col 2: {random.randint(1, 100)}'},
                2: {'text': f'Row 2, Col 3: {random.randint(1, 100)}'}}
        }
grid_data = {
    'header': {
        0: {'text': 'Column 1', 'align': 'left'},
        1: {'text': 'Column 2', 'align': 'center'},
        2: {'text': 'Column 3', 'align': 'right'}
    },
    1: {
        0: {'text': 'Data 1', 'align': 'left'},
        1: {'text': 'Data 2', 'align': 'center'},
        2: {'text': 'Data 3', 'align': 'right'}
    }
}
grid_data1 = {
    'header': {
        0: {'text': 'Column 1', 'align': 'left'},
        1: {'text': 'Column 2', 'align': 'center'},
        2: {'text': 'Column 3', 'align': 'right'}
    },
    1: {
        0: {'text': 'Data 1'},
        1: {'text': 'Data 2'},
        2: {'text': 'Data 3'}
    },
    3: {
        0: {'text': 'Data 1'},
        1: {'text': 'Data 2'},
        2: {'text': 'Data 3'}
    }
}
grid_data2 = {
    'header': {
        0: {'text': 'Column 1', 'align': 'left'},
        1: {'text': 'Column 2', 'align': 'center'},
        2: {'text': 'Column 3', 'align': 'right'}
    },

    2: {
        0: {'text': 'Data 1'},
        1: {'text': 'Data 2'},
        2: {'text': 'Data 3'}
    }
}


# data_variants = itertools.cycle([data_20x5, data_20x6, data_30x5, data, grid_data])
data_variants = itertools.cycle([ grid_data1, grid_data2])

def source_data(queue):
    while True:
        queue.put(next(data_variants))  # Отправляем данные в очередь
        time.sleep(1)  # Обновляем данные каждую секунду
        queue.put({'config': {'title': 'ЕЩЕ Новый заголовок окна'}})


if __name__ == "__main__":
    grid = TkGrid()
    queue = multiprocessing.Queue()
    spread_table_queue_data = multiprocessing.Queue()
    queue_out = multiprocessing.Queue()
    p = multiprocessing.Process(target=grid.run_gui)# args=(spread_table_queue_data, queue_out))
    p.start()
    # Создаём объект TkDataScrollGrid

    # Получаем очередь из объекта
    queue = grid.queue

    # Отправляем конфигурацию
    time.sleep(1)
    queue.put({'config': {'max_height': 400, 'row_header': True}})
    queue.put({'config': {'title': 'Новый заголовок окна'}})


    # Запускаем источник данных в основном процессе
    source_data(queue)

"""
### Описание класса `TkDataScrollGrid`

Класс `TkDataScrollGrid` предназначен для отображения данных в виде сетки с прокруткой, используя библиотеку Tkinter для создания графического интерфейса пользователя (GUI). 
Основная цель класса — предоставить функциональность для отображения таблиц с прокручиваемыми данными, которые могут быть обновлены динамически.

### Основные компоненты класса:

1. **Инициализация (метод `__init__`)**:
   При создании объекта `TkDataScrollGrid`, инициализируется очередь `multiprocessing.Queue`, в которую можно отправлять данные. 
   Затем создается отдельный процесс, в котором будет работать интерфейс Tkinter. Процесс запускается с помощью метода `start`, 
   и его задача — запускать GUI и обрабатывать поступающие данные.

2. **Очередь (свойство `queue`)**:
   Очередь — это механизм обмена данными между основным процессом и процессом с Tkinter. Главный процесс может отправлять данные в эту очередь, 
   а Tkinter будет извлекать данные и обновлять интерфейс. Это позволяет разделить логику обработки данных и отображение данных.

3. **Запуск GUI (метод `run_gui`)**:
   В методе `run_gui` создается окно Tkinter с помощью `tk.Tk()`, на котором будут размещаться компоненты интерфейса:
   - **Canvas** для прокрутки содержимого.
   - **Scrollbar** для вертикальной прокрутки.
   - **Frame** для размещения всех элементов, которые будут отображаться.
   Кроме того, в этом методе запускается цикл событий Tkinter (`self.root.mainloop()`), который управляет интерфейсом.

4. **Обновление области прокрутки (метод `update_scrollregion`)**:
   Этот метод отвечает за обновление области прокрутки, если размер содержимого изменяется. Он вызывается каждый раз, 
   когда добавляются новые элементы на Canvas. Этот метод помогает убедиться, что прокрутка работает корректно.

5. **Создание ячейки (метод `create_cell_label`)**:
   Метод `create_cell_label` отвечает за создание ячеек таблицы в виде компонентов `tk.Label`. В качестве параметров передаются:
   - **row** и **column** — координаты ячейки.
   - **text** — текст, который будет отображаться в ячейке.
   - **width, fg, bg** — параметры, определяющие внешний вид ячейки.

6. **Обновление данных в таблице (метод `update_grid_data`)**:
   Этот метод используется для обновления данных в таблице. Данные поступают в виде словаря, и метод обновляет соответствующие ячейки. 
   Он также поддерживает обновление заголовков строк и столбцов и занимается изменением вида ячеек в зависимости от данных (например, изменения фона и цвета текста). 
   Этот метод также позволяет скрывать неиспользуемые ячейки и оптимизировать отображение.

7. **Обработка очереди (метод `process_data_queue`)**:
   В этом методе происходит извлечение данных из очереди и передача их в метод `update_grid_data` для обновления отображения. 
   Метод вызывается каждые 100 миллисекунд, что позволяет в реальном времени обновлять данные, поступающие в очередь.

### Взаимодействие с основным процессом:

1. **Передача данных из основного процесса**:
   В основном процессе можно генерировать или получать данные (например, через вебсокеты, базы данных или другие источники). 
   Эти данные помещаются в очередь с помощью `queue.put(data)`.

2. **Обработка данных в Tkinter**:
   В процессе Tkinter (в отдельном процессе) данные извлекаются из очереди методом `process_data_queue`, и таблица обновляется. 
   Это позволяет изолировать работу Tkinter и не блокировать основной процесс.

### Пример использования:

#### 1. Запуск интерфейса:

Чтобы начать использовать `TkDataScrollGrid`, создается объект этого класса, который сразу же запускает отдельный процесс с GUI.

```python
grid = TkDataScrollGrid()  # Создаем объект интерфейса
queue = grid.queue  # Получаем очередь для отправки данных
```

#### 2. Генерация данных:

Затем можно использовать функцию (или любой другой источник данных) для создания данных, которые будут отображаться в таблице. 
Например, функция `source_data` генерирует случайные данные и отправляет их в очередь.

```python
import random
import time

def source_data(queue):
    headers = ['Header 1', 'Header 2', 'Header 3']
    while True:
        data = {
            'header': {i: {'text': headers[i]} for i in range(len(headers))},
            1: {0: {'text': f'Row 1, Col 1: {random.randint(1, 100)}'},
                1: {'text': f'Row 1, Col 2: {random.randint(1, 100)}'},
                2: {'text': f'Row 1, Col 3: {random.randint(1, 100)}'}},
            2: {0: {'text': f'Row 2, Col 1: {random.randint(1, 100)}'},
                1: {'text': f'Row 2, Col 2: {random.randint(1, 100)}'},
                2: {'text': f'Row 2, Col 3: {random.randint(1, 100)}'}}
        }
        queue.put(data)  # Отправляем данные в очередь
        time.sleep(1)  # Обновляем данные каждую секунду
```

#### 3. Запуск потока с данными:

Для того чтобы начать обновление данных в GUI, можно запустить генерацию данных, как показано ниже:

```python
if __name__ == "__main__":
    grid = TkDataScrollGrid()  # Создаем объект TkDataScrollGrid
    queue = grid.queue  # Получаем очередь для передачи данных

    # Запускаем источник данных в основном процессе
    source_data(queue)
```

В итоге, данные будут генерироваться в основном процессе и поступать в интерфейс, где таблица будет обновляться каждую секунду.

### Важные моменты:
- **Главный процесс и Tkinter в отдельном процессе**: Tkinter должен работать в отдельном процессе, так как он требует основного потока для корректной работы с GUI. 
Это позволяет избежать блокировки главного потока.
- **Очередь для обмена данными**: Использование очереди `multiprocessing.Queue` позволяет безопасно передавать данные между процессами.
- **Обновление интерфейса**: Интерфейс автоматически обновляется, когда данные поступают в очередь.

### Заключение:
Этот класс организует работу с Tkinter в отдельном процессе, изолируя логику приложения от отображения. 
Такой подход способствует лучшему разделению ответственности и повышает отказоустойчивость программы. 
Точно так же можно адаптировать этот код для работы с любыми другими источниками данных и процессами, требующими отображения результатов в GUI.
"""