__version__ = "2.1"
import logging
import os
from typing import Optional, List
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

import logging
import os
from datetime import datetime
from logging import Handler, Logger
from logging.handlers import TimedRotatingFileHandler
from typing import Optional, List

import logging
import os
from datetime import datetime
from logging import Handler, Logger
from logging.handlers import TimedRotatingFileHandler
from typing import Optional, List
from datetime import datetime, timezone


class LoggerMixin:
    """
    Миксин для добавления настраиваемого логгера с возможностью ротации, раздельного логирования по уровням
    и пользовательскими хендлерами.

    Основные возможности
    --------------------
    - Генерация логгера с уникальным именем на основе модуля и класса.
    - Поддержка ротации логов по времени (TimedRotatingFileHandler).
    - Разделение логов на debug/info и error/critical (split_levels).
    - Возможность добавления пользовательских хендлеров (self.logger_handlers).
    - Поддержка даты в имени файла или в подпапке.

    Атрибуты класса (можно переопределять в наследниках)
    ----------------------------------------------------
    logger_name : str | None
        Имя логгера. По умолчанию формируется из модуля и класса.
    logger_level : int
        Уровень логирования по умолчанию.
    logger_formatter : logging.Formatter
        Формат вывода сообщений.
    logger_propagate : bool
        Пропускать ли логи наверх (по умолчанию — нет).

    use_timed_rotating : bool
        Включить ротацию по времени.
    timed_when : str
        Интервал ротации ('midnight', 'D', 'H', и т.д.).
    timed_interval : int
        Количество интервалов до ротации.
    timed_backup_count : int
        Количество хранимых файлов.
    timed_utc : bool
        Использовать ли UTC.

    log_filename : str
        Имя основного лог-файла.
    add_date_to_filename : bool
        Добавлять ли дату в имя файла.
    date_filename_format : str
        Формат добавляемой даты.

    use_dated_folder : bool
        Использовать ли подпапку по дате.
    date_folder_format : str
        Формат подпапки по дате.

    split_levels : bool
        Разделять ли уровни логов (debug vs error).
    debug_filename : str
        Имя файла для debug/info/warning.
    error_filename : str
        Имя файла для error/critical.

    base_logs_dir : str
        Базовая папка для логов.

    logger_handlers : List[Handler] | None
        Необязательный список пользовательских хендлеров.
    """

    __annotations__ = {
        'logger_name': Optional [ str ],
        'logger_level': int,
        'logger_formatter': Optional [ logging.Formatter ],
        'logger_propagate': bool,
        'use_timed_rotating': bool,
        'timed_when': str,
        'timed_interval': int,
        'timed_backup_count': int,
        'timed_utc': bool,
        'log_filename': str,
        'add_date_to_filename': bool,
        'date_filename_format': str,
        'use_dated_folder': bool,
        'date_folder_format': str,
        'split_levels': bool,
        'debug_filename': str,
        'error_filename': str,
        'base_logs_dir': str,
        'logger_handlers': Optional [ List [ Handler ] ],
    }

    # Настройки по умолчанию
    logger_name = None
    logger_level = logging.DEBUG
    logger_formatter = logging.Formatter ('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger_propagate = False

    use_timed_rotating = False
    timed_when = 'midnight'
    timed_interval = 1
    timed_backup_count = 7
    timed_utc = True

    log_filename = "app.log"
    add_date_to_filename = False
    date_filename_format = "_%Y-%m-%d"

    use_dated_folder = False
    date_folder_format = "%Y-%m-%d"

    split_levels = False
    debug_filename = "debug.log"
    error_filename = "error.log"

    base_logs_dir = os.path.abspath (os.path.join (os.path.dirname (__file__), '..', 'logs'))

    logger_handlers: Optional [ List [ Handler ] ] = None

    @classmethod
    def _handler_path (cls, base_name: str) -> str:
        """
        Возвращает путь к лог-файлу с учетом опций использования подпапок и даты в имени.
        """
        logs_dir = cls.base_logs_dir

        if cls.use_dated_folder:
            dated_folder = datetime.utcnow ( ).strftime (cls.date_folder_format)
            logs_dir = os.path.join (logs_dir, dated_folder)

        os.makedirs (logs_dir, exist_ok = True)

        if cls.add_date_to_filename:
            name, ext = os.path.splitext (base_name)
            date_part = datetime.now(timezone.utc).strftime(cls.date_filename_format)
            base_name = f"{name}{date_part}{ext}"

        return os.path.join (logs_dir, base_name)

    @classmethod
    def _make_handler (cls, filepath: str, level: int) -> Handler:
        """
        Создает и возвращает лог-хендлер (с ротацией или без).
        """
        if cls.use_timed_rotating:
            handler = TimedRotatingFileHandler (
                filename = filepath,
                when = cls.timed_when,
                interval = cls.timed_interval,
                backupCount = cls.timed_backup_count,
                encoding = 'utf-8',
                utc = cls.timed_utc
            )
        else:
            handler = logging.FileHandler (filepath, encoding = 'utf-8')

        handler.setLevel (level)
        return handler

    @classmethod
    def _build_default_handlers (cls) -> List [ Handler ]:
        """
        Создает список хендлеров по умолчанию на основе настроек класса.
        """
        handlers = [ ]

        if cls.split_levels:
            debug_path = cls._handler_path (cls.debug_filename)
            debug_handler = cls._make_handler (debug_path, logging.DEBUG)
            debug_handler.addFilter (lambda r: r.levelno < logging.ERROR)
            handlers.append (debug_handler)

            error_path = cls._handler_path (cls.error_filename)
            error_handler = cls._make_handler (error_path, logging.ERROR)
            handlers.append (error_handler)
        else:
            log_path = cls._handler_path (cls.log_filename)
            handler = cls._make_handler (log_path, cls.logger_level)
            handlers.append (handler)

        return handlers

    @property
    def logger (self) -> Logger:
        """
        Возвращает и настраивает логгер, если он еще не был создан.
        """
        if not hasattr (self, '_logger'):
            name = self.logger_name or f"{self.__class__.__module__}.{self.__class__.__name__}"
            logger = logging.getLogger (name)
            logger.setLevel (self.logger_level)

            if not logger.handlers:
                # Используем пользовательские хендлеры, если заданы
                if self.logger_handlers:
                    for handler in self.logger_handlers:
                        if not handler.formatter:
                            handler.setFormatter (self.logger_formatter)
                        logger.addHandler (handler)
                else:
                    for handler in self._build_default_handlers ( ):
                        handler.setFormatter (self.logger_formatter)
                        logger.addHandler (handler)

                logger.propagate = self.logger_propagate

            self._logger = logger
        return self._logger


class ClassA(LoggerMixin):
    def do_something(self):
        self.logger.info("Лог из ClassA")


class ClassB(LoggerMixin):
    def do_something(self):
        self.logger.info("Лог из ClassB")

class MyClass(LoggerMixin):
    def __init__(self):
        self.logger_handlers = [
            logging.FileHandler(os.path.join(LoggerMixin.base_logs_dir, "example.log"))
        ]
        self.logger_level = logging.DEBUG

    def do_something(self):
        self.logger.info("Hello from log!")



if __name__ == "__main__":
    a1 = MyClass ( )
    a1.do_something ( )

    a = ClassA ( )
    b = ClassB ( )

    a.do_something ( )
    b.do_something ( )

    print (f"Logger для ClassA: {a.logger.name}")
    print (f"Logger для ClassB: {b.logger.name}")
