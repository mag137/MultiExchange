__version__ = "2.1"

import logging
from logging import Handler, Logger
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, UTC
import os
from typing import Optional, List, Any, Dict


class CustomLogger (logging.Logger):
    """
    Расширенный логгер с возможностью динамического указания файла для записи.

    Наследует стандартный logging.Logger и добавляет функциональность:
    - Установка целевого файла для логирования
    - Возврат пути к файлу после записи сообщения
    - Поддержка временного переключения файлов для отдельных сообщений

    Attributes:
        _current_log_file (Optional[str]): Текущий активный файл для логирования
        _file_handlers (Dict[str, Handler]): Кэш файловых обработчиков
    """

    def __init__ (self, name: str, level: int = logging.NOTSET) -> None:
        """
        Инициализирует кастомный логгер.

        Args:
            name: Имя логгера
            level: Уровень логирования (по умолчанию NOTSET)
        """
        super ( ).__init__ (name, level)
        self._current_log_file: Optional [ str ] = None
        self._file_handlers: Dict [ str, Handler ] = {}

    def set_log_file (self, filepath: str) -> None:
        """
        Устанавливает файл для записи последующих логов.

        Создает директорию если она не существует и инициализирует файловый обработчик.

        Args:
            filepath: Полный путь к файлу лога

        Raises:
            OSError: Если невозможно создать директорию или файл
        """
        if filepath not in self._file_handlers:
            # Создаем директорию если не существует
            directory = os.path.dirname (filepath)
            if directory:
                os.makedirs (directory, exist_ok = True)

            # Создаем файловый обработчик
            handler = logging.FileHandler (filepath, encoding = 'utf-8')
            handler.setLevel (self.level)

            # Используем форматтер из существующего обработчика или создаем стандартный
            if self.handlers:
                existing_formatter = None
                for h in self.handlers:
                    if hasattr (h, 'formatter') and h.formatter:
                        existing_formatter = h.formatter
                        break
                if existing_formatter:
                    handler.setFormatter (existing_formatter)
                else:
                    handler.setFormatter (logging.Formatter (
                        '%(asctime)s - %(levelname)s - [%(name)s.%(funcName)s():%(lineno)d] - %(message)s'
                    ))
            else:
                handler.setFormatter (logging.Formatter (
                    '%(asctime)s - %(levelname)s - [%(name)s.%(funcName)s():%(lineno)d] - %(message)s'
                ))

            self._file_handlers [ filepath ] = handler

        self._current_log_file = filepath

    def _log (
            self,
            level: int,
            msg: str,
            args: Any,
            exc_info: Any = None,
            extra: Any = None,
            stack_info: bool = False,
            stacklevel: int = 1,
            log_file: Optional [ str ] = None
    ) -> str:
        """
        Основной метод логирования с поддержкой указания файла.

        Args:
            level: Уровень логирования
            msg: Сообщение для записи
            args: Аргументы для форматирования сообщения
            exc_info: Информация об исключении
            extra: Дополнительная информация
            stack_info: Флаг включения информации о стеке
            stacklevel: Уровень стека для определения места вызова
            log_file: Опциональный путь к файлу для этого сообщения

        Returns:
            str: Путь к файлу, в который было записано сообщение
        """
        # Сохраняем текущие обработчики
        original_handlers = self.handlers.copy ( )

        # Если указан файл, добавляем соответствующий обработчик
        target_file = log_file or self._current_log_file
        file_used = ""

        if target_file and target_file in self._file_handlers:
            file_used = target_file
            # Временно удаляем все обработчики и добавляем только целевой
            for handler in self.handlers [ : ]:
                self.removeHandler (handler)

            self.addHandler (self._file_handlers [ target_file ])

        # Вызываем оригинальный метод логирования
        try:
            super ( )._log (level, msg, args, exc_info, extra, stack_info, stacklevel + 1)
        finally:
            # Всегда восстанавливаем оригинальные обработчики
            if target_file and target_file in self._file_handlers:
                self.removeHandler (self._file_handlers [ target_file ])
                for handler in original_handlers:
                    self.addHandler (handler)

        return file_used

    def debug (self, msg: str, *args: Any,
               log_file: Optional [ str ] = None, **kwargs: Any) -> str:
        """
        Логирует сообщение уровня DEBUG и возвращает путь к файлу.

        Args:
            msg: Сообщение для логирования
            log_file: Опциональный путь к файлу для этого сообщения
            *args: Дополнительные аргументы для форматирования
            **kwargs: Дополнительные ключевые аргументы

        Returns:
            str: Путь к файлу, в который было записано сообщение
        """
        return self._log (logging.DEBUG, msg, args, log_file = log_file, **kwargs)

    def info (self, msg: str, *args: Any,
              log_file: Optional [ str ] = None, **kwargs: Any) -> str:
        """
        Логирует сообщение уровня INFO и возвращает путь к файлу.

        Args:
            msg: Сообщение для логирования
            log_file: Опциональный путь к файлу для этого сообщения
            *args: Дополнительные аргументы для форматирования
            **kwargs: Дополнительные ключевые аргументы

        Returns:
            str: Путь к файлу, в который было записано сообщение
        """
        return self._log (logging.INFO, msg, args, log_file = log_file, **kwargs)

    def warning (self, msg: str, *args: Any,
                 log_file: Optional [ str ] = None, **kwargs: Any) -> str:
        """
        Логирует сообщение уровня WARNING и возвращает путь к файлу.

        Args:
            msg: Сообщение для логирования
            log_file: Опциональный путь к файлу для этого сообщения
            *args: Дополнительные аргументы для форматирования
            **kwargs: Дополнительные ключевые аргументы

        Returns:
            str: Путь к файлу, в который было записано сообщение
        """
        return self._log (logging.WARNING, msg, args, log_file = log_file, **kwargs)

    def error (self, msg: str, *args: Any,
               log_file: Optional [ str ] = None, **kwargs: Any) -> str:
        """
        Логирует сообщение уровня ERROR и возвращает путь к файлу.

        Args:
            msg: Сообщение для логирования
            log_file: Опциональный путь к файлу для этого сообщения
            *args: Дополнительные аргументы для форматирования
            **kwargs: Дополнительные ключевые аргументы

        Returns:
            str: Путь к файлу, в который было записано сообщение
        """
        return self._log (logging.ERROR, msg, args, log_file = log_file, **kwargs)

    def critical (self, msg: str, *args: Any,
                  log_file: Optional [ str ] = None, **kwargs: Any) -> str:
        """
        Логирует сообщение уровня CRITICAL и возвращает путь к файлу.

        Args:
            msg: Сообщение для логирования
            log_file: Опциональный путь к файлу для этого сообщения
            *args: Дополнительные аргументы для форматирования
            **kwargs: Дополнительные ключевые аргументы

        Returns:
            str: Путь к файлу, в который было записано сообщение
        """
        return self._log (logging.CRITICAL, msg, args, log_file = log_file, **kwargs)

    def fatal (self, msg: str, *args: Any,
               log_file: Optional [ str ] = None, **kwargs: Any) -> str:
        """
        Логирует сообщение уровня CRITICAL (синоним critical).

        Args:
            msg: Сообщение для логирования
            log_file: Опциональный путь к файлу для этого сообщения
            *args: Дополнительные аргументы для форматирования
            **kwargs: Дополнительные ключевые аргументы

        Returns:
            str: Путь к файлу, в который было записано сообщение
        """
        return self.critical (msg, *args, log_file = log_file, **kwargs)


class ColorFormatter (logging.Formatter):
    """
    Форматтер, добавляющий ANSI-цвета в сообщения лога в консоли.

    Цвета зависят от уровня логирования для улучшения читаемости.
    """

    COLOR_CODES = {
        logging.DEBUG: '\033[94m',  # Синий
        logging.INFO: '\033[92m',  # Зелёный
        logging.WARNING: '\033[93m',  # Жёлтый
        logging.ERROR: '\033[91m',  # Красный
        logging.CRITICAL: '\033[97;41m'  # Белый на красном фоне
    }
    RESET = '\033[0m'

    def format (self, record: logging.LogRecord) -> str:
        """
        Форматирует запись лога с добавлением цветовых кодов.

        Args:
            record: Запись лога для форматирования

        Returns:
            str: Отформатированное цветное сообщение
        """
        color = self.COLOR_CODES.get (record.levelno, self.RESET)
        message = super ( ).format (record)
        return f"{color}{message}{self.RESET}"


class LoggerFactory:
    """
    Фабрика для создания и настройки кастомных логгеров.

    Предоставляет гибкую систему настройки логгеров с поддержкой:
    - Разделения логов по уровням
    - Ротации файлов по времени
    - Цветного вывода в консоль
    - Динамического изменения файлов логирования
    """

    @classmethod
    def get_logger (
            cls,
            name: str,
            level: int = logging.DEBUG,
            formatter: Optional [ logging.Formatter ] = None,
            handlers: Optional [ List [ Handler ] ] = None,
            console_level: Optional [ int ] = None,
            split_levels: bool = False,
            use_timed_rotating: bool = False,
            log_filename: str = "app.log",
            debug_filename: str = "debug.log",
            info_filename: str = "info.log",
            error_filename: str = "error.log",
            add_date_to_filename: bool = False,
            date_filename_format: str = "_%Y-%m-%d",
            add_time_to_filename: bool = False,
            time_filename_format: str = "_%H-%M-%S",
            use_dated_folder: bool = True,
            date_folder_format: str = "%Y-%m-%d",
            timed_when: str = 'midnight',
            timed_interval: int = 1,
            timed_backup_count: int = 7,
            timed_utc: bool = True,
            propagate: bool = False,
            base_logs_dir: Optional [ str ] = None
    ) -> CustomLogger:
        """
        Создает и настраивает кастомный логгер с указанными параметрами.

        Args:
            name: Уникальное имя логгера
            level: Уровень логирования для файлов
            formatter: Кастомный форматтер сообщений
            handlers: Список кастомных обработчиков
            console_level: Уровень логирования для консоли
            split_levels: Разделять ли логи по файлам в зависимости от уровня
            use_timed_rotating: Включить ротацию файлов по времени
            log_filename: Имя основного файла лога
            debug_filename: Имя файла для debug логов
            info_filename: Имя файла для info логов
            error_filename: Имя файла для error логов
            add_date_to_filename: Добавлять дату к имени файла
            date_filename_format: Формат даты для имени файла
            add_time_to_filename: Добавлять время к имени файла
            time_filename_format: Формат времени для имени файла
            use_dated_folder: Создавать папки с датой для логов
            date_folder_format: Формат даты для папки
            timed_when: Интервал ротации файлов
            timed_interval: Периодичность ротации
            timed_backup_count: Количество хранимых backup файлов
            timed_utc: Использовать UTC время для ротации
            propagate: Пропускать ли сообщения родительским логгерам
            base_logs_dir: Базовая директория для хранения логов

        Returns:
            CustomLogger: Настроенный экземпляр кастомного логгера

        Raises:
            OSError: Если невозможно создать директории для логов
        """
        if base_logs_dir is None:
            base_logs_dir = os.path.abspath (os.path.join (
                os.path.dirname (__file__), '..', 'logs'
            ))

        # Устанавливаем кастомный класс логгера
        logging.setLoggerClass (CustomLogger)
        logger = logging.getLogger (name)

        # Проверяем, не был ли логгер уже инициализирован
        if logger.handlers:
            return logger

        logger.setLevel (level)

        # Стандартный форматтер, если не задан
        if formatter is None:
            formatter = logging.Formatter (
                '%(asctime)s - %(levelname)s - [%(name)s.%(funcName)s():%(lineno)d] - %(message)s'
            )

        def make_filepath (base_name: str) -> str:
            """Создает полный путь к файлу лога на основе настроек."""
            logs_dir = base_logs_dir
            if use_dated_folder:
                logs_dir = os.path.join (logs_dir, datetime.now (UTC).strftime (date_folder_format))

            os.makedirs (logs_dir, exist_ok = True)

            if add_date_to_filename:
                name_part, ext = os.path.splitext (base_name)
                base_name = f"{name_part}{datetime.now (UTC).strftime (date_filename_format)}{ext}"

            if add_time_to_filename:
                name_part, ext = os.path.splitext (base_name)
                base_name = f"{name_part}{datetime.now (UTC).strftime (time_filename_format)}{ext}"

            return os.path.join (logs_dir, base_name)

        def make_handler (path: str, lvl: int) -> Handler:
            """Создает файловый обработчик с указанными параметрами."""
            if use_timed_rotating:
                handler = TimedRotatingFileHandler (
                    path,
                    when = timed_when,
                    interval = timed_interval,
                    backupCount = timed_backup_count,
                    encoding = 'utf-8',
                    utc = timed_utc
                )
            else:
                handler = logging.FileHandler (path, encoding = 'utf-8')

            handler.setLevel (lvl)
            handler.setFormatter (formatter)
            return handler

        # Обработка кастомных обработчиков
        if handlers:
            for handler in handlers:
                if not handler.formatter:
                    handler.setFormatter (formatter)
                logger.addHandler (handler)

        elif split_levels:
            # Разделение логов по уровням
            debug_handler = make_handler (make_filepath (debug_filename), logging.DEBUG)
            debug_handler.addFilter (lambda record: record.levelno == logging.DEBUG)
            logger.addHandler (debug_handler)

            info_handler = make_handler (make_filepath (info_filename), logging.INFO)
            info_handler.addFilter (lambda r: r.levelno == logging.INFO)
            logger.addHandler (info_handler)

            error_handler = make_handler (make_filepath (error_filename), logging.WARNING)
            error_handler.addFilter (lambda r: r.levelno >= logging.WARNING)
            logger.addHandler (error_handler)

        else:
            # Единый файл для всех логов
            path = make_filepath (log_filename)
            handler = make_handler (path, level)
            logger.addHandler (handler)

        # Консольный обработчик с цветным выводом
        console_handler = logging.StreamHandler ( )
        console_handler.setLevel (console_level if console_level is not None else level)
        console_formatter = ColorFormatter (
            '%(asctime)s - %(levelname)s - [%(name)s.%(funcName)s():%(lineno)d] - %(message)s'
        )
        console_handler.setFormatter (console_formatter)
        logger.addHandler (console_handler)

        logger.propagate = propagate

        # Сохраняем путь к основному файлу для последующего использования
        if not split_levels:
            main_file_path = make_filepath (log_filename)
            logger.set_log_file (main_file_path)

        return logger


# Пример использования
if __name__ == "__main__":
    # Демонстрация работы логгера
    logger = LoggerFactory.get_logger ("demo.logger", split_levels = False)

    # Устанавливаем кастомный файл для записи
    custom_file = "/tmp/demo_log.log"
    logger.set_log_file (custom_file)

    # Пишем в указанный файл
    result_file = logger.info ("Тестовое сообщение в custom файл")
    print (f"Записано в файл: {result_file}")

    # Запись в другой файл через параметр
    another_file = "/tmp/another_demo.log"
    result_file2 = logger.debug ("Сообщение в другой файл", log_file = another_file)
    print (f"Записано в файл: {result_file2}")

    # Возвращаемся к основному файлу
    logger.info ("Сообщение в основной файл")

    # Тестирование разных уровней
    logger.debug ("Отладочное сообщение")
    logger.warning ("Предупреждение")
    logger.error ("Ошибка")
    logger.critical ("Критическая ошибка")