__version__ = "6.4"

import inspect
import asyncio
from typing import (Optional, Callable, Coroutine, Any, Dict, List)
from modules.logger import LoggerFactory

logger = LoggerFactory.get_logger("app." + __name__)
from typing import TypedDict

class TaskInfo(TypedDict):
    name: str
    status: str


class TaskManager:
    """
    Асинхронный менеджер задач без внутреннего хранения,
    использующий реальные задачи из текущего цикла событий через asyncio.all_tasks().

    Позволяет:
    - Создавать именованные задачи.
    - Получать статусы задач по имени.
    - Группировать задачи по статусам.
    - Отменять задачи по имени и ждать их завершения.
    - Отменять все задачи с именами.
    - Ожидать подтверждения отмены задачи с таймаутом.

    Статусы задач:
    - "active" — задача запущена и выполняется.
    - "done" — задача успешно завершена.
    - "cancelled" — задача отменена.
    - "error" — задача завершилась с исключением.
    """

    def __init__(self, *, shared: Optional[Dict[str, Any]] = None) -> None:
        """
        Инициализация TaskManager.

        :param shared: Опциональный словарь общих ресурсов, доступных задачам (не используется внутри TaskManager).
        """
        self.shared: Dict[str, Any] = shared or {}

    def add_task(
        self,
        name: str,
        coro_func: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        **kwargs: Any
    ) -> asyncio.Task:
        """
        Создаёт и запускает новую асинхронную задачу с уникальным именем.

        :param name: Уникальное имя задачи.
        :param coro_func: Асинхронная функция (корутина).
        :param args: Позиционные аргументы для корутины.
        :param kwargs: Именованные аргументы для корутины.
        :raises ValueError: если задача с таким именем уже существует.
        :return: Запущенный asyncio.Task
        """
        if self._find_task_by_name(name) is not None:
            raise ValueError(f"Task with name '{name}' already exists")

        async def task_wrapper() -> Any:
            try:
                return await coro_func(*args, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning (f"[TaskManager][add_task] Исключение {e} при создании задачи {name}")
                raise

        task: asyncio.Task = asyncio.create_task(task_wrapper(), name=name)
        # logger.debug(f"[TaskManager][add_task] создана задача {name}")
        return task

    def _find_task_by_name(self, name: str) -> Optional[asyncio.Task]:
        """
        Поиск задачи в текущем цикле событий по имени.

        :param name: Имя задачи.
        :return: Найденный asyncio.Task или None.
        """
        for task in asyncio.all_tasks():
            if task.get_name() == name:
                return task
        return None

    def get_status(self, name: str) -> Optional[str]:
        """
        Получить статус задачи по имени.

        :param name: Имя задачи.
        :return: Один из статусов {"active", "done", "cancelled", "error"} или None если задача не найдена.
        """
        task = self._find_task_by_name(name)
        if task is None:
            return None
        if not task.done():
            return "active"
        if task.cancelled():
            return "cancelled"
        if task.exception() is not None:
            return "error"
        return "done"

    def task_status_dict(self) -> Dict[str, str]:
        """
        Получить словарь всех задач с их статусами.

        :return: Словарь {имя задачи: статус}.
        """
        status_map: Dict[str, str] = {}
        for task in asyncio.all_tasks():
            name = task.get_name()
            if not name:
                continue
            status = self.get_status(name)
            if status is not None:
                status_map[name] = status
        return status_map

    def tasks_by_status(self) -> Dict[str, List[str]]:
        """
        Получить словарь, сгруппированный по статусам задач.

        :return: Словарь {статус: список имён задач}.
        """
        grouped: Dict[str, List[str]] = {
            "active": [],
            "done": [],
            "cancelled": [],
            "error": [],
        }
        for name, status in self.task_status_dict().items():
            grouped.setdefault(status, []).append(name)
        return grouped

    async def cancel_task(self, name: str, reason: str = None) -> None:
        """
        Безопасно отменяет задачу по имени.
        Использует asyncio.all_tasks() и корректно обрабатывает отмену и исключения.
        Сохраняет источник (инициатора) отмены и необязательную причину.
        """
        for task in asyncio.all_tasks():
            if task.get_name() == name:
                # Если задача уже завершена
                if task.done():
                    try:
                        exc = task.exception()
                        if exc:
                            logger.warning(f"[TaskManager][cancel_task] Задача {name} завершилась с исключением: {exc}")
                    except asyncio.CancelledError:
                        logger.debug(f"[TaskManager][cancel_task] Задача {name} уже отменена.")
                    return

                # Сохраняем контекст отмены
                frame = inspect.stack()[1]
                caller = f"{frame.function} ({frame.filename}:{frame.lineno})"
                setattr(task, "_cancel_context", caller)

                # Логируем причину отмены
                if reason:
                    logger.debug(f"[TaskManager][cancel_task] Отмена задачи {name}, инициатор: {caller}, причина: {reason}")
                else:
                    logger.debug(f"[TaskManager][cancel_task] Отмена задачи {name}, инициатор: {caller}")

                # Инициируем отмену
                task.cancel()

                # Ждем подтверждения отмены
                if await self.await_cancellation(name, timeout=3):
                    logger.debug(f"[TaskManager][await_cancellation] Задача {name} успешно отменена.")

                # Ждем завершения задачи (до 5 секунд)
                try:
                    await asyncio.wait_for(task, timeout=5)
                except asyncio.TimeoutError:
                    logger.warning(f"[TaskManager][cancel_task] Задача {name} не завершилась за 5 секунд.")
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.warning(f"[TaskManager][cancel_task] Исключение при завершении задачи {name}: {e}")
                return

        logger.warning(f"[TaskManager][cancel_task] Задача {name} не найдена для отмены.")

    async def cancel_all (self) -> None:
        """
        Отменяет все управляемые задачи (и, при необходимости, все в event loop),
        дожидается их корректного завершения с обработкой CancelledError.
        Очищает внутренний реестр задач.
        """
        current_task = asyncio.current_task ( )
        tasks_to_cancel = [ ]

        # Если вы управляете задачами через словарь/список (например, self.tasks)
        if hasattr (self, 'tasks') and isinstance (self.tasks, dict):
            tasks_to_cancel = list (self.tasks.values ( ))
            logger.debug (f"[TaskManager][cancel_all] Отменяются задачи из реестра: {list (self.tasks.keys ( ))}")
            # Можно также отменить все, кроме текущей
            tasks_to_cancel = [ t for t in tasks_to_cancel if t is not current_task ]
        else:
            # Альтернатива: отменить все задачи в цикле (осторожно!)
            all_tasks = asyncio.all_tasks ( )
            tasks_to_cancel = [ t for t in all_tasks if t is not current_task ]

        # Убираем дубликаты и фильтруем только активные
        tasks_to_cancel = list ({t for t in tasks_to_cancel if not t.done ( )})

        if not tasks_to_cancel:
            logger.debug ("[TaskManager][cancel_all] Нет активных задач для отмены.")
            return

        # Логируем имена задач до отмены
        task_names = [ t.get_name ( ) for t in tasks_to_cancel ]
        logger.debug (f"[TaskManager][cancel_all] Отменяются задачи: {task_names}")

        # Отменяем все задачи
        for task in tasks_to_cancel:
            task.cancel ( )

        # Дожидаемся завершения, включая CancelledError
        await asyncio.gather (*tasks_to_cancel, return_exceptions = True)

        # Опционально: очищаем реестр задач
        if hasattr (self, 'tasks'):
            self.tasks.clear ( )

        logger.debug (f"[TaskManager][cancel_all] Успешно отменено {len (tasks_to_cancel)} задач(и).")

    async def await_cancellation(self, name: str, timeout: Optional[float] = None) -> bool:
        """
        Отменить задачу по имени и ждать подтверждения отмены с таймаутом.

        :param name: Имя задачи.
        :param timeout: Максимальное время ожидания в секундах, None — ждать бесконечно.
        :raises KeyError: если задача не найдена.
        :return: True, если задача была отменена, False — если таймаут или задача завершилась без отмены.
        """

        task = self._find_task_by_name(name)
        if task is None:
            raise KeyError(f"Task '{name}' not found")

        if not task.cancelled() and not task.done():
            task.cancel()

        try:
            await asyncio.wait_for(task, timeout)

        except asyncio.CancelledError:
            return True
        except asyncio.TimeoutError:
            return False
        except Exception:
            return False

        return task.cancelled()

    def list_tasks_with_status(self) -> List[TaskInfo]:
        """
        Возвращает список задач с их текущими статусами.

        :return: [{ "name": str, "status": str }]
        """
        result: List[TaskInfo] = []

        for task in asyncio.all_tasks():
            name = task.get_name()
            if not name:
                continue

            if not task.done():
                status = "active"
            elif task.cancelled():
                status = "cancelled"
            elif task.exception() is not None:
                status = "error"
            else:
                status = "done"

            result.append({
                "name": name,
                "status": status,
            })

        return result

    def list_tasks (self) -> List [ str ]:
        return [ task.get_name ( ) for task in asyncio.all_tasks ( ) if task.get_name ( ) ]

    def list_tasks_status_map(self) -> Dict[str, str]:
        return {t["name"]: t["status"] for t in self.list_tasks_with_status()}


# --- Пример использования ---

async def example_worker(name: str) -> None:
    """
    Пример корутины-воркера, которая работает до отмены.
    """
    try:
        while True:
            print(f"[{name}] Работает...")
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        print(f"[{name}] Получил отмену")
        raise


async def main() -> None:
    manager = TaskManager()

    # Создаём и запускаем задачи
    manager.add_task("worker1", example_worker, "worker1")
    manager.add_task("worker2", example_worker, "worker2")

    # Немного поработаем
    await asyncio.sleep(3)

    # Получаем и печатаем статусы всех задач
    print("Статусы задач:", manager.task_status_dict())


    print(manager.list_tasks_status_map)



    # Отменяем и ждём отмены worker1
    was_cancelled = await manager.await_cancellation("worker1", timeout=2)
    print(f"worker1 отменён: {was_cancelled}")
    print('-----------------------------')
    for t in manager.list_tasks_with_status():
        print(f"{t['name']:40} {t['status']}")
    print('-----------------------------')
    await asyncio.sleep(2)

    # Печатаем статусы после отмены
    print("Статусы после отмены:", manager.task_status_dict())

    # Отменяем все задачи и ждём их завершения
    await manager.cancel_all()


if __name__ == "__main__":
    asyncio.run(main())

"""
Что делает TaskManager:
1) Управление задачами без внутреннего состояния — весь список и состояние задач берутся напрямую из текущего цикла событий через asyncio.all_tasks(). 
    Это гарантирует, что менеджер отображает реальное текущее состояние без рассинхрона.
2) Создание задач с уникальными именами (через asyncio.create_task(..., name=...)). При попытке создать задачу с уже существующим именем будет ошибка.
3) Получение статуса задачи по имени:
    - "active" — задача ещё не завершилась.
    - "done" — задача завершилась успешно.
    - "cancelled" — задача была отменена.
    - "error" — задача завершилась с ошибкой.
4) Группировка задач по статусам для удобного контроля.
5) Отмена задач — как по одной, так и всех сразу, с ожиданием завершения.
6) Метод await_cancellation — отменяет задачу и ждёт её подтверждения отмены с необязательным таймаутом, возвращая факт успешной отмены.

Пример использования:
 - Запускаем две задачи-воркера, которые печатают сообщения каждую секунду.
 - Спустя 3 секунды выводим текущие статусы.
 - Затем отменяем worker1 и ждём до 2 секунд подтверждения отмены.
 - Проверяем, действительно ли задача была отменена.
 - Печатаем обновлённые статусы.
 - В конце отменяем все оставшиеся задачи и завершаем программу.
"""
