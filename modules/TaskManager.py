__version__ = "3.0"

import asyncio
from typing import Dict
from abc import ABC
from modules.colored_console import cprint
from pprint import pprint

# Класс управления асинхронными задачами
class TaskManager:
    def __init__(self):
        self.tasks = {}  # { name: task }

    def add_task(self, name: str, coroutine):
        """Добавить новую задачу по имени"""
        if name in self.tasks:
            raise ValueError(f"Задача с именем '{name}' уже существует.")
        task = asyncio.create_task(coroutine, name=name)
        self.tasks[name] = task
        return task

    def stop_task(self, name: str):
        """Удалить задачу по имени (если существует)"""
        task = self.tasks.get(name)
        if task:
            if not task.done():
                task.cancel()
            del self.tasks[name]
            return True
        return False

    def get_task_status(self, name: str):
        """Получить статус задачи"""

        task = self.tasks.get(name)

        if not task:
            return "Не найдена"
        elif task.done():
            if task.exception():
                return f"Завершена с ошибкой: {task.exception()}"
            return "Завершена успешно"
        elif task.cancelled():
            return "Отменена"
        else:
            return "В процессе"

    def get_counts(self):
        """Получить количество активных и завершённых задач"""
        active = 0
        done = 0
        cancelled = 0
        for task in self.tasks.values():
            if task.done():
                if task.exception():
                    done += 1  # Завершена с ошибкой
                else:
                    done += 1  # Завершена успешно
            elif task.cancelled():
                cancelled += 1
            else:
                active += 1
        return {
            'active': active,
            'done': done,
            'cancelled': cancelled,
            'total': len(self.tasks)
        }

    async def stop_all_tasks(self):
        """Остановить все активные задачи"""
        for task in self.tasks.values():
            if not task.done():
                task.cancel()
        results = await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                cprint.error_b(f"Ошибка при завершении задачи: {result}")
        self.tasks.clear()

# интерфейс-Родитель с методами управления асинхронными задачами аналогично task_manager
class TaskManagerMixin(ABC):
    tasks = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.tasks = {}  # убираем!

    @classmethod
    def add_task(cls, name: str, coroutine) -> asyncio.Task | None:
        """Добавить новую задачу по имени"""
        if name in cls.tasks:
            cprint.warning_b(f"TaskManagerMixin: Задача '{name}' уже существует.")
            return None
        task = asyncio.create_task(coroutine, name=name)
        cls.tasks[name] = task
        cprint.success(f"TaskManagerMixin: Задача '{name}' добавлена.")
        return task

    @classmethod
    def stop_task(cls, name: str) -> bool:
        """Остановить задачу по имени"""
        task = cls.tasks.get(name)
        if task:
            if not task.done():
                task.cancel()
            del cls.tasks[name]
            cprint.success(f"TaskManagerMixin: Задача '{name}' остановлена.")
            return True
        cprint.warning_b(f"TaskManagerMixin: Задача '{name}' не найдена.")
        return False

    @classmethod
    def get_task_status(cls, name: str) -> str:
        """Получить статус задачи"""
        task = cls.tasks.get(name)
        if not task:
            return "Не найдена"
        elif task.cancelled():
            return "Отменена"
        elif task.done():
            if task.exception():
                return f"Завершена с ошибкой: {task.exception()}"
            return "Завершена успешно"
        else:
            return "В процессе"

    @classmethod
    def get_task_count(cls) -> dict:
        """Получить количество задач по статусу"""
        active = done = cancelled = 0
        for task in cls.tasks.values():
            if task.done():
                done += 1
            elif task.cancelled():
                cancelled += 1
            else:
                active += 1
        return {
            'active': active,
            'done': done,
            'cancelled': cancelled,
            'total': len(cls.tasks)
        }

    @classmethod
    async def stop_all_tasks(cls):
        for name in list(cls.tasks.keys()):
            cls.stop_task(name)

# Пример наследника с миксином TaskManagerMixin
class Example_TaskManagerMixin(TaskManagerMixin):
    def __init__(self):
        super().__init__()

    async def run_task(self, name: str, delay: int):
        try:
            print(f"[{name}] Запуск...")
            await asyncio.sleep(delay)
            print(f"[{name}] Завершено.")
        except asyncio.CancelledError:
            print(f"[{name}] Прервано.")

async def sample_coro(name: str, delay: int):
    print(f"[{name}] Начинаю выполнение...")
    try:
        await asyncio.sleep(delay)
        print(f"[{name}] Завершено")
    except asyncio.CancelledError:
        print(f"[{name}] Прервано")

async def main():
    mtm = Example_TaskManagerMixin()
    tm = TaskManager()

    tm.add_task("task-1", sample_coro("task-1", 5))
    tm.add_task("task-2", sample_coro("task-2", 10))
    tm.add_task("task-3", sample_coro("task-3", 3))

    mtm.add_task("name1", mtm.run_task("name1", 5))
    mtm.add_task("name2", mtm.run_task("name2", 15))

    print("\nТекущий статус:")
    for name in tm.tasks:
        print(f"{name} -> {tm.get_task_status(name)}")

    print("\nКоличество задач:", tm.get_counts())

    print("\nУдаляем task-2...")
    tm.stop_task("task-2")
    print("Статус после удаления task-2:", tm.get_task_status("task-2"))

    print("\nЖдём завершения всех задач...")
    await asyncio.sleep(4)

    print("\nФинальный статус:")
    for name in tm.tasks:
        print(f"{name} -> {tm.get_task_status(name)}")

    print("\nКоличество задач:", tm.get_counts())

    await tm.stop_all_tasks()

if __name__ == "__main__":
    asyncio.run(main())