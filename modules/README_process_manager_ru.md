# Класс ProcessManager: универсальный менеджер процессов для Python

## Описание

`ProcessManager` — это простой и универсальный класс-менеджер для запуска, завершения и ожидания процессов в Python.  
Он не создаёт автоматически очереди или разделяемые переменные.  
Всё, что необходимо для межпроцессного взаимодействия (очереди, shared_value и др.), пользователь создаёт сам и явно пробрасывает в процессы через `args` и/или `kwargs` при запуске.

### Дополнительно

В классе реализованы статические методы для работы с разделяемыми строками через `multiprocessing.Value` на основе `ctypes.c_char * N`:

- **create_shared_str(value, max_bytes=16, marker=b'\xFF')** — создание новой разделяемой строки.
- **update_shared_str(shared_str, value, max_bytes=16, marker=b'\xFF')** — обновление значения существующей разделяемой строки.
- **read_str(shared_str)** — безопасное чтение строки из разделяемой памяти (до маркера конца строки).

---

## Пример использования

```python
import multiprocessing
from process_manager import ProcessManager

def child(qin, qout, shared_str):
    while True:
        msg = qin.get()
        if msg == "stop":
            qout.put("done")
            break
        elif msg.startswith("update:"):
            # Обновление строки через статический метод ProcessManager
            to_set = msg.split(":", 1)[1]
            ProcessManager.update_shared_str(shared_str, to_set)
            qout.put("updated")
        else:
            # Чтение строки через статический метод ProcessManager
            val = ProcessManager.read_str(shared_str)
            qout.put(f"Текущее значение: {val}")

if __name__ == "__main__":
    manager = ProcessManager()
    qin = multiprocessing.Queue()
    qout = multiprocessing.Queue()

    # Создание разделяемой строки
    shared_str = ProcessManager.create_shared_str("Привет!")

    manager.start_process("worker", child, args=(qin, qout, shared_str))

    qin.put("ping")
    print(qout.get())  # Текущее значение: Привет!

    qin.put("update:Мир!")
    print(qout.get())  # updated

    qin.put("ping")
    print(qout.get())  # Текущее значение: Мир!

    qin.put("stop")
    print(qout.get())  # done

    manager.join_all()
    print("Все процессы завершены.")
```

---

## Основные преимущества

- Простота и прозрачность: все объекты межпроцессного взаимодействия создаются явно.
- Гибкость: любой процесс может получить любые очереди или shared_value в любом количестве.
- Production-ready: аннотации типов, подробные docstring, пример использования, поддержку безопасной работы со строками, включая обновление существующей строки.