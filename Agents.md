Project: MultiExchange arbitrage system

Stack:
- Python 3.14
- asyncio
- ccxt.pro
- multiprocessing
- Tkinter GUI

Important architecture:
- TaskManager управляет открытием и жизнь задач
- ProcessManager управляет открытием и жизнью процессов
- Связь между процессами через очереди  multiprocessing.Queue
- Связь между задачами через очереди  Asyncio.Queue

Цели:
- Данный бот - арбитражная торговая система между несколькими биржами посредством библиотеки ccxt
- Арбитраж между биржами на swap инструментах. Каждая биржа имеет свой изолированный баланс
- Вывод используется через веб-интерфейс

Заметки:
- Биржа OKX - Максимальное количество экземпляров CCXT - десять. При превышении ошибка "too many requests"