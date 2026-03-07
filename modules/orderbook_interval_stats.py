from __future__ import annotations

_version_ = '1.1'
"""Скользящая статистика интервалов между обновлениями ордербука.

Модуль изолирует аналитику качества входящего стрима `watchOrderBook`.
Он ничего не знает про биржи, очереди, `asyncio` и торговые решения. Его
единственная задача: измерять интервалы между соседними тиками и периодически
возвращать агрегированную статистику по скользящему окну.

Почему это полезно:
- можно оценить плотность потока данных;
- можно заметить неравномерность или лаги;
- можно сравнивать качество стрима между биржами и символами.

Что считается:
- `mean_dt`: средний интервал между тиками;
- `std_dt`: абсолютный разброс интервалов;
- `cv_dt`: относительный разброс, то есть `std_dt / mean_dt`.
- `min_dt`: минимальный интервал в текущем окне;
- `max_dt`: максимальный интервал в текущем окне.

Как работает окно:
- первый тик только запоминает timestamp;
- начиная со второго тика считается `Δt` между текущим и предыдущим тиком;
- последние `N` интервалов хранятся в `deque(maxlen=N)`;
- снимок публикуется либо в момент первого полного заполнения окна, либо по
  таймауту.

Почему используется `time.monotonic()`:
- системные часы могут сдвигаться;
- для интервалов важно монотонное время;
- это делает расчёт устойчивым к внешним корректировкам времени.

Notes:
    После публикации окно не очищается. Оно продолжает жить как обычное
    скользящее окно, чтобы статистика отражала текущее состояние потока,
    а не отдельные несвязанные батчи.
"""

from collections import deque
from math import sqrt
import time
from typing import Literal, TypedDict


class OrderbookIntervalStatsSnapshot(TypedDict):
    """TypedDict снимка статистики интервалов.

    Notes:
        Поля структуры описывают уже посчитанную статистику по текущему
        содержимому скользящего окна. Это сериализуемый результат, который
        можно безопасно класть в очередь, логировать или пробрасывать выше.
    """

    type: Literal["orderbook_interval_stats"]
    window_size: int
    window_capacity: int
    timeout_sec: float
    mean_dt: float
    std_dt: float
    cv_dt: float | None
    min_dt: float
    max_dt: float
    ts_to: float
    trigger: Literal["window_full", "timeout"]


class OrderbookIntervalStatsWindow:
    """Скользящее окно статистики интервалов между соседними тиками.

    Экземпляр класса накапливает последние интервалы `Δt` и по запросу через
    :meth:`observe` может вернуть снимок статистики. Класс удобно использовать
    рядом с любым источником последовательных событий, где важны:

    - плотность потока;
    - равномерность потока;
    - обнаружение задержек или рваного поведения.

    Алгоритм работы:
    1. При первом тике запоминается только его timestamp.
    2. Каждый следующий тик образует новый интервал с предыдущим тиком.
    3. Интервал добавляется в FIFO-окно фиксированного размера.
    4. Если окно только что впервые стало полным или истёк таймаут публикации,
       возвращается снимок.

    Notes:
        Статистика считается по формуле population standard deviation, то есть
        деление идёт на `N`, а не на `N - 1`. Это осознанный выбор, потому что
        окно рассматривается как текущее рабочее множество наблюдений, а не
        как выборка для оценки неизвестной генеральной совокупности.
        Поле `window_size` означает число интервалов `Δt`, а не число тиков.
    """

    def __init__(self, max_intervals: int = 50, emit_timeout_sec: float = 30.0) -> None:
        """Инициализировать окно статистики.

        Args:
            max_intervals: Максимальное число интервалов, одновременно
                хранимых в скользящем окне. Должно быть не меньше `2`.
            emit_timeout_sec: Максимально допустимая пауза между двумя
                публикациями статистики. Должна быть строго больше `0`.

        Raises:
            ValueError: Если `max_intervals < 2`.
            ValueError: Если `emit_timeout_sec <= 0`.
        """
        if max_intervals < 2:
            raise ValueError("max_intervals должен быть >= 2")
        if emit_timeout_sec <= 0:
            raise ValueError("emit_timeout_sec должен быть > 0")

        self.max_intervals: int = max_intervals
        self.emit_timeout_sec: float = emit_timeout_sec
        self._intervals: deque[float] = deque(maxlen=max_intervals)
        self._last_tick_ts: float | None = None
        self._last_emit_ts: float | None = None

    @property
    def size(self) -> int:
        """Вернуть текущее количество интервалов внутри окна.

        Это не число тиков, а именно число рассчитанных интервалов `Δt`.
        После первого тика `size == 0`, потому что интервала ещё нет.
        """
        return len(self._intervals)

    @property
    def is_full(self) -> bool:
        """Вернуть `True`, если окно заполнено до максимальной ёмкости."""
        return len(self._intervals) == self.max_intervals

    @property
    def last_tick_ts(self) -> float | None:
        """Вернуть timestamp последнего зарегистрированного тика.

        Возвращает:
        - `None`, если объект ещё не видел ни одного тика;
        - `float`, если хотя бы один тик уже был зарегистрирован.
        """
        return self._last_tick_ts

    @property
    def last_emit_ts(self) -> float | None:
        """Вернуть timestamp последней публикации статистики.

        Техническая деталь:
        после самого первого тика это поле тоже инициализируется, чтобы можно
        было отсчитывать `emit_timeout_sec` даже до полного заполнения окна.
        """
        return self._last_emit_ts

    def reset(self) -> None:
        """Полностью сбросить накопленное состояние объекта.

        После вызова:
        - все интервалы удаляются;
        - timestamp последнего тика забывается;
        - timestamp последней публикации забывается.

        Следующий вызов :meth:`observe` снова будет вести себя как первый.
        """
        self._intervals.clear()
        self._last_tick_ts = None
        self._last_emit_ts = None

    def _build_snapshot(
        self,
        *,
        now: float,
        trigger: Literal["window_full", "timeout"],
    ) -> OrderbookIntervalStatsSnapshot:
        """Собрать снимок статистики по текущему содержимому окна.

        Args:
            now: Timestamp текущего тика, на котором строится снимок.
            trigger: Причина публикации статистики.

        Returns:
            Готовый словарь со статистикой окна.

        Предполагается, что к моменту вызова окно уже содержит как минимум
        один интервал.
        """
        mean_dt = sum(self._intervals) / len(self._intervals)
        variance = sum((dt - mean_dt) ** 2 for dt in self._intervals) / len(self._intervals)
        std_dt = sqrt(variance)
        cv_dt = std_dt / mean_dt if mean_dt > 0 else None
        min_dt = min(self._intervals)
        max_dt = max(self._intervals)

        return {
            "type": "orderbook_interval_stats",
            "window_size": len(self._intervals),
            "window_capacity": self.max_intervals,
            "timeout_sec": self.emit_timeout_sec,
            "mean_dt": mean_dt,
            "std_dt": std_dt,
            "cv_dt": cv_dt,
            "min_dt": min_dt,
            "max_dt": max_dt,
            "ts_to": now,
            "trigger": trigger,
        }

    def observe(self, ts: float | None = None) -> OrderbookIntervalStatsSnapshot | None:
        """Зарегистрировать новый тик и при необходимости вернуть статистику.

        Args:
            ts: Timestamp нового тика в монотонной временной шкале.
                Если `None`, timestamp будет взят автоматически через
                `time.monotonic()` прямо в момент вызова метода.

        Returns:
            `None`, если:
            - это первый тик;
            - окно ещё не заполнено;
            - таймаут публикации ещё не истёк.

            `OrderbookIntervalStatsSnapshot`, если:
            - окно только что впервые стало полным;
            - или истёк `emit_timeout_sec` с момента предыдущей публикации.

        Замечание:
            Метод не очищает окно после публикации. Это важно для непрерывного
            мониторинга, где интересует текущее скользящее состояние потока, а
            не отдельные непересекающиеся батчи.
        """
        now = time.monotonic() if ts is None else ts
        previous_tick_ts = self._last_tick_ts
        self._last_tick_ts = now

        if previous_tick_ts is None:
            self._last_emit_ts = now
            return None

        was_full_before_append = self.is_full
        self._intervals.append(now - previous_tick_ts)

        window_just_filled = not was_full_before_append and self.is_full
        timeout_reached = (
            self._last_emit_ts is not None
            and now - self._last_emit_ts >= self.emit_timeout_sec
        )

        if not window_just_filled and not timeout_reached:
            return None

        self._last_emit_ts = now
        trigger: Literal["window_full", "timeout"] = "window_full" if window_just_filled else "timeout"
        return self._build_snapshot(now=now, trigger=trigger)
