"""Базовые сущности для учёта спреда и аналитики сигналов арбитража.

Назначение модуля:
1. Хранить скользящую историю спреда по каждому направлению в паре бирж.
2. Считать статистики окна для фильтров входа/выхода.
3. Держать один `ArbitragePair` на комбинацию `(exchange1, exchange2)` и
   обрабатывать оба направления внутри:
   - direct:  покупка на exchange1 / продажа на exchange2, `bid2 - ask1`
   - reverse: покупка на exchange2 / продажа на exchange1, `bid1 - ask2`
"""

from __future__ import annotations

from collections import deque
from math import sqrt
from typing import Any

_version_ = "1.1"


class SpreadWindow:
    """Скользящее окно спреда с методами статистики.

    Класс отвечает только за аналитику (не за торговые решения):
    хранит последние значения спреда и возвращает статистики по всему окну
    или по последним `period` точкам.
    """
        # Инициализация размера окна в тиках
    def __init__(self, size: int) -> None:
        if size < 2:
            raise ValueError("size должен быть >= 2")
        self._window: deque[float] = deque(maxlen=size)

    def append(self, value: float) -> None:
        """Добавить следующий тик спреда в окно."""
        self._window.append(float(value))

    def get_all(self) -> list[float]:
        """Вернуть копию всех значений, которые сейчас есть в окне."""
        return list(self._window)

    def count(self) -> int:
        """Вернуть текущее количество тиков в окне."""
        return len(self._window)

    def is_ready(self) -> bool:
        """Вернуть True, когда окно полностью заполнено."""
        return len(self._window) == self._window.maxlen

    def _tail(self, period: int | None = None) -> list[float]:
        """Вернуть хвост выборки для расчётов по периоду."""
        data = list(self._window)
        if not data:
            return []

        if period is None:
            return data
        if period <= 0:
            raise ValueError("period должен быть > 0")
        if period >= len(data):
            return data
        return data[-period:]

    def min(self, period: int | None = None) -> float | None:
        """Минимум по всему окну или по последним `period` тикам."""
        data = self._tail(period)
        return min(data) if data else None

    def max(self, period: int | None = None) -> float | None:
        """Максимум по всему окну или по последним `period` тикам."""
        data = self._tail(period)
        return max(data) if data else None

    def mean(self, period: int | None = None) -> float | None:
        """Среднее арифметическое по окну или по последним `period` тикам."""
        data = self._tail(period)
        if not data:
            return None
        return sum(data) / len(data)

    def std(self, period: int | None = None) -> float | None:
        """Стандартное отклонение (population) по окну или периоду."""
        data = self._tail(period)
        if len(data) < 2:
            return None
        mu = sum(data) / len(data)
        variance = sum((x - mu) ** 2 for x in data) / len(data)
        return sqrt(variance)

    def last(self) -> float | None:
        """Вернуть последнее значение спреда или None, если окно пустое."""
        return self._window[-1] if self._window else None

    def zscore_last(self, period: int | None = None) -> float | None:
        """Вернуть z-score последней точки для выбранной выборки.

        Возвращает:
        - None, если точек меньше 2
        - 0.0, если sigma равна 0 (плоская выборка)
        """
        data = self._tail(period)
        if len(data) < 2:
            return None

        mu = sum(data) / len(data)
        variance = sum((x - mu) ** 2 for x in data) / len(data)
        sigma = sqrt(variance)
        if sigma == 0:
            return 0.0
        return (data[-1] - mu) / sigma

    def snapshot(self, period: int | None = None) -> dict[str, float | int | None]:
        """Вернуть компактный срез статистик для логов/сигнального пайплайна."""
        data = self._tail(period)
        if not data:
            return {
                "count": 0,
                "mean": None,
                "std": None,
                "min": None,
                "max": None,
                "last": None,
                "zscore_last": None,
            }

        mu = sum(data) / len(data)
        if len(data) >= 2:
            variance = sum((x - mu) ** 2 for x in data) / len(data)
            sigma = sqrt(variance)
            zscore_last = 0.0 if sigma == 0 else (data[-1] - mu) / sigma
        else:
            sigma = None
            zscore_last = None

        return {
            "count": len(data),
            "mean": mu,
            "std": sigma,
            "min": min(data),
            "max": max(data),
            "last": data[-1],
            "zscore_last": zscore_last,
        }


class EMA:
    """EMA (экспоненциальное среднее) для сглаживания спреда."""

    def __init__(self, alpha: float) -> None:
        if not (0.0 < alpha <= 1.0):
            raise ValueError("alpha должен быть в диапазоне (0, 1]")
        self.alpha = alpha
        self.value: float | None = None

    def update(self, price: float) -> float:
        """Обновить EMA новым значением и вернуть обновлённую EMA."""
        px = float(price)
        if self.value is None:
            self.value = px
        else:
            self.value = self.alpha * px + (1.0 - self.alpha) * self.value
        return self.value


class ArbitrageDirectionState:
    """Менеджер состояния одного арбитражного направления.

    Хранит:
    - окно спреда (mean/std/min/max/z-score)
    - EMA спреда (для сглаженного контроля динамики)
    """

    def __init__(self, window_size: int = 120, ema_alpha: float = 0.15) -> None:
        self.spread_window = SpreadWindow(window_size)
        self.ema = EMA(ema_alpha)
        self.last_spread: float | None = None

    def update(self, spread_value: float, period: int | None = None) -> dict[str, float | int | None]:
        """Добавить тик спреда и вернуть актуальный срез статистик."""
        self.last_spread = float(spread_value)
        self.spread_window.append(self.last_spread)
        ema_value = self.ema.update(self.last_spread)
        stats = self.spread_window.snapshot(period=period)
        stats["ema"] = ema_value
        return stats

    def tick_count(self) -> int:
        """Вернуть количество обработанных тиков направления."""
        return self.spread_window.count()


class ArbitragePair:
    """Одна пара бирж с двумя направлениями и их статистикой.

    Важно:
    - один экземпляр представляет ровно одну комбинацию бирж;
    - оба направления обновляются вместе, когда есть цены обеих бирж.
    """

    def __init__(
        self,
        exchange1: str,
        exchange2: str,
        window_size: int = 120,
        ema_alpha: float = 0.15,
    ) -> None:
        self.exchange1 = exchange1
        self.exchange2 = exchange2

        # Последние известные средние цены (ask/bid) по обеим биржам.
        self.average_price_dict: dict[str, dict[str, float | None]] = {
            exchange1: {"average_ask": None, "average_bid": None},
            exchange2: {"average_ask": None, "average_bid": None},
        }

        # Текущие "сырые" значения спреда по направлениям.
        self.direct_spread: float | None = None   # bid2 - ask1
        self.reverse_spread: float | None = None  # bid1 - ask2

        # Состояния направлений:
        # direct  -> покупка на exchange1, продажа на exchange2
        # reverse -> покупка на exchange2, продажа на exchange1
        self.direct_state = ArbitrageDirectionState(window_size=window_size, ema_alpha=ema_alpha)
        self.reverse_state = ArbitrageDirectionState(window_size=window_size, ema_alpha=ema_alpha)

    def _update_average_prices(self, input_average_prices: dict[str, dict[str, Any]]) -> None:
        """Обновить внутренний кэш входящими средними ценами стакана."""
        if self.exchange1 in input_average_prices:
            self.average_price_dict[self.exchange1]["average_ask"] = float(
                input_average_prices[self.exchange1]["average_ask"]
            )
            self.average_price_dict[self.exchange1]["average_bid"] = float(
                input_average_prices[self.exchange1]["average_bid"]
            )

        if self.exchange2 in input_average_prices:
            self.average_price_dict[self.exchange2]["average_ask"] = float(
                input_average_prices[self.exchange2]["average_ask"]
            )
            self.average_price_dict[self.exchange2]["average_bid"] = float(
                input_average_prices[self.exchange2]["average_bid"]
            )

    def _is_prices_ready(self) -> bool:
        """Проверить, что есть ask+bid по обеим биржам."""
        p1 = self.average_price_dict[self.exchange1]
        p2 = self.average_price_dict[self.exchange2]
        return (
            p1["average_ask"] is not None
            and p1["average_bid"] is not None
            and p2["average_ask"] is not None
            and p2["average_bid"] is not None
        )

    def spread_calc(
        self,
        input_average_prices: dict[str, dict[str, Any]],
        stats_period: int | None = None,
    ) -> dict[str, Any] | None:
        """Обновить пару новыми ценами и вернуть аналитику по направлениям.

        Аргументы:
            input_average_prices:
                Словарь формата:
                {
                    "<exchange_id>": {"average_ask": float, "average_bid": float},
                    ...
                }
                Разрешены частичные обновления. Метрики считаются только когда
                в кэше есть полные данные по обеим биржам.
            stats_period:
                Необязательный короткий период расчёта статистик.
                Если None, используется всё текущее окно.

        Возвращает:
            None:
                недостаточно данных для расчёта спреда.
            dict:
                словарь аналитики с блоками `direct` и `reverse`.
        """
        self._update_average_prices(input_average_prices)
        if not self._is_prices_ready():
            return None

        ask1 = float(self.average_price_dict[self.exchange1]["average_ask"])
        bid1 = float(self.average_price_dict[self.exchange1]["average_bid"])
        ask2 = float(self.average_price_dict[self.exchange2]["average_ask"])
        bid2 = float(self.average_price_dict[self.exchange2]["average_bid"])

        # Direct:  покупка ex1 по ask1, продажа ex2 по bid2.
        self.direct_spread = bid2 - ask1
        # Reverse: покупка ex2 по ask2, продажа ex1 по bid1.
        self.reverse_spread = bid1 - ask2

        direct_stats = self.direct_state.update(self.direct_spread, period=stats_period)
        reverse_stats = self.reverse_state.update(self.reverse_spread, period=stats_period)

        return {
            "pair": (self.exchange1, self.exchange2),
            "direct": direct_stats,
            "reverse": reverse_stats,
            "direct_spread": self.direct_spread,
            "reverse_spread": self.reverse_spread,
        }
