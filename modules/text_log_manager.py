__version__ = "1.1"
__all__ = ["log_deal_to_file"]

import os
from datetime import datetime
from typing import Any, Dict, Optional


class TextLogManager:
    """Управляет созданием и сохранением текстового лога с нумерацией строк и временной меткой."""

    def __init__(self, base_dir: str = "logs") -> None:
        """
        Инициализирует менеджер логов.

        Args:
            base_dir (str): Базовая директория для хранения логов. По умолчанию — 'logs'.
        """
        self.base_dir: str = base_dir
        self.lines: list[str] = []
        self.date_dir: str = os.path.join(self.base_dir, datetime.now().strftime("%Y-%m-%d"))
        os.makedirs(self.date_dir, exist_ok=True)
        self.filename: str = f"{datetime.now().strftime('%H-%M-%S')}.txt"
        self.full_path: str = os.path.join(self.date_dir, self.filename)
        self.row_count: int = 1

    def add_line(self, text: str) -> None:
        """
        Добавляет строку в лог с нумерацией и отступом.

        Args:
            text (str): Текст строки для добавления.
        """
        spacing = ". " if len(str(self.row_count)) < 2 else "."
        self.lines.append(f"{self.row_count}{spacing} {text}")
        self.row_count += 1

    def save(self) -> str:
        """
        Сохраняет собранные строки в файл и возвращает абсолютный путь к нему.

        Returns:
            str: Абсолютный путь к сохранённому файлу.
        """
        with open(self.full_path, "w", encoding="utf-8") as f:
            f.write("\n".join(self.lines) + "\n")
        return os.path.abspath(self.full_path)


def log_deal_to_file(deal_dict: Dict[str, Any], base_dir: str = "deals_history") -> str:
    """
    Записывает результаты арбитражной сделки в текстовый файл с подробной аналитикой.

    Args:
        deal_dict (Dict[str, Any]): Словарь с данными о сделке. Ожидает наличие всех ключей,
            необходимых для расчёта PnL, комиссий, объёмов и временных меток.
        base_dir (str): Базовая директория для сохранения лога. По умолчанию — 'deals_history'.

    Returns:
        str: Абсолютный путь к созданному файлу лога.

    Raises:
        KeyError: Если в `deal_dict` отсутствует обязательный ключ.
        ValueError: Если значение не может быть приведено к нужному типу (например, float).
    """
    logger = TextLogManager(base_dir=base_dir)
    print(f"[INFO] Лог будет записан в: {os.path.abspath(logger.full_path)}")

    # --- Балансы до и после сделки ---
    try:
        spot_before = float(deal_dict["spot_balance_before"])
        spot_after = float(deal_dict["spot_balance_after"])
        swap_before = float(deal_dict["swap_balance_before"])
        swap_after = float(deal_dict["swap_balance_after"])
    except (KeyError, ValueError, TypeError) as e:
        raise ValueError(f"Ошибка при извлечении балансов: {e}")

    total_before = spot_before + swap_before
    total_after = spot_after + swap_after
    total_delta = total_after - total_before
    total_delta_pct = (total_delta / total_before * 100) if total_before != 0 else 0.0

    logger.add_line(f"Арбитражная пара: {deal_dict['arb_pair']}")
    logger.add_line(
        f"Суммарный баланс: {total_before:.3f} → {total_after:.3f} USDT "
        f"(Δ: {total_delta:+.3f} | {total_delta_pct:+.3f}%)"
    )
    logger.add_line(
        f"Баланс спота: {spot_before:.3f} → {spot_after:.3f} USDT "
        f"(Δ: {spot_after - spot_before:+.3f})"
    )
    logger.add_line(
        f"Баланс свопа: {swap_before:.3f} → {swap_after:.3f} USDT "
        f"(Δ: {swap_after - swap_before:+.3f})"
    )

    # --- Временные метки сигналов ---
    open_ts = deal_dict["signal_open_timestamp"]
    close_ts = deal_dict["signal_close_timestamp"]
    logger.add_line(f"Сигнал открытия: {datetime.fromtimestamp(open_ts).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    logger.add_line(f"Сигнал закрытия: {datetime.fromtimestamp(close_ts).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")

    # --- Объёмы и сигналы ---
    logger.add_line(
        f"Сигнальные объёмы — спот: {deal_dict['signal_spot_amount']}, "
        f"своп: {deal_dict['signal_swap_contracts']}"
    )
    logger.add_line(f"Сигнальное ratio открытия: {deal_dict['signal_open_ratio']:.6f}")
    logger.add_line(f"Сигнальное ratio закрытия: {deal_dict['signal_close_ratio']:.6f}")

    # --- Фактические объёмы и цены ---
    logger.add_line(
        f"Фактический объём спота (открытие/закрытие): "
        f"{deal_dict['deal_open_spot_amount']} / {deal_dict['deal_close_spot_amount']}"
    )
    logger.add_line(
        f"Фактический объём свопа (открытие/закрытие): "
        f"{deal_dict['deal_open_swap_contracts']} / {deal_dict['deal_close_swap_contracts']}"
    )
    logger.add_line(
        f"Цена спота (открытие/закрытие): "
        f"{deal_dict['deal_open_spot_avg']:.6f} / {deal_dict['deal_close_spot_avg']:.6f}"
    )
    logger.add_line(
        f"Цена свопа (открытие/закрытие): "
        f"{deal_dict['deal_open_swap_avg']:.6f} / {deal_dict['deal_close_swap_avg']:.6f}"
    )

    # --- Комиссии ---
    try:
        open_spot_fee_usdt = float(deal_dict["deal_open_spot_fee_usdt"])
        open_swap_fee_usdt = float(deal_dict["deal_open_swap_fee_usdt"])
        close_spot_fee_usdt = float(deal_dict["deal_close_spot_fee_usdt"])
        close_swap_fee_usdt = float(deal_dict["deal_close_swap_fee_usdt"])

        open_spot_fee_pct = float(deal_dict["deal_open_spot_fee_percent"])
        open_swap_fee_pct = float(deal_dict["deal_open_swap_fee_percent"])
        close_spot_fee_pct = float(deal_dict["deal_close_spot_fee_percent"])
        close_swap_fee_pct = float(deal_dict["deal_close_swap_fee_percent"])
    except (KeyError, ValueError, TypeError) as e:
        raise ValueError(f"Ошибка при обработке комиссий: {e}")

    total_fee = open_spot_fee_usdt + open_swap_fee_usdt + close_spot_fee_usdt + close_swap_fee_usdt
    logger.add_line(
        f"Комиссии открытия — спот: {open_spot_fee_usdt:.6f} USDT ({open_spot_fee_pct:.3f}%) | "
        f"своп: {open_swap_fee_usdt:.6f} USDT ({open_swap_fee_pct:.3f}%)"
    )
    logger.add_line(
        f"Комиссии закрытия — спот: {close_spot_fee_usdt:.6f} USDT ({close_spot_fee_pct:.3f}%) | "
        f"своп: {close_swap_fee_usdt:.6f} USDT ({close_swap_fee_pct:.3f}%)"
    )
    logger.add_line(f"Итого комиссий: {total_fee:.6f} USDT")

    # --- PnL и ROI ---
    try:
        pnl_gross = (
            (-float(deal_dict["deal_open_spot_cost"]) + float(deal_dict["deal_close_spot_cost"])) +
            (float(deal_dict["deal_open_swap_cost"]) - float(deal_dict["deal_close_swap_cost"]))
        )
    except (KeyError, ValueError, TypeError) as e:
        raise ValueError(f"Ошибка при расчёте PnL: {e}")

    pnl_net = pnl_gross - total_fee
    logger.add_line(f"Брутто PnL: {pnl_gross:.6f} USDT")
    logger.add_line(f"Нетто PnL (после комиссий): {pnl_net:.6f} USDT")

    capital = (float(deal_dict["deal_open_spot_cost"]) + float(deal_dict["deal_open_swap_cost"])) / 2
    roi = (pnl_net / capital) * 100 if capital > 0 else 0.0
    logger.add_line(f"ROI (нетто): {roi:.4f}%")

    # --- Отклонение цены от сигнала (если есть данные) ---
    avg_spot_ask = deal_dict.get("signal_average_spot_ask")
    open_spot_avg = deal_dict.get("deal_open_spot_avg")
    if avg_spot_ask is not None and open_spot_avg is not None:
        try:
            dev = (float(open_spot_avg) - float(avg_spot_ask)) / float(avg_spot_ask) * 100
            logger.add_line(f"Отклонение цены спота от сигнала: {dev:+.4f}%")
        except (ValueError, TypeError):
            pass  # Игнорируем, если не удалось преобразовать

    # --- Время исполнения ---
    logger.add_line(
        f"Время исполнения открытия — спот: {deal_dict['deal_open_spot_duration']:.3f} с, "
        f"своп: {deal_dict['deal_open_swap_duration']:.3f} с"
    )
    logger.add_line(
        f"Время исполнения закрытия — спот: {deal_dict['deal_close_spot_duration']:.3f} с, "
        f"своп: {deal_dict['deal_close_swap_duration']:.3f} с"
    )

    # --- Фактические ratios ---
    logger.add_line(f"Фактическое ratio открытия: {deal_dict['deal_open_ratio']:.6f}")
    logger.add_line(f"Фактическое ratio закрытия: {deal_dict['deal_close_ratio']:.6f}")

    # --- Пути к дампам ---
    logger.add_line(f"Дамп открытия: {deal_dict['open_dump_path']}")
    logger.add_line(f"Дамп закрытия: {deal_dict['close_dump_path']}")
    logger.add_line("-" * 60)

    return logger.save()


if __name__ == "__main__":
    # Пример использования
    deal_data = {
        "spot_balance_before": 123,
        "spot_balance_after": 121,
        "swap_balance_before": 122,
        "swap_balance_after": 121,
        "signal_close_timestamp": 1762868813.1691086,
        "signal_open_timestamp": 1762868757.8944385,
        "arb_pair": "SOMI/USDT_SOMI/USDT:USDT",
        "spot_symbol": "SOMI/USDT",
        "swap_symbol": "SOMI/USDT:USDT",
        "signal_spot_amount": 11,
        "signal_swap_contracts": 11,
        "signal_average_spot_ask": 0.3778,
        "signal_average_spot_bid": None,
        "signal_average_swap_ask": None,
        "signal_average_swap_bid": None,
        "signal_open_ratio": 1.1,
        "signal_close_ratio": 1.1,
        "signal_max_open_ratio": 1,
        "signal_max_close_ratio": 1,
        "signal_min_open_ratio": -1,
        "signal_min_close_ratio": -1,
        "signal_delta_ratios": 2,
        "deal_open_spot_id": 958128976030.0,
        "deal_open_swap_id": 2.6543090337728944e17,
        "deal_open_spot_cost": 4.1536,
        "deal_open_swap_cost": 4.1481,
        "deal_open_spot_avg": 0.3776,
        "deal_open_swap_avg": 0.3771,
        "deal_open_spot_amount": 11.0,
        "deal_open_swap_contracts": 11.0,
        "deal_open_ratio": -0.13241525423728814,
        "deal_open_spot_fee_usdt": 0.00373824,
        "deal_open_swap_fee_usdt": 0.00207405,
        "deal_open_spot_fee_percent": 0.09,
        "deal_open_swap_fee_percent": 0.05,
        "available_for_sell_spot_balance": 11.0699,
        "open_dump_path": "/home/mag137/PycharmProjects/Project4/orderdump/2025-11-11/13_45_59_SOMI_open_deal.json",
        "deal_open_spot_complete_timestamp": 1762868758.029,
        "deal_open_swap_complete_timestamp": 1762868758.406,
        "deal_open_spot_duration": 0.13456153869628906,
        "deal_open_swap_duration": 0.511561393737793,
        "deal_close_spot_id": "958129467497",
        "deal_close_swap_id": "265430903377289958",
        "deal_close_spot_cost": 4.1481,
        "deal_close_swap_cost": 4.1459,
        "deal_close_spot_avg": 0.3771,
        "deal_close_swap_avg": 0.3769,
        "deal_close_spot_amount": 11.0,
        "deal_close_swap_contracts": 11.0,
        "deal_close_ratio": 0.05306447333510215,
        "deal_close_spot_fee_usdt": "0.00373329",
        "deal_close_swap_fee_usdt": "0.00207295",
        "deal_close_spot_fee_percent": "0.0900",
        "deal_close_swap_fee_percent": "0.0500",
        "deal_close_spot_complete_timestamp": 1762868813.874,
        "deal_close_swap_complete_timestamp": 1762868813.665,
        "deal_close_spot_duration": 0.7048914432525635,
        "deal_close_swap_duration": 0.4958913326263428,
        "close_dump_path": "/home/mag137/PycharmProjects/Project4/orderdump/2025-11-11/13_46_54_SOMI_close_deal.json"
    }

    path = log_deal_to_file(deal_data, base_dir="deals_history")
    print(f"✅ Лог сохранён: {path}")