__version__ = "1.1"

import json
from pathlib import Path
from pprint import pprint
from modules.colored_console import cprint
from modules.utils import round_down, get_decimal_precision


def load_source_file ( ) -> dict:
    current_file_path = Path (__file__).resolve ( )
    project_root = current_file_path.parents[1]  # вместо [1]

    source_path = project_root / 'source'
    source_file = source_path / 'test_markets_data.json'

    try:
        with open (source_file, 'r', encoding = 'utf-8') as f:
            markets_dict = json.load (f)
            return markets_dict
    except FileNotFoundError:
        cprint.warning (f"Файл маркетов не найден: {source_file}")
    except json.JSONDecodeError:
        cprint.error (f"Ошибка при разборе JSON в файле: {source_file}")

    return {}  # Возвращаем пустой словарь при ошибке

def safe_get(d, *keys, default=None):
    """
    Безопасно извлекает вложенные значения из словаря.
    Если значение None — возвращает default.
    """
    for key in keys:
        if isinstance(d, dict) and key in d:
            d = d[key]
            if d is None:
                return default
        else:
            return default
    return d


def deal_amounts_calc_func(spot_swap_pair_data_dict: dict, deal_pair: str = '',spot_deal_usdt: float = None, swap_deal_usdt: float = None, price_spot: float = None, price_swap: float = None) -> dict:
    """
    Рассчитывает сбалансированные объёмы для открытия арбитражной позиции:
    - Покупка на споте
    - Продажа в свопе (шорт)
    Объёмы эквивалентны по базовому активу.
    При любой ошибке — выбрасывает ValueError.
    """
    if spot_swap_pair_data_dict is None:
        raise ValueError("markets не может быть None")

    spot_market = spot_swap_pair_data_dict.get(deal_pair, {}).get('spot')
    swap_market = spot_swap_pair_data_dict.get(deal_pair, {}).get('swap')

    if not spot_market:
        raise ValueError("Отсутствует 'spot' в markets")
    if not swap_market:
        raise ValueError("Отсутствует 'swap' в markets")

    # --- Извлечение обязательных параметров ---
    contract_size = swap_market.get('contractSize')
    if not contract_size or contract_size <= 0:
        raise ValueError(f"Некорректный contractSize: {contract_size}")

    spot_precision_amt = spot_market.get('precision', {}).get('amount')
    swap_precision_amt = swap_market.get('precision', {}).get('amount')

    spot_min_amt = spot_market.get('limits', {}).get('amount', {}).get('min')
    swap_min_amt = swap_market.get('limits', {}).get('amount', {}).get('min')

    # Получаем тейкер-комиссии
    spot_taker_fee = spot_market.get('taker')
    swap_taker_fee = swap_market.get('taker')
    fee_summary = spot_taker_fee + swap_taker_fee

    # Для spot_min_cost можно использовать fallback из info
    spot_min_cost = spot_market.get('limits', {}).get('cost', {}).get('min')
    if spot_min_cost is None:
        spot_min_cost = spot_market.get('info', {}).get('min-order-value')
        if spot_min_cost is not None:
            try:
                spot_min_cost = float(spot_min_cost)
            except (ValueError, TypeError):
                spot_min_cost = None
        if spot_min_cost is None:
            raise ValueError("spot.limits.cost.min не задан и не найден в info['min-order-value']")

    # swap_min_cost может быть None — это нормально
    swap_min_cost = swap_market.get('limits', {}).get('cost', {}).get('min')
    if swap_min_cost is None:
        swap_min_cost = 0.0

    # Проверяем обязательные числовые параметры
    required = [
        ('spot_precision_amt', spot_precision_amt),
        ('swap_precision_amt', swap_precision_amt),
        ('spot_min_amt', spot_min_amt),
        ('swap_min_amt', swap_min_amt),
    ]
    for name, value in required:
        if value is None:
            raise ValueError(f"Обязательное поле {name} не найдено в данных рынка")
        try:
            float(value)
        except (ValueError, TypeError):
            raise ValueError(f"Поле {name} должно быть числом, получено: {value}")

    spot_precision_amt = float(spot_precision_amt)
    swap_precision_amt = float(swap_precision_amt)
    spot_min_amt = float(spot_min_amt)
    spot_min_cost = float(spot_min_cost)
    swap_min_amt = float(swap_min_amt)
    swap_min_cost = float(swap_min_cost)

    # Количество знаков для округления количества контрактов
    swap_precision_digits = get_decimal_precision(swap_precision_amt)
    spot_precision_digits = get_decimal_precision(spot_precision_amt)

    # --- Расчёт объёмов в монетах ---
    swap_amount_coin = swap_deal_usdt / price_swap
    spot_amount_coin = spot_deal_usdt / price_spot
    min_amount_coin = min(swap_amount_coin, spot_amount_coin)

    # Количество контрактов (вниз с учётом шага)
    raw_contracts = min_amount_coin / contract_size
    swap_contract_amount = round_down(raw_contracts, swap_precision_digits)
    spot_align_amount = swap_contract_amount * contract_size

    if spot_align_amount != round_down (spot_align_amount, spot_precision_digits):
        raise ValueError (f"Проблема с точностью: spot_align_amount={spot_align_amount} != rounded {round_down (spot_align_amount, spot_precision_digits)}")

    # Стоимость сделок
    spot_cost = spot_align_amount * price_spot
    swap_cost = swap_contract_amount * contract_size * price_swap

    # --- Проверка лимитов с raise ---
    if spot_align_amount < spot_min_amt:
        raise ValueError(f"spot_amount={spot_align_amount:.6f} < min_amt={spot_min_amt}")

    if spot_cost < spot_min_cost:
        raise ValueError(f"spot_cost={spot_cost:.6f} < min_cost={spot_min_cost}")

    if swap_contract_amount < swap_min_amt:
        raise ValueError(f"swap_contracts={swap_contract_amount:.6f} < min_amt={swap_min_amt}")

    if swap_cost < swap_min_cost:
        raise ValueError(f"swap_cost={swap_cost:.6f} < min_cost={swap_min_cost}")

    # --- Успешный результат ---
    return {
        'spot_amount': spot_align_amount,
        'swap_contracts': swap_contract_amount,
        'spot_cost': spot_cost,
        'swap_cost': swap_cost,
        'contract_size': float(contract_size),
        'price_spot': price_spot,
        'price_swap': price_swap,
        'fee_summary': fee_summary,
        'debug': {
            'swap_amount_coin': round(swap_amount_coin, 6),
            'spot_amount_coin': round(spot_amount_coin, 6),
            'min_amount_coin': round(min_amount_coin, 6),
            'precision_digits': swap_precision_digits,
        }
    }

if __name__ == '__main__':
    all_markets = load_source_file()
    deal_pair = 'ZEREBRO/USDT_ZEREBRO/USDT:USDT'

    if deal_pair not in all_markets:
        print(f"Ключ {deal_pair} не найден в данных")
    else:
        try:
            res2 = deal_amounts_calc_func(
                spot_swap_pair_data_dict = all_markets,
                deal_pair=deal_pair,
                spot_deal_usdt=100,
                swap_deal_usdt=90,
                price_spot=0.02884,
                price_swap=0.0289
            )
            print("✅ calc: Успешно — объёмы сбалансированы")
            pprint(res2)
        except ValueError as e:
            print(f"❌ calc: Ошибка при расчёте — {e}")

        print("---------------------")
        # pprint(markets_data)