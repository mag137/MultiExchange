__version__ = "2.1"

import os
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from modules.ORJSON_file_manager import JsonFileManager
import orjson
from pprint import pprint


class DealRecorder:
    """
    Менеджер для атомарной записи ордерных дампов и структурированных данных сделок.

    Функции класса:
        - Создание структурированной директории /orderdump/YYYY-MM-DD/.
        - Атомарная запись исходных данных ордеров и GT-ордера в JSON (через orjson).
        - Добавление обработанных данных сделки в deals_log/active_deals.json.
        - Поддержка сигнального словаря (исходные данные сигнала).
        - Унифицированный сериализатор Decimal, datetime, timezone.

    Применение:
        Используется в подсистеме арбитражных сделок для логирования открытия/закрытия,
        сохранения сырого дампа и связанных параметров.
    """

    def __init__(self) -> None:
        """
        Инициализирует директории, пути и служебные структуры.
        """
        self.project_root: str = self._find_project_root()
        self.signal_deal_dict: Dict[str, Any] = {}

        self.dump_dir: str = os.path.join(self.project_root, "orderdump")
        self.active_dir: str = os.path.join(self.project_root, "deals_log")

        os.makedirs(self.dump_dir, exist_ok=True)
        os.makedirs(self.active_dir, exist_ok=True)

        self.active_path: str = os.path.join(self.active_dir, "active_deals.json")
        self.active_manager: Optional[JsonFileManager] = None

    # ----------------------------------------------------------------------
    # Работа с сигнальными данными
    # ----------------------------------------------------------------------

    def set_signal_deal_dict(self, signal_data: Dict[str, Any]) -> None:
        """
        Устанавливает входной словарь, описывающий сигнал сделки.

        Parameters
        ----------
        signal_data : dict
            Словарь с параметрами сигнала (arb_pair, объёмы, цены, комиссии).

        Raises
        ------
        TypeError
            При передаче структуры, отличной от dict.
        """
        if not isinstance(signal_data, dict):
            raise TypeError("signal_data must be a dict")
        self.signal_deal_dict = signal_data

    # ----------------------------------------------------------------------
    # Основные методы записи дампов
    # ----------------------------------------------------------------------

    def record_deal_dump(self, deal_data: Dict[str, Any]) -> str:
        """
        Атомарно сохраняет объединённые данные сделки (открытие/закрытие).

        Parameters
        ----------
        deal_data : dict
            Структура, содержащая все параметры завершённой сделки.

        Returns
        -------
        str
            Путь к созданному JSON-файлу.
        """
        def default(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            if isinstance(obj, (datetime, timezone)):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")

        timestamp = datetime.now(timezone.utc)
        date_dir = os.path.join(self.dump_dir, timestamp.strftime("%Y-%m-%d"))
        os.makedirs(date_dir, exist_ok=True)

        symbol = deal_data.get("swap_symbol", "UNKNOWN")
        base_asset = symbol.split("/")[0] if "/" in symbol else symbol

        filename = f"{timestamp.strftime('%H_%M_%S')}_{base_asset}_deal_dump.json"
        dump_path = os.path.join(date_dir, filename)
        temp_path = dump_path + ".tmp"

        data_bytes = orjson.dumps(deal_data, option=orjson.OPT_INDENT_2, default=default)

        with open(temp_path, "wb") as f:
            f.write(data_bytes)
        os.replace(temp_path, dump_path)

        print(f"\n💾 Данные сделки сохранёны атомарно в {dump_path}")
        return dump_path

    def record_orders_dump(self, deal_data: Dict[str, Any], insertion_descriptor: str = "UNDEFINE") -> str:
        """
        Атомарно сохраняет «сырой» дамп ордеров в уникальный JSON.

        Parameters
        ----------
        deal_data : dict
            Данные ордеров, содержащие spot/swap структуру.
        insertion_descriptor : str
            Суффикс для имени файла (open_deal, close_deal, GT_commission и т.п.).

        Returns
        -------
        str
            Путь к JSON-дампу.
        """
        def default(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            if isinstance(obj, (datetime, timezone)):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")

        timestamp = datetime.now(timezone.utc)
        date_dir = os.path.join(self.dump_dir, timestamp.strftime("%Y-%m-%d"))
        os.makedirs(date_dir, exist_ok=True)

        coin = deal_data.get("coin", "UNKNOWN")
        # pprint(deal_data)

        filename = f"{timestamp.strftime('%H_%M_%S')}_{coin}_{insertion_descriptor}.json"

        dump_path = os.path.join(date_dir, filename)
        temp_path = dump_path + ".tmp"

        data_bytes = orjson.dumps(deal_data, option=orjson.OPT_INDENT_2, default=default)

        with open(temp_path, "wb") as f:
            f.write(data_bytes)
        os.replace(temp_path, dump_path)

        print(f"\n💾 Сырой дамп сохранён атомарно в {dump_path}")
        return dump_path

    def record_gt_order_dump(self, order_data: Dict[str, Any]) -> str:
        """
        Атомарно сохраняет сырой дамп GT-ордера (обычно используется для покупки монеты под комиссию).

        Parameters
        ----------
        order_data : dict
            Информация об исполненном GT-ордере.

        Returns
        -------
        str
            Путь к JSON-дампу.
        """
        def default(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            if isinstance(obj, (datetime, timezone)):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")

        timestamp = datetime.now(timezone.utc)
        date_dir = os.path.join(self.dump_dir, timestamp.strftime("%Y-%m-%d"))
        os.makedirs(date_dir, exist_ok=True)

        filename = f"{timestamp.strftime('%H_%M_%S')}_GT_buy_for_commission.json"
        dump_path = os.path.join(date_dir, filename)
        temp_path = dump_path + ".tmp"

        data_bytes = orjson.dumps(order_data, option=orjson.OPT_INDENT_2, default=default)

        with open(temp_path, "wb") as f:
            f.write(data_bytes)
        os.replace(temp_path, dump_path)

        print(f"\n💾 Сырой дамп GT сохранён атомарно в {dump_path}")
        return dump_path

    # ----------------------------------------------------------------------
    # Работа с активными сделками
    # ----------------------------------------------------------------------

    def record_active_deal_dict(self, active_deal_data_dict: Dict[str, Any]) -> None:
        """
        Добавляет данные сделки в активный журнал deals_log/active_deals.json.

        Parameters
        ----------
        active_deal_data_dict : dict
            Словарь с параметрами сделки, включая ключ 'arb_pair'.

        Raises
        ------
        KeyError
            Если отсутствует ключ 'arb_pair'.
        Exception
            Любая ошибка записи в JsonFileManager.
        """
        try:
            key = active_deal_data_dict["arb_pair"]
            self.active_manager = JsonFileManager(self.active_path)
            self.active_manager.add(key, active_deal_data_dict)
        except Exception as e:
            pprint(active_deal_data_dict)
            print(f"[DealRecorder][record_active_deal_dict] Ошибка {e}")
            raise

    # ----------------------------------------------------------------------
    # Вспомогательная математика комиссий
    # ----------------------------------------------------------------------

    @staticmethod
    def _compute_spot_fee_usdt(order: Dict[str, Any]) -> Decimal:
        """
        Расчёт комиссии спота в USDT.

        Логика:
            - Если в fees есть USDT — возвращается напрямую.
            - Если комиссия в базовой монете, пересчитывается через average.

        Parameters
        ----------
        order : dict
            Структура исполненного спот-ордера.

        Returns
        -------
        Decimal
            Комиссия в USDT.
        """
        fees = order.get("fees") or []
        avg_price = Decimal(str(order.get("average", "0")))

        for f in fees:
            cur = f.get("currency")
            cost = Decimal(str(f.get("cost", "0")))

            if cur == "USDT":
                return cost
            if cur and cur != "USDT" and cost > 0 and avg_price > 0:
                return cost * avg_price

        return Decimal("0.0")

    @staticmethod
    def _compute_swap_fee_usdt(order: Dict[str, Any]) -> Decimal:
        """
        Расчёт своп-комиссии в USDT.

        Логика:
            - Если комиссия указана в USDT — используется напрямую.
            - В остальных случаях используется формула:
                filled * fill_price * tkfr

        Parameters
        ----------
        order : dict
            Структура исполненного swap-ордера.

        Returns
        -------
        Decimal
            Комиссия в USDT.
        """
        fees = order.get("fees") or []

        for f in fees:
            if f.get("currency") == "USDT":
                return Decimal(str(f.get("cost", "0")))

        try:
            tkfr = Decimal(str(order["info"].get("tkfr", "0")))
            filled = Decimal(str(order.get("filled", "0")))
            fill_price = Decimal(str(order["info"].get("fill_price", order.get("average", "0"))))
            return filled * fill_price * tkfr
        except (KeyError, InvalidOperation):
            return Decimal("0.0")

    # ----------------------------------------------------------------------
    # Служебное
    # ----------------------------------------------------------------------

    @staticmethod
    def _find_project_root() -> str:
        """
        Возвращает путь к корню проекта (на уровень выше текущего файла).

        Returns
        -------
        str
            Абсолютный путь.
        """
        return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


# ----------------------------------------------------------------------
# Тестовый запуск
# ----------------------------------------------------------------------
if __name__ == "__main__":
    print("Тестовый запуск DealRecorder.")
    signal_data = {
        'arb_pair': 'SOMI/USDT_SOMI/USDT:USDT',
        'signal_open_ratio': 0.0288,
        'signal_spot_amount': 0.1,
        'signal_spot_fee': 0.0001,
        'signal_spot_price': 69500.0,
        'signal_swap_contracts': 100,
        'signal_swap_fee': 0.5,
        'signal_swap_price': 69480.0,
        'signal_time': 1730289600.5,
        'spot_opening_balance': 10000.0,
        'spot_symbol': 'SOMI/USDT',
        'swap_contract_size': 0.001,
        'swap_opening_balance': 5.0,
        'swap_symbol': 'SOMI/USDT:USDT'
    }

    deal_data= {
    'arb_pair': 'SOMI/USDT_SOMI/USDT:USDT',
     'available_for_sell_spot_balance': 11.0699,
     'close_dump_path': '/home/mag137/PycharmProjects/Project4/orderdump/2025-11-11/13_29_27_SOMI_close_deal.json',
     'deal_close_ratio': Decimal('0.07955449482895783611774065235'),
     'deal_close_spot_amount': 11.0,
     'deal_close_spot_avg': 0.3774,
     'deal_close_spot_complete_timestamp': 1762867766.759,
     'deal_close_spot_cost': 4.1514,
     'deal_close_spot_duration': 0.1857891082763672,
     'deal_close_spot_fee_percent': '0.0900',
     'deal_close_spot_fee_usdt': '0.00373626',
     'deal_close_spot_id': '958120771320',
     'deal_close_swap_avg': 0.3771,
     'deal_close_swap_complete_timestamp': 1762867767.104,
     'deal_close_swap_contracts': 11.0,
     'deal_close_swap_cost': 4.1481,
     'deal_close_swap_duration': 0.5307891368865967,
     'deal_close_swap_fee_percent': '0.0500',
     'deal_close_swap_fee_usdt': '0.00207405',
     'deal_close_swap_id': '265430903377280813',
     'deal_open_ratio': Decimal('-0.1852832186341979883536262573'),
     'deal_open_spot_amount': 11.0,
     'deal_open_spot_avg': 0.3778,
     'deal_open_spot_complete_timestamp': 1762867740.274,
     'deal_open_spot_cost': 4.1558,
     'deal_open_spot_duration': 0.16222763061523438,
     'deal_open_spot_fee_percent': Decimal('0.0900'),
     'deal_open_spot_fee_usdt': Decimal('0.00374022'),
     'deal_open_spot_id': Decimal('958120562653'),
     'deal_open_swap_avg': 0.3771,
     'deal_open_swap_complete_timestamp': 1762867740.652,
     'deal_open_swap_contracts': 11.0,
     'deal_open_swap_cost': 4.1481,
     'deal_open_swap_duration': 0.5402276515960693,
     'deal_open_swap_fee_percent': Decimal('0.0500'),
     'deal_open_swap_fee_usdt': Decimal('0.00207405'),
     'deal_open_swap_id': Decimal('265430903377280664'),
     'open_dump_path': '/home/mag137/PycharmProjects/Project4/orderdump/2025-11-11/13_29_02_SOMI_open_deal.json',
     'signal_average_spot_ask': 0.378,
     'signal_average_spot_bid': None,
     'signal_average_swap_ask': None,
     'signal_average_swap_bid': None,
     'signal_close_ratio': 1.1,
     'signal_close_timestamp': 1762867766.573211,
     'signal_delta_ratios': 2,
     'signal_max_close_ratio': 1,
     'signal_max_open_ratio': 1,
     'signal_min_close_ratio': -1,
     'signal_min_open_ratio': -1,
     'signal_open_ratio': 1.1,
     'signal_open_timestamp': 1762867740.1117723,
     'signal_spot_amount': 11,
     'signal_swap_contracts': 11,
     'spot_symbol': 'SOMI/USDT',
     'swap_symbol': 'SOMI/USDT:USDT'
     }

    # -------------------------------------
    # 2. Инициализация DealRecorder
    # -------------------------------------
    recorder = DealRecorder()
    recorder.set_signal_deal_dict(signal_data)

    # -------------------------------------
    # 3. Создание дампа сделок (реальный)
    # -------------------------------------
    print("\nСоздание дампа orders...")
    dump_path = recorder.record_orders_dump(deal_data, "test_dump")
    print(f"→ dump_path: {dump_path}")

    # -------------------------------------
    # 4. Запись в active_deals.json
    # -------------------------------------
    print("\nОбновление active_deals.json...")
    recorder.record_active_deal_dict(deal_data)

    # -------------------------------------
    # 5. Вывод содержимого active_deals
    # -------------------------------------
    print("\nСодержимое active_deals.json:")
    if recorder.active_manager:
        recorder.active_manager.pretty_print()
    else:
        print("active_manager is None — файл не был создан.")
