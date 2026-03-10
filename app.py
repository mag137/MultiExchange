import multiprocessing
import queue
import signal
import threading
import time
from decimal import Decimal
from typing import Any

from modules.WebGrid_Socket_Polling import run_web_grid_process
from modules.arbitrage_manager import (
    calculate_worker_process_count,
    run_arbitrage_worker_process,
)
from modules.utils import to_decimal


WEB_GRID_TITLE = "Open Arbitrage Ratio"
EXCHANGE_ID_LIST = ["okx", "htx", "gateio"]
MAX_DEAL_SLOTS = to_decimal("2")


def _build_grid_snapshot(rows: dict[str, dict[str, Any]]) -> dict[Any, dict[int, dict[str, Any]]]:
    sorted_rows = sorted(
        rows.values(),
        key=lambda row: (-row["open_ratio_value"], row["symbol"]),
    )

    grid_data: dict[Any, dict[int, dict[str, Any]]] = {
        "header": {
            0: {"text": "#", "align": "right"},
            1: {"text": "symbol", "align": "left"},
            2: {"text": "ask_ex", "align": "left"},
            3: {"text": "ask_mean_dt", "align": "right"},
            4: {"text": "bid_ex", "align": "left"},
            5: {"text": "bid_mean_dt", "align": "right"},
            6: {"text": "open_ratio", "align": "right"},
        }
    }

    for row_num, row in enumerate(sorted_rows, start=1):
        grid_data[row_num] = {
            0: {"text": row_num, "align": "right"},
            1: {"text": row["symbol"], "align": "left"},
            2: {"text": row["ask_exchange"], "align": "left"},
            3: {"text": row["ask_mean_dt"], "align": "right"},
            4: {"text": row["bid_exchange"], "align": "left"},
            5: {"text": row["bid_mean_dt"], "align": "right"},
            6: {"text": row["open_ratio"], "align": "right"},
        }

    return grid_data


def _grid_aggregator_loop(
    *,
    worker_grid_queue: multiprocessing.Queue,
    web_grid_queue: multiprocessing.Queue,
    shared_values: dict[str, Any],
    stop_event: threading.Event,
) -> None:
    rows: dict[str, dict[str, Any]] = {}
    web_grid_queue.put({"title": WEB_GRID_TITLE})
    web_grid_queue.put(_build_grid_snapshot(rows))

    while not stop_event.is_set():
        if shared_values["shutdown"].value:
            return

        try:
            item = worker_grid_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        if not isinstance(item, dict):
            continue

        grid_event = item.get("grid_event")
        if grid_event == "upsert_row":
            symbol = item.get("symbol")
            row = item.get("row")
            if symbol and isinstance(row, dict):
                rows[symbol] = row
                web_grid_queue.put(_build_grid_snapshot(rows))
            continue

        if grid_event == "remove_row":
            symbol = item.get("symbol")
            if symbol is not None:
                rows.pop(symbol, None)
                web_grid_queue.put(_build_grid_snapshot(rows))


def _install_signal_handlers(stop_event: threading.Event, shared_values: dict[str, Any]) -> None:
    def handle_signal(_signum, _frame) -> None:
        shared_values["shutdown"].value = True
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


def _start_web_grid_process(
    *,
    web_grid_queue: multiprocessing.Queue,
    shared_values: dict[str, Any],
) -> multiprocessing.Process:
    process = multiprocessing.Process(
        target=run_web_grid_process,
        kwargs={
            "table_queue_data": web_grid_queue,
            "shared_values": shared_values,
            "queue_datadict_wrapper_key": None,
            "host": "127.0.0.1",
            "port": 8765,
            "row_header": False,
            "title": WEB_GRID_TITLE,
            "transport": "sse",
            "max_fps": 2.0,
            "client_poll_interval_ms": 500,
        },
        daemon=True,
        name="web-grid",
    )
    process.start()
    return process


def _start_worker_processes(
    *,
    process_count: int,
    exchange_id_list: list[str],
    max_deal_slots: Decimal,
    worker_grid_queue: multiprocessing.Queue,
    shared_values: dict[str, Any],
) -> list[multiprocessing.Process]:
    processes: list[multiprocessing.Process] = []

    for process_index in range(process_count):
        process = multiprocessing.Process(
            target=run_arbitrage_worker_process,
            kwargs={
                "process_index": process_index,
                "process_count": process_count,
                "exchange_id_list": exchange_id_list,
                "max_deal_slots": max_deal_slots,
                "web_grid_queue": worker_grid_queue,
                "shared_values": shared_values,
            },
            daemon=False,
            name=f"arbitrage-worker-{process_index}",
        )
        process.start()
        processes.append(process)

    return processes


def _stop_processes(processes: list[multiprocessing.Process], timeout_sec: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_sec
    for process in processes:
        remaining = max(0.0, deadline - time.monotonic())
        process.join(timeout=remaining)

    for process in processes:
        if process.is_alive():
            process.terminate()
            process.join(timeout=3)


def main() -> None:
    multiprocessing.freeze_support()

    shared_values = {"shutdown": multiprocessing.Value('b', False)}
    web_grid_queue: multiprocessing.Queue = multiprocessing.Queue()
    worker_grid_queue: multiprocessing.Queue = multiprocessing.Queue()
    stop_event = threading.Event()

    _install_signal_handlers(stop_event, shared_values)

    web_grid_process = _start_web_grid_process(
        web_grid_queue=web_grid_queue,
        shared_values=shared_values,
    )

    aggregator_thread = threading.Thread(
        target=_grid_aggregator_loop,
        kwargs={
            "worker_grid_queue": worker_grid_queue,
            "web_grid_queue": web_grid_queue,
            "shared_values": shared_values,
            "stop_event": stop_event,
        },
        daemon=True,
        name="web-grid-aggregator",
    )
    aggregator_thread.start()

    process_count = calculate_worker_process_count()
    worker_processes = _start_worker_processes(
        process_count=process_count,
        exchange_id_list=EXCHANGE_ID_LIST,
        max_deal_slots=MAX_DEAL_SLOTS,
        worker_grid_queue=worker_grid_queue,
        shared_values=shared_values,
    )

    print(f"Started {len(worker_processes)} arbitrage worker process(es)")

    try:
        while not stop_event.is_set():
            alive_processes = [process for process in worker_processes if process.is_alive()]
            if not alive_processes:
                break
            time.sleep(0.5)
    finally:
        shared_values["shutdown"].value = True
        stop_event.set()
        _stop_processes(worker_processes)
        aggregator_thread.join(timeout=3)
        web_grid_process.join(timeout=5)
        if web_grid_process.is_alive():
            web_grid_process.terminate()
            web_grid_process.join(timeout=3)


if __name__ == "__main__":
    main()
