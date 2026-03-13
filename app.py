_version_ = "0.2i"

"""• Сделано. Добавил контроль фактического старта воркеров и отдельную служебную веб‑страницу статусов.

  Ключевое поведение

  1. Каждый воркер шлёт события worker_started, worker_ready, worker_inactive, worker_heartbeat, worker_error.
  2. Главный процесс ждёт подтверждения старта всех воркеров (таймаут WORKER_START_TIMEOUT_SEC = 30), пишет предупреждение, если кто-то не подтвердился.
  3. В статусах фиксируется завершение воркера (state=dead) и exitcode.

  Служебная страница

  - URL: http://127.0.0.1:8765/status
  - Что показывается:
      - Uptime, Workers Ready, Shutdown Flag, Last Update
      - Таблица воркеров: Worker, PID, State, Heartbeat, Exchanges OK/Failed, Symbols
      - Лента сообщений: старт/готовность/неактивность, ошибки, завершение воркеров

  Где изменено

  - app.py
      - Добавлены очереди status_queue, control_queue
      - Запуск мониторинга статусов и ожидание готовности воркеров
      - Обработка внезапного завершения воркеров
  - modules/arbitrage_manager.py
      - Сигналы старта/готовности/heartbeat
      - Передача количества доступных бирж и символов
  - modules/WebGrid_Socket_Polling.py
      - Новый HTML /status
      - Новые API /api/status/state, /api/status/stream
      - Обработка очереди статус‑сообщений"""


import multiprocessing
import queue
import signal
import threading
import time
from decimal import Decimal
from typing import Any
import asyncio
import sys

from modules.WebGrid_Socket_Polling import run_web_grid_process
from modules.arbitrage_manager import (
    calculate_worker_process_count,
    run_arbitrage_worker_process,
)
from modules.utils import to_decimal


WEB_GRID_TITLE = "Open Arbitrage Ratio"
EXCHANGE_ID_LIST = ["okx", "htx", "gateio"]
MAX_DEAL_SLOTS = to_decimal("2")
WORKER_START_TIMEOUT_SEC = 30.0


def _publish_status_message(
    status_queue: multiprocessing.Queue,
    *,
    level: str,
    text: str,
    source: str = "app",
) -> None:
    try:
        status_queue.put(
            {
                "status_event": "message",
                "level": level,
                "text": text,
                "source": source,
                "ts": time.time(),
            }
        )
    except Exception:
        return


def _status_monitor_loop(
    *,
    control_queue: multiprocessing.Queue,
    status_queue: multiprocessing.Queue,
    shared_values: dict[str, Any],
    stop_event: threading.Event,
    expected_workers: int,
    ready_event: threading.Event,
) -> None:
    worker_states: dict[int, dict[str, Any]] = {
        idx: {"state": "starting"} for idx in range(expected_workers)
    }
    started_workers: set[int] = set()
    ready_workers: set[int] = set()

    try:
        status_queue.put(
            {
                "status_event": "app_state",
                "expected_workers": expected_workers,
                "started_workers": 0,
                "ready_workers": 0,
                "shutdown": False,
                "started_ts": time.time(),
                "ts": time.time(),
            }
        )
        for idx in range(expected_workers):
            status_queue.put(
                {
                    "status_event": "worker_update",
                    "worker_id": idx,
                    "state": "starting",
                    "ts": time.time(),
                }
            )
    except Exception:
        return

    while not stop_event.is_set():
        if shared_values["shutdown"].value:
            try:
                status_queue.put(
                    {
                        "status_event": "summary",
                        "shutdown": True,
                        "started_workers": len(started_workers),
                        "ready_workers": len(ready_workers),
                        "ts": time.time(),
                    }
                )
            except Exception:
                pass
            return

        try:
            event = control_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        if not isinstance(event, dict):
            continue

        event_type = event.get("event")
        worker_id = event.get("worker_id")

        if event_type == "worker_started" and isinstance(worker_id, int):
            started_workers.add(worker_id)
            worker_states.setdefault(worker_id, {})["state"] = "started"
            status_queue.put(
                {
                    "status_event": "worker_update",
                    "worker_id": worker_id,
                    "state": "started",
                    "pid": event.get("pid"),
                    "ts": time.time(),
                }
            )
            _publish_status_message(
                status_queue,
                level="info",
                text=f"Воркер {worker_id} стартовал (pid={event.get('pid')}).",
                source="workers",
            )

        elif event_type == "worker_ready" and isinstance(worker_id, int):
            ready_workers.add(worker_id)
            worker_states.setdefault(worker_id, {})["state"] = "ready"
            status_queue.put(
                {
                    "status_event": "worker_update",
                    "worker_id": worker_id,
                    "state": "ready",
                    "pid": event.get("pid"),
                    "exchanges_ok": event.get("exchanges_ok"),
                    "exchanges_failed": event.get("exchanges_failed"),
                    "symbols_assigned": event.get("symbols_assigned"),
                    "symbols_active": event.get("symbols_active"),
                    "ts": time.time(),
                }
            )
            _publish_status_message(
                status_queue,
                level="info",
                text=f"Воркер {worker_id} готов: symbols={event.get('symbols_assigned')}.",
                source="workers",
            )

        elif event_type == "worker_inactive" and isinstance(worker_id, int):
            worker_states.setdefault(worker_id, {})["state"] = "inactive"
            status_queue.put(
                {
                    "status_event": "worker_update",
                    "worker_id": worker_id,
                    "state": "inactive",
                    "pid": event.get("pid"),
                    "exchanges_ok": event.get("exchanges_ok"),
                    "exchanges_failed": event.get("exchanges_failed"),
                    "symbols_assigned": event.get("symbols_assigned"),
                    "symbols_active": event.get("symbols_active"),
                    "ts": time.time(),
                }
            )
            _publish_status_message(
                status_queue,
                level="warning",
                text=f"Воркер {worker_id} не активен: {event.get('reason')}.",
                source="workers",
            )

        elif event_type == "worker_heartbeat" and isinstance(worker_id, int):
            status_queue.put(
                {
                    "status_event": "worker_heartbeat",
                    "worker_id": worker_id,
                    "pid": event.get("pid"),
                    "symbols_active": event.get("symbols_active"),
                    "ts": event.get("ts") or time.time(),
                }
            )

        elif event_type == "worker_error" and isinstance(worker_id, int):
            _publish_status_message(
                status_queue,
                level="error",
                text=f"Воркер {worker_id}: {event.get('text')}",
                source="workers",
            )

        if len(ready_workers) >= expected_workers and not ready_event.is_set():
            ready_event.set()

        try:
            status_queue.put(
                {
                    "status_event": "summary",
                    "started_workers": len(started_workers),
                    "ready_workers": len(ready_workers),
                    "expected_workers": expected_workers,
                    "shutdown": False,
                    "ts": time.time(),
                }
            )
        except Exception:
            pass


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
    status_queue: multiprocessing.Queue,
    shared_values: dict[str, Any],
) -> multiprocessing.Process:
    process = multiprocessing.Process(
        target=run_web_grid_process,
        kwargs={
            "table_queue_data": web_grid_queue,
            "status_queue_data": status_queue,
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
    control_queue: multiprocessing.Queue,
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
                "control_queue": control_queue,
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
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    shared_values = {"shutdown": multiprocessing.Value('b', False)}
    web_grid_queue: multiprocessing.Queue = multiprocessing.Queue()
    worker_grid_queue: multiprocessing.Queue = multiprocessing.Queue()
    status_queue: multiprocessing.Queue = multiprocessing.Queue()
    control_queue: multiprocessing.Queue = multiprocessing.Queue()
    stop_event = threading.Event()
    ready_event = threading.Event()

    _install_signal_handlers(stop_event, shared_values)

    web_grid_process = _start_web_grid_process(
        web_grid_queue=web_grid_queue,
        status_queue=status_queue,
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

    process_count = min(calculate_worker_process_count(), 4)
    status_thread = threading.Thread(
        target=_status_monitor_loop,
        kwargs={
            "control_queue": control_queue,
            "status_queue": status_queue,
            "shared_values": shared_values,
            "stop_event": stop_event,
            "expected_workers": process_count,
            "ready_event": ready_event,
        },
        daemon=True,
        name="status-monitor",
    )
    status_thread.start()
    status_queue.put(
        {
            "status_event": "summary",
            "expected_workers": process_count,
            "started_workers": 0,
            "ready_workers": 0,
            "shutdown": False,
            "ts": time.time(),
        }
    )

    if status_thread.is_alive():
        try:
            status_queue.put(
                {
                    "status_event": "app_state",
                    "expected_workers": process_count,
                    "started_workers": 0,
                    "ready_workers": 0,
                    "shutdown": False,
                    "ts": time.time(),
                }
            )
        except Exception:
            pass

    worker_processes = _start_worker_processes(
        process_count=process_count,
        exchange_id_list=EXCHANGE_ID_LIST,
        max_deal_slots=MAX_DEAL_SLOTS,
        worker_grid_queue=worker_grid_queue,
        control_queue=control_queue,
        shared_values=shared_values,
    )

    print(f"Started {len(worker_processes)} arbitrage worker process(es)")
    _publish_status_message(
        status_queue,
        level="info",
        text=f"Запуск воркеров: {len(worker_processes)}.",
        source="app",
    )

    if not ready_event.wait(timeout=WORKER_START_TIMEOUT_SEC):
        _publish_status_message(
            status_queue,
            level="warning",
            text=f"Не все воркеры подтвердили запуск за {WORKER_START_TIMEOUT_SEC:.0f} сек.",
            source="app",
        )
    else:
        _publish_status_message(
            status_queue,
            level="info",
            text="Все воркеры подтвердили запуск.",
            source="app",
        )

    try:
        worker_alive = {idx: True for idx in range(len(worker_processes))}
        while not stop_event.is_set():
            alive_processes = [process for process in worker_processes if process.is_alive()]
            for idx, process in enumerate(worker_processes):
                if worker_alive.get(idx) and not process.is_alive():
                    worker_alive[idx] = False
                    status_queue.put(
                        {
                            "status_event": "worker_update",
                            "worker_id": idx,
                            "state": "dead",
                            "pid": process.pid,
                            "ts": time.time(),
                        }
                    )
                    _publish_status_message(
                        status_queue,
                        level="error",
                        text=f"Воркер {idx} завершился (exitcode={process.exitcode}).",
                        source="app",
                    )
            if not alive_processes:
                break
            time.sleep(0.5)
    finally:
        shared_values["shutdown"].value = True
        stop_event.set()
        _stop_processes(worker_processes)
        aggregator_thread.join(timeout=3)
        status_thread.join(timeout=3)
        web_grid_process.join(timeout=5)
        if web_grid_process.is_alive():
            web_grid_process.terminate()
            web_grid_process.join(timeout=3)


if __name__ == "__main__":
    main()
