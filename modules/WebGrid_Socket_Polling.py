__version__ = "1.0"

"""
Unified WebGrid: SSE (realtime) + polling in one class.

Почему имя файла без '&':
- символ '&' допустим в имени файла ОС,
- но неудобен/проблемен для обычного Python import.

Используйте:
    from modules.WebGrid_Socket_Polling import WebGridSocketPolling, run_web_grid_process
"""

import json
import multiprocessing
import queue as queue_module
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional

from modules.logger import LoggerFactory

logger = LoggerFactory.get_logger("app." + __name__)


HTML_TEMPLATE = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>WebGrid</title>
  <style>
    :root {
      --bg: #f4f6f8;
      --panel: #ffffff;
      --head-bg: #323030;
      --head-fg: #ffffff;
      --line: #d0d6dc;
      --text: #1f2933;
      --muted: #66788a;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Ubuntu", "Segoe UI", sans-serif;
      color: var(--text);
      background: linear-gradient(180deg, #eef2f6 0%, #f7f9fb 100%);
    }
    .wrap { max-width: 1400px; margin: 16px auto; padding: 0 12px; }
    .topbar { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 10px; }
    h1 { margin: 0; font-size: 20px; font-weight: 700; }
    .meta { color: var(--muted); font-size: 13px; }
    .table-card { border: 1px solid var(--line); background: var(--panel); border-radius: 10px; overflow: hidden; box-shadow: 0 8px 24px rgba(12, 23, 35, 0.06); }
    .table-scroll { overflow: auto; max-height: calc(100vh - 120px); }
    table { width: 100%; border-collapse: collapse; min-width: 720px; }
    th, td { border: 1px solid var(--line); padding: 7px 10px; white-space: nowrap; font-size: 14px; text-align: center; background: #d9d9d9; }
    thead th { position: sticky; top: 0; z-index: 2; background: var(--head-bg); color: var(--head-fg); font-weight: 700; }
    .row-head { background: var(--head-bg); color: var(--head-fg); font-weight: 700; position: sticky; left: 0; z-index: 1; }
    .empty { padding: 20px; color: var(--muted); }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <h1 id="title">WebGrid</h1>
      <div class="meta">ver <span id="ver">0</span> | mode <span id="mode"></span></div>
    </div>
    <div class="table-card">
      <div class="table-scroll" id="tableContainer">
        <div class="empty">Ожидание данных...</div>
      </div>
    </div>
  </div>

  <script>
    const TRANSPORT_MODE = "__TRANSPORT_MODE__";
    const POLL_INTERVAL_MS = __POLL_INTERVAL_MS__;
    let lastVersion = -1;
    let lastEventTs = 0;

    function asNum(v, fallback) {
      const n = Number(v);
      return Number.isFinite(n) ? n : fallback;
    }

    function sortedCellEntries(rowObj) {
      return Object.entries(rowObj || {}).sort((a, b) => asNum(a[0], 0) - asNum(b[0], 0));
    }

    function alignToCss(align) {
      if (align === 'left' || align === 'w') return 'left';
      if (align === 'right' || align === 'e') return 'right';
      return 'center';
    }

    function renderTable(state) {
      const container = document.getElementById('tableContainer');
      const titleEl = document.getElementById('title');
      const verEl = document.getElementById('ver');
      const modeEl = document.getElementById('mode');

      titleEl.textContent = state.title || 'WebGrid';
      document.title = state.title || 'WebGrid';
      verEl.textContent = String(state.version || 0);
      modeEl.textContent = TRANSPORT_MODE;

      const gridData = state.grid_data || {};
      const rowHeader = !!state.row_header;

      const header = gridData.header || {};
      const dataRows = Object.keys(gridData)
        .filter(k => k !== 'header')
        .sort((a, b) => asNum(a, 0) - asNum(b, 0));

      if (!Object.keys(header).length && !dataRows.length) {
        container.innerHTML = '<div class="empty">Ожидание данных...</div>';
        return;
      }

      let html = '<table><thead><tr>';
      if (rowHeader) html += '<th class="row-head">№</th>';

      for (const [_, cell] of sortedCellEntries(header)) {
        const text = String((cell && cell.text) ?? '');
        html += `<th>${text}</th>`;
      }
      html += '</tr></thead><tbody>';

      for (const rowKey of dataRows) {
        const row = gridData[rowKey] || {};
        html += '<tr>';
        if (rowHeader) html += `<td class="row-head">${rowKey}</td>`;

        for (const [_, cell] of sortedCellEntries(row)) {
          const text = String((cell && cell.text) ?? '');
          const fg = (cell && cell.fg) ? String(cell.fg) : 'black';
          const bg = (cell && cell.bg) ? String(cell.bg) : '#D9D9D9';
          const align = alignToCss(cell && cell.align);
          html += `<td style="color:${fg};background:${bg};text-align:${align}">${text}</td>`;
        }
        html += '</tr>';
      }

      html += '</tbody></table>';
      container.innerHTML = html;
    }

    async function fetchState() {
      try {
        const resp = await fetch('/api/state', { cache: 'no-store' });
        if (!resp.ok) return;
        const state = await resp.json();
        if ((state.version || 0) !== lastVersion) {
          lastVersion = state.version || 0;
          renderTable(state);
        }
        lastEventTs = Date.now();
      } catch (_) {}
    }

    function startPolling() {
      setInterval(fetchState, POLL_INTERVAL_MS);
      fetchState();
    }

    function startSSE() {
      const es = new EventSource('/api/stream');
      es.onmessage = (ev) => {
        try {
          const state = JSON.parse(ev.data);
          if ((state.version || 0) !== lastVersion) {
            lastVersion = state.version || 0;
            renderTable(state);
          }
          lastEventTs = Date.now();
        } catch (_) {}
      };
      es.onerror = () => {};

      // fallback snapshot если stream тишина
      setInterval(() => {
        if (!lastEventTs || (Date.now() - lastEventTs) > 2000) {
          fetchState();
        }
      }, 1000);

      fetchState();
    }

    if (TRANSPORT_MODE === 'polling') {
      startPolling();
    } else {
      startSSE();
    }
  </script>
</body>
</html>
"""


class WebGridSocketPolling:
    """Единый класс веб-таблицы с режимом `transport='sse'` или `transport='polling'`."""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 8765,
        row_header: bool = True,
        title: str = "WebGrid",
        queue_poll_interval: float = 0.03,
        max_fps: float = 5.0,
        transport: str = "sse",
        client_poll_interval_ms: int = 100,
    ) -> None:
        self.host = host
        self.port = port
        self.row_header = row_header
        self.title = title
        self.queue_poll_interval = queue_poll_interval
        self.max_fps = max(0.2, float(max_fps))
        self.transport = "polling" if str(transport).lower() == "polling" else "sse"
        self.client_poll_interval_ms = max(50, int(client_poll_interval_ms))

        self.data_queue: multiprocessing.Queue = multiprocessing.Queue()
        self.queue_datadict_wrapper_key: Optional[str] = None

        self.grid_data: dict[str, Any] = {}
        self.version: int = 0

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._httpd: Optional[ThreadingHTTPServer] = None

    @property
    def queue(self) -> multiprocessing.Queue:
        return self.data_queue

    def _html_page(self) -> str:
        return (
            HTML_TEMPLATE
            .replace("__TRANSPORT_MODE__", self.transport)
            .replace("__POLL_INTERVAL_MS__", str(self.client_poll_interval_ms))
        )

    def update_grid_data(self, grid_data: Optional[dict] = None) -> None:
        if grid_data is None:
            grid_data = {"header": {0: {"text": "test"}}}
        if not isinstance(grid_data, dict):
            raise ValueError("grid_data must be a dictionary")

        with self._lock:
            self.grid_data = grid_data
            self.version += 1

    def _apply_config(self, cfg: dict[str, Any]) -> None:
        with self._lock:
            for key, value in cfg.items():
                if key == "title":
                    self.title = str(value)
                elif hasattr(self, key):
                    setattr(self, key, value)
            self.version += 1

    def _extract_item(self, raw_item: Any) -> Any:
        if self.queue_datadict_wrapper_key:
            if isinstance(raw_item, dict) and self.queue_datadict_wrapper_key in raw_item:
                return raw_item[self.queue_datadict_wrapper_key]
            return None
        return raw_item

    def process_data_queue_once(self) -> None:
        while True:
            try:
                raw_item = self.data_queue.get_nowait()
            except queue_module.Empty:
                break

            item = self._extract_item(raw_item)
            if item is None or not isinstance(item, dict):
                continue

            if item.get("command") == "close":
                self.stop()
                return

            if "config" in item and isinstance(item["config"], dict):
                self._apply_config(item["config"])
                continue

            if "title" in item:
                with self._lock:
                    self.title = str(item["title"])
                    self.version += 1
                continue

            self.update_grid_data(item)

    def _queue_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.process_data_queue_once()
            except Exception as exc:
                logger.exception(f"[WebGridSocketPolling] queue worker error: {exc}")
            time.sleep(self.queue_poll_interval)

    def _shared_shutdown_worker(self, shared_values: Any) -> None:
        while not self._stop_event.is_set():
            try:
                if shared_values and shared_values["shutdown"].value:
                    self.stop()
                    return
            except Exception as exc:
                logger.error(f"[WebGridSocketPolling] shutdown check failed: {exc}")
            time.sleep(0.5)

    def _snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "title": self.title,
                "row_header": self.row_header,
                "grid_data": self.grid_data,
                "version": self.version,
            }

    def _build_handler(self):
        grid = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):  # noqa: N802
                if self.path in ("/", "/index.html"):
                    payload = grid._html_page().encode("utf-8")
                    try:
                        self.send_response(200)
                        self.send_header("Content-Type", "text/html; charset=utf-8")
                        self.send_header("Content-Length", str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload)
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    return

                if self.path == "/api/state":
                    state = grid._snapshot()
                    payload = json.dumps(state, ensure_ascii=False, default=str).encode("utf-8")
                    try:
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json; charset=utf-8")
                        self.send_header("Cache-Control", "no-store")
                        self.send_header("Content-Length", str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload)
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    return

                if self.path == "/api/stream":
                    if grid.transport != "sse":
                        self.send_response(404)
                        self.end_headers()
                        return

                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                    self.send_header("Cache-Control", "no-cache, no-transform")
                    self.send_header("Connection", "keep-alive")
                    self.send_header("X-Accel-Buffering", "no")
                    self.end_headers()

                    try:
                        self.wfile.write(b"retry: 1000\n\n")
                        self.wfile.flush()
                        min_interval = 1.0 / grid.max_fps
                        last_sent_version = -1
                        last_sent_ts = 0.0

                        while not grid._stop_event.is_set():
                            state = grid._snapshot()
                            version = int(state.get("version", 0))
                            now = time.monotonic()

                            if version != last_sent_version and (now - last_sent_ts) >= min_interval:
                                payload = json.dumps(state, ensure_ascii=False, default=str)
                                data = f"data: {payload}\n\n".encode("utf-8")
                                self.wfile.write(data)
                                self.wfile.flush()
                                last_sent_version = version
                                last_sent_ts = now
                            else:
                                self.wfile.write(b": ping\n\n")
                                self.wfile.flush()

                            time.sleep(0.1)
                    except (BrokenPipeError, ConnectionResetError, TimeoutError):
                        pass
                    return

                try:
                    self.send_response(404)
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(b"Not found")
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, fmt: str, *args) -> None:
                logger.debug("[WebGridSocketPolling HTTP] " + fmt % args)

        return Handler

    def run_server(self, shared_values: Any = None) -> None:
        self._stop_event.clear()
        handler = self._build_handler()
        self._httpd = ThreadingHTTPServer((self.host, self.port), handler)

        queue_thread = threading.Thread(target=self._queue_worker, daemon=True)
        queue_thread.start()

        if shared_values is not None:
            shutdown_thread = threading.Thread(
                target=self._shared_shutdown_worker,
                args=(shared_values,),
                daemon=True,
            )
            shutdown_thread.start()

        logger.info(f"WebGridSocketPolling started: http://{self.host}:{self.port} mode={self.transport}")
        print(f"WebGridSocketPolling started: http://{self.host}:{self.port} mode={self.transport}")

        try:
            self._httpd.serve_forever(poll_interval=0.5)
        finally:
            self._stop_event.set()
            if self._httpd is not None:
                self._httpd.server_close()
            logger.info("WebGridSocketPolling stopped")

    def stop(self) -> None:
        self._stop_event.set()
        if self._httpd is not None:
            threading.Thread(target=self._httpd.shutdown, daemon=True).start()


def run_web_grid_process(
    *,
    table_queue_data: multiprocessing.Queue,
    shared_values: Any,
    queue_datadict_wrapper_key: Optional[str] = None,
    **kwargs: Any,
) -> None:
    try:
        web_grid = WebGridSocketPolling(**kwargs)
        web_grid.queue_datadict_wrapper_key = queue_datadict_wrapper_key
        web_grid.data_queue = table_queue_data
        web_grid.run_server(shared_values=shared_values)
    except Exception as exc:
        logger.exception(f"Ошибка в run_web_grid_process: {exc}")


if __name__ == "__main__":
    import random

    grid = WebGridSocketPolling(
        host="127.0.0.1",
        port=8765,
        title="WebGrid Unified Demo",
        transport="sse",  # "sse" or "polling"
        max_fps=5.0,
    )

    def source_data(q: multiprocessing.Queue) -> None:
        while True:
            payload = {
                "header": {
                    0: {"text": "Pair", "align": "left"},
                    1: {"text": "Spread %", "align": "right"},
                    2: {"text": "Status", "align": "center"},
                },
                1: {
                    0: {"text": "BTC/USDT", "align": "left"},
                    1: {"text": round(random.uniform(0.1, 2.0), 2), "align": "right"},
                    2: {"text": "OPEN", "bg": "#d6f5d6"},
                },
                2: {
                    0: {"text": "ETH/USDT", "align": "left"},
                    1: {"text": round(random.uniform(-0.4, 1.2), 2), "align": "right"},
                    2: {"text": "WATCH", "bg": "#fff2cc"},
                },
            }
            q.put(payload)
            time.sleep(0.2)

    threading.Thread(target=source_data, args=(grid.queue,), daemon=True).start()
    grid.run_server()
