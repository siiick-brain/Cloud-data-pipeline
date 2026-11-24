"""HTTP trigger for the Spark jobs on port 8000. Contract: docs/contracts/delta_tables.md."""

from __future__ import annotations

import json
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from spark import jobs

VALID = {"bronze", "silver", "gold", "all"}
_lock = threading.Lock()  # one job at a time; later requests wait their turn


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._send(200, {"status": "ok"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        job = self.path.removeprefix("/run/")
        if not self.path.startswith("/run/") or job not in VALID:
            self._send(404, {"error": "unknown job"})
            return
        with _lock:
            start = time.monotonic()
            try:
                jobs.run(job, jobs.get_spark())
                status = "ok"
            except Exception:
                self.log_error("job %s failed", job)
                traceback.print_exc()
                status = "failed"
            duration = round(time.monotonic() - start, 3)
        self._send(200 if status == "ok" else 500, {"job": job, "status": status, "duration_s": duration})


def make_server(port: int = 8000) -> ThreadingHTTPServer:
    return ThreadingHTTPServer(("0.0.0.0", port), Handler)  # noqa: S104 - compose network only


def main() -> None:
    jobs.get_spark()  # start the shared session once, before the first request
    make_server().serve_forever()


if __name__ == "__main__":
    main()
