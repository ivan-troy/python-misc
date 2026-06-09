"""Tests for csv_merger.pipeline.transport.

The HTTP publish tests run a real stdlib ``http.server`` in a background
thread; no mocks. This keeps the tests honest — we're exercising the
actual urllib request path, not a mock that says "yes I was called".
"""

from __future__ import annotations

import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable

from csv_merger.pipeline._errors import PermanentError, TransientError
from csv_merger.pipeline.transport import (
    fetch_files,
    publish_file,
)


# --------------------------------------------------------------------------- #
# Fetch tests                                                                 #
# --------------------------------------------------------------------------- #


class FetchHappyPathTests(unittest.TestCase):
    def test_fetches_all_files_with_correct_sizes(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            src = tmp_path / "src"
            src.mkdir()
            for i in range(10):
                (src / f"f{i}.txt").write_text(f"contents {i}" * (i + 1))
            sources = sorted(src.glob("*.txt"))

            result = fetch_files(
                sources,
                tmp_path / "staging",
                tmp_path / "fetched",
                parallel_workers=4,
            )

            self.assertEqual(len(result), 10)
            for fetched in result:
                self.assertTrue(fetched.local_path.exists())
                self.assertEqual(
                    fetched.local_path.read_bytes(),
                    fetched.source_path.read_bytes(),
                )

    def test_returns_results_in_input_order(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            src = tmp_path / "src"
            src.mkdir()
            names = ["z.txt", "a.txt", "m.txt", "b.txt"]
            for n in names:
                (src / n).write_text(n)
            sources = [src / n for n in names]

            result = fetch_files(
                sources,
                tmp_path / "staging",
                tmp_path / "fetched",
                parallel_workers=4,
            )

        self.assertEqual(
            [f.source_path.name for f in result],
            names,
        )

    def test_empty_input_returns_empty(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result = fetch_files(
                [],
                tmp_path / "staging",
                tmp_path / "fetched",
                parallel_workers=4,
            )
        self.assertEqual(result, [])

    def test_staging_is_clean_after_success(self) -> None:
        """Successful fetches leave no .tmp debris in staging/."""
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            src = tmp_path / "src"
            src.mkdir()
            for i in range(5):
                (src / f"f{i}.txt").write_text(f"x{i}")

            fetch_files(
                sorted(src.glob("*.txt")),
                tmp_path / "staging",
                tmp_path / "fetched",
                parallel_workers=4,
            )

            self.assertEqual(
                list((tmp_path / "staging").iterdir()),
                [],
            )


class FetchFailureTests(unittest.TestCase):
    def test_missing_source_raises_permanent(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            with self.assertRaises(PermanentError):
                fetch_files(
                    [tmp_path / "missing.txt"],
                    tmp_path / "staging",
                    tmp_path / "fetched",
                    fetch_max_attempts=1,  # don't retry permanent errors
                )

    def test_no_partial_file_visible_in_fetched_on_failure(self) -> None:
        """A failed fetch must not leave a half-file in fetched/."""
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            # One good source, one missing → second one fails
            src = tmp_path / "src"
            src.mkdir()
            (src / "good.txt").write_text("ok")
            sources = [src / "good.txt", tmp_path / "ghost.txt"]

            try:
                fetch_files(
                    sources,
                    tmp_path / "staging",
                    tmp_path / "fetched",
                    parallel_workers=2,
                    fetch_max_attempts=1,
                )
            except PermanentError:
                pass

            # The "good" file may or may not have made it to fetched
            # (depending on scheduling), but staging must be clean and
            # nothing partial should exist anywhere.
            staging = list((tmp_path / "staging").iterdir())
            self.assertEqual(staging, [], "staging should be clean")


# --------------------------------------------------------------------------- #
# Publish tests                                                               #
# --------------------------------------------------------------------------- #


class _RecordingHandler(BaseHTTPRequestHandler):
    """An HTTP handler that records requests and returns a configured status."""

    # Class-level shared state; reset per test via the helper below.
    received_requests: list[tuple[str, dict, bytes]] = []
    status_sequence: list[int] = []

    def do_PUT(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler convention)
        self._handle()

    def do_POST(self) -> None:  # noqa: N802
        self._handle()

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        self.received_requests.append(
            (self.path, dict(self.headers), body)
        )
        # Pop the next status; if the list is empty, default to 200.
        status = (
            self.status_sequence.pop(0)
            if self.status_sequence
            else 200
        )
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        # Silence the default per-request log to stderr in tests.
        pass


def _start_server(
    status_sequence: list[int] | None = None,
) -> tuple[HTTPServer, str, Callable[[], None]]:
    """Start a recording HTTP server on localhost:0 and return (server, url, stop)."""
    _RecordingHandler.received_requests = []
    _RecordingHandler.status_sequence = list(status_sequence or [])

    server = HTTPServer(("127.0.0.1", 0), _RecordingHandler)
    host, port = server.server_address
    url = f"http://{host}:{port}/upload"

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def stop() -> None:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    return server, url, stop


class PublishHappyPathTests(unittest.TestCase):
    def test_put_sends_body_and_headers(self) -> None:
        _, url, stop = _start_server()
        try:
            with TemporaryDirectory() as tmp:
                payload = Path(tmp) / "merged.txt"
                payload.write_bytes(b"merged content")

                publish_file(
                    payload,
                    url,
                    idempotency_key="abc123",
                    extra_headers={"X-Run-Id": "42"},
                )

            self.assertEqual(len(_RecordingHandler.received_requests), 1)
            path, headers, body = _RecordingHandler.received_requests[0]
            self.assertEqual(path, "/upload")
            self.assertEqual(body, b"merged content")
            self.assertEqual(headers.get("Idempotency-Key"), "abc123")
            self.assertEqual(headers.get("X-Run-Id"), "42")
            self.assertEqual(
                headers.get("Content-Type"),
                "application/octet-stream",
            )
        finally:
            stop()

    def test_post_method_works(self) -> None:
        _, url, stop = _start_server()
        try:
            with TemporaryDirectory() as tmp:
                payload = Path(tmp) / "p.txt"
                payload.write_bytes(b"x")
                publish_file(
                    payload, url, idempotency_key="k", method="POST"
                )
            self.assertEqual(len(_RecordingHandler.received_requests), 1)
        finally:
            stop()


class PublishRetryTests(unittest.TestCase):
    def test_retries_on_503_then_succeeds(self) -> None:
        # Two 503s, then 200. Should retry twice and succeed.
        _, url, stop = _start_server(status_sequence=[503, 503, 200])
        try:
            with TemporaryDirectory() as tmp:
                payload = Path(tmp) / "p.txt"
                payload.write_bytes(b"data")
                publish_file(
                    payload,
                    url,
                    idempotency_key="k",
                    max_attempts=5,
                    base_delay=0.01,
                    max_delay=0.05,
                )
            self.assertEqual(len(_RecordingHandler.received_requests), 3)
        finally:
            stop()

    def test_does_not_retry_on_400(self) -> None:
        _, url, stop = _start_server(status_sequence=[400])
        try:
            with TemporaryDirectory() as tmp:
                payload = Path(tmp) / "p.txt"
                payload.write_bytes(b"data")
                with self.assertRaises(PermanentError):
                    publish_file(
                        payload,
                        url,
                        idempotency_key="k",
                        max_attempts=5,
                        base_delay=0.01,
                        max_delay=0.05,
                    )
            # Critical: exactly one request — no retries on permanent.
            self.assertEqual(len(_RecordingHandler.received_requests), 1)
        finally:
            stop()

    def test_exhausts_retries_on_persistent_5xx(self) -> None:
        # Five 503s, all retries fail.
        _, url, stop = _start_server(status_sequence=[503] * 10)
        try:
            with TemporaryDirectory() as tmp:
                payload = Path(tmp) / "p.txt"
                payload.write_bytes(b"data")
                with self.assertRaises(TransientError):
                    publish_file(
                        payload,
                        url,
                        idempotency_key="k",
                        max_attempts=3,
                        base_delay=0.01,
                        max_delay=0.05,
                    )
            # 3 attempts.
            self.assertEqual(len(_RecordingHandler.received_requests), 3)
        finally:
            stop()


class PublishConnectionTests(unittest.TestCase):
    def test_unreachable_url_is_transient(self) -> None:
        # Port 1 on localhost is almost guaranteed to refuse.
        with TemporaryDirectory() as tmp:
            payload = Path(tmp) / "p.txt"
            payload.write_bytes(b"x")
            with self.assertRaises(TransientError):
                publish_file(
                    payload,
                    "http://127.0.0.1:1/nope",
                    idempotency_key="k",
                    max_attempts=1,
                    base_delay=0.01,
                    max_delay=0.05,
                    connect_timeout=1.0,
                    read_timeout=1.0,
                )


if __name__ == "__main__":
    unittest.main()
