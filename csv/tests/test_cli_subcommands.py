"""Tests for the pipeline and report CLI subcommands + back-compat shim."""

from __future__ import annotations

import io
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Callable
from unittest.mock import patch

from csv_merger.cli import _apply_legacy_shim, main
from tests._helpers import SAMPLE_DIR


# --------------------------------------------------------------------------- #
# Back-compat shim                                                            #
# --------------------------------------------------------------------------- #


class LegacyShimTests(unittest.TestCase):
    def test_empty_argv_unchanged(self) -> None:
        self.assertEqual(_apply_legacy_shim([]), [])

    def test_help_flag_unchanged(self) -> None:
        self.assertEqual(_apply_legacy_shim(["--help"]), ["--help"])
        self.assertEqual(_apply_legacy_shim(["-h"]), ["-h"])

    def test_explicit_subcommand_unchanged(self) -> None:
        self.assertEqual(
            _apply_legacy_shim(["merge", "--inputs", "x"]),
            ["merge", "--inputs", "x"],
        )
        self.assertEqual(
            _apply_legacy_shim(["pipeline", "--config", "x"]),
            ["pipeline", "--config", "x"],
        )
        self.assertEqual(
            _apply_legacy_shim(["report", "--config", "x"]),
            ["report", "--config", "x"],
        )

    def test_legacy_flag_form_gets_merge_prepended(self) -> None:
        self.assertEqual(
            _apply_legacy_shim(["--inputs", "x", "--output", "y"]),
            ["merge", "--inputs", "x", "--output", "y"],
        )

    def test_legacy_invocation_still_works_end_to_end(self) -> None:
        """The original ``--inputs X --output Y`` form still merges."""
        with TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.txt"
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                rc = main(
                    ["--inputs", str(SAMPLE_DIR), "--output", str(output)]
                )
            self.assertEqual(rc, 0)
            self.assertIn("wrote", stdout.getvalue())
            self.assertTrue(output.exists())


# --------------------------------------------------------------------------- #
# Test scaffolding for pipeline / report subcommands                          #
# --------------------------------------------------------------------------- #


class _RecordingHandler(BaseHTTPRequestHandler):
    received_requests: list[tuple[str, dict, bytes]] = []

    def do_PUT(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        self.received_requests.append((self.path, dict(self.headers), body))
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


def _start_server() -> tuple[HTTPServer, str, Callable[[], None]]:
    _RecordingHandler.received_requests = []
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


def _write_minimal_config(tmp: Path, publish_url: str) -> Path:
    config = tmp / "pipeline.toml"
    folders = tmp / "f"
    config.write_text(
        dedent(f"""\
            [folders]
            source = "{folders / 'source'}"
            staging = "{folders / 'staging'}"
            fetched = "{folders / 'fetched'}"
            outbox_pending = "{folders / 'outbox' / 'pending'}"
            outbox_sent = "{folders / 'outbox' / 'sent'}"
            dead_letter = "{folders / 'dead_letter'}"

            [state]
            db_path = "{folders / 'state' / 'pipeline.db'}"
            lock_path = "{folders / 'state' / 'pipeline.lock'}"

            [publish]
            url = "{publish_url}"
            max_attempts = 2
            base_delay_seconds = 0.01
            max_delay_seconds = 0.05
            connect_timeout_seconds = 2.0
            read_timeout_seconds = 2.0

            [email]
            smtp_host = "smtp.test.invalid"
            smtp_port = 587
            from_address = "p@test.invalid"
            to_addresses = ["ops@test.invalid"]

            [quiescence]
            quiet_seconds = 0
            max_wait_seconds = 2
            poll_interval_seconds = 1

            [fetch]
            parallel_workers = 4
        """),
        encoding="utf-8",
    )
    return config


# --------------------------------------------------------------------------- #
# Pipeline subcommand                                                         #
# --------------------------------------------------------------------------- #


class PipelineSubcommandTests(unittest.TestCase):
    def test_pipeline_with_empty_source_succeeds(self) -> None:
        _, url, stop = _start_server()
        try:
            with TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                config = _write_minimal_config(tmp_path, url)
                # Source folder is auto-created by quiescence; nothing in it.
                (tmp_path / "f" / "source").mkdir(parents=True)

                stdout, stderr = io.StringIO(), io.StringIO()
                with patch("csv_merger.pipeline.alerting._smtp_send"):
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        rc = main(["pipeline", "--config", str(config)])

                self.assertEqual(rc, 0, msg=stderr.getvalue())
                self.assertIn("success", stdout.getvalue())
        finally:
            stop()

    def test_pipeline_with_invalid_config_returns_nonzero(self) -> None:
        with TemporaryDirectory() as tmp:
            bad = Path(tmp) / "missing.toml"
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(["pipeline", "--config", str(bad)])
            self.assertEqual(rc, 1)
            self.assertIn("error", stderr.getvalue())


# --------------------------------------------------------------------------- #
# Report subcommand                                                           #
# --------------------------------------------------------------------------- #


class ReportSubcommandTests(unittest.TestCase):
    def test_report_on_empty_state_runs_cleanly(self) -> None:
        _, url, stop = _start_server()
        try:
            with TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                config = _write_minimal_config(tmp_path, url)
                # Run pipeline once first to create the state DB.
                (tmp_path / "f" / "source").mkdir(parents=True)
                with patch("csv_merger.pipeline.alerting._smtp_send"):
                    main(["pipeline", "--config", str(config)])

                stdout, stderr = io.StringIO(), io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    rc = main(["report", "--config", str(config)])

                self.assertEqual(rc, 0, msg=stderr.getvalue())
                self.assertIn("Recent runs", stdout.getvalue())
        finally:
            stop()

    def test_report_with_custom_limit(self) -> None:
        _, url, stop = _start_server()
        try:
            with TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                config = _write_minimal_config(tmp_path, url)
                (tmp_path / "f" / "source").mkdir(parents=True)
                with patch("csv_merger.pipeline.alerting._smtp_send"):
                    main(["pipeline", "--config", str(config)])

                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    rc = main(
                        ["report", "--config", str(config), "--limit", "3"]
                    )
                self.assertEqual(rc, 0)
                self.assertIn("Recent runs (last 1)", stdout.getvalue())
        finally:
            stop()


if __name__ == "__main__":
    unittest.main()
