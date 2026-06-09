"""Integration tests for csv_merger.pipeline.runner.

These tests run the full pipeline against:

* A tempdir that simulates the source folder (with real sample batch
  and record files copied in).
* A stdlib ``http.server`` running in a background thread, simulating
  the publish endpoint.
* A patched ``_smtp_send`` so alert emails don't try to reach a real
  SMTP server.

The runner's contract is "never raise for operational conditions" — we
verify that by reading the :class:`RunResult` rather than catching
exceptions.
"""

from __future__ import annotations

import shutil
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable
from unittest.mock import patch

from csv_merger.pipeline.config import (
    EmailConfig,
    FetchConfig,
    FoldersConfig,
    OutboxConfig,
    PipelineConfig,
    PublishConfig,
    QuiescenceConfig,
    RetryPolicyConfig,
    StateConfig,
)
from csv_merger.pipeline.locking import FileLock
from csv_merger.pipeline.outbox import list_pending, manifest_path_for
from csv_merger.pipeline.runner import run_pipeline
from csv_merger.pipeline.state import (
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCESS,
    STEP_STATUS_SKIPPED,
    StateStore,
    compute_batch_signature,
    compute_file_hash,
)
from tests._helpers import SAMPLE_DIR


# --------------------------------------------------------------------------- #
# HTTP fake                                                                   #
# --------------------------------------------------------------------------- #


class _RecordingHandler(BaseHTTPRequestHandler):
    """Records every request and returns a configured status sequence."""

    received_requests: list[tuple[str, dict, bytes]] = []
    status_sequence: list[int] = []

    def do_PUT(self) -> None:  # noqa: N802
        self._handle()

    def do_POST(self) -> None:  # noqa: N802
        self._handle()

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        self.received_requests.append(
            (self.path, dict(self.headers), body)
        )
        status = (
            self.status_sequence.pop(0)
            if self.status_sequence
            else 200
        )
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass  # silence per-request stderr


def _start_server(
    status_sequence: list[int] | None = None,
) -> tuple[HTTPServer, str, Callable[[], None]]:
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


# --------------------------------------------------------------------------- #
# Test scaffolding                                                            #
# --------------------------------------------------------------------------- #


def _build_config(tmp: Path, publish_url: str) -> PipelineConfig:
    """Construct a PipelineConfig pointing at subdirectories under `tmp`."""
    folders = FoldersConfig(
        source=tmp / "source",
        staging=tmp / "staging",
        fetched=tmp / "fetched",
        outbox_pending=tmp / "outbox" / "pending",
        outbox_sent=tmp / "outbox" / "sent",
        dead_letter=tmp / "dead_letter",
    )
    state = StateConfig(
        db_path=tmp / "state" / "pipeline.db",
        lock_path=tmp / "state" / "pipeline.lock",
    )
    return PipelineConfig(
        folders=folders,
        state=state,
        publish=PublishConfig(
            url=publish_url,
            max_attempts=2,           # fail fast in tests
            base_delay_seconds=0.01,
            max_delay_seconds=0.05,
            connect_timeout_seconds=2.0,
            read_timeout_seconds=2.0,
        ),
        email=EmailConfig(
            smtp_host="smtp.test.invalid",
            smtp_port=587,
            from_address="pipeline@test.invalid",
            to_addresses=("ops@test.invalid",),
            rate_limit_per_hour=10,   # high so test alerts aren't suppressed
        ),
        quiescence=QuiescenceConfig(
            quiet_seconds=0,          # don't wait in tests
            max_wait_seconds=2,
            poll_interval_seconds=1,
        ),
        fetch=FetchConfig(parallel_workers=4),
        retry_policy=RetryPolicyConfig(max_attempts_per_batch=3),
        outbox=OutboxConfig(sent_retention_days=7),
    )


def _seed_source(source: Path, file_names: list[str]) -> None:
    """Copy named files from sample_data/ into the source folder."""
    source.mkdir(parents=True, exist_ok=True)
    for name in file_names:
        shutil.copy(SAMPLE_DIR / name, source / name)


def _all_samples() -> list[str]:
    """All sample batch + record file names."""
    return sorted(
        p.name for p in SAMPLE_DIR.iterdir()
        if p.is_file() and (p.name.startswith("batch-")
                            or p.name.startswith("record-"))
    )


# --------------------------------------------------------------------------- #
# Tests                                                                       #
# --------------------------------------------------------------------------- #


class HappyPathTests(unittest.TestCase):
    def test_full_pipeline_run_succeeds(self) -> None:
        _, url, stop = _start_server()
        try:
            with TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                config = _build_config(tmp_path, url)
                _seed_source(config.folders.source, _all_samples())

                with patch(
                    "csv_merger.pipeline.alerting._smtp_send",
                ):
                    result = run_pipeline(config)

                # Outcome
                self.assertEqual(result.status, RUN_STATUS_SUCCESS)
                self.assertIsNotNone(result.batch_signature)
                self.assertGreater(result.files_processed, 0)
                self.assertFalse(result.skipped_lock_held)
                self.assertFalse(result.skipped_quarantined)
                self.assertIsNone(result.error)

                # The HTTP fake received exactly one PUT
                self.assertEqual(
                    len(_RecordingHandler.received_requests), 1
                )
                path, headers, body = _RecordingHandler.received_requests[0]
                self.assertEqual(path, "/upload")
                # Body should be the merged output (non-empty)
                self.assertGreater(len(body), 0)
                # Idempotency-Key should be the batch signature
                self.assertEqual(
                    headers.get("Idempotency-Key"),
                    result.batch_signature,
                )

                # Outbox/sent should contain the file + manifest
                sent_files = sorted(config.folders.outbox_sent.iterdir())
                self.assertEqual(len(sent_files), 2)  # data + manifest

                # Pending should be empty
                self.assertEqual(
                    list_pending(config.folders.outbox_pending), []
                )

                # State should record the files as processed
                state = StateStore(config.state.db_path)
                try:
                    row_count = state._conn.execute(
                        "SELECT COUNT(*) FROM processed_files"
                    ).fetchone()[0]
                    self.assertEqual(row_count, result.files_processed)
                finally:
                    state.close()
        finally:
            stop()


class LockHeldTests(unittest.TestCase):
    def test_lock_held_returns_skipped_without_recording_run(self) -> None:
        _, url, stop = _start_server()
        try:
            with TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                config = _build_config(tmp_path, url)
                _seed_source(config.folders.source, _all_samples())

                # Hold the lock from another "process" (just another
                # FileLock instance in the same process).
                config.state.lock_path.parent.mkdir(
                    parents=True, exist_ok=True
                )
                holder = FileLock(config.state.lock_path)
                holder.acquire()
                try:
                    with patch(
                        "csv_merger.pipeline.alerting._smtp_send",
                    ):
                        result = run_pipeline(config)
                finally:
                    holder.release()

                self.assertTrue(result.skipped_lock_held)
                self.assertIsNone(result.run_id)
                # No run should have been recorded in state.
                # (state DB may not even exist yet if lock was held
                # before we ever got to state init)
                if config.state.db_path.exists():
                    state = StateStore(config.state.db_path)
                    try:
                        run_count = state._conn.execute(
                            "SELECT COUNT(*) FROM runs"
                        ).fetchone()[0]
                        self.assertEqual(run_count, 0)
                    finally:
                        state.close()
        finally:
            stop()


class NoNewFilesTests(unittest.TestCase):
    def test_empty_source_records_skipped_steps(self) -> None:
        _, url, stop = _start_server()
        try:
            with TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                config = _build_config(tmp_path, url)
                config.folders.source.mkdir(parents=True)
                # No source files

                with patch(
                    "csv_merger.pipeline.alerting._smtp_send",
                ):
                    result = run_pipeline(config)

                self.assertEqual(result.status, RUN_STATUS_SUCCESS)
                self.assertEqual(result.files_processed, 0)
                # No HTTP request should have been made.
                self.assertEqual(
                    len(_RecordingHandler.received_requests), 0
                )

                # Steps after discover should be recorded as skipped.
                state = StateStore(config.state.db_path)
                try:
                    steps = state.steps_for_run(result.run_id)
                    skipped = {
                        s.step_name for s in steps
                        if s.status == STEP_STATUS_SKIPPED
                    }
                    self.assertIn("fetch", skipped)
                    self.assertIn("publish", skipped)
                    self.assertIn("mark_processed", skipped)
                finally:
                    state.close()
        finally:
            stop()


class DrainBlocksNewBatchTests(unittest.TestCase):
    def test_pending_file_that_fails_to_publish_blocks_new_batch(self) -> None:
        """If draining the outbox leaves anything pending, defer the new batch."""
        # Server will reject everything with 500 → pending file stays.
        _, url, stop = _start_server(status_sequence=[500] * 10)
        try:
            with TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                config = _build_config(tmp_path, url)
                _seed_source(config.folders.source, _all_samples())

                # Pre-seed outbox/pending with a fake file + manifest.
                config.folders.outbox_pending.mkdir(parents=True)
                pending_file = config.folders.outbox_pending / (
                    f"99-{'a' * 16}.txt"
                )
                pending_file.write_text("prior merged output")
                manifest_path = manifest_path_for(pending_file)
                manifest_path.write_text(
                    '{"batch_signature": "' + ("a" * 64) + '", '
                    '"run_id": 99, '
                    '"source_files": []}'
                )

                with patch(
                    "csv_merger.pipeline.alerting._smtp_send",
                ):
                    result = run_pipeline(config)

                # Run should have failed due to incomplete drain
                self.assertEqual(result.status, RUN_STATUS_FAILED)
                self.assertIn("outbox drain", result.error or "")

                # No new files should have been processed
                self.assertEqual(result.files_processed, 0)

                # The pending file should still be there (publish failed)
                self.assertTrue(pending_file.exists())
        finally:
            stop()


class QuarantineTests(unittest.TestCase):
    def test_quarantined_batch_is_skipped(self) -> None:
        """Pre-existing quarantine record causes run to skip without work."""
        _, url, stop = _start_server()
        try:
            with TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                config = _build_config(tmp_path, url)
                _seed_source(config.folders.source, _all_samples())

                # Compute the batch signature that the runner will see
                # and pre-record it as quarantined.
                hashes = []
                for name in _all_samples():
                    path = config.folders.source / name
                    hashes.append((str(path), compute_file_hash(path)))
                expected_sig = compute_batch_signature(hashes)

                # Pre-populate state with a quarantine record.
                config.state.db_path.parent.mkdir(parents=True)
                state = StateStore(config.state.db_path)
                try:
                    for _ in range(3):
                        state.record_batch_failure(
                            expected_sig,
                            max_attempts=3,
                            error_text="pre-existing",
                        )
                    self.assertTrue(
                        state.is_batch_quarantined(expected_sig)
                    )
                finally:
                    state.close()

                with patch(
                    "csv_merger.pipeline.alerting._smtp_send",
                ):
                    result = run_pipeline(config)

                self.assertTrue(result.skipped_quarantined)
                self.assertEqual(result.status, RUN_STATUS_FAILED)
                # No HTTP request — work was skipped
                self.assertEqual(
                    len(_RecordingHandler.received_requests), 0
                )
        finally:
            stop()

    def test_batch_failure_increments_and_quarantines_on_nth_attempt(
        self,
    ) -> None:
        """After N consecutive failures, batch is quarantined and alert sent.

        Note: once run 1 writes the batch to outbox/pending, runs 2+ see
        it as a drain failure (not a new-batch failure). The drain path
        increments the same batch counter and fires the quarantine alert
        when the threshold is reached.
        """
        # Server rejects with 500; max_attempts_per_batch=3.
        _, url, stop = _start_server(status_sequence=[500] * 100)
        try:
            with TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                config = _build_config(tmp_path, url)
                _seed_source(config.folders.source, _all_samples())

                alerts_captured: list[tuple[str, str]] = []

                def fake_smtp_send(cfg, msg) -> None:
                    alerts_captured.append((msg["Subject"], msg.get_content()))

                with patch(
                    "csv_merger.pipeline.alerting._smtp_send",
                    side_effect=fake_smtp_send,
                ):
                    # Three runs in a row, each fails publish (run 1 as
                    # new-batch failure, runs 2-3 as drain failures).
                    result1 = run_pipeline(config)
                    result2 = run_pipeline(config)
                    result3 = run_pipeline(config)

                self.assertEqual(result1.status, RUN_STATUS_FAILED)
                self.assertEqual(result2.status, RUN_STATUS_FAILED)
                self.assertEqual(result3.status, RUN_STATUS_FAILED)

                # The batch should be quarantined after 3 attempts —
                # verify via state DB (not via RunResult, since drain-
                # path quarantines don't set quarantined_now).
                state = StateStore(config.state.db_path)
                try:
                    quar = state.quarantined_batches()
                    self.assertEqual(len(quar), 1)
                    self.assertEqual(quar[0].attempt_count, 3)
                finally:
                    state.close()

                # Alert should have been sent (once) when the threshold
                # was crossed on the 3rd attempt.
                self.assertEqual(len(alerts_captured), 1)
                self.assertIn("quarantined", alerts_captured[0][0].lower())
        finally:
            stop()


class EmergencyAlertTests(unittest.TestCase):
    def test_state_db_init_failure_triggers_emergency_alert(self) -> None:
        """If state DB cannot be opened, alert via emergency path."""
        _, url, stop = _start_server()
        try:
            with TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                config = _build_config(tmp_path, url)

                # Sabotage the state DB path: point it at a path that
                # cannot be created (parent is a non-directory).
                bad_parent = tmp_path / "blocking_file"
                bad_parent.write_text("not a directory")
                sabotaged_state = StateConfig(
                    db_path=bad_parent / "sub" / "pipeline.db",
                    lock_path=config.state.lock_path,
                )
                # Reconstruct config with the bad state path
                sabotaged_config = PipelineConfig(
                    folders=config.folders,
                    state=sabotaged_state,
                    publish=config.publish,
                    email=config.email,
                    quiescence=config.quiescence,
                    fetch=config.fetch,
                    retry_policy=config.retry_policy,
                    outbox=config.outbox,
                )

                alerts_captured: list[str] = []

                def fake_smtp_send(cfg, msg) -> None:
                    alerts_captured.append(msg["Subject"])

                with patch(
                    "csv_merger.pipeline.alerting._smtp_send",
                    side_effect=fake_smtp_send,
                ):
                    result = run_pipeline(sabotaged_config)

                self.assertEqual(result.status, RUN_STATUS_FAILED)
                self.assertIsNotNone(result.error)
                self.assertIn("state", result.error)
                # Emergency alert path should have fired
                self.assertEqual(len(alerts_captured), 1)
                self.assertIn("catastrophic", alerts_captured[0].lower())
        finally:
            stop()


if __name__ == "__main__":
    unittest.main()
