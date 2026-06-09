"""Tests for csv_merger.pipeline.state."""

from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from csv_merger.pipeline._errors import StateError
from csv_merger.pipeline.state import (
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCESS,
    STEP_STATUS_FAILED,
    STEP_STATUS_SUCCESS,
    StateStore,
    compute_batch_signature,
    compute_file_hash,
)


def _store(tmp: str) -> StateStore:
    return StateStore(Path(tmp) / "pipeline.db")


class SchemaSetupTests(unittest.TestCase):
    def test_fresh_db_creates_all_tables(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            conn = sqlite3.connect(Path(tmp) / "pipeline.db")
            # Filter sqlite_sequence — autocreated by AUTOINCREMENT.
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
                if not row[0].startswith("sqlite_")
            }
            store.close()
            conn.close()
            self.assertEqual(
                tables,
                {
                    "runs",
                    "run_steps",
                    "processed_files",
                    "failed_batches",
                    "alert_history",
                },
            )

    def test_wal_mode_enabled(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            mode = store._conn.execute(
                "PRAGMA journal_mode"
            ).fetchone()[0]
            store.close()
            self.assertEqual(mode, "wal")


class RunLifecycleTests(unittest.TestCase):
    def test_start_run_returns_increasing_ids(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            ids = [store.start_run() for _ in range(3)]
            self.assertEqual(ids, sorted(set(ids)))  # unique + monotonic
            store.close()

    def test_finish_run_records_status_and_signature(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            run_id = store.start_run()
            store.finish_run(
                run_id,
                status=RUN_STATUS_SUCCESS,
                batch_signature="sig",
            )
            runs = store.recent_runs()
            self.assertEqual(runs[0].status, RUN_STATUS_SUCCESS)
            self.assertEqual(runs[0].batch_signature, "sig")
            store.close()

    def test_finish_run_records_error_on_failure(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            run_id = store.start_run()
            store.finish_run(
                run_id,
                status=RUN_STATUS_FAILED,
                error_text="boom",
            )
            runs = store.recent_runs()
            self.assertEqual(runs[0].status, RUN_STATUS_FAILED)
            self.assertEqual(runs[0].error_text, "boom")
            store.close()


class StepTimingTests(unittest.TestCase):
    def test_successful_step_records_duration(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            run_id = store.start_run()
            with store.step(run_id, "fetch"):
                pass
            steps = store.steps_for_run(run_id)
            self.assertEqual(len(steps), 1)
            self.assertEqual(steps[0].step_name, "fetch")
            self.assertEqual(steps[0].status, STEP_STATUS_SUCCESS)
            self.assertIsNotNone(steps[0].duration_ms)
            self.assertGreaterEqual(steps[0].duration_ms, 0)
            store.close()

    def test_failed_step_records_exception_and_reraises(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            run_id = store.start_run()
            with self.assertRaises(ValueError):
                with store.step(run_id, "merge"):
                    raise ValueError("bad data")
            steps = store.steps_for_run(run_id)
            self.assertEqual(steps[0].status, STEP_STATUS_FAILED)
            self.assertIn("ValueError", steps[0].error_text or "")
            self.assertIn("bad data", steps[0].error_text or "")
            store.close()

    def test_steps_returned_in_start_order(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            run_id = store.start_run()
            for name in ("a", "b", "c"):
                with store.step(run_id, name):
                    pass
            steps = store.steps_for_run(run_id)
            self.assertEqual([s.step_name for s in steps], ["a", "b", "c"])
            store.close()


class ProcessedFilesTests(unittest.TestCase):
    def test_mark_processed_then_is_processed_returns_true(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            run_id = store.start_run()
            store.mark_processed(run_id, [("a.txt", "h1")])
            self.assertTrue(store.is_processed("a.txt", "h1"))
            store.close()

    def test_is_processed_distinguishes_hash(self) -> None:
        """Same path + different hash = different file."""
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            run_id = store.start_run()
            store.mark_processed(run_id, [("a.txt", "h1")])
            self.assertTrue(store.is_processed("a.txt", "h1"))
            self.assertFalse(store.is_processed("a.txt", "h2"))
            store.close()

    def test_mark_processed_is_atomic(self) -> None:
        """Bulk insert should be one transaction.

        We can't easily prove atomicity with a unit test, but we can
        verify it doesn't double-count on duplicate (path, hash) pairs.
        """
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            run_id = store.start_run()
            store.mark_processed(run_id, [("a.txt", "h1"), ("a.txt", "h1")])
            # On conflict do nothing → still one row.
            count = store._conn.execute(
                "SELECT COUNT(*) FROM processed_files"
            ).fetchone()[0]
            self.assertEqual(count, 1)
            store.close()

    def test_empty_input_is_noop(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            run_id = store.start_run()
            self.assertEqual(store.mark_processed(run_id, []), 0)
            store.close()


class FailedBatchTests(unittest.TestCase):
    def test_first_failure_creates_row(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            count = store.record_batch_failure(
                "sig", max_attempts=3, error_text="boom"
            )
            self.assertEqual(count, 1)
            self.assertFalse(store.is_batch_quarantined("sig"))
            store.close()

    def test_repeated_failures_increment_count(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            self.assertEqual(
                store.record_batch_failure("sig", max_attempts=5), 1
            )
            self.assertEqual(
                store.record_batch_failure("sig", max_attempts=5), 2
            )
            self.assertEqual(
                store.record_batch_failure("sig", max_attempts=5), 3
            )
            self.assertFalse(store.is_batch_quarantined("sig"))
            store.close()

    def test_reaching_max_attempts_quarantines(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            for _ in range(3):
                store.record_batch_failure("sig", max_attempts=3)
            self.assertTrue(store.is_batch_quarantined("sig"))
            store.close()

    def test_clear_batch_failure_removes_row(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.record_batch_failure("sig", max_attempts=3)
            store.clear_batch_failure("sig")
            self.assertFalse(store.is_batch_quarantined("sig"))
            # And further failures start fresh:
            count = store.record_batch_failure("sig", max_attempts=3)
            self.assertEqual(count, 1)
            store.close()

    def test_quarantined_batches_returns_only_quarantined(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.record_batch_failure("retrying", max_attempts=5)
            for _ in range(3):
                store.record_batch_failure("quarantined", max_attempts=3)
            quar = store.quarantined_batches()
            self.assertEqual(len(quar), 1)
            self.assertEqual(quar[0].batch_signature, "quarantined")
            store.close()


class BatchSignatureTests(unittest.TestCase):
    def test_signature_is_order_independent(self) -> None:
        a = compute_batch_signature([("a", "h1"), ("b", "h2")])
        b = compute_batch_signature([("b", "h2"), ("a", "h1")])
        self.assertEqual(a, b)

    def test_signature_differs_when_contents_differ(self) -> None:
        a = compute_batch_signature([("a", "h1"), ("b", "h2")])
        b = compute_batch_signature([("a", "h1"), ("b", "DIFFERENT")])
        self.assertNotEqual(a, b)

    def test_signature_differs_for_different_path(self) -> None:
        a = compute_batch_signature([("a", "h1")])
        b = compute_batch_signature([("DIFFERENT", "h1")])
        self.assertNotEqual(a, b)

    def test_empty_input_gives_consistent_hash(self) -> None:
        a = compute_batch_signature([])
        b = compute_batch_signature([])
        self.assertEqual(a, b)


class FileHashTests(unittest.TestCase):
    def test_known_content_hash(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "f.txt"
            path.write_text("hello", encoding="utf-8")
            # sha256("hello") = 2cf24d...
            expected = (
                "2cf24dba5fb0a30e26e83b2ac5b9e29e"
                "1b161e5c1fa7425e73043362938b9824"
            )
            self.assertEqual(compute_file_hash(path), expected)


class AlertHistoryTests(unittest.TestCase):
    def test_no_alert_returns_none(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            self.assertIsNone(store.seconds_since_last_alert("quarantine"))
            store.close()

    def test_recorded_alert_returns_recent_elapsed(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.record_alert_sent("quarantine", "Subj A")
            elapsed = store.seconds_since_last_alert("quarantine")
            self.assertIsNotNone(elapsed)
            # With second-precision timestamps, elapsed can be 0–1s
            # depending on subsecond luck. Bound generously.
            self.assertGreaterEqual(elapsed, 0.0)
            self.assertLess(elapsed, 5.0)
            store.close()

    def test_categories_are_independent(self) -> None:
        with TemporaryDirectory() as tmp:
            store = _store(tmp)
            store.record_alert_sent("quarantine", "Q")
            self.assertIsNotNone(
                store.seconds_since_last_alert("quarantine")
            )
            self.assertIsNone(
                store.seconds_since_last_alert("catastrophic")
            )
            store.close()


class ConstructionErrorsTests(unittest.TestCase):
    def test_unwritable_db_path_raises(self) -> None:
        with self.assertRaises(StateError):
            # Path under a file (not a directory) — open will fail.
            StateStore(Path("/dev/null/not-a-real-path/db.sqlite"))


if __name__ == "__main__":
    unittest.main()
