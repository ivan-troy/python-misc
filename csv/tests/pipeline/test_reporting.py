"""Tests for csv_merger.pipeline.reporting."""

from __future__ import annotations

import io
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from csv_merger.pipeline.reporting import (
    _format_duration,
    _short_ts,
    render_report,
    render_report_from_db,
)
from csv_merger.pipeline.state import (
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCESS,
    StateStore,
)


def _store(tmp: str) -> StateStore:
    return StateStore(Path(tmp) / "p.db")


class RenderReportTests(unittest.TestCase):
    def test_empty_state_renders_without_error(self) -> None:
        with TemporaryDirectory() as tmp:
            state = _store(tmp)
            try:
                report = render_report(state)
            finally:
                state.close()
            self.assertIn("Recent runs (last 0)", report)
            self.assertIn("(no runs recorded)", report)
            self.assertIn("Quarantined batches (0)", report)
            self.assertIn("(none)", report)

    def test_recent_runs_appear_newest_first(self) -> None:
        with TemporaryDirectory() as tmp:
            state = _store(tmp)
            try:
                r1 = state.start_run()
                state.finish_run(r1, status=RUN_STATUS_SUCCESS)
                r2 = state.start_run()
                state.finish_run(r2, status=RUN_STATUS_FAILED, error_text="x")
                report = render_report(state)
            finally:
                state.close()
            # r2 should appear before r1
            r1_pos = report.find(f" {r1}  ")
            r2_pos = report.find(f" {r2}  ")
            self.assertNotEqual(r1_pos, -1)
            self.assertNotEqual(r2_pos, -1)
            self.assertLess(r2_pos, r1_pos)

    def test_step_breakdown_for_last_run(self) -> None:
        with TemporaryDirectory() as tmp:
            state = _store(tmp)
            try:
                rid = state.start_run()
                with state.step(rid, "fetch"):
                    pass
                with state.step(rid, "merge"):
                    pass
                state.finish_run(rid, status=RUN_STATUS_SUCCESS)
                report = render_report(state)
            finally:
                state.close()
            self.assertIn(f"Steps for run {rid}:", report)
            self.assertIn("fetch", report)
            self.assertIn("merge", report)

    def test_quarantined_batches_shown(self) -> None:
        with TemporaryDirectory() as tmp:
            state = _store(tmp)
            try:
                sig = "f" * 64
                for _ in range(3):
                    state.record_batch_failure(
                        sig, max_attempts=3, error_text="oops"
                    )
                report = render_report(state)
            finally:
                state.close()
            self.assertIn("Quarantined batches (1)", report)
            self.assertIn("f" * 16, report)
            self.assertIn("oops", report)

    def test_writes_to_provided_stream(self) -> None:
        with TemporaryDirectory() as tmp:
            state = _store(tmp)
            try:
                stream = io.StringIO()
                returned = render_report(state, out=stream)
            finally:
                state.close()
            self.assertEqual(stream.getvalue(), returned)
            self.assertIn("Recent runs", stream.getvalue())

    def test_render_report_from_db_opens_and_closes(self) -> None:
        with TemporaryDirectory() as tmp:
            # Pre-seed the DB
            state = _store(tmp)
            try:
                rid = state.start_run()
                state.finish_run(rid, status=RUN_STATUS_SUCCESS)
            finally:
                state.close()

            # Now render via the convenience function
            report = render_report_from_db(Path(tmp) / "p.db")
            self.assertIn("Recent runs (last 1)", report)


class FormatDurationTests(unittest.TestCase):
    def test_none_finished_returns_dash(self) -> None:
        self.assertEqual(_format_duration("2026-01-01T00:00:00+00:00", None), "—")

    def test_subsecond_in_ms(self) -> None:
        result = _format_duration(
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:00:00+00:00",
        )
        # Same timestamp → 0ms
        self.assertEqual(result, "0 ms")

    def test_seconds_with_decimal(self) -> None:
        result = _format_duration(
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:00:12+00:00",
        )
        self.assertEqual(result, "12.0s")

    def test_minutes_and_seconds(self) -> None:
        result = _format_duration(
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:02:30+00:00",
        )
        self.assertEqual(result, "2m30s")

    def test_unparseable_returns_question(self) -> None:
        self.assertEqual(_format_duration("bogus", "2026-01-01T00:00:00"), "?")


class ShortTimestampTests(unittest.TestCase):
    def test_strips_utc_suffix(self) -> None:
        self.assertEqual(
            _short_ts("2026-01-01T12:34:56+00:00"),
            "2026-01-01T12:34:56",
        )

    def test_leaves_other_suffixes_alone(self) -> None:
        self.assertEqual(
            _short_ts("2026-01-01T12:34:56+05:30"),
            "2026-01-01T12:34:56+05:30",
        )

    def test_leaves_naive_alone(self) -> None:
        self.assertEqual(
            _short_ts("2026-01-01T12:34:56"),
            "2026-01-01T12:34:56",
        )


if __name__ == "__main__":
    unittest.main()
