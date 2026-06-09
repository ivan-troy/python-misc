"""Tests for csv_merger.pipeline.quiescence."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from csv_merger.pipeline._errors import QuiescenceTimeout
from csv_merger.pipeline.quiescence import _take_snapshot, wait_for_quiescence


class FakeClock:
    """Manually-advanced monotonic clock for deterministic tests."""

    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class SnapshotTests(unittest.TestCase):
    def test_empty_folder_returns_empty(self) -> None:
        with TemporaryDirectory() as tmp:
            self.assertEqual(_take_snapshot(Path(tmp), "*"), ())

    def test_missing_folder_returns_empty(self) -> None:
        self.assertEqual(_take_snapshot(Path("/no/such/path"), "*"), ())

    def test_snapshot_orders_by_path_for_determinism(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "z.txt").write_text("hi")
            (tmp_path / "a.txt").write_text("hi")
            (tmp_path / "m.txt").write_text("hi")
            snap = _take_snapshot(tmp_path, "*")
            paths = [p for p, _ in snap]
            self.assertEqual(paths, sorted(paths))

    def test_pattern_filters(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "a.csv").write_text("hi")
            (tmp_path / "b.txt").write_text("hi")
            snap = _take_snapshot(tmp_path, "*.csv")
            self.assertEqual(len(snap), 1)
            self.assertTrue(snap[0][0].endswith(".csv"))


class WaitForQuiescenceTests(unittest.TestCase):
    def _setup(self) -> tuple[Path, FakeClock, list[float]]:
        """Return (folder, clock, sleep-log)."""
        return Path("/tmp"), FakeClock(), []

    def test_empty_stable_folder_returns_immediately_after_quiet_period(self) -> None:
        clock = FakeClock()
        sleeps: list[float] = []
        # Advance time on each sleep so the loop progresses.
        def sleep(s: float) -> None:
            sleeps.append(s)
            clock.advance(s)

        with TemporaryDirectory() as tmp:
            result = wait_for_quiescence(
                Path(tmp),
                quiet_seconds=10,
                max_wait_seconds=60,
                poll_interval_seconds=5,
                clock=clock,
                sleep=sleep,
            )
        self.assertEqual(result, [])
        # The very first iteration sets last_change=0; the next
        # iteration (after one sleep) finds the snapshot unchanged and
        # checks (now - last_change >= 10). With 5-second poll, that's
        # the second poll at t=5 — not yet. Third poll at t=10 — yes.
        # So we expect at least 2 sleeps (sometimes 3 depending on
        # exact comparison).
        self.assertGreaterEqual(len(sleeps), 2)

    def test_growing_file_keeps_resetting_quiet_window(self) -> None:
        clock = FakeClock()
        with TemporaryDirectory() as tmp:
            folder = Path(tmp)
            f = folder / "growing.txt"
            f.write_bytes(b"x" * 10)
            iteration = [0]

            def sleep(s: float) -> None:
                clock.advance(s)
                iteration[0] += 1
                # Keep changing size for a while, then stop.
                if iteration[0] <= 3:
                    f.write_bytes(b"x" * (10 + iteration[0] * 10))

            result = wait_for_quiescence(
                folder,
                quiet_seconds=10,
                max_wait_seconds=120,
                poll_interval_seconds=5,
                clock=clock,
                sleep=sleep,
            )
            self.assertEqual(len(result), 1)
            # By the time we got here, growth has stopped.

    def test_timeout_raises_quiescence_timeout(self) -> None:
        clock = FakeClock()
        with TemporaryDirectory() as tmp:
            folder = Path(tmp)
            f = folder / "growing.txt"
            f.write_bytes(b"x")
            iteration = [0]

            def sleep(s: float) -> None:
                clock.advance(s)
                iteration[0] += 1
                # Grow forever — never quiesces.
                f.write_bytes(b"x" * (iteration[0] + 1))

            with self.assertRaises(QuiescenceTimeout) as ctx:
                wait_for_quiescence(
                    folder,
                    quiet_seconds=10,
                    max_wait_seconds=30,
                    poll_interval_seconds=5,
                    clock=clock,
                    sleep=sleep,
                )
            self.assertIn("did not quiesce", str(ctx.exception))

    def test_invalid_args_rejected(self) -> None:
        with self.assertRaises(ValueError):
            wait_for_quiescence(
                Path("/tmp"),
                quiet_seconds=-1,
                max_wait_seconds=30,
            )
        with self.assertRaises(ValueError):
            wait_for_quiescence(
                Path("/tmp"),
                quiet_seconds=10,
                max_wait_seconds=30,
                poll_interval_seconds=0,
            )


if __name__ == "__main__":
    unittest.main()
