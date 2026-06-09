"""Tests for csv_merger.pipeline.locking.

These tests run on POSIX in CI; the Windows path uses the same code
(``os.open`` with ``O_CREAT | O_EXCL`` and ``os.kill(pid, 0)`` work on
both platforms) but the actual Windows behaviour will be confirmed when
the operator runs the suite locally.
"""

from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from csv_merger.pipeline._errors import LockHeldError
from csv_merger.pipeline.locking import FileLock, _pid_alive, acquire_lock


class FileLockBasicTests(unittest.TestCase):
    def test_acquire_then_release_creates_and_removes_file(self) -> None:
        with TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "p.lock"
            lock = FileLock(lock_path)
            lock.acquire()
            self.assertTrue(lock_path.exists())
            lock.release()
            self.assertFalse(lock_path.exists())

    def test_pid_recorded_in_file(self) -> None:
        with TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "p.lock"
            with FileLock(lock_path):
                content = lock_path.read_text(encoding="ascii").strip()
            self.assertEqual(int(content), os.getpid())

    def test_context_manager_releases_on_exception(self) -> None:
        with TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "p.lock"
            with self.assertRaises(RuntimeError):
                with FileLock(lock_path):
                    raise RuntimeError("boom")
            self.assertFalse(lock_path.exists())

    def test_release_is_idempotent(self) -> None:
        with TemporaryDirectory() as tmp:
            lock = FileLock(Path(tmp) / "p.lock")
            lock.acquire()
            lock.release()
            lock.release()  # should not raise


class FileLockContentionTests(unittest.TestCase):
    def test_second_acquire_while_held_raises(self) -> None:
        with TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "p.lock"
            first = FileLock(lock_path)
            first.acquire()
            try:
                second = FileLock(lock_path)
                with self.assertRaises(LockHeldError):
                    second.acquire()
            finally:
                first.release()

    def test_stale_lock_with_dead_pid_is_reclaimed(self) -> None:
        """Simulate a crashed previous holder by writing a dead PID."""
        with TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "p.lock"
            # PID 1 belongs to init on POSIX (and definitely won't be
            # `os.getpid()`'s neighbour). We need a guaranteed-dead PID;
            # picking max PID + something is fraught. Use a very large
            # number that's practically guaranteed not to exist.
            dead_pid = 2**31 - 1  # max signed int, far beyond any real PID
            lock_path.write_text(f"{dead_pid}\n", encoding="ascii")

            # Acquire should succeed by reclaiming the stale lock.
            with FileLock(lock_path):
                # And it should now contain *our* PID.
                self.assertEqual(
                    int(lock_path.read_text(encoding="ascii").strip()),
                    os.getpid(),
                )

    def test_unreadable_lock_file_is_treated_as_held(self) -> None:
        """A garbage lockfile errs on the side of holding."""
        with TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "p.lock"
            lock_path.write_text("not a pid\n", encoding="ascii")
            with self.assertRaises(LockHeldError):
                FileLock(lock_path).acquire()


class PidAliveTests(unittest.TestCase):
    def test_own_pid_alive(self) -> None:
        self.assertTrue(_pid_alive(os.getpid()))

    def test_obviously_dead_pid(self) -> None:
        self.assertFalse(_pid_alive(2**31 - 1))

    def test_invalid_pid(self) -> None:
        self.assertFalse(_pid_alive(0))
        self.assertFalse(_pid_alive(-1))


class AcquireLockHelperTests(unittest.TestCase):
    def test_context_manager_yields_filelock(self) -> None:
        with TemporaryDirectory() as tmp:
            with acquire_lock(Path(tmp) / "p.lock") as lock:
                self.assertIsInstance(lock, FileLock)


if __name__ == "__main__":
    unittest.main()
