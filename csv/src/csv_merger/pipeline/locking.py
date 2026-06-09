"""Cross-platform exclusive lockfile.

The runner uses this to prevent overlapping pipeline runs. The 2-minute
tick cadence means a long-running run can collide with the next tick;
we want the colliding run to exit cleanly rather than process the same
files concurrently.

Implementation: ``os.open(path, O_CREAT | O_EXCL | O_RDWR)`` is atomic
on both POSIX and Windows. The PID of the holder is written into the
file. On startup, if the file already exists, the recorded PID is
checked for liveness:

* PID alive            → lock is genuinely held; raise :class:`LockHeldError`.
* PID dead             → previous holder crashed; reclaim and proceed.
* PID file unreadable  → conservative: treat as held; require manual cleanup.

This approach has no external dependencies and works identically on
Windows (where ``fcntl`` is unavailable). It does NOT protect against
two processes on different machines pointing at the same lockfile on a
network share — that requires a real distributed lock, out of scope for
this pipeline (one process per host).
"""

from __future__ import annotations

import errno
import logging
import os
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType
from typing import Iterator

from csv_merger.pipeline._errors import LockHeldError

logger = logging.getLogger(__name__)


class FileLock:
    """Exclusive lockfile with PID-based crash recovery.

    Use as a context manager:

    .. code-block:: python

        with FileLock(Path("pipeline.lock")):
            run_pipeline()

    On entry, acquires the lock or raises :class:`LockHeldError`. On
    exit, releases the lock (deletes the file) regardless of whether
    the wrapped block raised.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fd: int | None = None

    def acquire(self) -> None:
        """Acquire the lock or raise :class:`LockHeldError`."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._fd = os.open(
                self._path,
                os.O_CREAT | os.O_EXCL | os.O_RDWR,
            )
        except FileExistsError:
            self._handle_existing_lock()
            # _handle_existing_lock either raises (held) or reclaims and
            # falls through; recurse to retry the open atomically.
            self.acquire()
            return

        try:
            os.write(self._fd, f"{os.getpid()}\n".encode("ascii"))
            os.fsync(self._fd)
        except OSError:
            # If we couldn't write the PID, release the lock so we don't
            # leave a zero-byte file the next process can't reclaim.
            self.release()
            raise
        logger.debug("acquired lock: %s (pid=%d)", self._path, os.getpid())

    def release(self) -> None:
        """Release the lock. Idempotent."""
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None
        try:
            self._path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            logger.warning(
                "failed to unlink lockfile %s: %s", self._path, exc
            )

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.release()

    # ------------------------------------------------------------------ #
    # Crash recovery                                                      #
    # ------------------------------------------------------------------ #

    def _handle_existing_lock(self) -> None:
        """Decide whether an existing lockfile is stale and can be reclaimed.

        Reads the PID from the file. If the process is alive, raises
        :class:`LockHeldError`. If dead, deletes the file so the caller
        can retry. If the file is unreadable, errs on the side of
        treating the lock as held — manual cleanup is safer than racing.
        """
        try:
            pid_text = self._path.read_text(encoding="ascii").strip()
            pid = int(pid_text)
        except (OSError, ValueError) as exc:
            raise LockHeldError(
                f"lockfile {self._path} exists but is unreadable "
                f"({exc}); manual cleanup required"
            ) from exc

        if _pid_alive(pid):
            raise LockHeldError(
                f"lock held by live process {pid} (lockfile: {self._path})"
            )

        logger.warning(
            "reclaiming stale lockfile %s (pid %d is dead)",
            self._path,
            pid,
        )
        try:
            self._path.unlink()
        except FileNotFoundError:
            # Lost a race with another reclaimer; the next os.open will
            # either succeed for us or fail and re-enter recovery.
            pass


# --------------------------------------------------------------------------- #
# PID liveness                                                                #
# --------------------------------------------------------------------------- #


def _pid_alive(pid: int) -> bool:
    """Return ``True`` if a process with ``pid`` is alive on this machine.

    ``os.kill(pid, 0)`` is the portable check:

    * Success → the process exists (alive).
    * :data:`ESRCH` → no such process (dead).
    * :data:`EPERM` → the process exists but we lack permission; treat
      as alive (conservative).

    On Windows, ``os.kill(pid, 0)`` is implemented via ``TerminateProcess``
    semantics and follows the same error-code conventions.
    """
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        if exc.errno == errno.EPERM:
            return True
        # Any other error: conservative — treat as alive.
        return True
    return True


# --------------------------------------------------------------------------- #
# Convenience context manager                                                 #
# --------------------------------------------------------------------------- #


@contextmanager
def acquire_lock(path: Path) -> Iterator[FileLock]:
    """Context-manager convenience wrapper around :class:`FileLock`."""
    lock = FileLock(path)
    lock.acquire()
    try:
        yield lock
    finally:
        lock.release()
