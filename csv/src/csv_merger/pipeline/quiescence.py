"""Wait for a source folder to stop changing.

The producer drops files into a network folder; we want to start
processing only once the producer is done. The contract is a heuristic:
"if the folder's snapshot hasn't changed in ``quiet_seconds``, assume
the producer is finished."

A snapshot is the tuple ``(path, size)`` for every file matching the
glob, sorted. We deliberately don't use mtime as the primary signal:
network filesystems (SMB especially) can report stale or imprecise
mtimes, but file size always reflects what's been written.

This is a heuristic, not a proof. If the producer pauses for more than
``quiet_seconds`` mid-batch, we may start early. The producer-side fix
(write to ``staging/``, atomically rename into the watched folder) is
strictly better; consider this module a workaround for when you don't
control the producer.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from pathlib import Path

from csv_merger.pipeline._errors import QuiescenceTimeout

logger = logging.getLogger(__name__)

#: Snapshot type: sorted tuple of ``(path_str, size_bytes)``.
Snapshot = tuple[tuple[str, int], ...]


def wait_for_quiescence(
    folder: Path,
    pattern: str = "*",
    *,
    quiet_seconds: int,
    max_wait_seconds: int,
    poll_interval_seconds: int = 5,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> list[Path]:
    """Block until ``folder`` is quiet, then return the matching files.

    Args:
        folder: Folder to watch.
        pattern: Glob pattern (passed to :meth:`pathlib.Path.glob`).
        quiet_seconds: How long the snapshot must remain unchanged
            before we declare quiescence.
        max_wait_seconds: Give up after this many wall-clock seconds and
            raise :class:`QuiescenceTimeout`.
        poll_interval_seconds: How often to re-snapshot.
        clock: Monotonic time source, swappable for tests.
        sleep: Sleep function, swappable for tests.

    Returns:
        The list of matching files at the moment quiescence was reached,
        sorted by path. Empty list is valid: an empty folder is quiet.

    Raises:
        QuiescenceTimeout: if ``max_wait_seconds`` elapses without the
            snapshot stabilising.
    """
    if quiet_seconds < 0 or max_wait_seconds < 0 or poll_interval_seconds <= 0:
        raise ValueError(
            "quiet_seconds and max_wait_seconds must be ≥ 0, "
            "poll_interval_seconds must be > 0"
        )

    start = clock()
    last_snapshot: Snapshot | None = None
    last_change: float = start

    while True:
        snapshot = _take_snapshot(folder, pattern)
        now = clock()

        if last_snapshot is None or snapshot != last_snapshot:
            if last_snapshot is not None:
                logger.debug(
                    "folder %s changed: %d → %d files",
                    folder,
                    len(last_snapshot),
                    len(snapshot),
                )
            last_snapshot = snapshot
            last_change = now
        elif now - last_change >= quiet_seconds:
            logger.info(
                "folder %s quiet for %.1fs with %d files",
                folder,
                now - last_change,
                len(snapshot),
            )
            return [Path(p) for p, _ in snapshot]

        if now - start >= max_wait_seconds:
            raise QuiescenceTimeout(
                f"{folder} did not quiesce within {max_wait_seconds}s "
                f"(last change at +{last_change - start:.1f}s, "
                f"{len(snapshot)} files in final snapshot)"
            )

        sleep(poll_interval_seconds)


def _take_snapshot(folder: Path, pattern: str) -> Snapshot:
    """Return a deterministic ``(path, size)`` snapshot of the folder.

    Missing folder is treated as "empty snapshot" rather than an error:
    the producer may not have created the folder yet, and quiescence
    will simply return immediately once max_wait elapses (or earlier if
    the folder appears with content that stabilises).

    Files we can't stat (permission, transient) are silently dropped
    from the snapshot — they'll either appear next poll or stay
    inaccessible (in which case the runner will fail at fetch time
    with a clearer error than "stat failed during quiescence").
    """
    if not folder.exists():
        return ()
    entries: list[tuple[str, int]] = []
    for path in folder.glob(pattern):
        if not path.is_file():
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        entries.append((str(path), size))
    entries.sort()
    return tuple(entries)
