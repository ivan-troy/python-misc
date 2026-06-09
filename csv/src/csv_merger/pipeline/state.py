"""SQLite-backed pipeline state.

The state store is the resilience anchor for the pipeline. It persists:

* Every run, with its overall status and any error.
* Every step within a run, with its duration and status.
* Every source file successfully processed (path + content hash), so
  the next run can skip it.
* Every failed batch with its attempt count, supporting the quarantine
  policy.

Design choices worth flagging:

* **One SQLite file.** Not on the network share — keep on local disk
  for WAL safety. ``journal_mode=WAL`` gives us crash-safe writes and
  concurrent reads.
* **One connection per process.** No pooling needed — the runner is
  single-threaded for state operations.
* **Step timing via context manager.** ``with state.step(run_id, "fetch"):``
  records start/end/duration with no manual bookkeeping at call sites.
* **Bulk inserts in transactions.** Marking 500 files processed runs in
  one transaction, not 500 — both for speed and for atomicity (either
  all or none get recorded).

The schema is created idempotently at construction time. There is no
migration system: if you change the schema, point ``db_path`` at a new
file and let the next run rebuild. Given the per-run cadence and the
``processed_files`` semantics, the worst case of throwing away the DB
is "one batch gets re-processed", which is already idempotent at the
publish layer via the outbox pattern.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

from csv_merger.pipeline._errors import StateError

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Schema                                                                      #
# --------------------------------------------------------------------------- #


_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at       TEXT NOT NULL,
    finished_at      TEXT,
    status           TEXT NOT NULL,
    batch_signature  TEXT,
    error_text       TEXT
);

CREATE TABLE IF NOT EXISTS run_steps (
    run_id       INTEGER NOT NULL,
    step_name    TEXT NOT NULL,
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    duration_ms  INTEGER,
    status       TEXT NOT NULL,
    error_text   TEXT,
    PRIMARY KEY (run_id, step_name),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS processed_files (
    source_path   TEXT NOT NULL,
    content_hash  TEXT NOT NULL,
    run_id        INTEGER NOT NULL,
    processed_at  TEXT NOT NULL,
    PRIMARY KEY (source_path, content_hash),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS failed_batches (
    batch_signature  TEXT PRIMARY KEY,
    first_failure_at TEXT NOT NULL,
    last_failure_at  TEXT NOT NULL,
    attempt_count    INTEGER NOT NULL,
    last_error       TEXT,
    status           TEXT NOT NULL
);

-- alert_history backs the rate-limit decision in alerting.py.
-- Categories: 'quarantine' | 'catastrophic'. One row per send.
CREATE TABLE IF NOT EXISTS alert_history (
    alert_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    category  TEXT NOT NULL,
    sent_at   TEXT NOT NULL,
    subject   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runs_status_started
    ON runs(status, started_at);
CREATE INDEX IF NOT EXISTS idx_runs_batch_sig
    ON runs(batch_signature);
CREATE INDEX IF NOT EXISTS idx_processed_files_run
    ON processed_files(run_id);
CREATE INDEX IF NOT EXISTS idx_alert_history_cat_time
    ON alert_history(category, sent_at);
"""

# Run status values.
RUN_STATUS_RUNNING = "running"
RUN_STATUS_SUCCESS = "success"
RUN_STATUS_FAILED = "failed"

# Step status values.
STEP_STATUS_SUCCESS = "success"
STEP_STATUS_FAILED = "failed"
STEP_STATUS_SKIPPED = "skipped"

# Batch status values.
BATCH_STATUS_RETRYING = "retrying"
BATCH_STATUS_QUARANTINED = "quarantined"


# --------------------------------------------------------------------------- #
# Public types                                                                #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class RunRecord:
    """A row from the ``runs`` table, for reporting."""

    run_id: int
    started_at: str
    finished_at: str | None
    status: str
    batch_signature: str | None
    error_text: str | None


@dataclass(frozen=True)
class StepRecord:
    """A row from the ``run_steps`` table, for reporting."""

    run_id: int
    step_name: str
    started_at: str
    finished_at: str | None
    duration_ms: int | None
    status: str
    error_text: str | None


@dataclass(frozen=True)
class FailedBatchRecord:
    """A row from the ``failed_batches`` table."""

    batch_signature: str
    first_failure_at: str
    last_failure_at: str
    attempt_count: int
    last_error: str | None
    status: str


# --------------------------------------------------------------------------- #
# Batch signatures                                                            #
# --------------------------------------------------------------------------- #


def compute_batch_signature(files: Iterable[tuple[str, str]]) -> str:
    """Compute a deterministic signature for a batch of ``(path, hash)`` tuples.

    Order-independent: the same set of files always produces the same
    signature regardless of iteration order. Used as the key in
    :class:`failed_batches` and as the identity of an outbox file.
    """
    sorted_pairs = sorted(files)
    hasher = hashlib.sha256()
    for path, content_hash in sorted_pairs:
        hasher.update(path.encode("utf-8"))
        hasher.update(b"\x00")
        hasher.update(content_hash.encode("utf-8"))
        hasher.update(b"\x00")
    return hasher.hexdigest()


def compute_file_hash(path: Path, *, chunk_size: int = 65536) -> str:
    """Return the SHA-256 of a file's contents as a hex string."""
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


# --------------------------------------------------------------------------- #
# StateStore                                                                  #
# --------------------------------------------------------------------------- #


class StateStore:
    """SQLite-backed pipeline state.

    Construct once per process; close on exit. The connection is held
    open for the process lifetime — for the per-tick CLI, that's a few
    seconds of life.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        try:
            db_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise StateError(
                f"cannot create parent dir for state DB {db_path}: {exc}"
            ) from exc
        try:
            self._conn = sqlite3.connect(
                db_path,
                isolation_level=None,  # autocommit; we manage transactions
                detect_types=0,
            )
        except sqlite3.Error as exc:
            raise StateError(f"cannot open state DB {db_path}: {exc}") from exc

        self._conn.row_factory = sqlite3.Row
        self._apply_pragmas()
        self._apply_schema()

    def close(self) -> None:
        """Close the underlying connection. Idempotent."""
        try:
            self._conn.close()
        except sqlite3.Error:
            pass

    def __enter__(self) -> "StateStore":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Setup                                                              #
    # ------------------------------------------------------------------ #

    def _apply_pragmas(self) -> None:
        """Enable WAL + sensible durability settings."""
        for pragma in (
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
            "PRAGMA foreign_keys=ON",
        ):
            try:
                self._conn.execute(pragma)
            except sqlite3.Error as exc:
                raise StateError(f"pragma {pragma!r} failed: {exc}") from exc

    def _apply_schema(self) -> None:
        try:
            self._conn.executescript(_SCHEMA)
        except sqlite3.Error as exc:
            raise StateError(f"schema setup failed: {exc}") from exc

    # ------------------------------------------------------------------ #
    # Run lifecycle                                                      #
    # ------------------------------------------------------------------ #

    def start_run(self) -> int:
        """Insert a new ``runs`` row and return its ID."""
        now = _utc_now_iso()
        cur = self._conn.execute(
            "INSERT INTO runs (started_at, status) VALUES (?, ?)",
            (now, RUN_STATUS_RUNNING),
        )
        run_id = cur.lastrowid
        if run_id is None:  # pragma: no cover — sqlite always returns one
            raise StateError("INSERT into runs did not return a run_id")
        logger.debug("started run %d at %s", run_id, now)
        return run_id

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        error_text: str | None = None,
        batch_signature: str | None = None,
    ) -> None:
        """Mark a run finished with its terminal status.

        ``batch_signature`` may be set after the run started, once we
        know which files we attempted. ``None`` leaves it unchanged.
        """
        now = _utc_now_iso()
        if batch_signature is None:
            self._conn.execute(
                """
                UPDATE runs
                   SET finished_at = ?, status = ?, error_text = ?
                 WHERE run_id = ?
                """,
                (now, status, error_text, run_id),
            )
        else:
            self._conn.execute(
                """
                UPDATE runs
                   SET finished_at = ?, status = ?, error_text = ?,
                       batch_signature = ?
                 WHERE run_id = ?
                """,
                (now, status, error_text, batch_signature, run_id),
            )

    # ------------------------------------------------------------------ #
    # Step timing                                                        #
    # ------------------------------------------------------------------ #

    @contextmanager
    def step(
        self,
        run_id: int,
        step_name: str,
    ) -> Iterator[None]:
        """Time a step and record it in the ``run_steps`` table.

        On exit:
        * success → records status ``success``.
        * exception → records status ``failed`` with the exception text,
          then re-raises.
        """
        started_at = _utc_now_iso()
        start = time.monotonic()
        self._conn.execute(
            """
            INSERT INTO run_steps
                (run_id, step_name, started_at, status)
            VALUES (?, ?, ?, ?)
            """,
            (run_id, step_name, started_at, RUN_STATUS_RUNNING),
        )
        try:
            yield
        except BaseException as exc:
            duration_ms = int((time.monotonic() - start) * 1000)
            self._conn.execute(
                """
                UPDATE run_steps
                   SET finished_at = ?, duration_ms = ?,
                       status = ?, error_text = ?
                 WHERE run_id = ? AND step_name = ?
                """,
                (
                    _utc_now_iso(),
                    duration_ms,
                    STEP_STATUS_FAILED,
                    f"{type(exc).__name__}: {exc}",
                    run_id,
                    step_name,
                ),
            )
            raise
        else:
            duration_ms = int((time.monotonic() - start) * 1000)
            self._conn.execute(
                """
                UPDATE run_steps
                   SET finished_at = ?, duration_ms = ?, status = ?
                 WHERE run_id = ? AND step_name = ?
                """,
                (
                    _utc_now_iso(),
                    duration_ms,
                    STEP_STATUS_SUCCESS,
                    run_id,
                    step_name,
                ),
            )

    # ------------------------------------------------------------------ #
    # processed_files                                                    #
    # ------------------------------------------------------------------ #

    def is_processed(self, source_path: str, content_hash: str) -> bool:
        """Return ``True`` if this exact ``(path, hash)`` has been processed."""
        row = self._conn.execute(
            """
            SELECT 1 FROM processed_files
             WHERE source_path = ? AND content_hash = ?
            """,
            (source_path, content_hash),
        ).fetchone()
        return row is not None

    def mark_processed(
        self,
        run_id: int,
        files: Iterable[tuple[str, str]],
    ) -> int:
        """Insert ``(source_path, content_hash)`` rows for the given run.

        Wrapped in a transaction so the batch is recorded atomically:
        either every file is marked processed or none is.

        Returns the number of rows inserted. ``ON CONFLICT DO NOTHING``
        means re-marking an already-processed file is a no-op.
        """
        rows = list(files)
        if not rows:
            return 0
        now = _utc_now_iso()
        params = [(path, h, run_id, now) for path, h in rows]
        self._conn.execute("BEGIN")
        try:
            cur = self._conn.executemany(
                """
                INSERT INTO processed_files
                    (source_path, content_hash, run_id, processed_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (source_path, content_hash) DO NOTHING
                """,
                params,
            )
            self._conn.execute("COMMIT")
        except sqlite3.Error:
            self._conn.execute("ROLLBACK")
            raise
        return cur.rowcount

    # ------------------------------------------------------------------ #
    # failed_batches                                                     #
    # ------------------------------------------------------------------ #

    def record_batch_failure(
        self,
        batch_signature: str,
        *,
        max_attempts: int,
        error_text: str | None = None,
    ) -> int:
        """Record a failure for ``batch_signature`` and return the new count.

        If ``attempt_count`` reaches ``max_attempts``, the batch's
        status is updated to ``quarantined``. The caller decides what to
        do with that (typically: alert).
        """
        now = _utc_now_iso()
        existing = self._conn.execute(
            "SELECT attempt_count FROM failed_batches "
            "WHERE batch_signature = ?",
            (batch_signature,),
        ).fetchone()

        if existing is None:
            self._conn.execute(
                """
                INSERT INTO failed_batches
                    (batch_signature, first_failure_at, last_failure_at,
                     attempt_count, last_error, status)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_signature, now, now,
                    1, error_text, BATCH_STATUS_RETRYING,
                ),
            )
            return 1

        new_count = int(existing["attempt_count"]) + 1
        status = (
            BATCH_STATUS_QUARANTINED
            if new_count >= max_attempts
            else BATCH_STATUS_RETRYING
        )
        self._conn.execute(
            """
            UPDATE failed_batches
               SET last_failure_at = ?, attempt_count = ?,
                   last_error = ?, status = ?
             WHERE batch_signature = ?
            """,
            (now, new_count, error_text, status, batch_signature),
        )
        return new_count

    def is_batch_quarantined(self, batch_signature: str) -> bool:
        """Return ``True`` if this batch has been quarantined."""
        row = self._conn.execute(
            """
            SELECT status FROM failed_batches
             WHERE batch_signature = ? AND status = ?
            """,
            (batch_signature, BATCH_STATUS_QUARANTINED),
        ).fetchone()
        return row is not None

    def clear_batch_failure(self, batch_signature: str) -> None:
        """Delete a row from ``failed_batches`` (e.g. on retry success)."""
        self._conn.execute(
            "DELETE FROM failed_batches WHERE batch_signature = ?",
            (batch_signature,),
        )

    # ------------------------------------------------------------------ #
    # Reporting queries                                                  #
    # ------------------------------------------------------------------ #

    def recent_runs(self, limit: int = 10) -> list[RunRecord]:
        """Return the most recent ``limit`` runs, newest first."""
        rows = self._conn.execute(
            """
            SELECT run_id, started_at, finished_at, status,
                   batch_signature, error_text
              FROM runs
             ORDER BY run_id DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [_row_to_run(r) for r in rows]

    def steps_for_run(self, run_id: int) -> list[StepRecord]:
        """Return all step records for a given run, in start order."""
        rows = self._conn.execute(
            """
            SELECT run_id, step_name, started_at, finished_at,
                   duration_ms, status, error_text
              FROM run_steps
             WHERE run_id = ?
             ORDER BY started_at
            """,
            (run_id,),
        ).fetchall()
        return [_row_to_step(r) for r in rows]

    def quarantined_batches(self) -> list[FailedBatchRecord]:
        """Return all batches currently in ``quarantined`` status."""
        rows = self._conn.execute(
            """
            SELECT batch_signature, first_failure_at, last_failure_at,
                   attempt_count, last_error, status
              FROM failed_batches
             WHERE status = ?
             ORDER BY last_failure_at DESC
            """,
            (BATCH_STATUS_QUARANTINED,),
        ).fetchall()
        return [_row_to_failed_batch(r) for r in rows]

    # ------------------------------------------------------------------ #
    # alert_history                                                      #
    # ------------------------------------------------------------------ #

    def seconds_since_last_alert(self, category: str) -> float | None:
        """Return seconds elapsed since the last alert in ``category``.

        Returns ``None`` if no alert in this category has ever been
        recorded. The alerting module uses this for its rate-limit
        decision.
        """
        row = self._conn.execute(
            """
            SELECT sent_at FROM alert_history
             WHERE category = ?
             ORDER BY alert_id DESC
             LIMIT 1
            """,
            (category,),
        ).fetchone()
        if row is None:
            return None
        try:
            sent = datetime.fromisoformat(row["sent_at"])
        except ValueError:
            return None
        now = datetime.now(timezone.utc)
        return max(0.0, (now - sent).total_seconds())

    def record_alert_sent(self, category: str, subject: str) -> None:
        """Insert a row into ``alert_history`` after a successful send.

        The caller decides whether to record before or after the actual
        SMTP send. We recommend after — duplicate alerts on crash-mid-
        send are better than missed alerts on crash-after-record.
        """
        self._conn.execute(
            """
            INSERT INTO alert_history (category, sent_at, subject)
            VALUES (?, ?, ?)
            """,
            (category, _utc_now_iso(), subject),
        )


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _utc_now_iso() -> str:
    """ISO-8601 UTC timestamp with seconds precision, no microseconds.

    Microseconds add noise without helping when scanning logs by eye.
    Keep them out unless we hit a real performance need to distinguish
    sub-second events.
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _row_to_run(row: sqlite3.Row) -> RunRecord:
    return RunRecord(
        run_id=row["run_id"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        status=row["status"],
        batch_signature=row["batch_signature"],
        error_text=row["error_text"],
    )


def _row_to_step(row: sqlite3.Row) -> StepRecord:
    return StepRecord(
        run_id=row["run_id"],
        step_name=row["step_name"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        duration_ms=row["duration_ms"],
        status=row["status"],
        error_text=row["error_text"],
    )


def _row_to_failed_batch(row: sqlite3.Row) -> FailedBatchRecord:
    return FailedBatchRecord(
        batch_signature=row["batch_signature"],
        first_failure_at=row["first_failure_at"],
        last_failure_at=row["last_failure_at"],
        attempt_count=row["attempt_count"],
        last_error=row["last_error"],
        status=row["status"],
    )
