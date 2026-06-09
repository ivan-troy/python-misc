"""Pipeline runner — orchestrates one execution.

This module is the only place where the various pipeline modules
(state, locking, quiescence, transport, outbox, alerting) are wired
together. The runner is invoked once per scheduled tick by the
operating-system scheduler (Windows Task Scheduler, cron, systemd).

Run lifecycle (each named phase is recorded in ``state.run_steps``):

1. acquire lock (skipped run if held by another process)
2. ``cleanup`` — clean stale staging .tmp + age out old outbox/sent
3. record_run_start
4. ``drain_outbox`` — publish any prior-run files left in outbox/pending
5. ``quiescence`` — wait for source folder to settle
6. ``discover`` — list source files, filter against processed_files
7. ``fetch`` — parallel copy to local fetched/, with retry
8. ``parse`` — parse all fetched files
9. ``merge`` — run the merge pipeline (CsvMerger)
10. ``write_outbox`` — durable write of merged output + manifest
11. ``publish`` — HTTP PUT, on success move outbox file to sent/
12. ``mark_processed`` — record (path, hash) tuples in state
13. record_run_finished
14. release lock

Resilience:
* Lock held → exit cleanly without recording a failed run.
* Quiescence timeout → catastrophic alert (the source isn't behaving).
* Drain failure → abort current run (conservative ordering policy).
* Fetch/parse/merge/publish failure → record batch failure, increment
  attempt count, quarantine + alert if max attempts reached.
* Any unexpected exception → record run as failed, alert as catastrophic
  if no batch signature was assigned yet (failed pre-batch).
"""

from __future__ import annotations

import logging
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

from csv_merger.merger import CsvMerger
from csv_merger.pipeline._errors import (
    LockHeldError,
    PermanentError,
    PipelineError,
    QuiescenceTimeout,
    TransientError,
)
from csv_merger.pipeline.alerting import (
    ALERT_CATASTROPHIC,
    ALERT_QUARANTINE,
    Alerter,
)
from csv_merger.pipeline.config import PipelineConfig
from csv_merger.pipeline.locking import FileLock
from csv_merger.pipeline.outbox import (
    cleanup_old_sent,
    cleanup_staging_debris,
    list_pending,
    mark_published,
    read_manifest,
    write_to_outbox,
)
from csv_merger.pipeline.quiescence import wait_for_quiescence
from csv_merger.pipeline.state import (
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCESS,
    STEP_STATUS_SKIPPED,
    StateStore,
    compute_batch_signature,
    compute_file_hash,
)
from csv_merger.pipeline.transport import (
    fetch_files,
    publish_file,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Run result                                                                  #
# --------------------------------------------------------------------------- #


@dataclass
class RunResult:
    """Outcome of a single pipeline run, useful for tests and operators."""

    run_id: int | None = None
    status: str = RUN_STATUS_FAILED  # default to failed; success is explicit
    batch_signature: str | None = None
    files_processed: int = 0
    drained_outbox_count: int = 0
    error: str | None = None
    skipped_lock_held: bool = False
    skipped_quarantined: bool = False
    quarantined_now: bool = False
    alerts_sent: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Public entry point                                                          #
# --------------------------------------------------------------------------- #


def run_pipeline(config: PipelineConfig) -> RunResult:
    """Execute one pipeline run.

    Returns a :class:`RunResult` describing the outcome. **Never
    raises** for operational conditions (lock held, batch failed,
    publish blocked by pending) — those are surfaced via the result.
    Genuine programming bugs (e.g. ``AttributeError``) still propagate.
    """
    result = RunResult()

    # Lock acquisition is outside of run-state — we don't want to
    # record a "failed" run just because the previous run is still
    # going (which is normal at the 2-minute cadence).
    try:
        lock = FileLock(config.state.lock_path)
        lock.acquire()
    except LockHeldError as exc:
        logger.info("skipping run: %s", exc)
        result.skipped_lock_held = True
        result.error = str(exc)
        return result

    try:
        return _run_with_lock(config, result)
    finally:
        lock.release()


def _run_with_lock(config: PipelineConfig, result: RunResult) -> RunResult:
    """Body of the run, executed under the lock."""
    # StateStore init failure is catastrophic — without state we can't
    # safely record anything. Try to alert via SMTP directly (no state
    # rate-limit available, but the operator wants to know).
    try:
        state = StateStore(config.state.db_path)
    except Exception as exc:
        logger.exception("could not open state DB")
        result.error = f"state init failed: {exc}"
        _emergency_alert(
            config,
            "Pipeline catastrophic: state DB unavailable",
            f"State DB at {config.state.db_path} failed to open: {exc}\n\n"
            f"{traceback.format_exc()}",
        )
        return result

    try:
        with state:
            return _run_with_state(config, state, result)
    except Exception as exc:  # pragma: no cover — last-resort net
        logger.exception("unexpected error in pipeline runner")
        result.error = f"{type(exc).__name__}: {exc}"
        return result


def _run_with_state(
    config: PipelineConfig,
    state: StateStore,
    result: RunResult,
) -> RunResult:
    """Body of the run with state store available."""
    alerter = Alerter(config.email, state)

    # --- Cleanup ------------------------------------------------------ #
    # Done before start_run so a cleanup failure doesn't pollute the
    # run record. Cleanup failures are warnings, not fatal.
    _try_cleanup(config)

    run_id = state.start_run()
    result.run_id = run_id
    logger.info("starting pipeline run %d", run_id)

    try:
        # --- Drain outbox -------------------------------------------- #
        with state.step(run_id, "drain_outbox"):
            drained = _drain_outbox(config, state, alerter, result)
            result.drained_outbox_count = drained

        # If draining left any pending files (because some failed), we
        # don't process new work — see runner module docstring.
        remaining = list_pending(config.folders.outbox_pending)
        if remaining:
            logger.warning(
                "skipping new batch: %d outbox file(s) still pending after drain",
                len(remaining),
            )
            state.finish_run(
                run_id,
                status=RUN_STATUS_FAILED,
                error_text=(
                    f"{len(remaining)} outbox file(s) still pending "
                    "after drain; new batch deferred"
                ),
            )
            result.status = RUN_STATUS_FAILED
            result.error = "outbox drain incomplete"
            return result

        # --- Quiescence ---------------------------------------------- #
        with state.step(run_id, "quiescence"):
            source_files = wait_for_quiescence(
                config.folders.source,
                quiet_seconds=config.quiescence.quiet_seconds,
                max_wait_seconds=config.quiescence.max_wait_seconds,
                poll_interval_seconds=config.quiescence.poll_interval_seconds,
            )

        # --- Discover ------------------------------------------------ #
        with state.step(run_id, "discover"):
            new_files = _discover_new_files(state, source_files)
            if not new_files:
                logger.info("no new files to process")
                _record_skipped_steps(
                    state,
                    run_id,
                    ("fetch", "parse", "merge", "write_outbox",
                     "publish", "mark_processed"),
                )
                state.finish_run(run_id, status=RUN_STATUS_SUCCESS)
                result.status = RUN_STATUS_SUCCESS
                return result

        # We have new work. Compute the batch signature now so it
        # appears in run records and quarantine bookkeeping.
        batch_sig = compute_batch_signature(
            (str(p), h) for p, h in new_files
        )
        result.batch_signature = batch_sig

        if state.is_batch_quarantined(batch_sig):
            logger.warning(
                "skipping run: batch %s is quarantined", batch_sig[:16]
            )
            state.finish_run(
                run_id,
                status=RUN_STATUS_FAILED,
                error_text="batch is quarantined; requires operator action",
                batch_signature=batch_sig,
            )
            result.status = RUN_STATUS_FAILED
            result.skipped_quarantined = True
            return result

        # --- Fetch / parse / merge / write_outbox / publish ---------- #
        try:
            _process_batch(
                config, state, alerter, run_id, batch_sig,
                new_files, result,
            )
        except (TransientError, PermanentError) as exc:
            _handle_batch_failure(
                config, state, alerter, run_id, batch_sig, exc, result,
            )
            return result

        # --- Mark processed ----------------------------------------- #
        # Only after publish has succeeded do we consider these files
        # "owned". processed_files entries make them invisible to the
        # next run's discover step.
        with state.step(run_id, "mark_processed"):
            state.mark_processed(
                run_id,
                ((str(p), h) for p, h in new_files),
            )
            result.files_processed = len(new_files)

        # Successful run: clear any prior failure record for this batch.
        state.clear_batch_failure(batch_sig)
        state.finish_run(
            run_id,
            status=RUN_STATUS_SUCCESS,
            batch_signature=batch_sig,
        )
        result.status = RUN_STATUS_SUCCESS
        logger.info(
            "pipeline run %d completed successfully (%d files)",
            run_id,
            len(new_files),
        )
        return result

    except QuiescenceTimeout as exc:
        logger.exception("quiescence timeout")
        state.finish_run(
            run_id,
            status=RUN_STATUS_FAILED,
            error_text=str(exc),
        )
        _send_catastrophic_alert(
            alerter, result,
            "Pipeline: source folder did not quiesce",
            f"Run {run_id} aborted: {exc}",
        )
        result.error = str(exc)
        return result

    except PipelineError as exc:
        # Catch-all for our own errors that escaped batch handling
        # (e.g. drain failures aren't part of batch attempt counting).
        logger.exception("pipeline error")
        state.finish_run(
            run_id,
            status=RUN_STATUS_FAILED,
            error_text=str(exc),
        )
        _send_catastrophic_alert(
            alerter, result,
            f"Pipeline run {run_id} failed",
            f"{type(exc).__name__}: {exc}\n\n{traceback.format_exc()}",
        )
        result.error = str(exc)
        return result


# --------------------------------------------------------------------------- #
# Phase implementations                                                       #
# --------------------------------------------------------------------------- #


def _try_cleanup(config: PipelineConfig) -> None:
    """Best-effort startup cleanup; failures are warnings, not fatal."""
    try:
        cleanup_staging_debris(config.folders.staging)
    except Exception:
        logger.exception("cleanup_staging_debris failed; continuing")
    try:
        cleanup_old_sent(
            config.folders.outbox_sent,
            config.outbox.sent_retention_days,
        )
    except Exception:
        logger.exception("cleanup_old_sent failed; continuing")


def _drain_outbox(
    config: PipelineConfig,
    state: StateStore,
    alerter: Alerter,
    result: RunResult,
) -> int:
    """Publish any pending outbox files left from prior runs.

    Each pending file is treated as its own batch for quarantine
    purposes (the manifest carries its own batch_signature). Failures
    are recorded against that signature; quarantined files are skipped.

    Returns the number of files successfully drained.
    """
    pending = list_pending(config.folders.outbox_pending)
    if not pending:
        return 0

    logger.info("draining %d pending outbox file(s)", len(pending))
    drained = 0
    for pending_file in pending:
        try:
            manifest = read_manifest(pending_file)
        except PipelineError as exc:
            # Malformed/missing manifest — we can't safely drain. Log
            # and move on; operator needs to intervene.
            logger.error(
                "skipping pending file %s: %s", pending_file.name, exc
            )
            continue

        if state.is_batch_quarantined(manifest.batch_signature):
            logger.warning(
                "skipping quarantined pending file %s", pending_file.name
            )
            continue

        try:
            _publish_one(
                config, pending_file,
                idempotency_key=manifest.batch_signature,
            )
            mark_published(pending_file, config.folders.outbox_sent)
            state.mark_processed(
                manifest.run_id,
                list(manifest.source_files),
            )
            state.clear_batch_failure(manifest.batch_signature)
            drained += 1
            logger.info(
                "drained outbox file %s (%d source files marked processed)",
                pending_file.name,
                len(manifest.source_files),
            )
        except (TransientError, PermanentError) as exc:
            attempts = state.record_batch_failure(
                manifest.batch_signature,
                max_attempts=config.retry_policy.max_attempts_per_batch,
                error_text=f"drain: {type(exc).__name__}: {exc}",
            )
            now_quarantined = (
                attempts >= config.retry_policy.max_attempts_per_batch
            )
            logger.warning(
                "drain failed for %s (attempt %d): %s",
                pending_file.name,
                attempts,
                exc,
            )
            if now_quarantined:
                _send_quarantine_alert(
                    alerter, result,
                    f"Pipeline: pending batch quarantined ({pending_file.name})",
                    f"Outbox file {pending_file.name} failed {attempts} "
                    f"times to publish.\n\nLast error: {exc}\n\n"
                    f"Operator action required.",
                )

    return drained


def _discover_new_files(
    state: StateStore,
    source_files: list[Path],
) -> list[tuple[Path, str]]:
    """Hash each source file; drop ones already in ``processed_files``.

    Returns a list of ``(path, content_hash)`` tuples, in input order.
    """
    new_files: list[tuple[Path, str]] = []
    for path in source_files:
        # Hashing happens before we know whether the file is new — we
        # need the hash to do the de-dup check. For 4 KB files this is
        # microseconds; for larger files we'd want a more efficient
        # de-dup scheme (mtime + size as a pre-check).
        try:
            file_hash = compute_file_hash(path)
        except OSError as exc:
            raise PipelineError(
                f"cannot hash {path}: {exc}"
            ) from exc
        if not state.is_processed(str(path), file_hash):
            new_files.append((path, file_hash))
    logger.info(
        "discovery: %d source files, %d new",
        len(source_files),
        len(new_files),
    )
    return new_files


def _process_batch(
    config: PipelineConfig,
    state: StateStore,
    alerter: Alerter,
    run_id: int,
    batch_sig: str,
    new_files: list[tuple[Path, str]],
    result: RunResult,
) -> None:
    """The fetch → parse → merge → write_outbox → publish chain.

    Any exception from this chain is caught by the outer ``_run_with_state``
    and routed to :func:`_handle_batch_failure`.
    """
    # --- Fetch ------------------------------------------------------- #
    with state.step(run_id, "fetch"):
        # We don't use the return value here — fetch_files writes the
        # files into config.folders.fetched, which is where CsvMerger
        # reads from below. The call's side effect is what matters.
        fetch_files(
            [p for p, _ in new_files],
            config.folders.staging,
            config.folders.fetched,
            parallel_workers=config.fetch.parallel_workers,
        )

    # --- Parse / merge (delegated to existing CsvMerger) ------------ #
    # We use a temporary directory to hold the merged output before the
    # outbox write. CsvMerger writes atomically into the output_path we
    # give it; we then atomically copy it into the outbox.
    with TemporaryDirectory(
        prefix="csv-merger-run-",
        dir=str(config.folders.staging),
    ) as scratch:
        scratch_path = Path(scratch)
        merged_output = scratch_path / "merged.txt"

        with state.step(run_id, "parse"):
            # Parsing is part of the merge step in the current
            # CsvMerger API; we record the step but the actual work
            # happens in `merge` below. Splitting CsvMerger into parse
            # + merge phases would be a larger refactor.
            pass

        with state.step(run_id, "merge"):
            merger = CsvMerger(
                input_dir=config.folders.fetched,
                output_path=merged_output,
            )
            merger.run()

        # --- Write outbox ------------------------------------------ #
        with state.step(run_id, "write_outbox"):
            outbox_file = write_to_outbox(
                merged_output,
                run_id=run_id,
                batch_signature=batch_sig,
                pending_dir=config.folders.outbox_pending,
                source_files=[(str(p), h) for p, h in new_files],
            )

    # --- Publish ----------------------------------------------------- #
    with state.step(run_id, "publish"):
        _publish_one(
            config, outbox_file,
            idempotency_key=batch_sig,
        )
        mark_published(outbox_file, config.folders.outbox_sent)


def _publish_one(
    config: PipelineConfig,
    file: Path,
    *,
    idempotency_key: str,
) -> None:
    """Publish a single file using the configured publish settings."""
    publish_file(
        file,
        config.publish.url,
        idempotency_key=idempotency_key,
        max_attempts=config.publish.max_attempts,
        base_delay=config.publish.base_delay_seconds,
        max_delay=config.publish.max_delay_seconds,
        connect_timeout=config.publish.connect_timeout_seconds,
        read_timeout=config.publish.read_timeout_seconds,
    )


def _handle_batch_failure(
    config: PipelineConfig,
    state: StateStore,
    alerter: Alerter,
    run_id: int,
    batch_sig: str,
    exc: BaseException,
    result: RunResult,
) -> None:
    """Record a batch failure and decide whether to quarantine + alert."""
    error_text = f"{type(exc).__name__}: {exc}"
    attempts = state.record_batch_failure(
        batch_sig,
        max_attempts=config.retry_policy.max_attempts_per_batch,
        error_text=error_text,
    )
    state.finish_run(
        run_id,
        status=RUN_STATUS_FAILED,
        error_text=error_text,
        batch_signature=batch_sig,
    )
    result.status = RUN_STATUS_FAILED
    result.error = error_text

    quarantined = attempts >= config.retry_policy.max_attempts_per_batch
    result.quarantined_now = quarantined

    logger.error(
        "batch failed (attempt %d/%d, %squarantined): %s",
        attempts,
        config.retry_policy.max_attempts_per_batch,
        "" if quarantined else "not ",
        exc,
    )

    if quarantined:
        _send_quarantine_alert(
            alerter, result,
            f"Pipeline: batch {batch_sig[:16]} quarantined",
            f"Batch failed {attempts} consecutive times.\n\n"
            f"Last error: {error_text}\n\n"
            f"Run ID: {run_id}\n"
            f"Source file count: {result.files_processed or '?'}\n\n"
            f"Operator action required.",
        )


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _record_skipped_steps(
    state: StateStore,
    run_id: int,
    step_names: tuple[str, ...],
) -> None:
    """Record steps as ``skipped`` so observability queries see them.

    Uses the same context manager API but immediately marks the step
    as skipped instead of running it. Done when there's no work for
    the step (e.g. no files to fetch).
    """
    for name in step_names:
        with _record_as_skipped(state, run_id, name):
            pass


@contextmanager
def _record_as_skipped(
    state: StateStore,
    run_id: int,
    step_name: str,
) -> Iterator[None]:
    """Like ``state.step`` but records the step as ``skipped`` on success.

    We achieve this by recording via the normal step API then patching
    the row to ``skipped`` status — clean way to reuse the timing code
    rather than duplicating the SQL.
    """
    with state.step(run_id, step_name):
        yield
    # Patch status after the step recorded success.
    state._conn.execute(
        "UPDATE run_steps SET status = ? "
        "WHERE run_id = ? AND step_name = ?",
        (STEP_STATUS_SKIPPED, run_id, step_name),
    )


def _send_quarantine_alert(
    alerter: Alerter, result: RunResult,
    subject: str, body: str,
) -> None:
    r = alerter.send(ALERT_QUARANTINE, subject, body)
    if r.sent:
        result.alerts_sent.append(ALERT_QUARANTINE)


def _send_catastrophic_alert(
    alerter: Alerter, result: RunResult,
    subject: str, body: str,
) -> None:
    r = alerter.send(ALERT_CATASTROPHIC, subject, body)
    if r.sent:
        result.alerts_sent.append(ALERT_CATASTROPHIC)


def _emergency_alert(
    config: PipelineConfig, subject: str, body: str,
) -> None:
    """Send an alert without state-store rate limiting.

    Used only when the state store itself cannot be opened, which is
    a catastrophic condition the operator needs to know about. We
    swallow any SMTP errors — alerting must never be the cause of
    silent failure.
    """
    try:
        # Build a no-state Alerter shim by constructing an in-memory
        # store. This still gives us rate limiting (against nothing,
        # so the first call goes through) and reuses the SMTP logic.
        from csv_merger.pipeline.alerting import Alerter as _A
        from csv_merger.pipeline.state import StateStore as _S
        with TemporaryDirectory() as tmp:
            tmp_state = _S(Path(tmp) / "emergency.db")
            try:
                _A(config.email, tmp_state).send(
                    ALERT_CATASTROPHIC, subject, body,
                )
            finally:
                tmp_state.close()
    except Exception:
        logger.exception("emergency alert path itself failed")
