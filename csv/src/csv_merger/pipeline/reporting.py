"""Operator-facing reporting on pipeline state.

Powers the ``python -m csv_merger report --config ...`` subcommand.
Reads the SQLite state DB and prints a human-readable summary:

* Recent runs with status, duration, and any error.
* Step-level breakdown of the most recent run.
* Currently quarantined batches.

Designed for terminal output — short lines, fixed-width columns. If
you need machine-readable output later (for a dashboard, alerting
integration, etc.) the right answer is a separate ``--format json``
flag rather than reformatting this one. Not building that until a
concrete use case emerges.
"""

from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path
from typing import TextIO

from csv_merger.pipeline.state import StateStore


def _short_ts(iso_ts: str) -> str:
    """Strip the ``+00:00`` UTC suffix from an ISO timestamp.

    All our internal timestamps are written in UTC by ``_utc_now_iso``,
    so the suffix is redundant and just messes up column alignment.
    """
    if iso_ts.endswith("+00:00"):
        return iso_ts[:-6]
    return iso_ts


def render_report(
    state: StateStore,
    *,
    recent_run_limit: int = 10,
    out: TextIO | None = None,
) -> str:
    """Render the operator report.

    Args:
        state: Open :class:`StateStore` to read from.
        recent_run_limit: How many recent runs to include in the summary.
        out: Optional stream to write to. If ``None`` (default), the
            output is captured and returned as a string. If provided,
            the report is written to ``out`` AND returned (for
            convenience — callers can ignore the return).

    Returns:
        The full report text.
    """
    captured = io.StringIO()

    def write(line: str = "") -> None:
        captured.write(line + "\n")

    runs = state.recent_runs(limit=recent_run_limit)

    write("=" * 78)
    write(f"Pipeline report — {datetime.now().isoformat(timespec='seconds')}")
    write("=" * 78)
    write()

    # Recent runs
    write(f"Recent runs (last {len(runs)}):")
    if not runs:
        write("  (no runs recorded)")
    else:
        write(
            f"  {'run':>5}  {'started':<19}  {'status':<8}  "
            f"{'duration':>10}  error"
        )
        write(f"  {'-' * 5}  {'-' * 19}  {'-' * 8}  {'-' * 10}  -----")
        for run in runs:
            duration_str = _format_duration(run.started_at, run.finished_at)
            error_str = (run.error_text or "")[:40]
            write(
                f"  {run.run_id:>5}  {_short_ts(run.started_at):<19}  "
                f"{run.status:<8}  {duration_str:>10}  {error_str}"
            )
    write()

    # Last run step breakdown
    if runs:
        last_run = runs[0]
        steps = state.steps_for_run(last_run.run_id)
        write(f"Steps for run {last_run.run_id}:")
        if not steps:
            write("  (no steps recorded)")
        else:
            write(f"  {'step':<18}  {'status':<8}  {'duration':>10}")
            write(f"  {'-' * 18}  {'-' * 8}  {'-' * 10}")
            for step in steps:
                duration_str = (
                    f"{step.duration_ms} ms"
                    if step.duration_ms is not None
                    else "—"
                )
                write(
                    f"  {step.step_name:<18}  {step.status:<8}  "
                    f"{duration_str:>10}"
                )
    write()

    # Quarantined batches
    quarantined = state.quarantined_batches()
    write(f"Quarantined batches ({len(quarantined)}):")
    if not quarantined:
        write("  (none)")
    else:
        write(
            f"  {'sig (first 16)':<18}  {'attempts':>8}  "
            f"{'last failure':<19}  error"
        )
        write(
            f"  {'-' * 18}  {'-' * 8}  {'-' * 19}  -----"
        )
        for batch in quarantined:
            error_str = (batch.last_error or "")[:30]
            write(
                f"  {batch.batch_signature[:16]:<18}  "
                f"{batch.attempt_count:>8}  "
                f"{_short_ts(batch.last_failure_at):<19}  {error_str}"
            )
    write()

    text = captured.getvalue()
    if out is not None:
        out.write(text)
    return text


def render_report_from_db(
    db_path: Path,
    *,
    recent_run_limit: int = 10,
    out: TextIO | None = None,
) -> str:
    """Convenience: open the state DB, render the report, close.

    The CLI uses this; programmatic callers may prefer
    :func:`render_report` if they already have an open store.
    """
    state = StateStore(db_path)
    try:
        return render_report(
            state,
            recent_run_limit=recent_run_limit,
            out=out,
        )
    finally:
        state.close()


def _format_duration(started_at: str, finished_at: str | None) -> str:
    """Format the duration between two ISO timestamps as ``Ns`` or ``N.Ns``."""
    if finished_at is None:
        return "—"
    try:
        start = datetime.fromisoformat(started_at)
        end = datetime.fromisoformat(finished_at)
    except ValueError:
        return "?"
    seconds = (end - start).total_seconds()
    if seconds < 1.0:
        return f"{seconds * 1000:.0f} ms"
    if seconds < 60.0:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    rem_seconds = seconds - (minutes * 60)
    return f"{minutes}m{rem_seconds:.0f}s"
