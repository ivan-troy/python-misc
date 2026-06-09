"""Command-line interface for csv-merger.

The CLI exposes three subcommands:

* ``merge`` — the original merge operation (default if no subcommand given).
* ``pipeline`` — one execution of the scheduled pipeline.
* ``report`` — print recent pipeline runs, step timings, and quarantined
  batches from the state DB.

Backward compatibility: invocations without an explicit subcommand
(e.g. ``python -m csv_merger --inputs ...``) are treated as ``merge``,
so existing scripts and the original CLI tests continue to work.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

from csv_merger._constants import DATE_FORMAT, CsvMergerError
from csv_merger._logging import configure as configure_logging
from csv_merger._logging import level_for_verbosity
from csv_merger.merger import CsvMerger

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Argument helpers                                                            #
# --------------------------------------------------------------------------- #


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, DATE_FORMAT).date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid date {value!r}; expected MM/DD/YYYY"
        ) from exc


def _parse_log_level(value: str) -> int:
    """Accept either a name (``DEBUG``) or a numeric level (``10``)."""
    try:
        return int(value)
    except ValueError:
        pass
    name = value.upper()
    level = logging.getLevelName(name)
    if isinstance(level, int):
        return level
    raise argparse.ArgumentTypeError(
        f"unknown log level {value!r}; expected one of "
        "DEBUG, INFO, WARNING, ERROR, CRITICAL or a numeric level"
    )


def _add_common_logging_args(parser: argparse.ArgumentParser) -> None:
    """Add -v/-vv/--log-level/--color flags shared by every subcommand."""
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help=(
            "Increase log verbosity: -v=INFO, -vv=DEBUG. "
            "DEBUG logs cell values; route to a private file for "
            "sensitive data."
        ),
    )
    verbosity.add_argument(
        "--log-level",
        type=_parse_log_level,
        default=None,
        metavar="LEVEL",
        help=(
            "Set log level explicitly (DEBUG, INFO, WARNING, ERROR, "
            "CRITICAL, or a numeric level). Overrides -v/--verbose."
        ),
    )
    parser.add_argument(
        "--color",
        choices=("auto", "always", "never"),
        default="auto",
        help=(
            "Colorise log levels in stderr output. 'auto' (default) "
            "enables color on a TTY, respects NO_COLOR / FORCE_COLOR; "
            "'always' forces on; 'never' forces off."
        ),
    )


# --------------------------------------------------------------------------- #
# Parser construction                                                         #
# --------------------------------------------------------------------------- #


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="csv-merger",
        description=(
            "Merge sectioned batch and record CSV files into a single "
            "combined output file. Also runs the scheduled pipeline."
        ),
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=False,
    )

    # merge
    merge_p = subparsers.add_parser(
        "merge",
        help="Merge a folder of batch+record files into one output.",
    )
    merge_p.add_argument(
        "--inputs",
        type=Path,
        required=True,
        help="Directory containing batch-*.txt and record-*.txt files.",
    )
    merge_p.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path of the merged output file to create.",
    )
    merge_p.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Earliest batch date to include (MM/DD/YYYY, inclusive).",
    )
    merge_p.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="Latest batch date to include (MM/DD/YYYY, inclusive).",
    )
    _add_common_logging_args(merge_p)

    # pipeline
    pipeline_p = subparsers.add_parser(
        "pipeline",
        help=(
            "Execute one scheduled pipeline run (fetch, merge, publish, "
            "with retry and state persistence)."
        ),
    )
    pipeline_p.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the pipeline TOML configuration file.",
    )
    _add_common_logging_args(pipeline_p)

    # report
    report_p = subparsers.add_parser(
        "report",
        help="Print pipeline run history and quarantine status.",
    )
    report_p.add_argument(
        "--config",
        type=Path,
        required=True,
        help=(
            "Path to the pipeline TOML configuration file "
            "(used only to locate the state DB)."
        ),
    )
    report_p.add_argument(
        "--limit",
        type=int,
        default=10,
        help="How many recent runs to include in the report (default 10).",
    )
    _add_common_logging_args(report_p)

    return parser


# --------------------------------------------------------------------------- #
# Backward-compat shim                                                        #
# --------------------------------------------------------------------------- #


_SUBCOMMANDS = frozenset({"merge", "pipeline", "report"})


def _apply_legacy_shim(argv: list[str]) -> list[str]:
    """Inject ``merge`` if the user used the old flag-only invocation.

    Preserves the original CLI: ``python -m csv_merger --inputs ... --output ...``
    keeps working as if it were ``python -m csv_merger merge ...``.
    """
    if not argv:
        return argv
    first = argv[0]
    if first in _SUBCOMMANDS:
        return argv
    if first in {"-h", "--help"}:
        return argv
    if first.startswith("-"):
        # Legacy form: prepend "merge".
        return ["merge", *argv]
    return argv


# --------------------------------------------------------------------------- #
# Entry point                                                                 #
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    """Entry point used by ``python -m csv_merger`` and the script wrapper."""
    if argv is None:
        argv = sys.argv[1:]
    argv = _apply_legacy_shim(argv)

    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    # Logging setup is identical across subcommands.
    level = (
        args.log_level
        if args.log_level is not None
        else level_for_verbosity(args.verbose)
    )
    configure_logging(level, color=args.color)

    if args.command == "merge":
        return _run_merge(args, parser)
    if args.command == "pipeline":
        return _run_pipeline(args)
    if args.command == "report":
        return _run_report(args)
    parser.error(f"unknown command: {args.command}")
    return 2  # pragma: no cover


# --------------------------------------------------------------------------- #
# Subcommand handlers                                                         #
# --------------------------------------------------------------------------- #


def _run_merge(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Run the original merge operation."""
    logger.debug(
        "merge args: inputs=%s output=%s start=%s end=%s",
        args.inputs, args.output, args.start, args.end,
    )

    if not args.inputs.is_dir():
        parser.error(f"--inputs is not a directory: {args.inputs}")

    merger = CsvMerger(
        input_dir=args.inputs,
        output_path=args.output,
        start_date=args.start,
        end_date=args.end,
    )

    try:
        report = merger.run()
    except (FileNotFoundError, CsvMergerError) as exc:
        logger.debug("merge failed", exc_info=True)
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        f"wrote {report.output_path} "
        f"(batches={len(report.batches_in_range)}, "
        f"overrides={report.overrides_applied}, "
        f"unmatched_records={len(report.unmatched_records)})"
    )
    if report.unmatched_records:
        print("unmatched record files:", file=sys.stderr)
        for rec in report.unmatched_records:
            print(f"  - {rec.path.name}", file=sys.stderr)
    return 0


def _run_pipeline(args: argparse.Namespace) -> int:
    """Run one pipeline tick."""
    from csv_merger.pipeline.config import load_config
    from csv_merger.pipeline.runner import run_pipeline

    try:
        config = load_config(args.config)
    except CsvMergerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    result = run_pipeline(config)

    # Print a one-line summary to stdout.
    if result.skipped_lock_held:
        print("skipped: another pipeline run is in progress")
        return 0
    if result.skipped_quarantined:
        sig_short = (
            result.batch_signature[:16] if result.batch_signature else "?"
        )
        print(f"skipped: batch quarantined (sig={sig_short})")
        return 1
    print(
        f"run {result.run_id}: {result.status} "
        f"(files_processed={result.files_processed}, "
        f"drained_outbox={result.drained_outbox_count}, "
        f"alerts={','.join(result.alerts_sent) or 'none'})"
    )
    return 0 if result.status == "success" else 1


def _run_report(args: argparse.Namespace) -> int:
    """Print the operator report."""
    from csv_merger.pipeline.config import load_config
    from csv_merger.pipeline.reporting import render_report_from_db

    try:
        config = load_config(args.config)
    except CsvMergerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        render_report_from_db(
            config.state.db_path,
            recent_run_limit=args.limit,
        ),
        end="",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
