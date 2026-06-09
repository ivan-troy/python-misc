"""Top-level merge orchestrator.

Wires together the parsers, matcher, and writer. Exposes one public class,
:class:`CsvMerger`, that takes input/output paths and an optional date
range, plus a small :class:`MergeReport` summary type for callers and tests.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from csv_merger._constants import DEFAULT_SCHEMA, CsvMergerError, Schema
from csv_merger.matcher import RecordMatcher
from csv_merger.models import BatchFile, RecordFile
from csv_merger.parsers import parse_batch_file, parse_record_file
from csv_merger.writer import write_output

logger = logging.getLogger(__name__)


@dataclass
class MergeReport:
    """Summary of a merge run, useful for tests and CLI feedback."""

    batches_in_range: list[BatchFile] = field(default_factory=list)
    batches_out_of_range: list[BatchFile] = field(default_factory=list)
    record_files: list[RecordFile] = field(default_factory=list)
    unmatched_records: list[RecordFile] = field(default_factory=list)
    overrides_applied: int = 0
    output_path: Path | None = None


class NoBatchesInRangeError(CsvMergerError):
    """Raised when a date filter excludes every batch file."""


# Matches a trailing ``-N`` ordinal in a filename stem, e.g. ``record-7``.
# Used purely as an ordering key — never as part of matching.
_ORDINAL_RE = re.compile(r"-(\d+)$")


class CsvMerger:
    """Run the full merge pipeline."""

    BATCH_GLOB = "batch-*.txt"
    RECORD_GLOB = "record-*.txt"

    def __init__(
        self,
        input_dir: Path,
        output_path: Path,
        start_date: date | None = None,
        end_date: date | None = None,
        schema: Schema | None = None,
    ) -> None:
        self.input_dir = input_dir
        self.output_path = output_path
        self.start_date = start_date
        self.end_date = end_date
        self.schema = schema if schema is not None else DEFAULT_SCHEMA

    # ------------------------------------------------------------------ #
    # Pipeline                                                            #
    # ------------------------------------------------------------------ #

    def run(self) -> MergeReport:
        """Execute the full pipeline and return a report."""
        logger.info(
            "starting merge: input_dir=%s output=%s start=%s end=%s",
            self.input_dir,
            self.output_path,
            self.start_date,
            self.end_date,
        )
        report = MergeReport()

        all_batches = self._load_batches()
        logger.info("discovered %d batch file(s)", len(all_batches))

        in_range, out_of_range = self._partition_by_date(all_batches)
        report.batches_in_range = in_range
        report.batches_out_of_range = out_of_range
        logger.info(
            "date filter: %d in range, %d out of range",
            len(in_range),
            len(out_of_range),
        )
        if logger.isEnabledFor(logging.DEBUG):
            for b in in_range:
                logger.debug("in-range:    %s (date=%s)", b.path.name, b.header.date)
            for b in out_of_range:
                logger.debug("out-of-range: %s (date=%s)", b.path.name, b.header.date)

        if not in_range:
            raise NoBatchesInRangeError(
                "no batch files fall within the requested date range"
            )

        # Sort batches by date so the output order is deterministic and
        # independent of filesystem listing order.
        in_range.sort(key=lambda b: b.header.date)
        logger.debug(
            "batch order after sort: %s",
            [b.path.name for b in in_range],
        )

        records = self._load_records()
        logger.info("discovered %d record file(s)", len(records))
        report.record_files = records

        match_result = RecordMatcher(in_range, records).match()
        report.unmatched_records = match_result.unmatched_records

        overrides = {
            key: rec.xy_pos
            for key, rec in match_result.bindings.items()
        }
        report.overrides_applied = len(overrides)

        write_output(self.output_path, in_range, overrides)
        report.output_path = self.output_path

        logger.info(
            "merge complete: wrote %s (%d batches, %d overrides, %d unmatched records)",
            self.output_path,
            len(in_range),
            report.overrides_applied,
            len(report.unmatched_records),
        )
        return report

    # ------------------------------------------------------------------ #
    # File discovery                                                     #
    # ------------------------------------------------------------------ #

    def _load_batches(self) -> list[BatchFile]:
        paths = sorted(self.input_dir.glob(self.BATCH_GLOB))
        logger.debug(
            "batch glob %r in %s -> %d path(s)",
            self.BATCH_GLOB,
            self.input_dir,
            len(paths),
        )
        if not paths:
            raise FileNotFoundError(
                f"no files matching {self.BATCH_GLOB} in {self.input_dir}"
            )
        return [parse_batch_file(p, schema=self.schema) for p in paths]

    def _load_records(self) -> list[RecordFile]:
        paths = sorted(
            self.input_dir.glob(self.RECORD_GLOB),
            key=_ordinal_sort_key,
        )
        logger.debug(
            "record glob %r in %s -> %d path(s) (sorted by ordinal)",
            self.RECORD_GLOB,
            self.input_dir,
            len(paths),
        )
        return [parse_record_file(p, schema=self.schema) for p in paths]

    # ------------------------------------------------------------------ #
    # Date filtering                                                      #
    # ------------------------------------------------------------------ #

    def _partition_by_date(
        self, batches: list[BatchFile]
    ) -> tuple[list[BatchFile], list[BatchFile]]:
        in_range = [b for b in batches if self._in_range(b.header.date)]
        out_of_range = [b for b in batches if not self._in_range(b.header.date)]
        return in_range, out_of_range

    def _in_range(self, candidate: date) -> bool:
        if self.start_date is not None and candidate < self.start_date:
            return False
        if self.end_date is not None and candidate > self.end_date:
            return False
        return True


def _ordinal_sort_key(path: Path) -> tuple[int, str]:
    """Sort by the trailing ``-N`` in the filename, falling back to name.

    Ensures ``record-2`` sorts before ``record-10`` (lexicographic order
    would invert that). Files with no trailing ordinal sort first.
    """
    match = _ORDINAL_RE.search(path.stem)
    return (int(match.group(1)) if match is not None else 0, path.name)
