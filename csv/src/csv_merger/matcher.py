"""Match record files to batch rows.

Strategy (full discussion in ``README.md`` section 2):

* A record file is *linked* to a batch when the record's ``key_2`` agrees
  with the batch header's ``key_2``.
* Within a linked batch, a record file is *bound* to the first unbound row
  whose values for the ``type1`` and ``type2`` *role* columns agree with
  the record's ``type1_value`` / ``type2_value``. The role columns are
  resolved by header at parse time, so column reordering does not break
  matching.
* The trailing **sentinel row** of every records block is excluded from
  candidacy purely by position (last element of ``rows``); we never
  inspect cell values to detect it.

Performance: matching is implemented with a precomputed index
``(key_2, type1_value, type2_value) -> queue of (batch_path, row_idx)``
so the per-record cost is O(1) instead of O(batches * rows).
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path

from csv_merger.models import BatchFile, RecordFile

logger = logging.getLogger(__name__)


# Type alias for a batch+row binding key: ``(batch_path_str, data_row_index)``.
BindingKey = tuple[str, int]
# Type alias for the matching index key: ``(key_2, type1_value, type2_value)``.
IndexKey = tuple[str, str, str]


@dataclass(frozen=True)
class MatchResult:
    """The outcome of running the matcher.

    Attributes:
        bindings: Map of ``(batch_path, row_index)`` to the assigned
            record. ``row_index`` is the row's position in
            :attr:`BatchFile.data_rows` (i.e. the trailer is never counted).
        unmatched_records: Record files that could not be assigned.
    """

    bindings: dict[BindingKey, RecordFile]
    unmatched_records: list[RecordFile]


class RecordMatcher:
    """Bind a list of record files to rows across a list of batch files."""

    def __init__(
        self,
        batches: list[BatchFile],
        records: list[RecordFile],
    ) -> None:
        self._batches = batches
        self._records = records

    # ------------------------------------------------------------------ #
    # Public API                                                          #
    # ------------------------------------------------------------------ #

    def match(self) -> MatchResult:
        """Run the matcher and return a :class:`MatchResult`."""
        logger.debug(
            "matcher starting: %d batches, %d records",
            len(self._batches),
            len(self._records),
        )
        candidates = self._build_index()

        bindings: dict[BindingKey, RecordFile] = {}
        unmatched: list[RecordFile] = []

        # Process records in a deterministic order so the choice of "first
        # available" candidate is reproducible across runs and platforms.
        for record in self._sorted_records():
            key: IndexKey = (
                record.key_2,
                record.type1_value,
                record.type2_value,
            )
            queue = candidates.get(key)
            if queue:
                binding = queue.popleft()
                bindings[binding] = record
                logger.debug(
                    "match: %s -> %s row %d (queue depth now %d)",
                    record.path.name,
                    Path(binding[0]).name,
                    binding[1],
                    len(queue),
                )
            else:
                logger.info(
                    "no matching batch row for record file %s "
                    "(key_2=%s field_2=%s type1=%s type2=%s)",
                    record.path.name,
                    record.key_2,
                    record.field_2,
                    record.type1_value,
                    record.type2_value,
                )
                unmatched.append(record)

        logger.info(
            "matcher done: %d bound, %d unmatched",
            len(bindings),
            len(unmatched),
        )
        return MatchResult(bindings=bindings, unmatched_records=unmatched)

    # ------------------------------------------------------------------ #
    # Internals                                                           #
    # ------------------------------------------------------------------ #

    def _sorted_records(self) -> list[RecordFile]:
        """Sort records by trailing filename ordinal, then by path string.

        Records without a numeric suffix sort first (key=0).
        """
        return sorted(
            self._records,
            key=lambda r: (
                r.file_index if r.file_index is not None else 0,
                str(r.path),
            ),
        )

    def _build_index(self) -> dict[IndexKey, deque[BindingKey]]:
        """Build ``(key_2, type1, type2) -> queue of binding keys``.

        The queue preserves the in-batch row order, so popping the left
        end yields the first available row — matching the "first unbound
        row in batch order" semantics.
        """
        index: dict[IndexKey, deque[BindingKey]] = defaultdict(deque)
        for batch in self._batches:
            type1_header = batch.roles.type1
            type2_header = batch.roles.type2
            batch_key_2 = batch.header.key_2
            batch_path = str(batch.path)
            for idx, row in enumerate(batch.data_rows):
                index_key = (
                    batch_key_2,
                    row.cells[type1_header],
                    row.cells[type2_header],
                )
                index[index_key].append((batch_path, idx))
                logger.debug(
                    "index: %s row %d -> key=%s",
                    batch.path.name,
                    idx,
                    index_key,
                )
        logger.debug(
            "matcher index built: %d distinct keys, %d total candidate rows",
            len(index),
            sum(len(q) for q in index.values()),
        )
        return index
