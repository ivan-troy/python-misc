"""Domain models for batch and record files.

These dataclasses hold parsed values in a way that makes the matching and
merging logic readable. They are :class:`frozen <dataclasses.dataclass>` so
attribute *bindings* cannot be accidentally rebound by a later pipeline
stage.

Note on immutability: ``frozen=True`` prevents reassigning attributes, but
it does **not** make any nested mutable containers (lists, dicts) immutable.
Callers must treat ``BatchRow.cells``, ``BatchFile.rows``, etc. as
read-only by convention. Helpers like :meth:`BatchRow.with_replacements`
return new instances rather than mutating in place.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Mapping

from csv_merger._constants import DEFAULT_ROLE_PREFIXES


# --------------------------------------------------------------------------- #
# Column-role resolution                                                      #
# --------------------------------------------------------------------------- #
#
# The records block has fixed *roles* but the column **headers** may carry
# decorations such as ``type1(a)`` rather than ``type1``. We resolve role
# names to the actual header strings once (at parse time) and look up cell
# values by header string from there on. This makes the code immune to
# column reordering and tolerates extra columns, while still surfacing an
# explicit error if a required role is missing.
#
# The role-prefix map itself is configurable via :class:`Schema`; see
# :data:`csv_merger._constants.DEFAULT_ROLE_PREFIXES` for today's defaults.


class MissingColumnRole(ValueError):
    """Raised when an expected role cannot be resolved against the headers.

    This is wrapped by the parser into
    :class:`csv_merger._constants.MalformedBatchFile` so callers see a
    consistent exception type. It is kept as its own class because the
    parser test suite asserts on it specifically and re-uses it across
    helpers.
    """


@dataclass(frozen=True)
class ColumnRoles:
    """Maps semantic roles to the actual header strings of a records block.

    The four roles the merger cares about are:

    * ``field_a`` / ``field_b`` — the two columns whose values are replaced
      by ``XY_POS`` on output.
    * ``type1`` / ``type2`` — the two columns used to match a record file
      to a row inside its batch.
    """

    field_a: str
    field_b: str
    type1: str
    type2: str

    @classmethod
    def from_headers(
        cls,
        headers: list[str],
        prefixes: Mapping[str, str] | None = None,
    ) -> "ColumnRoles":
        """Resolve role -> header string against ``headers``.

        Args:
            headers: Column-header strings from the records block.
            prefixes: Role -> header-prefix map. Defaults to
                :data:`DEFAULT_ROLE_PREFIXES`. Pass a custom mapping (or
                better, a :class:`~csv_merger._constants.Schema`'s
                ``role_prefixes``) to support a sister format.

        Raises:
            MissingColumnRole: if any required role has no matching header.
        """
        active = DEFAULT_ROLE_PREFIXES if prefixes is None else prefixes
        resolved: dict[str, str] = {}
        for role, prefix in active.items():
            for header in headers:
                if header.startswith(prefix):
                    resolved[role] = header
                    break
            if role not in resolved:
                raise MissingColumnRole(
                    f"no header starts with {prefix!r} for role {role!r} "
                    f"(headers were: {headers!r})"
                )
        return cls(
            field_a=resolved["field_a"],
            field_b=resolved["field_b"],
            type1=resolved["type1"],
            type2=resolved["type2"],
        )


# --------------------------------------------------------------------------- #
# Header (top key/value block of a batch file)                                #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Header:
    """The top key/value block of a batch file."""

    date: date
    key_1: str
    key_2: str
    batch: str
    title: str = "title"


# --------------------------------------------------------------------------- #
# Batch row (dict-keyed by header string)                                     #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class BatchRow:
    """A single data row inside a batch's ``records`` block.

    Cells are stored in a dict keyed by the header string from the file.
    The model is therefore immune to column reordering: ``row.cells['type1(a)']``
    returns the type1 value regardless of physical column position.

    Attributes:
        cells: Mapping from header string to cell value. Treat as read-only;
            use :meth:`with_replacements` to derive a modified row.
        column_order: The column order to use when serialising back to CSV
            (a copy of the parent batch's ``column_headers``, as a tuple
            because the surrounding dataclass is frozen).
    """

    cells: Mapping[str, str]
    column_order: tuple[str, ...]

    def get(self, header: str) -> str:
        """Return the cell value for ``header`` or raise ``KeyError``."""
        return self.cells[header]

    def to_csv_row(self) -> list[str]:
        """Return cells in declared column order, ready for ``csv.writer``."""
        return [self.cells[h] for h in self.column_order]

    def with_replacements(self, replacements: Mapping[str, str]) -> "BatchRow":
        """Return a copy with the given header -> value replacements applied.

        Unknown headers raise ``KeyError`` so a typo in a role name surfaces
        immediately rather than silently appending a phantom column.
        """
        new_cells = dict(self.cells)
        for header, value in replacements.items():
            if header not in new_cells:
                raise KeyError(
                    f"cannot replace {header!r}: not in row columns "
                    f"{list(self.cells)}"
                )
            new_cells[header] = value
        return BatchRow(cells=new_cells, column_order=self.column_order)


# --------------------------------------------------------------------------- #
# Batch file                                                                  #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class BatchFile:
    """A parsed batch file.

    Attributes:
        path: Source path (kept for diagnostics).
        header: Top key/value block.
        column_headers: The CSV column names from the records block, in order.
        roles: Resolved role -> header mapping.
        rows: The data rows of the *include* records block. The **last**
            element is the trailer sentinel row, by file-format contract.
        data_count: The integer from the ``data,N`` line.
    """

    path: Path
    header: Header
    column_headers: list[str]
    roles: ColumnRoles
    rows: list[BatchRow]
    data_count: int

    @property
    def data_rows(self) -> list[BatchRow]:
        """Rows excluding the trailing sentinel row.

        The trailer is identified **by position** (last row of the block),
        not by inspecting cell values. This is the file-format contract.
        """
        if not self.rows:
            return []
        return self.rows[:-1]

    @property
    def trailer_row(self) -> BatchRow | None:
        """The trailer sentinel row, or ``None`` for an empty block."""
        return self.rows[-1] if self.rows else None


# --------------------------------------------------------------------------- #
# Record file                                                                 #
# --------------------------------------------------------------------------- #


# Pull a trailing ordinal off filenames like ``record-7.txt`` -> 7. We use
# a strict pattern instead of "any digits in the stem" because the latter
# turns ``record-2024-01.txt`` into 202401, which is misleading.
_FILE_INDEX_PATTERN = re.compile(r"-(\d+)$")


@dataclass(frozen=True)
class RecordFile:
    """A parsed record file.

    Attributes:
        path: Source path (kept for diagnostics).
        key_2: Used to link to the parent batch.
        field_2: Used to link to the parent batch.
        xy_pos: ``(x, y)`` values that will overwrite ``field_a, field_b``.
        type1_value: The matching value for the ``type1`` column in the batch row.
        type2_value: The matching value for the ``type2`` column in the batch row.
        count: The integer from the ``count`` line (kept for completeness).
    """

    path: Path
    key_2: str
    field_2: str
    xy_pos: tuple[str, str]
    type1_value: str
    type2_value: str
    count: int = 1

    @property
    def file_index(self) -> int | None:
        """Best-effort filename ordinal (e.g. ``record-7.txt`` -> ``7``).

        Returns ``None`` when the filename does not have a trailing ``-N``
        suffix. Used only as a deterministic ordering key, never for
        matching.
        """
        match = _FILE_INDEX_PATTERN.search(self.path.stem)
        return int(match.group(1)) if match is not None else None
