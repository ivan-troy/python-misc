"""Parser for the *type-2* batch file format.

The type-2 format is an instrument-style report that differs from the
type-1 batch format handled by :mod:`csv_merger.parsers`. A type-2 file
contains a single report with:

* A ``===== result =====`` banner.
* A ``title`` line followed by ``key : value`` header lines.
* Two parallel data sections, each with:

  * A row of column **names** (e.g. ``value1 value2 ... value5``).
  * A row of column **units** that also names the index columns
    (e.g. ``r Phi (cps) (cps) ...``).
  * A horizontal rule (``-----``).
  * Data rows: zero or more numeric rows keyed on ``(r, Phi)``, then
    a second horizontal rule, then zero or more ``Calc N`` rows.

The two sections share the ``(r, Phi)`` index. This module joins them
horizontally — numeric rows by ``(r, Phi)`` equality (in order, since
both sections list the same pairs), calc rows by their ``Calc N`` label.

The output is a flat CSV with:

* A synthetic ``rec_label`` column — sequential ``1, 2, 3 ...`` for
  numeric rows; the literal ``Calc N`` for calculation rows.
* Numeric values for each section column.
* A ``processname`` column populated from the file header.
* Two placeholder columns filled with the literal string ``"na"``.

Calc rows have no ``(r, Phi)`` index, so their ``r`` and ``Phi`` cells
are the literal string ``"blank"`` (deliberately, to match the consumer's
expected output).

The module is independent of the type-1 parser; it shares the file-size
guard, encoding, and exception base classes from
:mod:`csv_merger._constants` but has its own data model and writer.
"""

from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

from csv_merger._constants import (
    InputTooLargeError,
    MAX_INPUT_FILE_BYTES,
    MalformedBatchFile,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Format constants                                                            #
# --------------------------------------------------------------------------- #

#: BOM-tolerant; CRLF line endings are normalised by text-mode read.
_INPUT_ENCODING = "utf-8-sig"

#: Banner that marks the start of a type-2 report. Anything before this
#: line is ignored.
_RESULT_BANNER = "===== result ====="

#: Horizontal rule between data sub-sections. We match by prefix because
#: the actual rule lengths vary slightly between files.
_RULE_PREFIX = "----"

#: Literal placeholder filler.
_PLACEHOLDER_VALUE = "na"

#: Literal r/Phi value used for calc rows in the output.
_CALC_BLANK = "blank"

#: ``Calc N`` row labels start with this prefix (case-sensitive — the
#: spec spells it ``Calc`` not ``calc``).
_CALC_PREFIX = "Calc"

#: Index column names, in order. The parser requires the units-row of
#: each section to start with these two tokens.
_INDEX_COLUMNS: tuple[str, ...] = ("r", "Phi")

#: Pattern for ``key : value`` header lines (allowing arbitrary whitespace
#: around the colon).
_HEADER_LINE_RE = re.compile(r"^(?P<key>[^:]+?)\s*:\s*(?P<value>.*?)\s*$")


# --------------------------------------------------------------------------- #
# Public model                                                                #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Type2DataRow:
    """A single output row.

    Attributes:
        rec_label: ``"1"``, ``"2"``, ... for numeric rows; ``"Calc N"`` for
            calculation rows.
        r: The ``r`` index value, or :data:`_CALC_BLANK` for calc rows.
        phi: The ``Phi`` index value, or :data:`_CALC_BLANK` for calc rows.
        values: Section-column values in declared output order.
    """

    rec_label: str
    r: str
    phi: str
    values: tuple[str, ...]


@dataclass(frozen=True)
class Type2Report:
    """The full parsed result of a type-2 batch file.

    Attributes:
        title: First non-banner line of the file (typically ``"title"``).
        header: ``key`` -> ``value`` mapping parsed from the colon-headers.
        column_names: Output column names for the joined sections, in
            order (e.g. ``("value1(cps)", ..., "value8(nm)")``).
        rows: All output rows — numeric rows first, then calc rows.
    """

    title: str
    header: dict[str, str]
    column_names: tuple[str, ...]
    rows: tuple[Type2DataRow, ...]


# --------------------------------------------------------------------------- #
# Internal section representation                                             #
# --------------------------------------------------------------------------- #


@dataclass
class _Section:
    """One of the two parallel data sections."""

    column_names: list[str] = field(default_factory=list)
    units: list[str] = field(default_factory=list)
    numeric_rows: list[list[str]] = field(default_factory=list)
    calc_rows: list[list[str]] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #


def parse_batch_file_type2(path: Path) -> Type2Report:
    """Parse a type-2 batch file.

    Args:
        path: Path to the input file.

    Returns:
        A :class:`Type2Report`.

    Raises:
        MalformedBatchFile: if the file structure is unexpected.
        InputTooLargeError: if the file exceeds the size limit.
    """
    _check_size(path)
    logger.debug("parsing type-2 batch file: %s", path)

    lines = _read_lines(path)
    cursor = _Cursor(lines, path)

    cursor.advance_to(_RESULT_BANNER)
    title = cursor.next_nonblank()
    header = _parse_header_block(cursor)
    section_a = _parse_section(cursor)
    section_b = _parse_section(cursor)

    _validate_sections(section_a, section_b, path)

    column_names = _build_column_names(section_a) + _build_column_names(section_b)
    rows = _join_sections(section_a, section_b)

    logger.debug(
        "%s: parsed %d numeric rows, %d calc rows, %d columns",
        path.name,
        sum(1 for r in rows if r.r != _CALC_BLANK),
        sum(1 for r in rows if r.r == _CALC_BLANK),
        len(column_names),
    )
    return Type2Report(
        title=title,
        header=header,
        column_names=column_names,
        rows=rows,
    )


def write_type2_csv(
    report: Type2Report,
    output_path: Path,
    *,
    processname_key: str = "process name",
    placeholder_columns: tuple[str, ...] = ("placeholder1", "placeholder2"),
) -> None:
    """Write a :class:`Type2Report` to ``output_path`` in the flat CSV format.

    Args:
        report: The parsed report.
        output_path: Destination CSV file.
        processname_key: Which header key supplies the ``processname``
            column's value. Defaults to ``"process name"``.
        placeholder_columns: Names of the placeholder columns, filled
            with the literal string ``"na"``. Defaults to two columns.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    processname = report.header.get(processname_key, "")

    column_headers = (
        "rec_label",
        *_INDEX_COLUMNS,
        *report.column_names,
        "processname",
        *placeholder_columns,
    )

    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(column_headers)
        for row in report.rows:
            writer.writerow([
                row.rec_label,
                row.r,
                row.phi,
                *row.values,
                processname,
                *([_PLACEHOLDER_VALUE] * len(placeholder_columns)),
            ])
    logger.info("wrote type-2 CSV to %s", output_path)


# --------------------------------------------------------------------------- #
# I/O helpers                                                                 #
# --------------------------------------------------------------------------- #


def _check_size(path: Path) -> None:
    """Reject files larger than :data:`MAX_INPUT_FILE_BYTES`."""
    size = path.stat().st_size
    if size > MAX_INPUT_FILE_BYTES:
        raise InputTooLargeError(
            f"{path}: file size {size} bytes exceeds limit "
            f"{MAX_INPUT_FILE_BYTES}"
        )


def _read_lines(path: Path) -> list[str]:
    """Read the file as text, stripping trailing whitespace per line.

    Universal-newlines mode normalises ``\\r\\n`` to ``\\n``. Each line
    is right-stripped (line endings and trailing tabs) but the leading
    indent is preserved because some section labels start with whitespace.
    """
    with path.open(encoding=_INPUT_ENCODING) as fh:
        return [line.rstrip("\r\n\t ") for line in fh]


# --------------------------------------------------------------------------- #
# Cursor: a thin index-with-error-context over the list of lines           #
# --------------------------------------------------------------------------- #


class _Cursor:
    """Line-by-line cursor with file-aware error messages.

    Keeping cursor state out of the section/header parsers makes those
    functions short and testable on their own line lists.
    """

    def __init__(self, lines: list[str], path: Path) -> None:
        self._lines = lines
        self._path = path
        self._idx = 0

    @property
    def at_end(self) -> bool:
        return self._idx >= len(self._lines)

    @property
    def position(self) -> int:
        return self._idx + 1  # 1-based for human-friendly messages

    def peek(self) -> str:
        if self.at_end:
            return ""
        return self._lines[self._idx]

    def pop(self) -> str:
        if self.at_end:
            self._fail("unexpected end of file")
        line = self._lines[self._idx]
        self._idx += 1
        return line

    def advance_to(self, target: str) -> None:
        """Advance past the first line equal to ``target`` (after strip)."""
        while not self.at_end:
            if self._lines[self._idx].strip() == target:
                self._idx += 1
                return
            self._idx += 1
        self._fail(f"expected line {target!r} not found")

    def next_nonblank(self) -> str:
        """Advance past blank lines and return the next content line."""
        while not self.at_end and not self._lines[self._idx].strip():
            self._idx += 1
        if self.at_end:
            self._fail("unexpected end of file while seeking content")
        return self.pop().strip()

    def skip_blank(self) -> None:
        while not self.at_end and not self._lines[self._idx].strip():
            self._idx += 1

    def _fail(self, msg: str) -> None:
        raise MalformedBatchFile(f"{self._path}:{self.position}: {msg}")


# --------------------------------------------------------------------------- #
# Phase parsers                                                               #
# --------------------------------------------------------------------------- #


def _parse_header_block(cursor: _Cursor) -> dict[str, str]:
    """Parse colon-separated ``key : value`` lines until a blank line."""
    header: dict[str, str] = {}
    while not cursor.at_end:
        line = cursor.peek()
        if not line.strip():
            cursor.skip_blank()
            return header
        match = _HEADER_LINE_RE.match(line.strip())
        if not match:
            # Not a header line; stop and let the next phase try.
            return header
        cursor.pop()
        header[match.group("key").strip()] = match.group("value").strip()
    return header


def _parse_section(cursor: _Cursor) -> _Section:
    """Parse one data section.

    The section consists of:

    * A names row.
    * A units row that names the index columns and gives units for the rest.
    * A horizontal rule.
    * Numeric data rows.
    * A horizontal rule.
    * Calc rows.
    """
    cursor.skip_blank()
    if cursor.at_end:
        cursor._fail("expected start of data section but reached EOF")

    section = _Section()
    section.column_names = cursor.next_nonblank().split()
    section.units = cursor.next_nonblank().split()

    rule = cursor.next_nonblank()
    if not rule.startswith(_RULE_PREFIX):
        cursor._fail(f"expected horizontal rule, got {rule!r}")

    # Numeric rows until the next rule line.
    while not cursor.at_end:
        line = cursor.peek()
        if not line.strip():
            cursor.skip_blank()
            continue
        if line.startswith(_RULE_PREFIX) or line.lstrip().startswith(_RULE_PREFIX):
            cursor.pop()
            break
        section.numeric_rows.append(cursor.pop().split())

    # Calc rows until a blank line, EOF, or the next section's header.
    while not cursor.at_end:
        line = cursor.peek()
        if not line.strip():
            return section
        if not line.lstrip().startswith(_CALC_PREFIX):
            return section
        # ``Calc N\t\tv1\tv2...`` — the first token is ``Calc``, the
        # second is the calc number, the rest are numeric values.
        tokens = cursor.pop().split()
        if len(tokens) < 2:
            cursor._fail(f"malformed Calc row: {tokens!r}")
        label = f"{tokens[0]} {tokens[1]}"
        section.calc_rows.append([label, *tokens[2:]])

    return section


# --------------------------------------------------------------------------- #
# Validation and join                                                         #
# --------------------------------------------------------------------------- #


def _validate_sections(a: _Section, b: _Section, path: Path) -> None:
    """Sanity-check that the two sections can be joined."""
    # Each section's units row must start with the index columns.
    for label, section in (("A", a), ("B", b)):
        if tuple(section.units[: len(_INDEX_COLUMNS)]) != _INDEX_COLUMNS:
            raise MalformedBatchFile(
                f"{path}: section {label} units row must start with "
                f"{_INDEX_COLUMNS!r}, got {section.units!r}"
            )

    # Numeric rows must align by (r, Phi) in order.
    if len(a.numeric_rows) != len(b.numeric_rows):
        raise MalformedBatchFile(
            f"{path}: section row counts differ "
            f"({len(a.numeric_rows)} vs {len(b.numeric_rows)})"
        )
    for i, (row_a, row_b) in enumerate(
        zip(a.numeric_rows, b.numeric_rows, strict=True)
    ):
        if row_a[: len(_INDEX_COLUMNS)] != row_b[: len(_INDEX_COLUMNS)]:
            raise MalformedBatchFile(
                f"{path}: row {i} (r, Phi) mismatch between sections: "
                f"{row_a[:2]!r} vs {row_b[:2]!r}"
            )

    # Calc rows must align by label.
    if len(a.calc_rows) != len(b.calc_rows):
        raise MalformedBatchFile(
            f"{path}: calc row counts differ "
            f"({len(a.calc_rows)} vs {len(b.calc_rows)})"
        )
    for i, (row_a, row_b) in enumerate(
        zip(a.calc_rows, b.calc_rows, strict=True)
    ):
        if row_a[0] != row_b[0]:
            raise MalformedBatchFile(
                f"{path}: calc row {i} label mismatch: "
                f"{row_a[0]!r} vs {row_b[0]!r}"
            )


def _build_column_names(section: _Section) -> tuple[str, ...]:
    """Combine names and units into ``value1(cps)`` style headers.

    The first two units (``r``, ``Phi``) are the index columns and are
    dropped here — they're handled separately by the writer.
    """
    value_units = section.units[len(_INDEX_COLUMNS):]
    if len(section.column_names) != len(value_units):
        raise MalformedBatchFile(
            f"column-name count ({len(section.column_names)}) does not "
            f"match value-unit count ({len(value_units)}): "
            f"names={section.column_names!r}, units={value_units!r}"
        )
    return tuple(
        f"{name}{unit}"
        for name, unit in zip(section.column_names, value_units, strict=True)
    )


def _join_sections(a: _Section, b: _Section) -> tuple[Type2DataRow, ...]:
    """Produce the merged output rows."""
    rows: list[Type2DataRow] = []

    # Numeric rows, labelled by 1-based sequence number.
    for seq, (row_a, row_b) in enumerate(
        zip(a.numeric_rows, b.numeric_rows, strict=True), start=1
    ):
        r_value = _normalise_number(row_a[0])
        phi_value = _normalise_number(row_a[1])
        values_a = [_normalise_number(v) for v in row_a[len(_INDEX_COLUMNS):]]
        values_b = [_normalise_number(v) for v in row_b[len(_INDEX_COLUMNS):]]
        rows.append(
            Type2DataRow(
                rec_label=str(seq),
                r=r_value,
                phi=phi_value,
                values=tuple(values_a + values_b),
            )
        )

    # Calc rows, labelled by their literal name.
    for row_a, row_b in zip(a.calc_rows, b.calc_rows, strict=True):
        label = row_a[0]
        values_a = [_normalise_number(v) for v in row_a[1:]]
        values_b = [_normalise_number(v) for v in row_b[1:]]
        rows.append(
            Type2DataRow(
                rec_label=label,
                r=_CALC_BLANK,
                phi=_CALC_BLANK,
                values=tuple(values_a + values_b),
            )
        )

    return tuple(rows)


def _normalise_number(token: str) -> str:
    """Strip trailing zeros from decimal values.

    Input ``"0.60"`` -> ``"0.6"``; ``"0.00"`` -> ``"0"``; ``"5"`` ->
    ``"5"``. Non-numeric tokens are returned unchanged so we don't
    accidentally rewrite labels.
    """
    try:
        value = float(token)
    except ValueError:
        return token
    # ``str(float(x))`` already normalises trailing zeros for sane inputs.
    # But ``str(0.0)`` returns ``"0.0"``, not ``"0"``; the expected output
    # has ``"0"`` for zeros, so special-case the integer values.
    if value == int(value):
        return str(int(value))
    return str(value)
