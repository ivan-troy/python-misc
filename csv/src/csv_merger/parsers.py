"""Parsers for batch files and record files.

The file format is line-oriented and CSV-ish but **sectioned**: a key/value
header at the top, then one or more named data blocks separated by blank
lines. We use the stdlib :mod:`csv` module for tokenisation so quoted
values (common in record files) are handled correctly.

Inputs are read with the ``utf-8-sig`` codec so a leading byte-order mark
is silently consumed instead of being attached to the first cell.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from csv_merger._constants import (
    DATE_FORMAT,
    DEFAULT_SCHEMA,
    EXCLUDE_LABEL_PREFIX,
    INCLUDE_LABEL,
    InputTooLargeError,
    MAX_INPUT_FILE_BYTES,
    MalformedBatchFile,
    MalformedRecordFile,
    Schema,
)
from csv_merger.models import (
    BatchFile,
    BatchRow,
    ColumnRoles,
    Header,
    MissingColumnRole,
    RecordFile,
)

logger = logging.getLogger(__name__)

# --- File encoding ---------------------------------------------------------- #
# ``utf-8-sig`` strips a leading BOM if present and behaves identically to
# ``utf-8`` for files without one. This prevents a BOM from silently being
# attached to the first cell of the first row (which would corrupt the
# ``title`` value).
_INPUT_ENCODING = "utf-8-sig"

# --- Indices inside ``record_1`` / ``record_2`` rows ------------------------ #
# Layout: [0] label, [1] type-name, [2] type-letter, [3] value, [4] value
_RECORD_VALUE_COL = 3
_RECORD_ROW_MIN_WIDTH = _RECORD_VALUE_COL + 1


# --------------------------------------------------------------------------- #
# Internal exception for key/value-block lookup                               #
# --------------------------------------------------------------------------- #


class _KeyLookupError(LookupError):
    """Raised by :func:`_kv_lookup` when a key is missing or empty.

    ``empty`` distinguishes "key not present" (``False``) from "key
    present with no value" (``True``) so the caller can produce a
    targeted error message.
    """

    def __init__(self, key: str, *, empty: bool) -> None:
        super().__init__(key)
        self.key = key
        self.empty = empty


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _check_size(path: Path, max_bytes: int = MAX_INPUT_FILE_BYTES) -> None:
    """Raise :class:`InputTooLargeError` if ``path`` exceeds ``max_bytes``."""
    size = path.stat().st_size
    logger.debug("file size: %s = %d bytes (limit %d)", path, size, max_bytes)
    if size > max_bytes:
        raise InputTooLargeError(
            f"{path}: file size {size} bytes exceeds limit {max_bytes}"
        )


def _read_csv_rows(path: Path) -> list[list[str]]:
    """Return every line of ``path`` as a list-of-fields, preserving blanks.

    Blank lines come back as ``[]`` so callers can split sections on them.
    Cells are stripped of surrounding whitespace (incl. stray tabs we have
    seen in real-world inputs).
    """
    _check_size(path)
    logger.debug("reading %s with encoding %s", path, _INPUT_ENCODING)
    with path.open(newline="", encoding=_INPUT_ENCODING) as fh:
        rows = [
            [cell.strip() for cell in row]
            for row in csv.reader(fh)
        ]
    logger.debug("read %d raw rows from %s", len(rows), path.name)
    return rows


def _kv_lookup(rows: list[list[str]], key: str) -> str:
    """Return the value of the first ``[key, value, ...]`` row.

    Raises:
        _KeyLookupError: with ``empty=False`` if absent, ``empty=True`` if
            present with no value.
    """
    for row in rows:
        if row and row[0] == key:
            if len(row) < 2 or row[1] == "":
                raise _KeyLookupError(key, empty=True)
            return row[1]
    raise _KeyLookupError(key, empty=False)


def _split_into_sections(rows: list[list[str]]) -> list[list[list[str]]]:
    """Split ``rows`` on blank-line boundaries; empty sections are dropped."""
    sections: list[list[list[str]]] = []
    current: list[list[str]] = []
    for row in rows:
        if not row or all(cell == "" for cell in row):
            if current:
                sections.append(current)
                current = []
        else:
            current.append(row)
    if current:
        sections.append(current)
    return sections


# --------------------------------------------------------------------------- #
# Batch files                                                                 #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class _RecordsSection:
    """Internal: result of parsing a single ``records`` block."""

    column_headers: list[str]
    rows: list[BatchRow]
    data_count: int
    roles: ColumnRoles


def parse_batch_file(
    path: Path,
    schema: Schema | None = None,
) -> BatchFile:
    """Parse a single batch file.

    The file has three logical sections separated by blank lines:

    1. Header block (``title``, ``date``, ``key_1``, ``key_2``, ``batch``).
    2. ``records`` block — the include section we keep.
    3. ``records (do not include)`` block(s) — skipped.

    Args:
        path: Path to the file.
        schema: Input vocabulary. Defaults to
            :data:`~csv_merger._constants.DEFAULT_SCHEMA`. Pass a custom
            :class:`~csv_merger._constants.Schema` to support a sister
            format whose labels differ from ours.

    Returns:
        A :class:`BatchFile`.

    Raises:
        MalformedBatchFile: if any required structure is missing or invalid.
        InputTooLargeError: if the file exceeds the size limit.
    """
    schema = schema if schema is not None else DEFAULT_SCHEMA
    logger.debug("parsing batch file: %s", path)
    raw = _read_csv_rows(path)
    sections = _split_into_sections(raw)
    logger.debug("%s: split into %d sections", path.name, len(sections))
    if len(sections) < 2:
        raise MalformedBatchFile(
            f"{path}: expected at least 2 blank-line separated sections, "
            f"got {len(sections)}"
        )

    header = _parse_header(sections[0], path, schema)
    logger.debug(
        "%s: header date=%s key_1=%s key_2=%s batch=%s",
        path.name,
        header.date,
        header.key_1,
        header.key_2,
        header.batch,
    )

    include_section = _find_include_section(sections, path)
    parsed = _parse_records_section(include_section, path, schema)
    logger.debug(
        "%s: parsed %d rows (incl. trailer), %d columns, roles=%s",
        path.name,
        len(parsed.rows),
        len(parsed.column_headers),
        parsed.roles,
    )

    return BatchFile(
        path=path,
        header=header,
        column_headers=parsed.column_headers,
        roles=parsed.roles,
        rows=parsed.rows,
        data_count=parsed.data_count,
    )


def _parse_header(
    header_section: list[list[str]],
    path: Path,
    schema: Schema,
) -> Header:
    """Parse the top key/value block into a :class:`Header`."""
    keys = schema.batch_header_keys
    try:
        date_str = _kv_lookup(header_section, keys["date"])
        key_1 = _kv_lookup(header_section, keys["key_1"])
        key_2 = _kv_lookup(header_section, keys["key_2"])
        batch = _kv_lookup(header_section, keys["batch"])
    except _KeyLookupError as exc:
        problem = "is empty" if exc.empty else "is missing"
        raise MalformedBatchFile(
            f"{path}: header field {exc.key!r} {problem}"
        ) from exc

    try:
        date_value = datetime.strptime(date_str, DATE_FORMAT).date()
    except ValueError as exc:
        raise MalformedBatchFile(
            f"{path}: invalid date {date_str!r}, expected {DATE_FORMAT}"
        ) from exc

    title = header_section[0][0] if header_section[0] else "title"
    return Header(
        date=date_value,
        key_1=key_1,
        key_2=key_2,
        batch=batch,
        title=title,
    )


def _find_include_section(
    sections: list[list[list[str]]], path: Path
) -> list[list[str]]:
    """Pick the ``records`` section, ignoring ``records (do not include)``."""
    for section in sections[1:]:
        if not section:
            continue
        label_row = section[0]
        if not label_row:
            continue
        label = label_row[0]
        if label == INCLUDE_LABEL:
            logger.debug("%s: found include section %r", path.name, label)
            return section
        if label.startswith(EXCLUDE_LABEL_PREFIX):
            logger.debug("%s: skipping section %r", path.name, label)
            continue
        logger.debug(
            "%s: ignoring unrecognised section label %r", path.name, label
        )
    raise MalformedBatchFile(
        f"{path}: no '{INCLUDE_LABEL}' section found"
    )


def _parse_records_section(
    section: list[list[str]],
    path: Path,
    schema: Schema,
) -> _RecordsSection:
    """Parse a single records section.

    Layout:

    * ``[0]``: ``['records']``
    * ``[1]``: ``['data', 'N']``
    * ``[2]``: column headers
    * ``[3..]``: data rows; the **last** is the trailer sentinel.

    The block must contain at least one real data row plus the trailer
    (i.e. at least 5 lines total).
    """
    if len(section) < 5:
        raise MalformedBatchFile(
            f"{path}: records section must contain header, data line, "
            f"column headers, at least one data row, and a trailer "
            f"(got {len(section)} lines)"
        )

    data_count = _parse_data_count(section[1], path)
    column_headers = _validate_column_headers(section[2], path)

    try:
        roles = ColumnRoles.from_headers(
            column_headers, prefixes=schema.role_prefixes
        )
    except MissingColumnRole as exc:
        raise MalformedBatchFile(f"{path}: {exc}") from exc

    column_order = tuple(column_headers)
    rows: list[BatchRow] = []
    for raw_row in section[3:]:
        if len(raw_row) != len(column_headers):
            raise MalformedBatchFile(
                f"{path}: data row has {len(raw_row)} columns, expected "
                f"{len(column_headers)}: {raw_row!r}"
            )
        cells = dict(zip(column_headers, raw_row, strict=True))
        rows.append(BatchRow(cells=cells, column_order=column_order))

    return _RecordsSection(
        column_headers=column_headers,
        rows=rows,
        data_count=data_count,
        roles=roles,
    )


def _parse_data_count(data_row: list[str], path: Path) -> int:
    if len(data_row) < 2 or data_row[0] != "data":
        raise MalformedBatchFile(
            f"{path}: expected 'data,N' as second line of records section"
        )
    try:
        return int(data_row[1])
    except ValueError as exc:
        raise MalformedBatchFile(
            f"{path}: invalid data count {data_row[1]!r}"
        ) from exc


def _validate_column_headers(
    column_headers: list[str], path: Path
) -> list[str]:
    if not column_headers:
        raise MalformedBatchFile(f"{path}: empty column headers row")
    if len(set(column_headers)) != len(column_headers):
        raise MalformedBatchFile(
            f"{path}: duplicate column headers: {column_headers!r}"
        )
    return column_headers


# --------------------------------------------------------------------------- #
# Record files                                                                #
# --------------------------------------------------------------------------- #


def parse_record_file(
    path: Path,
    schema: Schema | None = None,
) -> RecordFile:
    """Parse a record file into a :class:`RecordFile`.

    The format is a flat key/value list:

    .. code-block:: text

        "fielda",value a
        "key_2",value_2
        "field_2",abc
        "count",1
        "XY_POS",x,y
        "record_1",type1,a,t1_value,t1_value
        "record_2",type2,b,t2_value,t2_value
        "record_3",type3,c,9.4,9.4

    Some inputs in the wild contain partially-quoted labels and stray tabs
    after values. The parser tolerates both: ``csv.reader`` strips matched
    quotes during tokenisation, and we additionally strip any residual
    quote/whitespace characters from each cell.

    Args:
        path: Path to the file.
        schema: Input vocabulary. Defaults to
            :data:`~csv_merger._constants.DEFAULT_SCHEMA`.

    Raises:
        MalformedRecordFile: if any required field is missing or malformed.
        InputTooLargeError: if the file exceeds the size limit.
    """
    schema = schema if schema is not None else DEFAULT_SCHEMA
    keys = schema.record_keys

    raw = _read_csv_rows(path)
    rows = [
        [cell.strip().strip('"') for cell in row]
        for row in raw
        if row
    ]

    try:
        key_2 = _kv_lookup(rows, keys["link_batch"])
        field_2 = _kv_lookup(rows, keys["link_row"])
    except _KeyLookupError as exc:
        problem = "is empty" if exc.empty else "is missing"
        raise MalformedRecordFile(
            f"{path}: key {exc.key!r} {problem}"
        ) from exc

    xy_row = _find_row(rows, keys["xy_pos"], path)
    if len(xy_row) < 3:
        raise MalformedRecordFile(
            f"{path}: {keys['xy_pos']!r} row needs at least 3 cells "
            f"(label, x, y)"
        )
    xy_pos = (xy_row[1], xy_row[2])

    type1_value = _extract_record_value(rows, keys["type1"], path)
    type2_value = _extract_record_value(rows, keys["type2"], path)
    count = _read_count(rows, path, count_key=keys["count"])

    record = RecordFile(
        path=path,
        key_2=key_2,
        field_2=field_2,
        xy_pos=xy_pos,
        type1_value=type1_value,
        type2_value=type2_value,
        count=count,
    )
    logger.debug(
        "parsed record %s: key_2=%s field_2=%s xy_pos=%s type1=%s type2=%s",
        path.name,
        key_2,
        field_2,
        xy_pos,
        type1_value,
        type2_value,
    )
    return record


def _find_row(rows: list[list[str]], label: str, path: Path) -> list[str]:
    for row in rows:
        if row and row[0] == label:
            return row
    raise MalformedRecordFile(f"{path}: missing row {label!r}")


def _extract_record_value(
    rows: list[list[str]], label: str, path: Path
) -> str:
    """Read the matching cell from a ``record_1`` / ``record_2`` row."""
    row = _find_row(rows, label, path)
    if len(row) < _RECORD_ROW_MIN_WIDTH:
        raise MalformedRecordFile(
            f"{path}: {label!r} row needs at least "
            f"{_RECORD_ROW_MIN_WIDTH} cells, got {len(row)}"
        )
    return row[_RECORD_VALUE_COL]


def _read_count(
    rows: list[list[str]],
    path: Path,
    count_key: str = "count",
) -> int:
    """Read the optional ``count`` field, defaulting to 1 with a warning."""
    try:
        raw = _kv_lookup(rows, count_key)
    except _KeyLookupError:
        logger.warning(
            "%s: %r missing or empty; defaulting to 1", path, count_key
        )
        return 1
    try:
        return int(raw)
    except ValueError:
        logger.warning(
            "%s: %r value %r is not an integer; defaulting to 1",
            path,
            count_key,
            raw,
        )
        return 1
