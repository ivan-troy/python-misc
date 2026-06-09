"""Output writer for the merged file.

Produces the format shown in ``sample_data/expected-output.txt``: the
header block of the earliest in-range batch followed by every batch's
records section.

Writes are **atomic**: the body is written to a sibling temp file in the
destination directory and then renamed into place. If anything raises
mid-write (a bad override key, a disk-full error, etc.), the destination
either still holds the previous version or never exists at all — we never
leave a partially-written file behind.
"""

from __future__ import annotations

import csv
import logging
import os
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import IO

from csv_merger._constants import DATE_FORMAT, INCLUDE_LABEL
from csv_merger.models import BatchFile

logger = logging.getLogger(__name__)

_OUTPUT_ENCODING = "utf-8"
_LINE_TERMINATOR = "\n"


def write_output(
    output_path: Path,
    batches: list[BatchFile],
    overrides: dict[tuple[str, int], tuple[str, str]],
) -> None:
    """Write the merged file atomically.

    Args:
        output_path: Destination file.
        batches: Batches to include, in the order they should appear.
        overrides: ``(batch_path, data_row_index)`` -> ``(field_a, field_b)``
            pairs to splice into the output. ``data_row_index`` is the
            row's index within :attr:`BatchFile.data_rows` (trailer
            excluded).
    """
    if not batches:
        raise ValueError("cannot write an output with zero batches")

    logger.debug(
        "write_output: target=%s batches=%d overrides=%d",
        output_path,
        len(batches),
        len(overrides),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(
        output_path,
        lambda fh: _serialise(fh, batches, overrides),
    )
    logger.info("wrote merged output to %s", output_path)


def _serialise(
    fh: IO[str],
    batches: list[BatchFile],
    overrides: dict[tuple[str, int], tuple[str, str]],
) -> None:
    """Write the full merged document to ``fh``."""
    writer = csv.writer(fh, lineterminator=_LINE_TERMINATOR)

    # Top header block (taken from the earliest in-range batch).
    primary = batches[0]
    writer.writerow([primary.header.title])
    writer.writerow(["date", primary.header.date.strftime(DATE_FORMAT)])
    writer.writerow(["key_1", primary.header.key_1])
    writer.writerow(["key_2", primary.header.key_2])
    writer.writerow(["batches", str(len(batches))])
    writer.writerow([])

    # Per-batch records sections.
    for batch in batches:
        logger.debug(
            "writing batch %s: %d data rows, trailer=%s",
            batch.path.name,
            len(batch.data_rows),
            batch.trailer_row is not None,
        )
        writer.writerow([INCLUDE_LABEL])
        writer.writerow(["data", str(batch.data_count)])
        writer.writerow(batch.column_headers)

        field_a_header = batch.roles.field_a
        field_b_header = batch.roles.field_b
        batch_path_str = str(batch.path)
        overrides_applied = 0

        for idx, row in enumerate(batch.data_rows):
            key = (batch_path_str, idx)
            if key in overrides:
                x, y = overrides[key]
                logger.debug(
                    "override: %s row %d -> (%s, %s) = (%s, %s)",
                    batch.path.name, idx,
                    field_a_header, field_b_header, x, y,
                )
                row = row.with_replacements(
                    {field_a_header: x, field_b_header: y}
                )
                overrides_applied += 1
            writer.writerow(row.to_csv_row())

        if batch.trailer_row is not None:
            writer.writerow(batch.trailer_row.to_csv_row())
        writer.writerow([])

        logger.debug(
            "batch %s done: %d overrides applied",
            batch.path.name, overrides_applied,
        )


def _atomic_write(
    output_path: Path,
    body: Callable[[IO[str]], None],
) -> None:
    """Write to a temp file in the destination directory, then rename.

    The temp file is unlinked on any error so we don't leave debris on disk.
    """
    # delete=False because we manage cleanup explicitly; using a
    # NamedTemporaryFile in the destination directory ensures the final
    # ``os.replace`` is on the same filesystem (otherwise it would fall
    # back to a non-atomic copy + delete).
    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=str(output_path.parent),
    )
    tmp_path = Path(tmp_name)
    logger.debug("atomic write: tmp=%s -> final=%s", tmp_path, output_path)
    try:
        with os.fdopen(
            tmp_fd, "w", encoding=_OUTPUT_ENCODING, newline=""
        ) as fh:
            body(fh)
            fh.flush()
            try:
                os.fsync(fh.fileno())
                logger.debug("fsync ok on %s", tmp_path)
            except OSError:
                # fsync may be unsupported on some filesystems (e.g. tmpfs
                # used in test sandboxes). Atomicity of the subsequent
                # ``os.replace`` is unaffected.
                logger.debug("fsync not supported on %s", tmp_path)
        os.replace(tmp_path, output_path)
        logger.debug("atomic write: rename succeeded -> %s", output_path)
    except BaseException as exc:
        # Clean up the temp file on any error, including KeyboardInterrupt.
        logger.warning(
            "atomic write failed (%s); cleaning up temp file %s",
            type(exc).__name__,
            tmp_path,
        )
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise
