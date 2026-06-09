"""Project-wide constants and shared exceptions.

Centralising these here gives us a single source of truth for the date
format, file-size limits, and exception hierarchy. Modules import from this
rather than redeclaring constants locally.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

# --------------------------------------------------------------------------- #
# Format constants                                                            #
# --------------------------------------------------------------------------- #

#: ``strftime``/``strptime`` format used by every date in the project
#: (batch headers, CLI ``--start``/``--end`` flags, output writer).
DATE_FORMAT: str = "%m/%d/%Y"

#: Records-block label that should be included in the merged output.
INCLUDE_LABEL: str = "records"

#: Records-block label prefix that signals the section should be skipped
#: (e.g. ``records (do not include)``).
EXCLUDE_LABEL_PREFIX: str = "records ("

# --------------------------------------------------------------------------- #
# Safety limits                                                               #
# --------------------------------------------------------------------------- #

#: Refuse to read input files larger than this many bytes. Any single batch
#: or record file in this project should be well under a megabyte; the limit
#: is a defence-in-depth guard against being pointed at a pathologically
#: large file. Override per-call if you legitimately need to.
MAX_INPUT_FILE_BYTES: int = 8 * 1024 * 1024  # 8 MiB

# --------------------------------------------------------------------------- #
# Exceptions                                                                  #
# --------------------------------------------------------------------------- #


class CsvMergerError(ValueError):
    """Base class for every error raised by this package.

    Inheriting from :class:`ValueError` keeps backward compatibility with
    callers that catch the broader type, while letting newer callers catch
    only ``CsvMergerError`` and avoid swallowing unrelated value errors.
    """


class MalformedBatchFile(CsvMergerError):
    """Raised when a batch file cannot be parsed."""


class MalformedRecordFile(CsvMergerError):
    """Raised when a record file cannot be parsed."""


class InputTooLargeError(CsvMergerError):
    """Raised when an input file exceeds :data:`MAX_INPUT_FILE_BYTES`."""


class SchemaError(CsvMergerError):
    """Raised when a :class:`Schema` is missing required role keys."""


# --------------------------------------------------------------------------- #
# Schema (input vocabulary)                                                   #
# --------------------------------------------------------------------------- #
#
# The :class:`Schema` describes which **labels** in the input files map to
# the parser's internal **roles**. The parser's dataclass field names
# (``Header.date``, ``RecordFile.key_2``, ...) and the matcher's link logic
# stay constant; only the strings the parser looks up in the input change.
#
# This is the seam to use when a sister system uses different vocabulary
# (e.g. ``effective_date`` instead of ``date``). Default-construct
# :class:`Schema` to get today's behaviour.

#: Role -> input-label map for the batch-file header block.
DEFAULT_BATCH_HEADER_KEYS: Mapping[str, str] = MappingProxyType({
    "date": "date",
    "key_1": "key_1",
    "key_2": "key_2",
    "batch": "batch",
})

#: Role -> input-label map for the record-file key/value block.
DEFAULT_RECORD_KEYS: Mapping[str, str] = MappingProxyType({
    "link_batch": "key_2",   # links a record to its batch
    "link_row":   "field_2", # batch-level metadata carried by the record
    "xy_pos":     "XY_POS",  # (x, y) override applied to field_a / field_b
    "type1":      "record_1",
    "type2":      "record_2",
    "count":      "count",
})

#: Role -> header-prefix map used to resolve column roles inside a
#: records block. Headers are matched by ``startswith`` so ``type1(a)``
#: resolves to the ``type1`` role.
DEFAULT_ROLE_PREFIXES: Mapping[str, str] = MappingProxyType({
    "field_a": "field_a",
    "field_b": "field_b",
    "type1":   "type1",
    "type2":   "type2",
})


@dataclass(frozen=True)
class Schema:
    """Configurable input vocabulary.

    Default-construct to keep today's behaviour. Override any of the three
    maps to adapt to a sister system whose labels differ from ours.

    The matcher does not consult the Schema directly — it reads named
    fields off :class:`RecordFile` and :class:`Header`. Linking logic is
    therefore stable regardless of which input labels populate those
    fields.

    Required role keys (the parser will look these up by name):

    * ``batch_header_keys``: ``date``, ``key_1``, ``key_2``, ``batch``.
    * ``record_keys``: ``link_batch``, ``link_row``, ``xy_pos``,
      ``type1``, ``type2``, ``count``.
    * ``role_prefixes``: ``field_a``, ``field_b``, ``type1``, ``type2``.

    Constructing a :class:`Schema` with any required role missing raises
    :class:`SchemaError` immediately, so misconfiguration surfaces at
    construction time rather than as a confusing ``KeyError`` mid-parse.
    """

    batch_header_keys: Mapping[str, str] = field(
        default_factory=lambda: DEFAULT_BATCH_HEADER_KEYS
    )
    record_keys: Mapping[str, str] = field(
        default_factory=lambda: DEFAULT_RECORD_KEYS
    )
    role_prefixes: Mapping[str, str] = field(
        default_factory=lambda: DEFAULT_ROLE_PREFIXES
    )

    def __post_init__(self) -> None:
        self._validate_complete(
            "batch_header_keys",
            self.batch_header_keys,
            DEFAULT_BATCH_HEADER_KEYS.keys(),
        )
        self._validate_complete(
            "record_keys",
            self.record_keys,
            DEFAULT_RECORD_KEYS.keys(),
        )
        self._validate_complete(
            "role_prefixes",
            self.role_prefixes,
            DEFAULT_ROLE_PREFIXES.keys(),
        )

    @staticmethod
    def _validate_complete(
        name: str,
        provided: Mapping[str, str],
        required: Iterable[str],
    ) -> None:
        missing = [key for key in required if key not in provided]
        if missing:
            raise SchemaError(
                f"Schema.{name} is missing required role(s): {missing!r}"
            )


#: A shared default-constructed schema, used when callers don't pass one
#: explicitly. Constructed once for efficiency; safe because :class:`Schema`
#: is frozen and its default mappings are read-only proxies.
DEFAULT_SCHEMA: "Schema" = Schema()
