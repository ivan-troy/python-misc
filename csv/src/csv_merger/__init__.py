"""csv_merger — merge sectioned batch and record CSV files into a single output."""

from csv_merger._constants import (
    CsvMergerError,
    InputTooLargeError,
    MalformedBatchFile,
    MalformedRecordFile,
    Schema,
    SchemaError,
)
from csv_merger.batch_type2 import (
    Type2DataRow,
    Type2Report,
    parse_batch_file_type2,
    write_type2_csv,
)
from csv_merger.merger import CsvMerger, MergeReport, NoBatchesInRangeError
from csv_merger.models import (
    BatchFile,
    BatchRow,
    ColumnRoles,
    Header,
    RecordFile,
)

__all__ = [
    # Models
    "BatchFile",
    "BatchRow",
    "ColumnRoles",
    "Header",
    "RecordFile",
    "Type2DataRow",
    "Type2Report",
    # Pipeline
    "CsvMerger",
    "MergeReport",
    "Schema",
    "parse_batch_file_type2",
    "write_type2_csv",
    # Exceptions
    "CsvMergerError",
    "InputTooLargeError",
    "MalformedBatchFile",
    "MalformedRecordFile",
    "NoBatchesInRangeError",
    "SchemaError",
]

__version__ = "0.5.0"
