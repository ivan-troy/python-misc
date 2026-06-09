"""Tests for the public package surface."""

from __future__ import annotations

import unittest

import csv_merger


class PublicApiTests(unittest.TestCase):
    """The public exception hierarchy should be importable from the root."""

    def test_exceptions_exposed_at_package_root(self) -> None:
        for name in (
            "CsvMergerError",
            "MalformedBatchFile",
            "MalformedRecordFile",
            "InputTooLargeError",
            "NoBatchesInRangeError",
        ):
            self.assertTrue(
                hasattr(csv_merger, name),
                f"csv_merger should export {name!r}",
            )

    def test_subclass_relationships(self) -> None:
        """All concrete errors derive from CsvMergerError; CsvMergerError
        is in turn a ValueError so legacy callers still catch it."""
        self.assertTrue(
            issubclass(csv_merger.CsvMergerError, ValueError)
        )
        for cls in (
            csv_merger.MalformedBatchFile,
            csv_merger.MalformedRecordFile,
            csv_merger.InputTooLargeError,
            csv_merger.NoBatchesInRangeError,
        ):
            self.assertTrue(
                issubclass(cls, csv_merger.CsvMergerError),
                f"{cls.__name__} should derive from CsvMergerError",
            )


if __name__ == "__main__":
    unittest.main()
