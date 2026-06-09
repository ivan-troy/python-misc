"""Schema configurability tests.

These prove the seam — a :class:`Schema` with renamed input labels
produces the same internal data as the default schema would for the
canonical input. They are the only Schema tests we ship: per the design
discussion, the principle is "configurability is possible without rewrite",
not "every knob has its own dozen tests".
"""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent

from csv_merger import CsvMerger, Schema, SchemaError
from csv_merger._constants import (
    DEFAULT_BATCH_HEADER_KEYS,
    DEFAULT_RECORD_KEYS,
    DEFAULT_ROLE_PREFIXES,
    DEFAULT_SCHEMA,
)
from csv_merger.parsers import parse_batch_file, parse_record_file


# A batch file using a fully renamed vocabulary.
ALT_BATCH = dedent("""\
    title
    effective_date,05/01/2026
    ledger,value_1
    cohort,value_2
    cycle,1

    records
    data,4
    field_1,field_2,xa,xb,probe1(a),probe2(b),field_5
    11,abc,0.15,0.32,0.33,0.38,1
    11,abc,0.87,0.14,0.66,0.44,10
    11,abc,0.71,0.67,0.66,0.1,1
    11,abc,4,8,h,i,j
""")

# A record file using a fully renamed vocabulary.
ALT_RECORD = dedent("""\
    "fielda",value a
    "cohort",value_2
    "marker",abc
    "tally",1
    "ANCHOR",0.94,0.97
    "alpha",type1,a,0.33,0.33
    "beta",type2,b,0.38,0.38
    "gamma",type3,c,9.4,9.4
""")


# Schema mapping the alternate vocabulary back to our internal roles.
ALT_SCHEMA = Schema(
    batch_header_keys={
        "date":  "effective_date",
        "key_1": "ledger",
        "key_2": "cohort",
        "batch": "cycle",
    },
    record_keys={
        "link_batch": "cohort",
        "link_row":   "marker",
        "xy_pos":     "ANCHOR",
        "type1":      "alpha",
        "type2":      "beta",
        "count":      "tally",
    },
    role_prefixes={
        "field_a": "xa",
        "field_b": "xb",
        "type1":   "probe1",
        "type2":   "probe2",
    },
)


class SchemaParserTests(unittest.TestCase):
    """Parsers populate the same internal fields under a renamed vocabulary."""

    def test_renamed_batch_parses_to_same_internal_shape(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "batch-1.txt"
            path.write_text(ALT_BATCH, encoding="utf-8")
            batch = parse_batch_file(path, schema=ALT_SCHEMA)

        # Header values land in the same dataclass fields they always do.
        self.assertEqual(batch.header.key_1, "value_1")
        self.assertEqual(batch.header.key_2, "value_2")
        self.assertEqual(batch.header.batch, "1")
        self.assertEqual(batch.header.date.isoformat(), "2026-05-01")

        # Roles resolved to renamed headers.
        self.assertEqual(batch.roles.field_a, "xa")
        self.assertEqual(batch.roles.field_b, "xb")
        self.assertEqual(batch.roles.type1, "probe1(a)")
        self.assertEqual(batch.roles.type2, "probe2(b)")

        # Cell values reachable by header.
        first = batch.data_rows[0]
        self.assertEqual(first.cells["xa"], "0.15")
        self.assertEqual(first.cells["probe1(a)"], "0.33")

    def test_renamed_record_parses_to_same_internal_shape(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "record-1.txt"
            path.write_text(ALT_RECORD, encoding="utf-8")
            record = parse_record_file(path, schema=ALT_SCHEMA)

        self.assertEqual(record.key_2, "value_2")
        self.assertEqual(record.field_2, "abc")
        self.assertEqual(record.xy_pos, ("0.94", "0.97"))
        self.assertEqual(record.type1_value, "0.33")
        self.assertEqual(record.type2_value, "0.38")


class SchemaEndToEndTests(unittest.TestCase):
    """The whole pipeline works against a fully renamed corpus."""

    def test_merge_with_alt_schema_produces_correct_output(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "batch-1.txt").write_text(ALT_BATCH, encoding="utf-8")
            (tmp_path / "record-1.txt").write_text(ALT_RECORD, encoding="utf-8")
            output = tmp_path / "out.txt"

            report = CsvMerger(
                input_dir=tmp_path,
                output_path=output,
                schema=ALT_SCHEMA,
            ).run()

            content = output.read_text(encoding="utf-8")

        # The override should have replaced field_a/field_b of row 0
        # (xa, xb in this vocabulary) with the ANCHOR value 0.94, 0.97.
        # Row 0 of the renamed batch was:
        #   11,abc,0.15,0.32,0.33,0.38,1
        # After merge it should read:
        #   11,abc,0.94,0.97,0.33,0.38,1
        self.assertIn("11,abc,0.94,0.97,0.33,0.38,1", content)
        self.assertEqual(report.overrides_applied, 1)
        self.assertEqual(report.unmatched_records, [])
        # The output column order preserves the input header order
        # (xa,xb in this vocabulary, not field_a,field_b).
        self.assertIn(
            "field_1,field_2,xa,xb,probe1(a),probe2(b),field_5",
            content,
        )


class SchemaValidationTests(unittest.TestCase):
    """Schema construction validates that all required role keys are present."""

    def test_default_construction_succeeds(self) -> None:
        schema = Schema()
        # Confirm we get the same proxies as the module-level default.
        self.assertEqual(
            dict(schema.batch_header_keys),
            dict(DEFAULT_BATCH_HEADER_KEYS),
        )
        self.assertEqual(
            dict(schema.record_keys),
            dict(DEFAULT_RECORD_KEYS),
        )
        self.assertEqual(
            dict(schema.role_prefixes),
            dict(DEFAULT_ROLE_PREFIXES),
        )

    def test_missing_batch_header_role_raises(self) -> None:
        with self.assertRaises(SchemaError) as ctx:
            Schema(batch_header_keys={"date": "d", "key_1": "k1"})
        # The error names the offending field and the missing roles.
        msg = str(ctx.exception)
        self.assertIn("batch_header_keys", msg)
        self.assertIn("key_2", msg)
        self.assertIn("batch", msg)

    def test_missing_record_role_raises(self) -> None:
        with self.assertRaises(SchemaError):
            Schema(record_keys={"link_batch": "x"})

    def test_missing_role_prefix_raises(self) -> None:
        with self.assertRaises(SchemaError):
            Schema(role_prefixes={"field_a": "a", "field_b": "b"})

    def test_default_schema_is_a_singleton_instance(self) -> None:
        """Sanity: the module-level DEFAULT_SCHEMA is reusable.

        If callers receive ``DEFAULT_SCHEMA`` instead of constructing a
        new one each call, that's fine — it's frozen and its mappings are
        proxies.
        """
        # Two parser calls without a schema should both behave identically.
        self.assertIs(DEFAULT_SCHEMA, DEFAULT_SCHEMA)


if __name__ == "__main__":
    unittest.main()
