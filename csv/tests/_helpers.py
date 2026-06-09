"""Shared test fixtures and helpers."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "sample_data"


# --- Reference fixtures ---------------------------------------------------- #


BATCH_1 = dedent("""\
    title
    date,05/01/2026
    key_1,value_1
    key_2,value_2
    batch,1

    records
    data,4
    field_1,field_2,field_a,field_b,type1(a),type2(b),field_5
    11,abc,0.15,0.32,0.33,0.38,1
    11,abc,0.87,0.14,0.66,0.44,10
    11,abc,0.71,0.67,0.66,0.1,1
    11,abc,4,8,h,i,j

    records (do not include)
    data,4
    field_1,field_2,field_a,field_b,type1(a),type2(b),field_5
    11,abc,0.57,0.08,0.47,0.56,3
    11,abc,0.73,0.27,0.25,0.4,3
    11,abc,0.8,0.54,0.93,0.23,3
    11,abc,4,8,h,i,j
""")


RECORD_1 = dedent("""\
    "fielda",value a
    "key_2",value_2
    "field_2",abc
    "count",1
    "XY_POS",0.94,0.97
    "record_1",type1,a,0.33,0.33
    "record_2",type2,b,0.38,0.38
    "record_3",type3,c,9.4,9.4
""")


# --- Builders -------------------------------------------------------------- #


def write_file(directory: Path, name: str, content: str) -> Path:
    """Write ``content`` to ``directory/name`` and return the path."""
    path = directory / name
    path.write_text(content, encoding="utf-8")
    return path


def make_batch(
    *,
    date_str: str = "05/01/2026",
    key_2: str = "value_2",
    column_headers: str = "field_1,field_2,field_a,field_b,type1(a),type2(b),field_5",
    data_rows: tuple[str, ...] = (
        "11,abc,0.15,0.32,0.33,0.38,1",
        "11,abc,0.87,0.14,0.66,0.44,10",
        "11,abc,0.71,0.67,0.66,0.1,1",
    ),
    trailer: str = "11,abc,4,8,h,i,j",
) -> str:
    """Build a batch-file fixture (defaults reproduce canonical batch-1)."""
    rows = "\n".join(data_rows + (trailer,))
    data_count = len(data_rows) + 1
    return dedent(f"""\
        title
        date,{date_str}
        key_1,value_1
        key_2,{key_2}
        batch,1

        records
        data,{data_count}
        {column_headers}
        {rows}
    """)


def make_record(
    *,
    key_2: str = "value_2",
    field_2: str = "abc",
    xy_pos: tuple[str, str] = ("0.94", "0.97"),
    type1_value: str = "0.33",
    type2_value: str = "0.38",
    count: int = 1,
) -> str:
    """Build a record-file fixture. Defaults reproduce record-1."""
    x, y = xy_pos
    return dedent(f"""\
        "fielda",value a
        "key_2",{key_2}
        "field_2",{field_2}
        "count",{count}
        "XY_POS",{x},{y}
        "record_1",type1,a,{type1_value},{type1_value}
        "record_2",type2,b,{type2_value},{type2_value}
        "record_3",type3,c,9.4,9.4
    """)
