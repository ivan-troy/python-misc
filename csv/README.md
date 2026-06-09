# csv-merger

A small, dependency-free Python tool that merges a set of **batch** files and
**record** files into a single combined output file.

The tool is intentionally written using **only the Python standard library**
(`csv`, `dataclasses`, `pathlib`, `argparse`, `datetime`, `logging`, `unittest`).
It avoids `pandas` and other heavy dependencies as requested.

---

## 1. The data model (what's actually in the files)

Both input file kinds are line-oriented CSV-ish text. They are not pure CSV —
they are sectioned, with header blocks and data blocks separated by blank lines.

### Batch file (`batch-N.txt`)

```
title
date,05/01/2026
key_1,value_1
key_2,value_2
batch,1

records                                    <-- "include" section header
data,4
field_1,field_2,field_a,field_b,type1(a),type2(b),field_5
11,abc,0.15,0.32,0.33,0.38,1
11,abc,0.87,0.14,0.66,0.44,10
11,abc,0.71,0.67,0.66,0.1,1
11,abc,4,8,h,i,j                           <-- trailer / sentinel row

records (do not include)                   <-- "exclude" section header
... (skipped entirely)
```

Each batch contributes exactly **one** `records` block to the output (the one
labelled `records`, not `records (do not include)`).

### Record file (`record-N.txt`)

```
"fielda",value a
"key_2",value_2
"field_2",abc
"count",1
"XY_POS",0.94,0.97
"record_1",type1,a,0.33,0.33
"record_2",type2,b,0.38,0.38
"record_3",type3,c,9.4,9.4
```

Record files carry **override values** that replace fields in batch rows:
- `XY_POS` → replaces `field_a`, `field_b` of one batch row
- `record_1` → carries the `type1` value used to **match** the batch row
- `record_2` → carries the `type2` value used to **match** the batch row
- `key_2` and `field_2` link the record file back to the right batch

### Output file

The output concatenates batch headers (taken from the **earliest** batch in
range) followed by every batch's `records` section, with each row's
`field_a, field_b` replaced by the matching record's `XY_POS`.

---

## 2. Matching: how a record file finds its row

I considered three possible strategies. The chosen strategy is **(C)**.

### (A) Filename-positional mapping
Use `record-N.txt`'s number to compute `(batch_index, row_index)` —
e.g. record-1 → batch 1 row 1, record-4 → batch 2 row 1.

- ✅ Trivial to implement.
- ❌ Brittle: rename a file and everything breaks.
- ❌ Doesn't match the prompt's stated criteria (`key_2`, `field_2`,
  `record_1`/`record_2` values).
- ❌ Doesn't explain why `record-13.txt` exists (a 13th record when there are
  only 12 batch rows).

### (B) Pure value-based matching
Match on `(key_2, field_2, type1_value, type2_value)` — ignore filenames
entirely.

- ✅ Robust, declarative, handles renames, handles extras like `record-13`.
- ❌ Two batch rows can legitimately share the same `(type1, type2)` pair (see
  batch-1: rows 2 and 3 both have `type1=0.66`). Pure value matching becomes
  ambiguous and you need a tiebreaker.

### (C) Value-based matching with positional tiebreak ✅ recommended
Match each record file to the **batch** whose `key_2` and `field_2` agree, then
within that batch find rows whose `type1`/`type2` agree, breaking ties by row
order.

- ✅ Handles the real ambiguity in the sample data (batch-1 has duplicate
  `type1=0.66`).
- ✅ Tolerates extra/orphan record files like `record-13.txt` — they simply
  don't get assigned and the merger reports them.
- ✅ Doesn't depend on filename numbering.
- ✅ Explainable: every assignment is traceable back to data, not file index.

This is what `RecordMatcher` implements.

---

## 3. Date-range filter

Batches are included only if their `date` falls within `[start, end]`
inclusive. Dates are parsed as `MM/DD/YYYY` (matches the sample data).
Batches outside the window are dropped before matching runs.

If no `--start` / `--end` is given, all batches are included.

---

## 4. Project layout

```
csv_merger/
├── pyproject.toml
├── README.md
├── src/csv_merger/
│   ├── __init__.py
│   ├── __main__.py        # `python -m csv_merger ...`
│   ├── models.py          # dataclasses: BatchFile, RecordFile, Header
│   ├── parsers.py         # parse_batch_file, parse_record_file
│   ├── matcher.py         # RecordMatcher: assigns records to batch rows
│   ├── merger.py          # CsvMerger: orchestrates the whole pipeline
│   ├── writer.py          # write_output
│   └── cli.py             # argparse entry point
├── tests/
│   ├── test_parsers.py
│   ├── test_matcher.py
│   ├── test_merger.py
│   └── test_cli.py
└── sample_data/           # the files from the prompt
```

---

## 5. Setup, test, run (with `uv`)

```bash
# one-time: install uv (https://github.com/astral-sh/uv) if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# from inside the project directory:
uv sync                              # creates .venv, installs the package
uv run pytest                        # run all tests
uv run python -m csv_merger \
     --inputs sample_data \
     --output ./out.txt              # produce the merged output
uv run python -m csv_merger --help   # see all flags
```

Verbosity (Unix-style `-v` count, or explicit `--log-level`):

```bash
# default: only WARNINGs and the final summary
uv run python -m csv_merger --inputs sample_data --output ./out.txt

# -v: one INFO line per pipeline milestone
uv run python -m csv_merger --inputs sample_data --output ./out.txt -v

# -vv: full DEBUG trace (every file read, every match attempt, every
# override application, atomic-write target/temp pair, fsync status...)
uv run python -m csv_merger --inputs sample_data --output ./out.txt -vv

# Equivalent explicit form:
uv run python -m csv_merger --inputs sample_data --output ./out.txt \
     --log-level DEBUG
```

Equivalent without uv:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
pytest
python -m csv_merger --inputs sample_data --output ./out.txt
```

---

## 5a. Logging

The package is a well-behaved library: it never adds handlers or sets
levels itself; every module just calls `logging.getLogger(__name__)`. The
CLI is the only place where logging is configured, via
`csv_merger._logging.configure(level)`.

Verbosity levels (matching the standard Unix idiom):

| Flag         | Level     | What you see                                              |
|--------------|-----------|-----------------------------------------------------------|
| (none)       | `WARNING` | Only warnings and the final summary on stdout             |
| `-v`         | `INFO`    | One line per pipeline milestone (8–10 lines for sample)   |
| `-vv`        | `DEBUG`   | Per-file, per-row, per-match detail (~150 lines for sample) |
| `--log-level NAME` | (explicit) | Accepts DEBUG/INFO/WARNING/ERROR/CRITICAL or a numeric level |

`-v` / `-vv` and `--log-level` are mutually exclusive (argparse rejects
the combination).

### Color

Log levels can be ANSI-colored on stderr:

| Flag                 | Behaviour                                                          |
|----------------------|--------------------------------------------------------------------|
| `--color auto` (default) | Color when stderr is a TTY; respects `NO_COLOR` and `FORCE_COLOR` |
| `--color always`     | Force color on, even when piping to a file or aggregator           |
| `--color never`      | Force color off, even on a TTY                                     |

Color decision priority (highest wins): explicit flag → `FORCE_COLOR` env var → `NO_COLOR` env var → `stderr.isatty()`.

The `NO_COLOR` and `FORCE_COLOR` conventions follow [no-color.org](https://no-color.org): any non-empty value (including `0`) takes effect. Only the level token is colorised; the reset sequence is always emitted immediately after, so terminal state never bleeds into the rest of the line.

What gets logged at each level:

- **INFO**: pipeline lifecycle — start, file discovery counts, in/out of
  date range, matcher result, write path. Use this for routine operation.
- **DEBUG**: parser internals (file size, encoding, sections, headers,
  role resolution), the matcher's full index (every `(key_2, type1, type2)`
  key with its candidate rows, every match attempt with hit/miss and
  remaining queue depth), the writer's temp/final paths and fsync status,
  every override application. Use this for diagnosing "why didn't record-X
  match?" or "why is the output wrong?".

**Privacy note:** DEBUG output includes cell values (`type1`, `type2`,
`key_2`, `field_2`, `XY_POS`). If your input data is sensitive, route
logs to a private destination, raise the level, or filter at the
handler.

Library callers can integrate with their own logging:

```python
import logging
from csv_merger import CsvMerger

logging.basicConfig(level=logging.DEBUG)
CsvMerger(input_dir=..., output_path=...).run()
```

---

## 5b. Adapting to a different input vocabulary

If a sister system uses different labels (e.g. `effective_date` instead
of `date`, or `record_1` is renamed to `alpha`), construct a custom
`Schema` and pass it to `CsvMerger`. The parser's internal field names
stay the same; only the strings it looks up in the input change.

```python
from csv_merger import CsvMerger, Schema

alt_schema = Schema(
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

CsvMerger(
    input_dir=Path("./data"),
    output_path=Path("./out.txt"),
    schema=alt_schema,
).run()
```

The three maps are independent — override only what differs from the
defaults. Any required role missing from the maps you provide raises
`SchemaError` at construction time, so misconfiguration surfaces
immediately instead of as a confusing parse error.

The matcher does not consult the Schema; it reads named fields off the
parsed dataclasses. The link logic (`key_2` for batch, `(type1, type2)`
for row) is therefore stable across schemas — `RecordFile.key_2` will
hold whatever value the schema's `record_keys["link_batch"]` label
pointed at.

---

## 5c. Type-2 batch files (instrument-report format)

A separate parser, `parse_batch_file_type2`, handles a different input
format: whitespace-aligned instrument reports with a `===== result =====`
banner, colon-separated `key : value` headers, and two parallel data
sections that share an `(r, Phi)` index. The two sections are joined
horizontally and flattened to a CSV with a synthetic `rec_label`
column (sequential numbers for numeric rows, literal `Calc N` for
calculation rows), a `processname` column populated from the header,
and configurable placeholder columns.

```python
from pathlib import Path
from csv_merger import parse_batch_file_type2, write_type2_csv

report = parse_batch_file_type2(Path("instrument-report.txt"))
write_type2_csv(report, Path("out.csv"))
```

The type-2 parser is **independent of the type-1 pipeline** — it does
not produce `BatchFile`, it does not feed the matcher, and it does not
share globs with `CsvMerger`. The two formats coexist as separate tools
in the same package. See `sample_data/type2/` for the canonical input
and expected output.

Optional keyword arguments to `write_type2_csv`:

- `processname_key` — which header key populates the `processname`
  column. Defaults to `"process name"`.
- `placeholder_columns` — names of the placeholder columns, each
  filled with the literal string `"na"`. Defaults to two columns.

---

## 5d. The pipeline (scheduled production runs)

The `csv_merger.pipeline` sub-package adds the operational layer
needed for production: scheduled runs every couple of minutes, durable
state, parallel network I/O, retries, atomic delivery, observability,
and email alerts. It's **stdlib-only** (no Airflow, no Prefect, no
runtime dependencies beyond the standard library).

### Invocation

```
python -m csv_merger pipeline --config C:\path\to\pipeline.toml
python -m csv_merger report   --config C:\path\to\pipeline.toml
```

The pipeline subcommand executes one tick of the lifecycle:

1. Acquire a process lock (skipped if another run is in progress).
2. Clean stale staging files and old outbox/sent files.
3. Drain any prior-run outbox/pending files (publish + mark processed).
4. Wait for the source folder to quiesce (size+path snapshot stable).
5. Discover new files (filter against `processed_files`).
6. Check if this batch is quarantined; skip if so.
7. Fetch files in parallel to local staging, atomic rename to fetched.
8. Parse and merge via the existing `CsvMerger`.
9. Write merged output + manifest to outbox/pending atomically.
10. HTTP PUT to publish endpoint with `Idempotency-Key` header.
11. Move outbox file to outbox/sent on success.
12. Mark files processed in state.

If any step fails, the run is recorded as failed and the next tick
retries naturally (no special re-run command needed). After
`max_attempts_per_batch` consecutive failures (default 3), the batch
is quarantined: subsequent runs skip it until an operator clears it.

### Configuration (TOML)

Minimal viable config:

```toml
[folders]
source         = '\\fileserver\incoming\batches'
staging        = 'C:\csv-merger\staging'
fetched        = 'C:\csv-merger\fetched'
outbox_pending = 'C:\csv-merger\outbox\pending'
outbox_sent    = 'C:\csv-merger\outbox\sent'
dead_letter    = 'C:\csv-merger\dead_letter'

[state]
db_path   = 'C:\csv-merger\state\pipeline.db'
lock_path = 'C:\csv-merger\state\pipeline.lock'

[publish]
url = 'https://api.example.com/ingest'

[email]
smtp_host    = 'smtp.example.com'
smtp_port    = 587
from_address = 'pipeline@example.com'
to_addresses = ['ops@example.com']
```

Optional sections (`[quiescence]`, `[fetch]`, `[retry_policy]`,
`[outbox]`) all have sensible defaults — see
`csv_merger/pipeline/config.py` for the full schema. SMTP password
is read from the `CSV_MERGER_SMTP_PASSWORD` environment variable
(name configurable per deployment); never written in TOML.

### Windows Task Scheduler unit

Schedule the tick every 2 minutes:

```
schtasks /Create /SC MINUTE /MO 2 /TN "csv-merger pipeline" ^
  /TR "C:\csv-merger\.venv\Scripts\python.exe -m csv_merger pipeline --config C:\csv-merger\pipeline.toml" ^
  /RU SYSTEM
```

The process exits non-zero on failure, so Task Scheduler records the
condition. Successive failures will eventually quarantine the batch
and email the operator.

### Operator inspection

```
python -m csv_merger report --config C:\csv-merger\pipeline.toml
```

Prints recent runs (status, duration, error), step-level breakdown of
the most recent run, and any currently quarantined batches.

### Resilience properties

- **Idempotent re-runs.** Source files are tracked by `(path, content_hash)`
  in SQLite. A file processed yesterday will not be reprocessed today
  unless its contents change.
- **Outbox pattern.** Once the merge succeeds, the result is durable on
  local disk before any publish attempt. A publish failure does not
  re-run the merge; the next tick drains the outbox first.
- **Atomic local staging.** Files are downloaded to `staging/<name>.tmp`,
  size-verified, fsynced, then atomically renamed to `fetched/<name>`.
  A crash mid-download never produces a partial file in `fetched/`.
- **Quarantine + alert.** After N consecutive failures on the same
  batch signature, the batch is marked quarantined and the operator is
  emailed. Subsequent runs skip the quarantined batch but the pipeline
  continues running.
- **Lock-based mutual exclusion.** A PID-keyed lockfile prevents
  overlapping runs (common at 2-minute cadence). Stale locks (dead
  PIDs) are reclaimed automatically.

### Trade-offs we explicitly accepted

- **No web UI for status.** Use `--report` or query the SQLite DB.
- **No multi-pipeline DAG.** Designed for one pipeline; if you grow to
  ten interacting pipelines, Prefect or Dagster start to look better.
- **No streaming.** Batch-per-run only.
- **One process per host.** No multi-host coordination.

These limits are deliberate. Adding any of them would mean adopting a
framework, which would dominate the codebase for a problem that's
fundamentally "run a 200-line script every 2 minutes."

---

## 6. Design notes & best practices applied

- **Single-responsibility modules** — parsing, matching, writing, CLI, and
  shared constants are separate so each is independently testable.
- **Configurable input vocabulary via `Schema`** — three role -> label
  maps (header keys, record keys, column-role prefixes) thread through
  the parsers with sensible defaults that match today's input format.
  Validation at construction time means misconfiguration fails fast.
  See section 5b above.
- **Public exception hierarchy** — every error the package raises derives
  from `CsvMergerError` (which itself derives from `ValueError` for
  backward compatibility). Concrete classes: `MalformedBatchFile`,
  `MalformedRecordFile`, `InputTooLargeError`, `NoBatchesInRangeError`,
  `SchemaError`. All are exported from the package root.
- **Immutable dataclasses** (`frozen=True`) with documented "treat nested
  collections as read-only" semantics; helpers return new instances rather
  than mutating in place.
- **Dict-keyed rows + role resolution** — `BatchRow` stores cells in a
  `Mapping[str, str]` keyed by header string, and `ColumnRoles` resolves
  the four semantic roles (`field_a`, `field_b`, `type1`, `type2`) to
  their actual headers at parse time. Reordering input columns no longer
  silently breaks matching, and extra columns are tolerated and preserved
  on output.
- **Trailer detected by position, not value** — `BatchFile.data_rows`
  excludes the last row by file-format contract; `BatchFile.trailer_row`
  exposes it. Matcher iterates `data_rows` only, so a fully-numeric
  trailer cannot bind to a record.
- **Single source of truth for shared constants** — date format, label
  prefixes, and size limits live in `_constants.py`; CLI, parser, and
  writer all import from there.
- **BOM-tolerant input** — input files are read with `utf-8-sig`, so a
  leading byte-order mark is silently consumed instead of being attached
  to the first cell of the first row.
- **Atomic output writes** — the merged file is written to a sibling
  temp file in the destination directory and renamed into place via
  `os.replace`. If anything raises mid-write, the destination either
  still holds the previous version or never exists at all; we never leave
  a partially-written file behind. The temp file is cleaned up on any
  exception, including `KeyboardInterrupt`.
- **O(records) matcher** — a precomputed
  `(key_2, type1, type2) -> deque[row_locations]` index makes the
  per-record cost O(1) regardless of how many batches are in scope.
- **Defence-in-depth file-size limit** — input files larger than 8 MiB
  are rejected up-front (configurable via `_constants.MAX_INPUT_FILE_BYTES`).
- **Narrow exception handling at boundaries** — the CLI catches only
  `FileNotFoundError` and `CsvMergerError`; unrelated `ValueError` from a
  programming bug propagates and surfaces as a traceback rather than
  being silently swallowed.
- **Type hints everywhere** — the project type-checks cleanly under
  `mypy --strict`.
- **Lint-clean** — passes `ruff check` with default rules.
- **Comprehensive test coverage** — 50 unit tests across 7 test files
  covering: parsing, matching, writing, CLI, logging, and the public API
  surface. Tests include column-reordering immunity, extra-column
  tolerance, numeric-trailer exclusion, missing/duplicate-header
  detection, BOM handling, file-size enforcement, file-index regex
  correctness, atomic-write rollback (file unchanged on error, temp file
  cleaned up), cross-batch matching with shared `key_2`, verbosity-level
  mapping (`-v` = INFO, `-vv` = DEBUG), `--log-level` parsing
  (named + numeric + invalid), end-to-end log emission at each level,
  and `-v`/`--log-level` mutual exclusion.
- **Configurable logging via `-v` / `--log-level`** — the package itself
  never adds handlers; the CLI configures the root logger via
  `_logging.configure()`. Three levels: WARNING (default, summary only),
  INFO (`-v`, pipeline milestones), DEBUG (`-vv`, full trace including
  per-row match attempts and override applications). Lazy `%`-formatting
  is used everywhere so disabled-level log calls have negligible cost.
