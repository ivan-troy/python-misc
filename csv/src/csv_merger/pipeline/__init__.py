"""csv_merger.pipeline — scheduled, resilient pipeline orchestration.

This sub-package adds the operational layer on top of the existing
:class:`~csv_merger.CsvMerger`: scheduled execution, persistent state,
parallel network I/O, retries, outbox-pattern publish, dead-letter for
poison data, quarantine for repeatedly-failing batches, and email alerts.

It is **stdlib + httpx only** by deliberate choice. See the README for
the design rationale.

The modules are layered:

* :mod:`._errors` — exception hierarchy (no dependencies)
* :mod:`.config` — frozen dataclass loaded from TOML
* :mod:`.retry` — pure retry helper
* :mod:`.locking` — cross-platform exclusive lockfile
* :mod:`.quiescence` — "wait for source folder to settle"
* :mod:`.state` — SQLite-backed run/file/batch state
* :mod:`.transport` — fetch + publish via httpx (batch 2)
* :mod:`.outbox` — durable publish queue (batch 2)
* :mod:`.alerting` — rate-limited SMTP emails (batch 2)
* :mod:`.runner` — orchestrator (batch 3)
* :mod:`.reporting` — operator-facing ``--report`` subcommand (batch 3)
"""
