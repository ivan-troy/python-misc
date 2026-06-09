"""Exception hierarchy for the pipeline sub-package.

All errors derive from :class:`PipelineError`, which itself derives from
:class:`~csv_merger._constants.CsvMergerError` so the existing CLI
catch-all continues to work. Concrete subclasses let callers distinguish
the kinds of failure that matter operationally:

* :class:`TransientError` — retryable; the retry helper consumes this.
* :class:`PermanentError` — do not retry; surface immediately.
* :class:`ConfigError` — invalid configuration; fail loudly at startup.
* :class:`StateError` — the state DB is unreadable or inconsistent.
* :class:`LockHeldError` — another pipeline process is already running.
* :class:`QuiescenceTimeout` — source folder never settled.
"""

from __future__ import annotations

from csv_merger._constants import CsvMergerError


class PipelineError(CsvMergerError):
    """Base class for every error raised by the pipeline sub-package."""


class TransientError(PipelineError):
    """A failure that may succeed if retried.

    Network blips, HTTP 5xx, rate limits, transient SMB hiccups. The
    retry helper consumes this class explicitly. Never raise for
    permanent data problems — those should be :class:`PermanentError`
    so retries don't paper over bugs.
    """


class PermanentError(PipelineError):
    """A failure that will not succeed on retry.

    HTTP 4xx (except 408/429), parse failures, schema violations.
    """


class ConfigError(PipelineError):
    """Pipeline configuration is missing or invalid."""


class StateError(PipelineError):
    """State database is unreadable, locked, or schema-incompatible."""


class LockHeldError(PipelineError):
    """Another pipeline process is already holding the lockfile.

    The runner catches this and exits cleanly without recording a failed
    run — overlap is expected when a long run spans the next tick.
    """


class QuiescenceTimeout(PipelineError):
    """The source folder never reached a quiet state within the window.

    Treated as a run failure: better to skip the batch than process
    a folder that the producer is still writing to.
    """
