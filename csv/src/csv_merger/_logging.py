"""Logging configuration helpers for the CLI.

The package itself only ever calls ``logging.getLogger(__name__)`` — it
never adds handlers or sets levels. That is the well-behaved library
pattern: by default the package emits no output, and the embedding
application (here the CLI) controls verbosity.

This module centralises the verbosity-to-level mapping, the format
string, and (optional) ANSI colorisation so the CLI, tests, and any
future entry point all share one definition.

Verbosity levels (matching the conventional Unix ``-v`` / ``-vv`` idiom):

* ``0`` -> ``WARNING`` — quiet by default; only warnings and errors.
* ``1`` -> ``INFO``    — pipeline milestones (one line per major step).
* ``2`` -> ``DEBUG``   — per-record/per-row detail. **Logs cell values**,
  so route logs to a private destination if your input data is sensitive.

Color decision priority (highest wins):

1. Explicit ``color="always"`` or ``color="never"`` argument.
2. ``FORCE_COLOR`` env var (any non-empty value forces on).
3. ``NO_COLOR`` env var (any non-empty value forces off — see no-color.org).
4. ``stream.isatty()`` — color only when writing to a terminal.

The ``NO_COLOR`` and ``FORCE_COLOR`` conventions are widely respected by
modern CLI tools; following them avoids escape codes leaking into log
files and CI output, while still allowing forced color in environments
that lie about being a TTY.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Mapping
from typing import IO, Literal

# --------------------------------------------------------------------------- #
# Public format constant                                                      #
# --------------------------------------------------------------------------- #

# Default format: ``LEVEL logger.name: message``. Compact, greppable, and
# avoids a timestamp by default (the user can wrap with ``ts`` or similar
# if they need timestamps; including one here pollutes test output).
LOG_FORMAT = "%(levelname)s %(name)s: %(message)s"

# Verbosity -> level. Index 0 is the default, then -v, -vv, -vvv (capped).
_LEVELS_BY_VERBOSITY: tuple[int, ...] = (
    logging.WARNING,
    logging.INFO,
    logging.DEBUG,
)


# --------------------------------------------------------------------------- #
# ANSI color codes                                                            #
# --------------------------------------------------------------------------- #
# Standard SGR escape sequences. We deliberately keep this map small and
# only colorise the levelname; coloring the whole message is excessive and
# can interact badly if the message itself contains ANSI.

ColorMode = Literal["auto", "always", "never"]

_RESET = "\x1b[0m"
_LEVEL_COLORS: dict[int, str] = {
    logging.DEBUG: "\x1b[36m",     # cyan
    logging.INFO: "\x1b[32m",      # green
    logging.WARNING: "\x1b[33m",   # yellow
    logging.ERROR: "\x1b[31m",     # red
    logging.CRITICAL: "\x1b[1;31m",  # bold red
}


# --------------------------------------------------------------------------- #
# Color decision                                                              #
# --------------------------------------------------------------------------- #


def should_use_color(
    stream: IO[str] | None = None,
    mode: ColorMode = "auto",
    env: Mapping[str, str] | None = None,
) -> bool:
    """Decide whether ANSI color should be emitted to ``stream``.

    Args:
        stream: Destination stream; only its ``isatty()`` is consulted.
            ``None`` is treated as "not a TTY".
        mode: ``"auto"`` consults env + isatty; ``"always"`` and
            ``"never"`` short-circuit.
        env: Environment mapping. Defaults to :data:`os.environ`. Exposed
            for testing so we don't have to mutate the real environment.

    The decision priority is documented in the module docstring.
    """
    if mode == "always":
        return True
    if mode == "never":
        return False

    # mode == "auto"
    effective_env: Mapping[str, str] = os.environ if env is None else env
    if effective_env.get("FORCE_COLOR"):
        return True
    if effective_env.get("NO_COLOR"):
        return False

    if stream is None:
        return False
    isatty = getattr(stream, "isatty", None)
    return bool(isatty and isatty())


# --------------------------------------------------------------------------- #
# Color formatter                                                             #
# --------------------------------------------------------------------------- #


class _ColorFormatter(logging.Formatter):
    """A :class:`~logging.Formatter` that wraps the levelname in ANSI color.

    Only the levelname token is colorised. The reset sequence is always
    emitted right after the levelname, so terminals never carry color
    state into the rest of the line.
    """

    def format(self, record: logging.LogRecord) -> str:
        color = _LEVEL_COLORS.get(record.levelno)
        if color is None:
            # Unknown level — fall back to plain rendering.
            return super().format(record)
        # Save/restore so concurrent handlers reading the same record
        # don't see a colored levelname.
        original = record.levelname
        record.levelname = f"{color}{original}{_RESET}"
        try:
            return super().format(record)
        finally:
            record.levelname = original


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #


def level_for_verbosity(verbosity: int) -> int:
    """Map a non-negative ``-v`` count to a logging level.

    Values above the highest defined level (currently 2 -> DEBUG) clamp to
    DEBUG; negative values clamp to WARNING. This mirrors how most CLIs
    behave when users repeat ``-v`` more times than there are levels.
    """
    if verbosity < 0:
        verbosity = 0
    if verbosity >= len(_LEVELS_BY_VERBOSITY):
        verbosity = len(_LEVELS_BY_VERBOSITY) - 1
    return _LEVELS_BY_VERBOSITY[verbosity]


def configure(
    level: int,
    *,
    color: ColorMode = "auto",
    stream: IO[str] | None = None,
) -> None:
    """Configure the root logger with our format and the given level.

    Args:
        level: Logging level for the root logger.
        color: Color mode (see :func:`should_use_color`).
        stream: Override the destination stream (mainly for testing).
            Defaults to :data:`sys.stderr`, matching ``logging.basicConfig``.

    Idempotent: each call replaces the previously installed handler so
    repeated calls with different settings work as expected.
    """
    target = stream if stream is not None else sys.stderr
    use_color = should_use_color(target, mode=color)

    formatter: logging.Formatter
    if use_color:
        formatter = _ColorFormatter(LOG_FORMAT)
    else:
        formatter = logging.Formatter(LOG_FORMAT)

    handler = logging.StreamHandler(target)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    # Replace any handlers we may have installed previously. We don't try
    # to be clever about preserving foreign handlers: this is a CLI entry
    # point that owns the root logger configuration.
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(handler)
    root.setLevel(level)
