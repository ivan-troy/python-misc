"""Allow ``python -m csv_merger`` invocation."""

from csv_merger.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
