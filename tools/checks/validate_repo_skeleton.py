from __future__ import annotations

import argparse
import sys
from pathlib import Path

from _control_plane import format_report, repo_root, validate_repo_skeleton


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate the docs/control-plane skeleton.")
    parser.add_argument(
        "--root", type=Path, default=repo_root(), help="Repository root to validate."
    )
    args = parser.parse_args(argv)
    report = validate_repo_skeleton(args.root)
    print(format_report(report))
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
