from __future__ import annotations

import hashlib
import inspect
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class RunProvenance:
    run_id: str
    runner_name: str
    dry_run: bool
    script_path: str | None
    script_content_hash: str | None
    started_at: str


def run_provenance(*, run_id: str, runner_name: str, dry_run: bool) -> RunProvenance:
    script_path = _script_path()
    return RunProvenance(
        run_id=run_id,
        runner_name=runner_name,
        dry_run=dry_run,
        script_path=str(script_path) if script_path is not None else None,
        script_content_hash=_script_hash(script_path),
        started_at=datetime.now(timezone.utc).isoformat(),
    )


def _script_path() -> Path | None:
    for frame in inspect.stack()[2:]:
        filename = frame.filename
        if not filename or filename.startswith("<"):
            continue
        path = Path(filename)
        if path.name in {"executor.py", "runner.py", "provenance.py"}:
            continue
        if path.exists() and path.is_file():
            return path.resolve()
    return None


def _script_hash(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


__all__ = ["RunProvenance", "run_provenance"]
