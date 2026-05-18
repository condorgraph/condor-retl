from __future__ import annotations

import sys
from collections.abc import Mapping
from typing import Protocol, TextIO, TypeAlias

from retl.runtime.redaction import redact_text, redact_value

ConsoleName: TypeAlias = str
ConsoleEvent: TypeAlias = Mapping[str, object]
ConsoleInput: TypeAlias = "ConsoleRenderer | ConsoleName | None"


class ConsoleRenderer(Protocol):
    """Receives bounded runtime summaries and renders operator-facing progress."""

    def run_started(self, **event: object) -> None: ...

    def collect_group_completed(self, **event: object) -> None: ...

    def sync_started(self, **event: object) -> None: ...

    def stage_completed(self, **event: object) -> None: ...

    def reconcile_completed(self, **event: object) -> None: ...

    def destination_batch_attempt_recorded(self, **event: object) -> None: ...

    def destination_submission_completed(self, **event: object) -> None: ...

    def progress_commit_decided(self, **event: object) -> None: ...

    def sync_report_recorded(self, **event: object) -> None: ...

    def sync_completed(self, **event: object) -> None: ...

    def run_completed(self, **event: object) -> None: ...


class NullConsoleRenderer:
    """Console renderer that intentionally emits no output."""

    def run_started(self, **event: object) -> None:
        return None

    def collect_group_completed(self, **event: object) -> None:
        return None

    def sync_started(self, **event: object) -> None:
        return None

    def stage_completed(self, **event: object) -> None:
        return None

    def reconcile_completed(self, **event: object) -> None:
        return None

    def destination_batch_attempt_recorded(self, **event: object) -> None:
        return None

    def destination_submission_completed(self, **event: object) -> None:
        return None

    def progress_commit_decided(self, **event: object) -> None:
        return None

    def sync_report_recorded(self, **event: object) -> None:
        return None

    def sync_completed(self, **event: object) -> None:
        return None

    def run_completed(self, **event: object) -> None:
        return None


class TextConsoleRenderer:
    """Plain text renderer for concise live operator progress."""

    def __init__(self, *, stream: TextIO | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout
        self._started_syncs: set[str] = set()

    def run_started(self, **event: object) -> None:
        self._started_syncs.clear()
        self._write(f"RETL run {_value(event, 'runner_name')}")
        self._write(
            " ".join(
                _item(key, event)
                for key in ("run_id", "dry_run", "sync_count", "collect_group_count")
                if key in event
            )
        )
        self._write("")

    def collect_group_completed(self, **event: object) -> None:
        parts = [
            _phase_line(
                "collect",
                event,
                (
                    ("rows", "work_row_count"),
                    ("collect_id", "collect_id"),
                    ("source", "source_name"),
                    ("mode", "source_mode"),
                ),
            )
        ]
        source_group = _optional_value(event, "source_group_key")
        if source_group is not None:
            parts.insert(0, f"source group {source_group}")
        for line in parts:
            self._write(line)

    def sync_started(self, **event: object) -> None:
        sync_name = _value(event, "sync_name")
        self._started_syncs.add(sync_name)
        self._write("")
        destination = _value(event, "destination_binding_name")
        surface = _value(event, "surface")
        self._write(f"{sync_name} -> {destination}/{surface}")

    def stage_completed(self, **event: object) -> None:
        self._ensure_sync_header(event)
        self._write(
            _phase_line(
                "stage",
                event,
                (
                    ("rows", "row_count"),
                    ("page", "page_index"),
                    ("mode", "mode"),
                    ("progress_before", "progress_before"),
                ),
            )
        )

    def reconcile_completed(self, **event: object) -> None:
        self._ensure_sync_header(event)
        self._write(
            _phase_line(
                "reconcile",
                event,
                (
                    ("operations", "operation_count"),
                    ("upserts", "upsert_count"),
                    ("removes", "remove_count"),
                    ("imports", "event_import_count"),
                    ("pages", "page_count"),
                ),
            )
        )

    def destination_batch_attempt_recorded(self, **event: object) -> None:
        self._ensure_sync_header(event)
        self._write(
            _phase_line(
                "batch",
                event,
                (
                    ("index", "destination_batch_index"),
                    ("rows", "row_count"),
                    ("attempts", "attempt_count"),
                    ("completion", "completion_state"),
                    ("run_action", "run_action"),
                    ("progress", "progress_implication"),
                    ("retry_eligible", "retry_eligible"),
                    ("http_status", "http_status"),
                    ("partner_code", "partner_error_code"),
                    ("partner_subcode", "partner_error_subcode"),
                    ("diagnostic", "diagnostic_summary"),
                ),
            )
        )

    def destination_submission_completed(self, **event: object) -> None:
        self._ensure_sync_header(event)
        self._write(
            _phase_line(
                "sync",
                event,
                (
                    ("request_batches", "request_batch_count"),
                    ("destination_batches", "destination_batch_count"),
                    ("attempted", "attempted_count"),
                    ("confirmed", "confirmed_count"),
                    ("accepted", "accepted_count"),
                    ("retryable_failures", "retryable_failure_count"),
                    ("terminal_failures", "terminal_failure_count"),
                    ("blocking_failures", "pre_acceptance_failure_count"),
                ),
            )
        )

    def progress_commit_decided(self, **event: object) -> None:
        self._ensure_sync_header(event)
        reason = _optional_value(event, "reason")
        suffix = f"     {reason}" if reason else ""
        self._write(
            _phase_line(
                "progress",
                event,
                (
                    ("advanced", "progress_advanced"),
                    ("allowed", "progress_decision_allowed"),
                    ("planned_batches", "planned_batch_count"),
                    ("expected_batches", "expected_batch_count"),
                ),
            )
            + suffix
        )

    def sync_report_recorded(self, **event: object) -> None:
        self._ensure_sync_header(event)
        report_reference = _item("report_reference", event)
        self._write(f"  {'report':<10} {_value(event, 'status'):<10} {report_reference}")

    def sync_completed(self, **event: object) -> None:
        self._ensure_sync_header(event)
        self._write(
            _phase_line(
                "summary",
                event,
                (
                    ("operations", "operation_count"),
                    ("destination_batches", "destination_batch_count"),
                    ("confirmed", "confirmed_count"),
                    ("accepted", "accepted_count"),
                    ("retryable_failures", "retryable_failure_count"),
                    ("terminal_failures", "terminal_failure_count"),
                    ("blocking_failures", "pre_acceptance_failure_count"),
                    ("progress_advanced", "progress_advanced"),
                    ("report", "report_reference"),
                ),
            )
        )

    def run_completed(self, **event: object) -> None:
        self._write("")
        status = _value(event, "status")
        summary = _present_values(
            event,
            (
                ("syncs", "sync_count"),
                ("succeeded", "sync_succeeded_count"),
                ("failed", "sync_failed_count"),
                ("partial", "sync_partial_count"),
                ("planned", "sync_planned_count"),
                ("confirmed", "confirmed_count"),
                ("accepted", "accepted_count"),
                ("retryable_failures", "retryable_failure_count"),
                ("terminal_failures", "terminal_failure_count"),
                ("blocking_failures", "pre_acceptance_failure_count"),
                ("progress_advanced", "progress_advanced"),
            ),
        )
        line = f"Run {status}"
        if summary:
            line = f"{line}: {summary}"
        self._write(line)
        if "run_index_reference" in event:
            self._write(f"Run index: {_value(event, 'run_index_reference')}")

    def _ensure_sync_header(self, event: ConsoleEvent) -> None:
        sync_name = _optional_value(event, "sync_name")
        if sync_name is None or sync_name in self._started_syncs:
            return
        self._started_syncs.add(sync_name)
        self._write("")
        self._write(
            f"{sync_name} -> {_value(event, 'destination_binding_name')}/{_value(event, 'surface')}"
        )

    def _write(self, line: str) -> None:
        self._stream.write(f"{line}\n")
        self._stream.flush()


def null() -> NullConsoleRenderer:
    return NullConsoleRenderer()


def text(*, stream: TextIO | None = None) -> TextConsoleRenderer:
    return TextConsoleRenderer(stream=stream)


def resolve_console(console: ConsoleInput) -> ConsoleRenderer:
    if console is None:
        return null()
    if isinstance(console, str):
        normalized = console.strip().lower()
        if normalized in {"", "none", "null", "off", "quiet"}:
            return null()
        if normalized in {"text", "console"}:
            return text()
        raise ValueError(
            "Console renderer name must be one of: text, console, null, none, off, quiet."
        )
    return console


def _phase_line(
    label: str,
    event: ConsoleEvent,
    fields: tuple[tuple[str, str], ...],
) -> str:
    status = _value(event, "status")
    suffix = _present_values(event, fields)
    line = f"  {label:<10} {status:<10}"
    if suffix:
        line = f"{line} {suffix}"
    return line


def _present_values(event: ConsoleEvent, fields: tuple[tuple[str, str], ...]) -> str:
    return " ".join(
        f"{display}={_value(event, key)}" for display, key in fields if _should_render(event, key)
    )


def _item(key: str, event: ConsoleEvent) -> str:
    return f"{key}={_value(event, key)}"


def _should_render(event: ConsoleEvent, key: str) -> bool:
    value = event.get(key)
    return value is not None and value != ""


def _optional_value(event: ConsoleEvent, key: str) -> str | None:
    if not _should_render(event, key):
        return None
    return _value(event, key)


def _value(event: ConsoleEvent, key: str) -> str:
    value = event.get(key, "")
    if isinstance(value, bool):
        return str(value).lower()
    return redact_text(redact_value(key, event.get(key, "")))


__all__ = [
    "ConsoleEvent",
    "ConsoleInput",
    "ConsoleName",
    "ConsoleRenderer",
    "NullConsoleRenderer",
    "TextConsoleRenderer",
    "null",
    "resolve_console",
    "text",
]
