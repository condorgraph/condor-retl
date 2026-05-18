from __future__ import annotations

import io
import json
import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb
import pytest

import retl
from retl.backends.duckdb import DuckDBRuntimeStore, DuckDBSqlBackend
from retl.collect_identity import is_uuidv7
from retl.console import NullConsoleRenderer, TextConsoleRenderer, null, resolve_console, text
from retl.destinations.acknowledgements import DestinationSubmissionEvidence
from retl.sync_runtime import submission as submission_module


@contextmanager
def preserve_logger_state(*names: str) -> Iterator[None]:
    root = logging.getLogger()
    root_state = (list(root.handlers), root.level, root.propagate, root.disabled)
    logger_states = {
        name: (
            list(logging.getLogger(name).handlers),
            logging.getLogger(name).level,
            logging.getLogger(name).propagate,
            logging.getLogger(name).disabled,
        )
        for name in names
    }
    try:
        yield
    finally:
        root.handlers[:] = root_state[0]
        root.setLevel(root_state[1])
        root.propagate = root_state[2]
        root.disabled = root_state[3]
        for name, state in logger_states.items():
            logger = logging.getLogger(name)
            logger.handlers[:] = state[0]
            logger.setLevel(state[1])
            logger.propagate = state[2]
            logger.disabled = state[3]


def _assert_collect_line(
    output: str,
    *,
    rows: int,
    source: str,
    mode: str,
) -> str:
    match = re.search(
        rf"  collect    succeeded  rows={rows} collect_id=([0-9a-f-]{{36}}) "
        rf"source={source} mode={mode}",
        output,
    )
    assert match is not None
    collect_id = match.group(1)
    assert is_uuidv7(collect_id)
    return collect_id


def _assert_collect_line_without_collect_id(
    output: str,
    *,
    rows: int,
    source: str,
    mode: str,
) -> None:
    match = re.search(
        rf"  collect    succeeded  rows={rows} source={source} mode={mode}",
        output,
    )
    assert match is not None


def test_root_console_namespace_is_public_export() -> None:
    from retl import console as root_console

    stream = io.StringIO()

    assert root_console is retl.console
    assert retl.console.text(stream=stream).__class__ is TextConsoleRenderer
    assert "console" in retl.__all__


def test_null_console_renderer_is_no_op() -> None:
    renderer = NullConsoleRenderer()

    renderer.run_started(runner_name="runner", run_id="run-1")
    renderer.collect_group_completed(status="succeeded", work_row_count=1)
    renderer.sync_started(
        sync_name="sync", destination_binding_name="destination", surface="surface"
    )
    renderer.stage_completed(sync_name="sync", status="succeeded", row_count=1)
    renderer.reconcile_completed(sync_name="sync", status="succeeded", operation_count=1)
    renderer.destination_batch_attempt_recorded(sync_name="sync", status="succeeded")
    renderer.destination_submission_completed(sync_name="sync", status="confirmed")
    renderer.progress_commit_decided(sync_name="sync", status="allowed", reason="ok")
    renderer.sync_report_recorded(sync_name="sync", status="succeeded", report_reference="report")
    renderer.sync_completed(sync_name="sync", status="succeeded")
    renderer.run_completed(runner_name="runner", status="succeeded")


def test_console_constructors_return_renderers() -> None:
    stream = io.StringIO()

    assert isinstance(null(), NullConsoleRenderer)
    assert isinstance(text(stream=stream), TextConsoleRenderer)


def test_runner_construction_accepts_console_inputs() -> None:
    stream = io.StringIO()
    text_renderer = retl.console.text(stream=stream)
    null_renderer = retl.console.null()

    assert isinstance(retl.runner(name="runner").console, NullConsoleRenderer)
    assert retl.runner(name="runner", console=text_renderer).console is text_renderer
    assert retl.runner(name="runner", console=null_renderer).console is null_renderer
    assert isinstance(retl.runner(name="runner", console="text").console, TextConsoleRenderer)
    assert isinstance(retl.runner(name="runner", console="quiet").console, NullConsoleRenderer)
    assert isinstance(retl.runner(name="runner", console=None).console, NullConsoleRenderer)
    assert isinstance(retl.Runner(name="runner", console="text").console, TextConsoleRenderer)


def test_runner_passes_console_through_execution() -> None:
    from retl.runtime import executor

    stream = io.StringIO()
    renderer = retl.console.text(stream=stream)
    sync = retl.sync(
        name="customer_profiles",
        declaration=retl.state(
            name="customer_state",
            source=retl.source(name="customers", query="select * from customers"),
            key={"customer": "customer_id"},
        ),
        destination=object(),
        surface="user_profile",
    )
    calls: list[dict[str, object]] = []

    def fake_run_syncs(**kwargs: object) -> retl.RunResult:
        calls.append(kwargs)
        return retl.RunResult(
            runner_name=str(kwargs["runner_name"]),
            status="succeeded",
            dry_run=bool(kwargs["dry_run"]),
            source_groups=(),
            declaration_stages=(),
            syncs=(),
        )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(executor, "run_syncs", fake_run_syncs)
    try:
        retl.runner(name="runner", console=renderer).run(sync, dry_run=True)
    finally:
        monkeypatch.undo()

    assert calls[0]["console"] is renderer
    assert stream.getvalue() == ""


def test_runner_default_console_stays_quiet(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    store = _store(tmp_path)

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(
        _state_sync(_state_declaration(tmp_path))
    )

    captured = capsys.readouterr()
    assert result.status == "succeeded"
    assert captured.out == ""
    assert captured.err == ""


def test_text_console_emits_successful_state_sync_summary(tmp_path: Path) -> None:
    store = _store(tmp_path)
    stream = io.StringIO()

    result = retl.runner(
        name="crm_to_lifecycle",
        runtime_store=store,
        console=retl.console.text(stream=stream),
    ).run(_state_sync(_state_declaration(tmp_path), on_failure="stop_on_any"))

    output = stream.getvalue()
    assert result.status == "succeeded"
    assert "RETL run crm_to_lifecycle" in output
    assert f"run_id={result.run_id} dry_run=false sync_count=1 collect_group_count=1" in output
    _assert_collect_line(output, rows=2, source="customers", mode="snapshot")
    assert "customer_profiles -> mock_profiles/profile_properties" in output
    assert "  stage      succeeded  rows=2 page=1 mode=pending" in output
    assert "  reconcile  succeeded  operations=2 upserts=2 removes=0 imports=0 pages=1" in output
    assert (
        "  batch      succeeded  index=0 rows=2 attempts=1 completion=resolved "
        "run_action=attempted progress=resolved_for_progress retry_eligible=false"
    ) in output
    assert (
        "  sync       confirmed  request_batches=1 destination_batches=1 attempted=2 confirmed=2"
        in output
    )
    assert (
        "  progress   allowed    advanced=true allowed=true planned_batches=1 expected_batches=1"
        in output
    )
    assert f"report_reference={result.syncs[0].report_reference}" in output
    assert (
        "Run succeeded: syncs=1 succeeded=1 failed=0 partial=0 planned=0 confirmed=2 accepted=0"
        in output
    )
    assert f"Run index: {result.run_index_reference}" in output
    assert "cust_1" not in output
    assert "one@example.com" not in output
    assert "tier_alpha_raw" not in output


def test_text_console_emits_dry_run_summary(tmp_path: Path) -> None:
    store = _store(tmp_path)
    stream = io.StringIO()

    result = retl.runner(
        name="crm_to_lifecycle",
        runtime_store=store,
        console=retl.console.text(stream=stream),
    ).run(_state_sync(_state_declaration(tmp_path)), dry_run=True)

    output = stream.getvalue()
    assert result.status == "planned"
    assert result.dry_run is True
    assert f"run_id={result.run_id} dry_run=true sync_count=1 collect_group_count=1" in output
    assert "customer_profiles -> mock_profiles/profile_properties" in output
    _assert_collect_line(output, rows=2, source="customers", mode="snapshot")
    assert "  stage      succeeded  rows=2 page=1 mode=pending" in output
    assert "  reconcile  succeeded  operations=2 upserts=2 removes=0 imports=0 pages=1" in output
    assert (
        "  sync       planned    request_batches=0 destination_batches=1 attempted=2 "
        "confirmed=0 accepted=0"
    ) in output
    assert (
        "  progress   blocked    advanced=false allowed=false planned_batches=1 expected_batches=0"
    ) in output
    assert "Dry run does not advance destination progress." in output
    assert "  report     planned    report_reference=sync-report:" in output
    assert (
        "Run planned: syncs=1 succeeded=0 failed=0 partial=0 planned=1 confirmed=0 accepted=0"
        in output
    )
    assert f"Run index: {result.run_index_reference}" in output
    assert "select customer_id, email, plan from customers" not in output
    assert "cust_1" not in output
    assert "one@example.com" not in output
    assert "tier_alpha_raw" not in output


def test_text_console_emits_empty_event_progress_blocked_summary(tmp_path: Path) -> None:
    store = _store(tmp_path)
    stream = io.StringIO()

    result = retl.runner(
        name="event_imports",
        runtime_store=store,
        console=retl.console.text(stream=stream),
    ).run(_event_sync(_empty_event_declaration(tmp_path)))

    output = stream.getvalue()
    assert result.status == "succeeded"
    assert "purchase_imports -> mock_events/purchase_event" in output
    _assert_collect_line_without_collect_id(
        output,
        rows=0,
        source="purchases",
        mode="checkpointed",
    )
    assert "  reconcile  succeeded  operations=0 upserts=0 removes=0 imports=0 pages=1" in output
    assert (
        "  sync       confirmed  request_batches=0 destination_batches=1 attempted=0 confirmed=0"
        in output
    )
    assert (
        "  progress   blocked    advanced=false allowed=false planned_batches=0 expected_batches=0"
        in output
    )
    assert "No reconciled work was submitted; progress remains unchanged." in output
    assert (
        "Run succeeded: syncs=1 succeeded=1 failed=0 partial=0 planned=0 confirmed=0 accepted=0"
        in output
    )


def test_text_console_run_many_renders_shared_collect_once_and_per_sync_blocks(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    stream = io.StringIO()
    declaration = _state_declaration(tmp_path)
    primary = _state_sync(
        declaration,
        name="customer_profiles",
        binding_name="mock_profiles",
    )
    backup = _state_sync(
        declaration,
        name="customer_backup",
        binding_name="mock_backup",
    )

    result = retl.runner(
        name="crm_to_lifecycle",
        runtime_store=store,
        console=retl.console.text(stream=stream),
    ).run_many([primary, backup])

    output = stream.getvalue()
    assert result.status == "succeeded"
    assert f"run_id={result.run_id} dry_run=false sync_count=2 collect_group_count=1" in output
    assert result.source_groups[0].sync_names == ("customer_profiles", "customer_backup")
    assert output.count("source group state:customer_state:customers:") == 1
    collect_lines = re.findall(
        r"  collect    succeeded  rows=2 collect_id=([0-9a-f-]{36}) "
        r"source=customers mode=snapshot",
        output,
    )
    assert len(collect_lines) == 1
    assert is_uuidv7(collect_lines[0])
    assert output.index("source group state:customer_state:customers:") < output.index(
        "customer_profiles -> mock_profiles/profile_properties"
    )
    assert output.index("  collect    succeeded") < output.index(
        "customer_profiles -> mock_profiles/profile_properties"
    )
    assert "customer_profiles -> mock_profiles/profile_properties" in output
    assert "customer_backup -> mock_backup/profile_properties" in output
    assert output.count("  stage      succeeded  rows=2 page=1 mode=pending") == 2
    assert output.count("  reconcile  succeeded  operations=2") == 2
    assert output.count("  sync       confirmed  request_batches=1 destination_batches=1") == 2
    assert output.count("  progress   allowed    advanced=true allowed=true") == 2
    assert output.count("  report     succeeded  report_reference=sync-report:") == 2
    assert (
        "Run succeeded: syncs=2 succeeded=2 failed=0 partial=0 planned=0 confirmed=4 accepted=0"
        in output
    )
    assert "select customer_id, email, plan from customers" not in output
    assert "cust_1" not in output
    assert "one@example.com" not in output
    assert "tier_alpha_raw" not in output


def test_text_console_runs_separately_from_configured_json_logging(tmp_path: Path) -> None:
    store = _store(tmp_path)
    console_stream = io.StringIO()
    log_stream = io.StringIO()

    with preserve_logger_state("retl", "retl.runtime", "retl.runtime.executor"):
        retl.configure_logging(level="INFO", format="json", stream=log_stream)

        result = retl.runner(
            name="crm_to_lifecycle",
            runtime_store=store,
            console=retl.console.text(stream=console_stream),
        ).run(_state_sync(_state_declaration(tmp_path)))

    console_output = console_stream.getvalue()
    log_lines = [line for line in log_stream.getvalue().splitlines() if line]
    log_payloads = [json.loads(line) for line in log_lines]

    assert result.status == "succeeded"
    assert "RETL run crm_to_lifecycle" in console_output
    assert "customer_profiles -> mock_profiles/profile_properties" in console_output
    assert f"Run index: {result.run_index_reference}" in console_output
    assert '{"event"' not in console_output
    assert '"logger"' not in console_output
    assert "run_started" not in console_output
    assert "sync_completed" not in console_output
    assert log_payloads
    assert {payload["event"] for payload in log_payloads} >= {"run_started", "run_completed"}
    assert all(payload["logger"].startswith("retl") for payload in log_payloads)
    assert all("RETL run crm_to_lifecycle" not in line for line in log_lines)
    assert all(
        "customer_profiles -> mock_profiles/profile_properties" not in line for line in log_lines
    )
    assert all("Run index:" not in line for line in log_lines)


def test_text_console_failure_output_is_bounded_and_redacted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    stream = io.StringIO()

    def fail_submission(**_: Any) -> DestinationSubmissionEvidence:
        return DestinationSubmissionEvidence(
            status="pre_acceptance_failure",
            attempted_count=2,
            pre_acceptance_failure_count=2,
            pre_acceptance_failure_category="auth",
            http_status=401,
            partner_error_code="auth_failed",
            partner_error_detail="Authorization: Bearer raw-secret-token",
            summary="Partner rejected Authorization: Bearer raw-secret-token",
        )

    monkeypatch.setattr(submission_module, "_submission_evidence", fail_submission)

    result = retl.runner(
        name="crm_to_lifecycle",
        runtime_store=store,
        console=retl.console.text(stream=stream),
    ).run(_state_sync(_state_declaration(tmp_path), on_failure="stop_on_any"))

    output = stream.getvalue()
    assert result.status == "failed"
    assert "pre_acceptance_failure" in output
    assert "blocking_failures=2" in output
    assert "Run failed:" in output
    assert "  batch      failed     index=0 rows=2 attempts=1 completion=unresolved" in output
    assert "http_status=401" in output
    assert "diagnostic=Partner rejected Authorization=[redacted]" in output
    assert "raw-secret-token" not in output
    assert "Bearer" not in output
    assert "one@example.com" not in output
    assert "cust_1" not in output


def test_resolve_console_accepts_names_and_existing_renderers() -> None:
    renderer = NullConsoleRenderer()

    assert isinstance(resolve_console(None), NullConsoleRenderer)
    assert isinstance(resolve_console("null"), NullConsoleRenderer)
    assert isinstance(resolve_console("quiet"), NullConsoleRenderer)
    assert isinstance(resolve_console("text"), TextConsoleRenderer)
    assert isinstance(resolve_console("console"), TextConsoleRenderer)
    assert resolve_console(renderer) is renderer


def test_resolve_console_rejects_unknown_name() -> None:
    with pytest.raises(ValueError, match="Console renderer name"):
        resolve_console("spinner")


def test_text_console_renderer_outputs_bounded_operator_shape() -> None:
    stream = io.StringIO()
    renderer = text(stream=stream)

    renderer.run_started(
        runner_name="sample_runner",
        run_id="run-1",
        dry_run=False,
        sync_count=1,
        collect_group_count=1,
    )
    renderer.sync_started(
        sync_name="sample_sync",
        destination_binding_name="destination",
        surface="custom_audiences",
    )
    renderer.stage_completed(
        sync_name="sample_sync",
        destination_binding_name="destination",
        surface="custom_audiences",
        status="succeeded",
        row_count=42,
        page_index=2,
        mode="pending",
        progress_before=10,
    )
    renderer.reconcile_completed(
        sync_name="sample_sync",
        destination_binding_name="destination",
        surface="custom_audiences",
        status="succeeded",
        operation_count=42,
        upsert_count=40,
        remove_count=2,
        page_count=2,
    )
    renderer.destination_batch_attempt_recorded(
        sync_name="sample_sync",
        destination_binding_name="destination",
        surface="custom_audiences",
        status="failed",
        destination_batch_index=3,
        row_count=1000,
        completion_state="unresolved",
        attempt_count=1,
        run_action="attempted",
        progress_implication="unresolved_failure",
        retry_eligible=False,
        http_status=400,
        partner_error_code="100",
        partner_error_subcode="2804003",
        diagnostic_summary="Invalid parameter code=100 subcode=2804003",
    )
    renderer.destination_submission_completed(
        sync_name="sample_sync",
        destination_binding_name="destination",
        surface="custom_audiences",
        status="confirmed",
        request_batch_count=3,
        destination_batch_count=3,
        confirmed_count=42,
        accepted_count=0,
    )
    renderer.progress_commit_decided(
        sync_name="sample_sync",
        destination_binding_name="destination",
        surface="custom_audiences",
        status="allowed",
        progress_advanced=True,
        progress_decision_allowed=True,
        reason="Progress advanced after destination confirmation.",
        planned_batch_count=3,
        expected_batch_count=3,
    )
    renderer.sync_report_recorded(
        sync_name="sample_sync",
        destination_binding_name="destination",
        surface="custom_audiences",
        status="succeeded",
        report_reference="sync-report:1",
    )
    renderer.run_completed(
        runner_name="sample_runner",
        status="succeeded",
        sync_count=1,
        sync_succeeded_count=1,
        sync_failed_count=0,
        sync_partial_count=0,
        sync_planned_count=0,
        confirmed_count=42,
        progress_advanced=True,
        run_index_reference="run-index:1",
    )

    output = stream.getvalue()
    assert "RETL run sample_runner" in output
    assert "run_id=run-1 dry_run=false sync_count=1 collect_group_count=1" in output
    assert "sample_sync -> destination/custom_audiences" in output
    assert "  stage      succeeded  rows=42 page=2 mode=pending progress_before=10" in output
    assert "  reconcile  succeeded  operations=42 upserts=40 removes=2 pages=2" in output
    assert (
        "  batch      failed     index=3 rows=1000 attempts=1 completion=unresolved "
        "run_action=attempted progress=unresolved_failure retry_eligible=false "
        "http_status=400 partner_code=100 partner_subcode=2804003 "
        "diagnostic=Invalid parameter code=100 subcode=2804003"
    ) in output
    assert (
        "  sync       confirmed  request_batches=3 destination_batches=3 confirmed=42 accepted=0"
        in output
    )
    assert (
        "  progress   allowed    advanced=true allowed=true planned_batches=3 expected_batches=3"
        in output
    )
    assert "Progress advanced after destination confirmation." in output
    assert "  report     succeeded  report_reference=sync-report:1" in output
    assert (
        "Run succeeded: syncs=1 succeeded=1 failed=0 partial=0 planned=0 confirmed=42 progress_advanced=true"
        in output
    )
    assert "Run index: run-index:1" in output


def test_text_console_renderer_redacts_secret_shaped_values() -> None:
    stream = io.StringIO()
    renderer = text(stream=stream)

    renderer.run_started(
        runner_name="runner token=raw-token",
        run_id="run-1",
        dry_run=False,
    )
    renderer.sync_started(
        sync_name="sync",
        destination_binding_name="destination",
        surface="https://user:raw-url-secret@example.test/private",
    )
    renderer.progress_commit_decided(
        sync_name="sync",
        destination_binding_name="destination",
        surface="surface",
        status="blocked",
        progress_advanced=False,
        progress_decision_allowed=False,
        reason="Partner rejected Authorization: Bearer raw-bearer-token",
    )

    output = stream.getvalue()
    assert "token=[redacted]" in output
    assert "https://[redacted]@example.test/private" in output
    assert "Authorization=[redacted]" in output
    assert "raw-token" not in output
    assert "raw-url-secret" not in output
    assert "raw-bearer-token" not in output


def test_text_console_renderer_resets_sync_headers_between_runs() -> None:
    stream = io.StringIO()
    renderer = text(stream=stream)

    renderer.run_started(runner_name="runner", run_id="run-1", dry_run=False)
    renderer.stage_completed(
        sync_name="sync",
        destination_binding_name="destination",
        surface="surface",
        status="succeeded",
        row_count=1,
    )
    renderer.run_completed(runner_name="runner", status="succeeded")
    renderer.run_started(runner_name="runner", run_id="run-2", dry_run=False)
    renderer.stage_completed(
        sync_name="sync",
        destination_binding_name="destination",
        surface="surface",
        status="succeeded",
        row_count=2,
    )

    output = stream.getvalue()
    assert output.count("sync -> destination/surface") == 2
    assert "run_id=run-1" in output
    assert "run_id=run-2" in output
    assert "rows=1" in output
    assert "rows=2" in output


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return _backend(tmp_path).runtime_store()


def _backend(tmp_path: Path) -> DuckDBSqlBackend:
    return DuckDBSqlBackend(
        database=_warehouse_database(tmp_path),
        source_schema="main",
        runtime_schema="retl",
    )


def _warehouse_database(tmp_path: Path) -> Path:
    return tmp_path / "warehouse.duckdb"


def _state_declaration(tmp_path: Path) -> retl.State:
    source_database = _warehouse_database(tmp_path)
    connection = duckdb.connect(str(source_database))
    connection.execute("create table customers (customer_id varchar, email varchar, plan varchar)")
    connection.executemany(
        "insert into customers values (?, ?, ?)",
        [
            ("cust_1", "one@example.com", "tier_alpha_raw"),
            ("cust_2", "two@example.com", "tier_beta_raw"),
        ],
    )
    connection.close()
    return retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query="select customer_id, email, plan from customers",
            backend=_backend(tmp_path).source_backend(),
        ),
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )


def _state_sync(
    declaration: retl.State,
    *,
    name: str = "customer_profiles",
    binding_name: str = "mock_profiles",
    surface: str = "profile_properties",
    on_failure: retl.FailureHandlingMode = "continue_on_any",
) -> retl.Sync:
    return retl.sync(
        name=name,
        declaration=declaration,
        destination=retl.destinations.load("retl/mock", binding_name=binding_name),
        surface=surface,
        on_failure=on_failure,
    )


def _empty_event_declaration(tmp_path: Path) -> retl.Event:
    source_database = _warehouse_database(tmp_path)
    connection = duckdb.connect(str(source_database))
    connection.execute(
        """
        create table purchases (
            purchase_id varchar,
            email varchar,
            purchased_at varchar,
            order_total integer
        )
        """
    )
    connection.close()
    return retl.event(
        name="purchase",
        source=retl.source(
            name="purchases",
            mode="checkpointed",
            query="select purchase_id, email, purchased_at, order_total from purchases",
            checkpoint={
                "cursor": "purchased_at",
                "primary_key": "purchase_id",
                "cursor_type": "string",
                "primary_key_type": "string",
            },
            backend=_backend(tmp_path).source_backend(),
        ),
        key={"purchase": "purchase_id"},
        occurred_at="purchased_at",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"order_total": "order_total"},
    )


def _event_sync(declaration: retl.Event) -> retl.Sync:
    return retl.sync(
        name="purchase_imports",
        declaration=declaration,
        destination=retl.destinations.load("retl/mock", binding_name="mock_events"),
        surface="purchase_event",
    )
