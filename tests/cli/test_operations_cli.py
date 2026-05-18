from __future__ import annotations

import importlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

import retl
from retl.cli import main as cli_main
from retl.config import MappingConfigResolver
from retl.stores.contracts import (
    CanonicalKeyScalar,
    DestinationProgressScope,
    DestinationScanRange,
    EventKeysetScanPosition,
)


class RecordingStore:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def _record(self, method: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append((method, kwargs))
        return {"method": method, "kwargs": kwargs, "password": "resolved-secret"}

    def inspect_runtime_store(self) -> dict[str, Any]:
        return self._record("inspect_runtime_store")

    def inspect_declaration(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("inspect_declaration", **kwargs)

    def inspect_destination_scope(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("inspect_destination_scope", **kwargs)

    def inspect_collect_id(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("inspect_collect_id", **kwargs)

    def inspect_target_registry(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("inspect_target_registry", **kwargs)

    def inspect_run(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("inspect_run", **kwargs)

    def dismiss_unresolved_destination_batches(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("dismiss_unresolved_destination_batches", **kwargs)

    def skip_range(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("skip_range", **kwargs)

    def reset_runtime_store(self) -> dict[str, Any]:
        return self._record("reset_runtime_store")

    def reset_destination_scope(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("reset_destination_scope", **kwargs)

    def cleanup_ordered_work_operation(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("cleanup_ordered_work_operation", **kwargs)

    def delete_ordered_work(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("delete_ordered_work", **kwargs)

    def cleanup_cursors(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("cleanup_cursors", **kwargs)

    def cleanup_evidence(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("cleanup_evidence", **kwargs)

    def delete_collect_id(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("delete_collect_id", **kwargs)

    def delete_ordered_work_range(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("delete_ordered_work_range", **kwargs)

    def rebaseline_state(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("rebaseline_state", **kwargs)

    def reset_target_registry(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("reset_target_registry", **kwargs)

    def delete_run_evidence(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("delete_run_evidence", **kwargs)

    def delete_report_evidence(self, **kwargs: Any) -> dict[str, Any]:
        return self._record("delete_report_evidence", **kwargs)


class SecretStringStore(RecordingStore):
    def inspect_runtime_store(self) -> dict[str, Any]:
        return {
            "message": (
                "operation failed with password=super-secret private_key=key bearer abc.def"
            )
        }


@pytest.fixture(autouse=True)
def reset_config() -> Iterator[None]:
    retl.configure()
    yield
    retl.configure()


def test_duckdb_inspect_runtime_cli_uses_temp_runtime_database(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "runtime.duckdb"

    code = cli_main(
        [
            "operations",
            "inspect-runtime",
            "--backend",
            "duckdb",
            "--database",
            str(database),
            "--schema",
            "retl",
        ]
    )

    assert code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["kind"] == "runtime_store"
    assert output["sql_context"]["backend"] == "duckdb"


def test_operations_commands_dispatch_through_runner_operations(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    store = RecordingStore()
    monkeypatch.setattr(cli, "_runtime_store", lambda args: store)

    cases = [
        (["inspect-runtime"], "inspect_runtime_store"),
        (["inspect-declaration", "--declaration-name", "customer_state"], "inspect_declaration"),
        (
            ["inspect-destination-scope", *_scope_args()],
            "inspect_destination_scope",
        ),
        (
            [
                "inspect-collect-id",
                "--declaration-name",
                "customer_state",
                "--collect-id",
                "00000000-0001-7000-8000-000000000000",
            ],
            "inspect_collect_id",
        ),
        (["inspect-target-registry", "--destination-name", "crm"], "inspect_target_registry"),
        (["inspect-run", "--run-id", "run_1"], "inspect_run"),
        (["dismiss-unresolved", *_scope_args()], "dismiss_unresolved_destination_batches"),
        (["skip-ordered-work-range", *_scope_args(), *_range_args()], "skip_range"),
        (
            ["skip-event-keyset-range", *_event_scope_args(), *_event_keyset_range_args()],
            "skip_range",
        ),
        (["reset-runtime-store"], "reset_runtime_store"),
        (["reset-destination-scope", *_scope_args()], "reset_destination_scope"),
        (
            [
                "cleanup-ordered-work",
                "--declaration-name",
                "customer_state",
                "--family",
                "state",
                "--through-collect-id",
                "00000000-0001-7000-8000-000000000000",
                "--dry-run",
            ],
            "cleanup_ordered_work_operation",
        ),
        (
            [
                "delete-ordered-work",
                "--declaration-name",
                "customer_state",
                "--family",
                "state",
                "--force",
            ],
            "delete_ordered_work",
        ),
        (
            ["cleanup-cursors", "--older-than-seconds", "3600", "--dry-run"],
            "cleanup_cursors",
        ),
        (
            ["cleanup-evidence", "--older-than-seconds", "3600", "--sync-name", "customer_sync"],
            "cleanup_evidence",
        ),
        (
            [
                "delete-collect-id",
                "--declaration-name",
                "customer_state",
                "--collect-id",
                "00000000-0001-7000-8000-000000000000",
                "--force",
            ],
            "delete_collect_id",
        ),
        (
            [
                "delete-ordered-work-range",
                "--declaration-name",
                "customer_state",
                *_range_args(),
                "--force",
            ],
            "delete_ordered_work_range",
        ),
        (
            [
                "rebaseline-state",
                "--declaration-name",
                "customer_state",
                "--source-name",
                "customers",
            ],
            "rebaseline_state",
        ),
        (
            ["reset-target-registry", "--destination-name", "crm", "--target", "vip"],
            "reset_target_registry",
        ),
        (["delete-run-evidence", "--run-id", "run_1"], "delete_run_evidence"),
        (["delete-report-evidence", "--sync-name", "customer_sync"], "delete_report_evidence"),
    ]

    for args, expected_method in cases:
        assert cli_main(["operations", *args]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["method"] == expected_method

    assert [method for method, _ in store.calls] == [expected for _, expected in cases]


def test_scope_and_range_flags_build_operation_values(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    store = RecordingStore()
    monkeypatch.setattr(cli, "_runtime_store", lambda args: store)

    code = cli_main(["operations", "skip-ordered-work-range", *_scope_args(), *_range_args()])

    assert code == 0
    capsys.readouterr()
    method, kwargs = store.calls[-1]
    assert method == "skip_range"
    assert kwargs["scope"].sync_name == "customer_sync"
    assert kwargs["scope"].destination_name == "crm"
    assert kwargs["scope"].surface == "profile"
    assert kwargs["scope"].family == "state"
    assert (
        kwargs["scan_range"].first_record_position.collect_id
        == "00000000-0001-7000-8000-000000000000"
    )
    assert kwargs["scan_range"].last_record_position.sequence_order == 4


def test_event_keyset_skip_flags_build_typed_destination_scan_range(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    store = RecordingStore()
    monkeypatch.setattr(cli, "_runtime_store", lambda args: store)

    code = cli_main(
        [
            "operations",
            "skip-event-keyset-range",
            *_event_scope_args(),
            *_event_keyset_range_args(
                first_cursor_kind="integer",
                first_cursor_value="42",
                first_primary_key_kind="string",
                first_primary_key_value="purchase_042",
                last_cursor_kind="integer",
                last_cursor_value="43",
                last_primary_key_kind="string",
                last_primary_key_value="purchase_043",
                upper_cursor_kind="integer",
                upper_cursor_value="44",
                upper_primary_key_kind="string",
                upper_primary_key_value="purchase_044",
            ),
        ]
    )

    assert code == 0
    capsys.readouterr()
    method, kwargs = store.calls[-1]
    assert method == "skip_range"
    assert kwargs["scope"].family == "event"
    assert kwargs["scope"].declaration_name == "purchase"
    scan_range = kwargs["scan_range"]
    assert isinstance(scan_range, DestinationScanRange)
    assert scan_range.lower_bound_exclusive is None
    assert scan_range.first_record_position == EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.integer(42),
        primary_key_value=CanonicalKeyScalar.string("purchase_042"),
    )
    assert scan_range.last_record_position == EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.integer(43),
        primary_key_value=CanonicalKeyScalar.string("purchase_043"),
    )
    assert scan_range.upper_bound_inclusive == EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.integer(44),
        primary_key_value=CanonicalKeyScalar.string("purchase_044"),
    )


def test_event_keyset_skip_cli_round_trips_typed_range_through_duckdb(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from retl.backends.duckdb import DuckDBRuntimeStore

    database = tmp_path / "runtime.duckdb"
    expected_range = DestinationScanRange(
        first_record_position=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.integer(42),
            primary_key_value=CanonicalKeyScalar.string("purchase_042"),
        ),
        last_record_position=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.integer(43),
            primary_key_value=CanonicalKeyScalar.string("purchase_043"),
        ),
        upper_bound_inclusive=EventKeysetScanPosition(
            cursor_value=CanonicalKeyScalar.integer(44),
            primary_key_value=CanonicalKeyScalar.string("purchase_044"),
        ),
    )

    code = cli_main(
        [
            "operations",
            "skip-event-keyset-range",
            "--backend",
            "duckdb",
            "--database",
            str(database),
            *_event_scope_args(),
            *_event_keyset_range_args(
                first_cursor_kind="integer",
                first_cursor_value="42",
                first_primary_key_value="purchase_042",
                last_cursor_kind="integer",
                last_cursor_value="43",
                last_primary_key_value="purchase_043",
                upper_cursor_kind="integer",
                upper_cursor_value="44",
                upper_primary_key_value="purchase_044",
            ),
        ]
    )

    assert code == 0
    capsys.readouterr()
    store = DuckDBRuntimeStore(database=database, schema="retl")
    try:
        batches = store.list_destination_batches(scope=_event_scope(), statuses=("skipped",))
    finally:
        store.close()
    assert len(batches) == 1
    assert batches[0].identity.source_range == expected_range


def test_event_keyset_skip_accepts_complete_exclusive_lower_bound(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    store = RecordingStore()
    monkeypatch.setattr(cli, "_runtime_store", lambda args: store)

    code = cli_main(
        [
            "operations",
            "skip-event-keyset-range",
            *_event_scope_args(),
            *_event_keyset_range_args(),
            "--lower-cursor-kind",
            "string",
            "--lower-cursor-value",
            "2025-12-31T00:00:00",
            "--lower-primary-key-kind",
            "string",
            "--lower-primary-key-value",
            "purchase_0",
        ]
    )

    assert code == 0
    capsys.readouterr()
    scan_range = store.calls[-1][1]["scan_range"]
    assert scan_range.lower_bound_exclusive == EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2025-12-31T00:00:00"),
        primary_key_value=CanonicalKeyScalar.string("purchase_0"),
    )


def test_event_keyset_skip_rejects_incomplete_lower_bound_before_store_call(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    store = RecordingStore()
    monkeypatch.setattr(cli, "_runtime_store", lambda args: store)

    code = cli_main(
        [
            "operations",
            "skip-event-keyset-range",
            *_event_scope_args(),
            *_event_keyset_range_args(),
            "--lower-cursor-kind",
            "integer",
            "--lower-cursor-value",
            "41",
        ]
    )

    assert code == 2
    captured = capsys.readouterr()
    assert "lower Event keyset bound is incomplete" in captured.err
    assert store.calls == []


def test_event_keyset_skip_rejects_malformed_scalar_before_store_call(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    store = RecordingStore()
    monkeypatch.setattr(cli, "_runtime_store", lambda args: store)

    code = cli_main(
        [
            "operations",
            "skip-event-keyset-range",
            *_event_scope_args(),
            *_event_keyset_range_args(
                first_cursor_kind="boolean",
                first_cursor_value="1",
                last_cursor_kind="boolean",
                last_cursor_value="false",
                upper_cursor_kind="boolean",
                upper_cursor_value="true",
            ),
        ]
    )

    assert code == 2
    captured = capsys.readouterr()
    assert "boolean value must be exactly" in captured.err
    assert store.calls == []


def test_event_keyset_skip_rejects_inconsistent_scalar_kinds_before_store_call(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    cases = [
        (
            _event_keyset_range_args(first_cursor_kind="integer", first_cursor_value="42"),
            "cursor scalar kind must be identical",
        ),
        (
            _event_keyset_range_args(
                first_primary_key_kind="integer",
                first_primary_key_value="42",
            ),
            "primary-key scalar kind must be identical",
        ),
    ]
    for range_args, message in cases:
        store = RecordingStore()
        monkeypatch.setattr(cli, "_runtime_store", lambda args, store=store: store)

        code = cli_main(
            ["operations", "skip-event-keyset-range", *_event_scope_args(), *range_args]
        )

        assert code == 2
        captured = capsys.readouterr()
        assert message in captured.err
        assert store.calls == []


def test_event_keyset_skip_rejects_state_scope_before_store_call(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    store = RecordingStore()
    monkeypatch.setattr(cli, "_runtime_store", lambda args: store)

    code = cli_main(
        ["operations", "skip-event-keyset-range", *_scope_args(), *_event_keyset_range_args()]
    )

    assert code == 2
    captured = capsys.readouterr()
    assert "requires --family event" in captured.err
    assert store.calls == []


def test_ordered_work_skip_rejects_event_scope_before_store_call(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    runtime_store_called = False

    def fail_runtime_store(args: object) -> RecordingStore:
        nonlocal runtime_store_called
        runtime_store_called = True
        raise AssertionError("runtime store should not be initialized")

    monkeypatch.setattr(cli, "_runtime_store", fail_runtime_store)

    code = cli_main(["operations", "skip-ordered-work-range", *_event_scope_args(), *_range_args()])

    assert code == 2
    captured = capsys.readouterr()
    assert "requires --family state" in captured.err
    assert runtime_store_called is False


def test_event_keyset_skip_rejects_unsupported_scalar_kind_before_store_call(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    store = RecordingStore()
    monkeypatch.setattr(cli, "_runtime_store", lambda args: store)

    with pytest.raises(SystemExit):
        cli_main(
            [
                "operations",
                "skip-event-keyset-range",
                *_event_scope_args(),
                *_event_keyset_range_args(first_cursor_kind="timestamp"),
            ]
        )

    captured = capsys.readouterr()
    assert "invalid choice" in captured.err
    assert store.calls == []


def test_json_output_redacts_secret_shaped_fields(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    monkeypatch.setattr(cli, "_runtime_store", lambda args: RecordingStore())

    code = cli_main(["operations", "inspect-runtime"])

    assert code == 0
    text = capsys.readouterr().out
    assert "resolved-secret" not in text
    assert json.loads(text)["password"] == "[REDACTED]"


def test_pretty_outputs_indented_json_from_shared_command_flag(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    monkeypatch.setattr(cli, "_runtime_store", lambda args: RecordingStore())

    code = cli_main(["operations", "inspect-runtime", "--pretty"])

    assert code == 0
    text = capsys.readouterr().out
    assert text.startswith("{\n")
    assert '  "method": "inspect_runtime_store"' in text
    assert json.loads(text)["method"] == "inspect_runtime_store"


def test_default_json_output_remains_compact(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    monkeypatch.setattr(cli, "_runtime_store", lambda args: RecordingStore())

    code = cli_main(["operations", "inspect-runtime"])

    assert code == 0
    text = capsys.readouterr().out
    assert text.startswith('{"')
    assert "\n  " not in text
    assert json.loads(text)["method"] == "inspect_runtime_store"


def test_json_output_redacts_secret_shaped_substrings_in_non_sensitive_string_keys(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    monkeypatch.setattr(cli, "_runtime_store", lambda args: SecretStringStore())

    code = cli_main(["operations", "inspect-runtime"])

    assert code == 0
    text = capsys.readouterr().out
    assert "super-secret" not in text
    assert "private_key=key" not in text
    assert "bearer abc.def" not in text.lower()
    payload = json.loads(text)
    assert "password=[redacted]" in payload["message"]
    assert "private_key=[redacted]" in payload["message"]


def test_error_json_redacts_secret_shaped_substrings_in_exception_text(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cli = importlib.import_module("retl.cli.main")

    def raise_secret_error(args: object) -> object:
        raise retl.DeclarationValidationError(
            "backend failed password=super-secret private_key=key Bearer abc.def"
        )

    monkeypatch.setattr(cli, "_runtime_store", raise_secret_error)

    code = cli_main(["operations", "inspect-runtime"])

    assert code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "super-secret" not in captured.err
    assert "private_key=key" not in captured.err
    assert "bearer abc.def" not in captured.err.lower()
    payload = json.loads(captured.err)
    assert "password=[redacted]" in payload["error"]
    assert "private_key=[redacted]" in payload["error"]


def test_snowflake_backend_factory_uses_config_namespace_and_credential_namespace(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from retl.backends.snowflake import SnowflakeSqlBackend

    retl.configure(
        config_resolver=MappingConfigResolver(
            {
                "backends.snowflake.account": "acct",
                "backends.snowflake.warehouse": "WH",
                "backends.snowflake.source_database": "SRC_DB",
                "backends.snowflake.runtime_database": "RUNTIME_DB",
            }
        ),
        secret_resolver=retl.auth.MappingSecretResolver(
            {
                "backends.snowflake.key_pair.user": "user",
                "backends.snowflake.key_pair.private_key": "private",
            }
        ),
    )
    captured: dict[str, Any] = {}

    def fake_runtime_store(self: SnowflakeSqlBackend) -> RecordingStore:
        captured["backend"] = self
        return RecordingStore()

    monkeypatch.setattr(SnowflakeSqlBackend, "runtime_store", fake_runtime_store)

    code = cli_main(
        [
            "operations",
            "inspect-runtime",
            "--backend",
            "snowflake",
            "--namespace",
            "backends.snowflake",
            "--auth-mode",
            "key_pair",
        ]
    )

    assert code == 0
    capsys.readouterr()
    backend = captured["backend"]
    assert backend.account == "acct"
    assert backend.runtime_database == "RUNTIME_DB"
    assert backend.auth.mode.name == "key_pair"
    assert backend.auth.credentials["user"].name == "backends.snowflake.key_pair.user"


def test_bigquery_backend_factory_uses_config_namespace_and_credential_namespace(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from retl.backends.bigquery import BigQuerySqlBackend

    retl.configure(
        config_resolver=MappingConfigResolver(
            {
                "backends.bigquery.project": "example-analytics-project",
                "backends.bigquery.location": "US",
                "backends.bigquery.source_project": "example-source-project",
                "backends.bigquery.source_dataset": "mart",
                "backends.bigquery.runtime_project": "example-runtime-project",
                "backends.bigquery.runtime_dataset": "retl_runtime",
            }
        ),
        secret_resolver=retl.auth.MappingSecretResolver(
            {
                "backends.bigquery.service_account.credentials_json": "{}",
            }
        ),
    )
    captured: dict[str, Any] = {}

    def fake_runtime_store(self: BigQuerySqlBackend) -> RecordingStore:
        captured["backend"] = self
        return RecordingStore()

    monkeypatch.setattr(BigQuerySqlBackend, "runtime_store", fake_runtime_store)

    code = cli_main(
        [
            "operations",
            "inspect-runtime",
            "--backend",
            "bigquery",
            "--bigquery-namespace",
            "backends.bigquery",
            "--auth-mode",
            "service_account_json",
            "--credential-namespace",
            "backends.bigquery.service_account",
        ]
    )

    assert code == 0
    capsys.readouterr()
    backend = captured["backend"]
    assert backend.project == "example-analytics-project"
    assert backend.runtime_project == "example-runtime-project"
    assert backend.auth.mode.name == "service_account_json"
    assert (
        backend.auth.credentials["credentials_json"].name
        == "backends.bigquery.service_account.credentials_json"
    )


def test_databricks_backend_factory_uses_config_namespace_and_credential_namespace(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from retl.backends.databricks import DatabricksSqlBackend

    retl.configure(
        config_resolver=MappingConfigResolver(
            {
                "backends.databricks.server_hostname": "dbc.example.com",
                "backends.databricks.http_path": "/sql/1.0/warehouses/abc",
                "backends.databricks.source_catalog": "source_catalog",
                "backends.databricks.source_schema": "source_schema",
                "backends.databricks.runtime_catalog": "runtime_catalog",
                "backends.databricks.runtime_schema": "runtime_schema",
            }
        ),
        secret_resolver=retl.auth.MappingSecretResolver(
            {
                "backends.databricks.production.client_id": "client-id",
                "backends.databricks.production.client_secret": "client-secret",
            }
        ),
    )
    captured: dict[str, Any] = {}

    def fake_runtime_store(self: DatabricksSqlBackend) -> RecordingStore:
        captured["backend"] = self
        return RecordingStore()

    monkeypatch.setattr(DatabricksSqlBackend, "runtime_store", fake_runtime_store)

    code = cli_main(
        [
            "operations",
            "inspect-runtime",
            "--backend",
            "databricks",
            "--databricks-namespace",
            "backends.databricks",
            "--auth-mode",
            "oauth_m2m",
            "--credential-namespace",
            "backends.databricks.production",
        ]
    )

    assert code == 0
    capsys.readouterr()
    backend = captured["backend"]
    assert backend.server_hostname == "dbc.example.com"
    assert backend.runtime_catalog == "runtime_catalog"
    assert backend.auth.mode.name == "oauth_m2m"
    assert (
        backend.auth.credentials["client_secret"].name
        == "backends.databricks.production.client_secret"
    )


def test_postgresql_backend_factory_uses_config_namespace_and_credential_namespace(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from retl.backends.postgresql import PostgreSqlBackend

    retl.configure(
        config_resolver=MappingConfigResolver(
            {
                "backends.postgresql.host": "db.example.com",
                "backends.postgresql.port": "5433",
                "backends.postgresql.database": "app",
                "backends.postgresql.source_schema": "analytics",
                "backends.postgresql.runtime_schema": "retl_runtime",
                "backends.postgresql.sslmode": "require",
            }
        ),
        secret_resolver=retl.auth.MappingSecretResolver(
            {
                "backends.postgresql.password.user": "user",
                "backends.postgresql.password.password": "secret",
            }
        ),
    )
    captured: dict[str, Any] = {}

    def fake_runtime_store(self: PostgreSqlBackend) -> RecordingStore:
        captured["backend"] = self
        return RecordingStore()

    monkeypatch.setattr(PostgreSqlBackend, "runtime_store", fake_runtime_store)

    code = cli_main(
        [
            "operations",
            "inspect-runtime",
            "--backend",
            "postgresql",
            "--postgresql-namespace",
            "backends.postgresql",
            "--credential-namespace",
            "backends.postgresql.password",
        ]
    )

    assert code == 0
    capsys.readouterr()
    backend = captured["backend"]
    assert backend.host == "db.example.com"
    assert backend.port == 5433
    assert backend.runtime_schema == "retl_runtime"
    assert backend.sslmode == "require"
    assert backend.auth.mode.name == "password"
    assert backend.auth.credentials["password"].name == "backends.postgresql.password.password"


def test_legacy_operation_command_names_are_not_accepted(
    capsys: pytest.CaptureFixture[str],
) -> None:
    for name in (
        "reset-active",
        "discard-pending",
        "recover",
        "reset-local-state",
        "repair-progress",
    ):
        with pytest.raises(SystemExit):
            cli_main(["operations", name])
        capsys.readouterr()


def _scope_args() -> list[str]:
    return [
        "--sync-name",
        "customer_sync",
        "--destination-name",
        "crm",
        "--surface",
        "profile",
        "--family",
        "state",
        "--declaration-name",
        "customer_state",
    ]


def _event_scope_args() -> list[str]:
    return [
        "--sync-name",
        "purchase_sync",
        "--destination-name",
        "crm",
        "--surface",
        "purchase_event",
        "--family",
        "event",
        "--declaration-name",
        "purchase",
    ]


def _event_scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="purchase_sync",
        destination_name="crm",
        surface="purchase_event",
        family="event",
        declaration_name="purchase",
    )


def _range_args() -> list[str]:
    return [
        "--first-collect-id",
        "00000000-0001-7000-8000-000000000000",
        "--first-sequence-order",
        "2",
        "--last-collect-id",
        "00000000-0003-7000-8000-000000000000",
        "--last-sequence-order",
        "4",
    ]


def _event_keyset_range_args(
    *,
    first_cursor_kind: str = "string",
    first_cursor_value: str = "2026-01-01T00:00:00",
    first_primary_key_kind: str = "string",
    first_primary_key_value: str = "purchase_1",
    last_cursor_kind: str = "string",
    last_cursor_value: str = "2026-01-02T00:00:00",
    last_primary_key_kind: str = "string",
    last_primary_key_value: str = "purchase_2",
    upper_cursor_kind: str = "string",
    upper_cursor_value: str = "2026-01-03T00:00:00",
    upper_primary_key_kind: str = "string",
    upper_primary_key_value: str = "purchase_3",
) -> list[str]:
    return [
        "--first-cursor-kind",
        first_cursor_kind,
        "--first-cursor-value",
        first_cursor_value,
        "--first-primary-key-kind",
        first_primary_key_kind,
        "--first-primary-key-value",
        first_primary_key_value,
        "--last-cursor-kind",
        last_cursor_kind,
        "--last-cursor-value",
        last_cursor_value,
        "--last-primary-key-kind",
        last_primary_key_kind,
        "--last-primary-key-value",
        last_primary_key_value,
        "--upper-cursor-kind",
        upper_cursor_kind,
        "--upper-cursor-value",
        upper_cursor_value,
        "--upper-primary-key-kind",
        upper_primary_key_kind,
        "--upper-primary-key-value",
        upper_primary_key_value,
    ]
