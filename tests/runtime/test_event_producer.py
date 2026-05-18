from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pytest

import retl
from retl.backends.duckdb import DuckDBRuntimeStore, DuckDBSqlBackend
from retl.collect_identity import is_uuidv7
from retl.errors import DeclarationValidationError
from retl.events.producer import produce_event_collect
from retl.stores.contracts import (
    CanonicalKeyScalar,
    DestinationProgressScope,
    EventKeysetScanPosition,
    EventProductionStore,
    EventSourceWindowRequest,
    EventSourceWindowSource,
)
from retl.stores.sql_runtime.errors import RuntimeStoreError


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return _backend(tmp_path).runtime_store()


def _backend(tmp_path: Path) -> DuckDBSqlBackend:
    return DuckDBSqlBackend(
        database=_source_database(tmp_path),
        source_schema="main",
        runtime_schema="retl",
    )


def _source_database(tmp_path: Path) -> Path:
    return tmp_path / "warehouse.duckdb"


def _declaration(backend: DuckDBSqlBackend) -> retl.Event:
    return retl.event(
        name="purchase_event",
        source=retl.source(
            name="purchases",
            query="""
                select purchase_id, customer_id, email, occurred_at, amount, sku
                from purchases
            """,
            mode="checkpointed",
            checkpoint={
                "cursor": "occurred_at",
                "primary_key": "purchase_id",
                "cursor_type": "string",
                "primary_key_type": "string",
            },
            backend=backend.source_backend(),
        ),
        key={"purchase": "purchase_id"},
        occurred_at="occurred_at",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"amount": "amount", "sku": "sku"},
    )


def _schema_declaration(backend: DuckDBSqlBackend) -> retl.Event:
    return retl.event(
        name="purchase_event",
        source=retl.source(
            name="purchases",
            query="""
                select purchase_id, customer_id, email, occurred_at, amount, sku
                from purchases
            """,
            mode="checkpointed",
            checkpoint={
                "cursor": "occurred_at",
                "primary_key": "purchase_id",
                "cursor_type": "string",
                "primary_key_type": "string",
            },
            backend=backend.source_backend(),
        ),
        key={"purchase": "purchase_id"},
        occurred_at="occurred_at",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"amount": "amount", "sku": "sku"},
    )


def _scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="purchase_sync",
        destination_name="test_destination",
        surface="purchase_event",
        family="event",
        declaration_name="purchase_event",
    )


def _create_source_table(database: Path) -> None:
    connection = duckdb.connect(str(database))
    connection.execute(
        """
        create table purchases (
            purchase_id varchar,
            customer_id varchar,
            email varchar,
            occurred_at varchar,
            amount integer,
            sku varchar
        )
        """
    )
    connection.close()


def _create_source_schema_table(database: Path, schema: str) -> None:
    connection = duckdb.connect(str(database))
    connection.execute(f"create schema {schema}")
    connection.execute(
        f"""
        create table {schema}.purchases (
            purchase_id varchar,
            customer_id varchar,
            email varchar,
            occurred_at varchar,
            amount integer,
            sku varchar
        )
        """
    )
    connection.close()


def _create_typed_timestamp_source_table(database: Path) -> None:
    connection = duckdb.connect(str(database))
    connection.execute(
        """
        create table purchases (
            purchase_id varchar,
            customer_id varchar,
            email varchar,
            occurred_at timestamp,
            amount integer,
            sku varchar
        )
        """
    )
    connection.close()


def _replace_source_rows(
    database: Path,
    rows: list[tuple[str, str, str, str, int, str]],
) -> None:
    connection = duckdb.connect(str(database))
    connection.execute("delete from purchases")
    connection.executemany("insert into purchases values (?, ?, ?, ?, ?, ?)", rows)
    connection.close()


def _replace_source_schema_rows(
    database: Path,
    schema: str,
    rows: list[tuple[str, str, str, str, int, str]],
) -> None:
    connection = duckdb.connect(str(database))
    connection.execute(f"delete from {schema}.purchases")
    connection.executemany(f"insert into {schema}.purchases values (?, ?, ?, ?, ?, ?)", rows)
    connection.close()


def _replace_typed_timestamp_source_rows(
    database: Path,
    rows: list[tuple[str, str, str, datetime, int, str]],
) -> None:
    connection = duckdb.connect(str(database))
    connection.execute("delete from purchases")
    connection.executemany("insert into purchases values (?, ?, ?, ?, ?, ?)", rows)
    connection.close()


def _pending(store: DuckDBRuntimeStore) -> list[tuple[str, int, str, str, int]]:
    page = store.read_pending_work(scope=_scope(), max_rows=100)
    if page.row_count == 0:
        return []
    collect_ids = _column_values(page.payload, "collect_id")
    sequence_orders = _column_values(page.payload, "sequence_order")
    keys = _json_column_values(page.payload, "key_json")
    occurred_at_values = _column_values(page.payload, "event_occurred_at")
    payloads = _json_column_values(page.payload, "payload_json")
    return [
        (
            str(collect_id),
            int(sequence_order),
            str(key["purchase"]),
            str(occurred_at),
            int(payload["amount"]),
        )
        for collect_id, sequence_order, key, occurred_at, payload in zip(
            collect_ids,
            sequence_orders,
            keys,
            occurred_at_values,
            payloads,
            strict=True,
        )
    ]


def _source_page(
    store: EventProductionStore,
    declaration: retl.Event,
    *,
    max_rows: int = 100,
) -> pa.RecordBatch:
    checkpoint = declaration.source.checkpoint
    assert checkpoint is not None
    adapter = cast(Any, declaration.source.backend).adapter()
    assert isinstance(adapter, EventSourceWindowSource)
    return store.read_event_source_window(
        declaration=declaration,
        window=adapter.prepare_event_source_window(
            EventSourceWindowRequest(
                source_name=declaration.source.name,
                query=declaration.source.query,
                cursor_column=checkpoint["cursor"],
                primary_key_column=checkpoint["primary_key"],
                limit=max_rows,
            )
        ),
        max_rows=max_rows,
    ).payload


def _column_values(batch: pa.RecordBatch, column: str) -> list[Any]:
    return batch.column(column).to_pylist()


def _json_column_values(batch: pa.RecordBatch, column: str) -> list[Any]:
    return [json.loads(value) if value else None for value in _column_values(batch, column)]


def test_first_checkpointed_event_collect_emits_imports_under_one_collect_id(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    _replace_source_rows(
        source_database,
        [
            ("p_2", "cust_2", "two@example.com", "2024-01-02T00:00:00Z", 200, "sku_b"),
            ("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a"),
        ],
    )

    evidence = produce_event_collect(declaration=_declaration(_backend(tmp_path)), store=store)

    assert is_uuidv7(evidence.collect_id)
    assert evidence.window_row_count == 2
    assert evidence.work_row_count == 0
    assert evidence.scan_after is None
    assert evidence.scan_upper_bound == EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2024-01-02T00:00:00Z"),
        primary_key_value=CanonicalKeyScalar.string("p_2"),
    )
    assert _pending(store) == []


def test_event_collect_reads_same_database_source_schema(tmp_path: Path) -> None:
    source_database = _source_database(tmp_path)
    _create_source_schema_table(source_database, "source")
    _replace_source_schema_rows(
        source_database,
        "source",
        [("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a")],
    )
    backend = DuckDBSqlBackend(
        database=source_database,
        source_schema="source",
        runtime_schema="retl",
    )
    store = backend.runtime_store()

    evidence = produce_event_collect(
        declaration=_schema_declaration(backend),
        store=store,
    )

    assert is_uuidv7(evidence.collect_id)
    assert evidence.window_row_count == 1
    assert _pending(store) == []


def test_event_collect_temp_window_does_not_mutate_same_named_source_table(
    tmp_path: Path,
) -> None:
    source_database = _source_database(tmp_path)
    _create_source_schema_table(source_database, "source")
    _replace_source_schema_rows(
        source_database,
        "source",
        [("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a")],
    )
    connection = duckdb.connect(str(source_database))
    connection.execute(
        """
        create table source.retl_event_collect_window (
            marker varchar
        )
        """
    )
    connection.execute("insert into source.retl_event_collect_window values ('source-owned')")
    connection.close()
    backend = DuckDBSqlBackend(
        database=source_database,
        source_schema="source",
        runtime_schema="retl",
    )
    store = backend.runtime_store()

    evidence = produce_event_collect(
        declaration=_schema_declaration(backend),
        store=store,
    )

    assert evidence.work_row_count == 0
    source_rows = (
        duckdb.connect(str(source_database))
        .execute("select marker from source.retl_event_collect_window")
        .fetchall()
    )
    assert source_rows == [("source-owned",)]


def test_event_collect_does_not_write_runtime_ordered_work_when_source_has_same_table(
    tmp_path: Path,
) -> None:
    source_database = _source_database(tmp_path)
    _create_source_schema_table(source_database, "source")
    _replace_source_schema_rows(
        source_database,
        "source",
        [("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a")],
    )
    connection = duckdb.connect(str(source_database))
    connection.execute("create table source.ordered_work (marker varchar)")
    connection.execute("insert into source.ordered_work values ('source-owned')")
    connection.close()
    backend = DuckDBSqlBackend(
        database=source_database,
        source_schema="source",
        runtime_schema="retl",
    )
    store = backend.runtime_store()

    produce_event_collect(
        declaration=_schema_declaration(backend),
        store=store,
    )

    source_rows = (
        duckdb.connect(str(source_database))
        .execute("select marker from source.ordered_work")
        .fetchall()
    )
    runtime_rows = store._connection.execute(  # noqa: SLF001
        f"""
        select family, kind
        from {store.schema}.ordered_work
        order by sequence_order
        """
    ).fetchall()

    assert source_rows == [("source-owned",)]
    assert runtime_rows == []


def test_event_collect_id_order_is_deterministic_for_same_keyset_scan(
    tmp_path: Path,
) -> None:
    def collect_ordered_work(
        workspace: Path,
        rows: list[tuple[str, str, str, str, int, str]],
    ) -> list[tuple[int, str, str, int]]:
        workspace.mkdir()
        source_database = workspace / "warehouse.duckdb"
        backend = DuckDBSqlBackend(
            database=source_database,
            source_schema="main",
            runtime_schema="retl",
        )
        store = backend.runtime_store()
        _create_source_table(source_database)
        _replace_source_rows(source_database, rows)

        produce_event_collect(declaration=_declaration(backend), store=store)

        page = _source_page(store, _declaration(backend), max_rows=100)
        sequence_orders = _column_values(page, "sequence_order")
        keys = _json_column_values(page, "key_json")
        occurred_at_values = _column_values(page, "event_occurred_at")
        payloads = _json_column_values(page, "payload_json")
        return [
            (
                int(sequence_order),
                str(key["purchase"]),
                str(occurred_at),
                int(payload["amount"]),
            )
            for sequence_order, key, occurred_at, payload in zip(
                sequence_orders,
                keys,
                occurred_at_values,
                payloads,
                strict=True,
            )
        ]

    keyset_scan_rows = [
        ("p_3", "cust_3", "three@example.com", "2024-01-01T00:00:00Z", 300, "sku_c"),
        ("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a"),
        ("p_2", "cust_2", "two@example.com", "2024-01-01T00:00:00Z", 200, "sku_b"),
        ("p_4", "cust_4", "four@example.com", "2024-01-02T00:00:00Z", 400, "sku_d"),
    ]

    first_order = collect_ordered_work(tmp_path / "first", keyset_scan_rows)
    second_order = collect_ordered_work(tmp_path / "second", list(reversed(keyset_scan_rows)))

    assert (
        first_order
        == second_order
        == [
            (0, "p_1", "2024-01-01T00:00:00Z", 100),
            (1, "p_2", "2024-01-01T00:00:00Z", 200),
            (2, "p_3", "2024-01-01T00:00:00Z", 300),
            (3, "p_4", "2024-01-02T00:00:00Z", 400),
        ]
    )


def test_empty_event_window_records_collect_evidence_without_work_rows(tmp_path: Path) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)

    evidence = produce_event_collect(declaration=_declaration(_backend(tmp_path)), store=store)

    assert is_uuidv7(evidence.collect_id)
    assert evidence.window_row_count == 0
    assert evidence.work_row_count == 0
    assert evidence.scan_upper_bound is None
    assert store.read_pending_work(scope=_scope(), max_rows=10).row_count == 0


def test_typed_timestamp_event_cursor_records_json_safe_scan_upper_bound(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_typed_timestamp_source_table(source_database)
    _replace_typed_timestamp_source_rows(
        source_database,
        [
            (
                "p_1",
                "cust_1",
                "one@example.com",
                datetime(2024, 1, 1, 12, 30),
                100,
                "sku_a",
            ),
            (
                "p_2",
                "cust_2",
                "two@example.com",
                datetime(2024, 1, 2, 12, 30),
                200,
                "sku_b",
            ),
        ],
    )

    evidence = produce_event_collect(declaration=_declaration(_backend(tmp_path)), store=store)

    assert evidence.work_row_count == 0
    assert evidence.scan_upper_bound == EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.string("2024-01-02T12:30:00"),
        primary_key_value=CanonicalKeyScalar.string("p_2"),
    )


def test_event_identity_is_deterministic_from_key_mapping(tmp_path: Path) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    _replace_source_rows(
        source_database,
        [("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a")],
    )

    declaration = _declaration(_backend(tmp_path))
    produce_event_collect(declaration=declaration, store=store)
    page = _source_page(store, declaration, max_rows=10)
    keys = _json_column_values(page, "key_json")

    assert keys[0] == {"purchase": "p_1"}
    assert _column_values(page, "event_occurred_at") == ["2024-01-01T00:00:00Z"]


def test_event_fingerprint_is_stable_for_payload_and_changes_with_identity(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    declaration = _declaration(_backend(tmp_path))
    _replace_source_rows(
        source_database,
        [("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a")],
    )
    produce_event_collect(declaration=declaration, store=store)
    baseline_page = _source_page(store, declaration, max_rows=10)
    baseline_payload = _json_column_values(baseline_page, "payload_json")[0]
    baseline_occurred_at = _column_values(baseline_page, "event_occurred_at")[0]

    _replace_source_rows(
        source_database,
        [("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 101, "sku_a")],
    )
    produce_event_collect(declaration=declaration, store=store)
    payload_changed_page = _source_page(store, declaration, max_rows=10)
    payload_changed_payload = _json_column_values(
        payload_changed_page,
        "payload_json",
    )[0]
    payload_changed_occurred_at = _column_values(
        payload_changed_page,
        "event_occurred_at",
    )[0]

    _replace_source_rows(
        source_database,
        [("p_1", "cust_1", "one@example.com", "2024-01-02T00:00:00Z", 101, "sku_a")],
    )
    produce_event_collect(declaration=declaration, store=store)
    occurred_changed_page = _source_page(store, declaration, max_rows=10)
    occurred_changed = _column_values(
        occurred_changed_page,
        "event_occurred_at",
    )[0]

    assert baseline_occurred_at == payload_changed_occurred_at
    assert baseline_payload["amount"] == 100
    assert payload_changed_payload["amount"] == 101
    assert payload_changed_occurred_at != occurred_changed


def test_duplicate_event_identity_fails_without_partial_work(tmp_path: Path) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    _replace_source_rows(
        source_database,
        [
            ("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a"),
            ("p_1", "cust_2", "two@example.com", "2024-01-01T00:00:00Z", 200, "sku_b"),
        ],
    )

    with pytest.raises(DeclarationValidationError, match="duplicate Event identity"):
        produce_event_collect(declaration=_declaration(_backend(tmp_path)), store=store)

    assert store.read_pending_work(scope=_scope(), max_rows=10).row_count == 0


def test_event_source_replay_page_is_visible_without_ordered_work(tmp_path: Path) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    _replace_source_rows(
        source_database,
        [("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a")],
    )

    declaration = _declaration(_backend(tmp_path))
    produce_event_collect(declaration=declaration, store=store)

    payload = _source_page(store, declaration, max_rows=10)
    assert payload.num_rows == 1
    assert _column_values(payload, "family") == ["event"]
    assert _column_values(payload, "kind") == ["import"]
    assert _column_values(payload, "event_cursor_value") == ["2024-01-01T00:00:00Z"]
    assert _column_values(payload, "event_primary_key_value") == ["p_1"]


def test_event_producer_rejects_separate_source_and_runtime_duckdb_databases(
    tmp_path: Path,
) -> None:
    store = DuckDBRuntimeStore(database=tmp_path / "runtime_only.duckdb")
    source_database = tmp_path / "source_only.duckdb"
    _create_source_table(source_database)
    _replace_source_rows(
        source_database,
        [("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a")],
    )

    with pytest.raises(RuntimeStoreError, match="DuckDBSqlBackend-owned runtime store"):
        produce_event_collect(
            declaration=_declaration(
                DuckDBSqlBackend(
                    database=source_database,
                    source_schema="main",
                    runtime_schema="retl",
                )
            ),
            store=store,
        )


def test_event_producer_rejects_backend_owned_store_with_separate_source_file_before_mutation(
    tmp_path: Path,
) -> None:
    runtime_database = tmp_path / "runtime_only.duckdb"
    source_database = tmp_path / "source_only.duckdb"
    store = DuckDBSqlBackend(
        database=runtime_database,
        source_schema="main",
        runtime_schema="retl",
    ).runtime_store()
    _create_source_table(source_database)
    _replace_source_rows(
        source_database,
        [("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a")],
    )

    with pytest.raises(RuntimeStoreError, match="source space must match"):
        produce_event_collect(
            declaration=_declaration(
                DuckDBSqlBackend(
                    database=source_database,
                    source_schema="main",
                    runtime_schema="retl",
                )
            ),
            store=store,
        )

    assert store.read_pending_work(scope=_scope(), max_rows=10).row_count == 0
    declaration_count = store._connection.execute(  # noqa: SLF001
        f"select count(*) from {store.schema}.declarations"
    ).fetchone()
    assert declaration_count == (0,)


def test_event_producer_uses_backend_neutral_source_and_store_protocols(
    tmp_path: Path,
) -> None:
    store: EventProductionStore = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    _replace_source_rows(
        source_database,
        [("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a")],
    )
    declaration = _declaration(_backend(tmp_path))
    adapter = cast(Any, declaration.source.backend).adapter()

    assert isinstance(adapter, EventSourceWindowSource)
    produce_event_collect(declaration=declaration, store=store)

    assert store.read_pending_work(scope=_scope(), max_rows=10).row_count == 0
    page = _source_page(store, declaration, max_rows=10)
    assert _column_values(page, "kind") == ["import"]


def test_event_producer_outputs_do_not_return_old_isolated_runtime_vocabulary(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    _replace_source_rows(
        source_database,
        [("p_1", "cust_1", "one@example.com", "2024-01-01T00:00:00Z", 100, "sku_a")],
    )

    evidence = produce_event_collect(declaration=_declaration(_backend(tmp_path)), store=store)

    text = repr(evidence) + repr(store.read_pending_work(scope=_scope(), max_rows=10))
    for removed in (
        "collected_events",
        "stage_event_declaration",
        "reconcile_event_imports",
        "scan_advanced=True",
    ):
        assert removed not in text
