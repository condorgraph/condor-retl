from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pytest

import retl
from retl.backends.duckdb import DuckDBRuntimeStore, DuckDBSqlBackend
from retl.collect_identity import is_uuidv7
from retl.errors import DeclarationValidationError
from retl.state_runtime.producer import produce_state_collect
from retl.stores.contracts import DestinationProgressScope, StateProductionStore
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


def _declaration(backend: DuckDBSqlBackend) -> retl.State:
    return retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query="""
                select customer_id, email, plan, audience_key
                from customers
            """,
            backend=backend.source_backend(),
        ),
        key={"customer": "customer_id"},
        target="audience_key",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )


def _static_target_declaration(backend: DuckDBSqlBackend) -> retl.State:
    return retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query="""
                select customer_id, email, plan
                from customers
            """,
            backend=backend.source_backend(),
        ),
        key={"customer": "customer_id"},
        target=retl.target("newsletter_customers"),
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )


def _schema_declaration(backend: DuckDBSqlBackend) -> retl.State:
    return retl.state(
        name="customer_state",
        source=retl.source(
            name="customers",
            query="""
                select customer_id, email, plan, audience_key
                from customers
            """,
            backend=backend.source_backend(),
        ),
        key={"customer": "customer_id"},
        target="audience_key",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )


def _scope() -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="customer_sync",
        destination_name="test_destination",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )


def _create_source_table(database: Path) -> None:
    connection = duckdb.connect(str(database))
    connection.execute(
        """
        create table customers (
            customer_id varchar,
            email varchar,
            plan varchar,
            audience_key varchar
        )
        """
    )
    connection.close()


def _create_source_schema_table(database: Path, schema: str) -> None:
    connection = duckdb.connect(str(database))
    connection.execute(f"create schema {schema}")
    connection.execute(
        f"""
        create table {schema}.customers (
            customer_id varchar,
            email varchar,
            plan varchar,
            audience_key varchar
        )
        """
    )
    connection.close()


def _replace_source_rows(
    database: Path,
    rows: list[tuple[str, str, str, str]],
) -> None:
    connection = duckdb.connect(str(database))
    connection.execute("delete from customers")
    connection.executemany("insert into customers values (?, ?, ?, ?)", rows)
    connection.close()


def _replace_source_schema_rows(
    database: Path,
    schema: str,
    rows: list[tuple[str, str, str, str]],
) -> None:
    connection = duckdb.connect(str(database))
    connection.execute(f"delete from {schema}.customers")
    connection.executemany(f"insert into {schema}.customers values (?, ?, ?, ?)", rows)
    connection.close()


def _pending(store: DuckDBRuntimeStore) -> list[tuple[str, str, str, str | None]]:
    page = store.read_pending_work(scope=_scope(), max_rows=100)
    collect_ids = _column_values(page.payload, "collect_id")
    kinds = _column_values(page.payload, "kind")
    keys = _json_column_values(page.payload, "key_json")
    payloads = _json_column_values(page.payload, "payload_json")
    return [
        (
            str(collect_id),
            str(kind),
            str(key["customer"]),
            str(payload["plan"]) if "plan" in payload else None,
        )
        for collect_id, kind, key, payload in zip(
            collect_ids,
            kinds,
            keys,
            payloads,
            strict=True,
        )
    ]


def _column_values(batch: pa.RecordBatch, column: str) -> list[Any]:
    return batch.column(column).to_pylist()


def _json_column_values(batch: pa.RecordBatch, column: str) -> list[Any]:
    return [json.loads(value) if value else None for value in _column_values(batch, column)]


def test_first_state_collect_emits_upserts_under_one_collect_id(tmp_path: Path) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    _replace_source_rows(
        source_database,
        [
            ("cust_1", "one@example.com", "pro", "audience_a"),
            ("cust_2", "two@example.com", "free", "audience_a"),
        ],
    )

    evidence = produce_state_collect(
        declaration=_declaration(_backend(tmp_path)),
        store=store,
    )

    assert is_uuidv7(evidence.collect_id)
    assert evidence.current_row_count == 2
    assert evidence.upsert_count == 2
    assert evidence.remove_count == 0
    assert {row[0] for row in _pending(store)} == {evidence.collect_id}
    assert [row[1:] for row in _pending(store)] == [
        ("upsert", "cust_1", "pro"),
        ("upsert", "cust_2", "free"),
    ]


def test_static_target_state_collect_emits_canonical_target_json(tmp_path: Path) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    _replace_source_rows(
        source_database,
        [
            ("cust_1", "one@example.com", "pro", "ignored_audience"),
            ("cust_2", "two@example.com", "free", "ignored_audience"),
        ],
    )

    produce_state_collect(
        declaration=_static_target_declaration(_backend(tmp_path)),
        store=store,
    )

    page = store.read_pending_work(scope=_scope(), max_rows=100)
    assert _json_column_values(page.payload, "target_json") == [
        {"value": "newsletter_customers"},
        {"value": "newsletter_customers"},
    ]
    identities = [
        json.loads(row[0])
        for row in store._connection.execute(
            """
            select identity_json
            from retl.state_current
            order by key_json
            """
        ).fetchall()
    ]
    assert identities == [
        {
            "key": {"customer": "cust_1"},
            "target": {"value": "newsletter_customers"},
        },
        {
            "key": {"customer": "cust_2"},
            "target": {"value": "newsletter_customers"},
        },
    ]


def test_state_collect_reads_same_database_source_schema(tmp_path: Path) -> None:
    source_database = _source_database(tmp_path)
    _create_source_schema_table(source_database, "source")
    _replace_source_schema_rows(
        source_database,
        "source",
        [("cust_1", "one@example.com", "pro", "audience_a")],
    )
    backend = DuckDBSqlBackend(
        database=source_database,
        source_schema="source",
        runtime_schema="retl",
    )
    store = backend.runtime_store()

    evidence = produce_state_collect(
        declaration=_schema_declaration(backend),
        store=store,
    )

    assert is_uuidv7(evidence.collect_id)
    assert evidence.current_row_count == 1
    assert _pending(store) == [(evidence.collect_id, "upsert", "cust_1", "pro")]


def test_state_collect_does_not_mutate_source_schema_relations(tmp_path: Path) -> None:
    source_database = _source_database(tmp_path)
    _create_source_schema_table(source_database, "source")
    _replace_source_schema_rows(
        source_database,
        "source",
        [("cust_1", "one@example.com", "pro", "audience_a")],
    )
    connection = duckdb.connect(str(source_database))
    try:
        connection.execute("create table source.retl_state_collect_snapshot(marker varchar)")
        connection.execute("insert into source.retl_state_collect_snapshot values ('source')")
        connection.execute("create table source.ordered_work(marker varchar)")
        connection.execute("insert into source.ordered_work values ('source')")
        connection.execute("create table source.state_current(marker varchar)")
        connection.execute("insert into source.state_current values ('source')")
    finally:
        connection.close()
    backend = DuckDBSqlBackend(
        database=source_database,
        source_schema="source",
        runtime_schema="retl",
    )
    store = backend.runtime_store()

    evidence = produce_state_collect(
        declaration=_schema_declaration(backend),
        store=store,
    )

    assert is_uuidv7(evidence.collect_id)
    assert _pending(store) == [(evidence.collect_id, "upsert", "cust_1", "pro")]
    connection = duckdb.connect(str(source_database))
    try:
        source_snapshot_rows = connection.execute(
            "select marker from source.retl_state_collect_snapshot"
        ).fetchall()
        source_ordered_work_rows = connection.execute(
            "select marker from source.ordered_work"
        ).fetchall()
        source_state_current_rows = connection.execute(
            "select marker from source.state_current"
        ).fetchall()
        runtime_ordered_work_count = connection.execute(
            "select count(*) from retl.ordered_work"
        ).fetchone()
        runtime_state_current_count = connection.execute(
            "select count(*) from retl.state_current"
        ).fetchone()
    finally:
        connection.close()

    assert source_snapshot_rows == [("source",)]
    assert source_ordered_work_rows == [("source",)]
    assert source_state_current_rows == [("source",)]
    assert runtime_ordered_work_count == (1,)
    assert runtime_state_current_count == (1,)


def test_unchanged_second_state_collect_emits_no_work_and_updates_current_state(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    _replace_source_rows(source_database, [("cust_1", "one@example.com", "pro", "audience_a")])
    declaration = _declaration(_backend(tmp_path))

    first = produce_state_collect(declaration=declaration, store=store)
    second = produce_state_collect(declaration=declaration, store=store)

    assert is_uuidv7(first.collect_id)
    assert is_uuidv7(second.collect_id)
    assert second.collect_id != first.collect_id
    assert second.work_row_count == 0
    summary = store.state_current_summary(
        declaration_name="customer_state",
        source_name="customers",
    )
    assert summary.collect_id == second.collect_id
    assert summary.row_count == 1
    assert _pending(store) == [(first.collect_id, "upsert", "cust_1", "pro")]


def test_changed_new_and_missing_rows_emit_upsert_and_remove_work(tmp_path: Path) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    declaration = _declaration(_backend(tmp_path))
    _replace_source_rows(
        source_database,
        [
            ("cust_1", "one@example.com", "pro", "audience_a"),
            ("cust_2", "two@example.com", "free", "audience_a"),
        ],
    )
    produce_state_collect(declaration=declaration, store=store)
    _replace_source_rows(
        source_database,
        [
            ("cust_1", "one@example.com", "enterprise", "audience_a"),
            ("cust_3", "three@example.com", "pro", "audience_a"),
        ],
    )

    evidence = produce_state_collect(declaration=declaration, store=store)

    assert is_uuidv7(evidence.collect_id)
    assert evidence.upsert_count == 2
    assert evidence.remove_count == 1
    assert _pending(store) == [
        (_pending(store)[0][0], "upsert", "cust_1", "pro"),
        (_pending(store)[1][0], "upsert", "cust_2", "free"),
        (evidence.collect_id, "remove", "cust_2", "free"),
        (evidence.collect_id, "upsert", "cust_1", "enterprise"),
        (evidence.collect_id, "upsert", "cust_3", "pro"),
    ]
    summary = store.state_current_summary(
        declaration_name="customer_state",
        source_name="customers",
    )
    assert summary.collect_id == evidence.collect_id
    assert summary.row_count == 2


def test_state_collect_id_order_is_deterministic_for_same_logical_snapshot(
    tmp_path: Path,
) -> None:
    def collect_ordered_work(
        workspace: Path,
        *,
        initial_rows: list[tuple[str, str, str, str]],
        changed_rows: list[tuple[str, str, str, str]],
    ) -> list[tuple[int, str, str, str | None]]:
        workspace.mkdir()
        source_database = workspace / "warehouse.duckdb"
        backend = DuckDBSqlBackend(
            database=source_database,
            source_schema="main",
            runtime_schema="retl",
        )
        store = backend.runtime_store()
        _create_source_table(source_database)
        declaration = _declaration(backend)
        _replace_source_rows(source_database, initial_rows)
        produce_state_collect(declaration=declaration, store=store)
        _replace_source_rows(source_database, changed_rows)
        second_collect = produce_state_collect(declaration=declaration, store=store)

        page = store.read_pending_work(scope=_scope(), max_rows=100)
        collect_ids = _column_values(page.payload, "collect_id")
        sequence_orders = _column_values(page.payload, "sequence_order")
        kinds = _column_values(page.payload, "kind")
        keys = _json_column_values(page.payload, "key_json")
        payloads = _json_column_values(page.payload, "payload_json")
        return [
            (
                int(sequence_order),
                str(kind),
                str(key["customer"]),
                str(payload["plan"]) if "plan" in payload else None,
            )
            for collect_id, sequence_order, kind, key, payload in zip(
                collect_ids,
                sequence_orders,
                kinds,
                keys,
                payloads,
                strict=True,
            )
            if str(collect_id) == second_collect.collect_id
        ]

    initial_snapshot = [
        ("cust_2", "two@example.com", "free", "audience_a"),
        ("cust_4", "four@example.com", "trial", "audience_a"),
        ("cust_5", "five@example.com", "pro", "audience_a"),
    ]
    changed_snapshot = [
        ("cust_4", "four@example.com", "enterprise", "audience_a"),
        ("cust_1", "one@example.com", "pro", "audience_a"),
        ("cust_3", "three@example.com", "free", "audience_a"),
    ]

    first_order = collect_ordered_work(
        tmp_path / "first",
        initial_rows=initial_snapshot,
        changed_rows=changed_snapshot,
    )
    second_order = collect_ordered_work(
        tmp_path / "second",
        initial_rows=list(reversed(initial_snapshot)),
        changed_rows=list(reversed(changed_snapshot)),
    )

    assert (
        first_order
        == second_order
        == [
            (0, "remove", "cust_2", "free"),
            (1, "remove", "cust_5", "pro"),
            (2, "upsert", "cust_1", "pro"),
            (3, "upsert", "cust_3", "free"),
            (4, "upsert", "cust_4", "enterprise"),
        ]
    )


def test_targeted_state_collect_orders_diff_work_by_target_then_identity(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    declaration = _declaration(_backend(tmp_path))
    _replace_source_rows(
        source_database,
        [
            ("cust_1", "one@example.com", "free", "audience_b"),
            ("cust_2", "two@example.com", "free", "audience_a"),
            ("cust_3", "three@example.com", "free", "audience_b"),
            ("cust_4", "four@example.com", "free", "audience_a"),
        ],
    )
    produce_state_collect(declaration=declaration, store=store)

    page = store.read_pending_work(scope=_scope(), max_rows=100)
    sequence_orders = _column_values(page.payload, "sequence_order")
    keys = _json_column_values(page.payload, "key_json")
    targets = _json_column_values(page.payload, "target_json")

    assert [
        (int(sequence_order), str(target["value"]), str(key["customer"]))
        for sequence_order, target, key in zip(sequence_orders, targets, keys, strict=True)
    ] == [
        (0, "audience_a", "cust_2"),
        (1, "audience_a", "cust_4"),
        (2, "audience_b", "cust_1"),
        (3, "audience_b", "cust_3"),
    ]


def test_duplicate_state_identity_fails_without_partial_work(tmp_path: Path) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    _replace_source_rows(
        source_database,
        [
            ("cust_1", "one@example.com", "pro", "audience_a"),
            ("cust_1", "other@example.com", "free", "audience_a"),
        ],
    )

    with pytest.raises(DeclarationValidationError, match="duplicate State identity"):
        produce_state_collect(declaration=_declaration(_backend(tmp_path)), store=store)

    assert store.read_pending_work(scope=_scope(), max_rows=100).row_count == 0
    assert (
        store.state_current_summary(
            declaration_name="customer_state",
            source_name="customers",
        ).row_count
        == 0
    )


def test_ordered_work_is_visible_through_backend_neutral_pending_read(
    tmp_path: Path,
) -> None:
    store: StateProductionStore = _store(tmp_path)
    concrete_store = store
    assert isinstance(concrete_store, DuckDBRuntimeStore)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    _replace_source_rows(source_database, [("cust_1", "one@example.com", "pro", "audience_a")])

    produce_state_collect(
        declaration=_declaration(_backend(tmp_path)),
        store=store,
    )

    page = store.read_pending_work(scope=_scope(), max_rows=10)
    assert page.row_count == 1
    assert isinstance(page.payload, pa.RecordBatch)
    assert page.payload.num_rows == page.row_count
    assert _column_values(page.payload, "kind") == ["upsert"]


def test_state_producer_rejects_separate_source_and_runtime_duckdb_databases(
    tmp_path: Path,
) -> None:
    store = DuckDBRuntimeStore(database=tmp_path / "runtime_only.duckdb")
    source_database = tmp_path / "source_only.duckdb"
    _create_source_table(source_database)
    _replace_source_rows(source_database, [("cust_1", "one@example.com", "pro", "audience_a")])

    with pytest.raises(RuntimeStoreError, match="DuckDBSqlBackend-owned runtime store"):
        produce_state_collect(
            declaration=_declaration(
                DuckDBSqlBackend(
                    database=source_database,
                    source_schema="main",
                    runtime_schema="retl",
                )
            ),
            store=store,
        )


def test_state_producer_rejects_backend_owned_store_with_separate_source_file_before_mutation(
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
    _replace_source_rows(source_database, [("cust_1", "one@example.com", "pro", "audience_a")])

    with pytest.raises(RuntimeStoreError, match="source space must match"):
        produce_state_collect(
            declaration=_declaration(
                DuckDBSqlBackend(
                    database=source_database,
                    source_schema="main",
                    runtime_schema="retl",
                )
            ),
            store=store,
        )

    assert store.read_pending_work(scope=_scope(), max_rows=100).row_count == 0
    declaration_count = store._connection.execute(  # noqa: SLF001
        f"select count(*) from {store.schema}.declarations"
    ).fetchone()
    assert declaration_count == (0,)


def test_state_producer_outputs_do_not_return_removed_policy_vocabulary(tmp_path: Path) -> None:
    store = _store(tmp_path)
    source_database = _source_database(tmp_path)
    _create_source_table(source_database)
    _replace_source_rows(source_database, [("cust_1", "one@example.com", "pro", "audience_a")])

    evidence = produce_state_collect(
        declaration=_declaration(_backend(tmp_path)),
        store=store,
    )

    text = repr(evidence) + repr(store.read_pending_work(scope=_scope(), max_rows=10))
    for removed in (
        "state_strategy",
        "send_snapshot",
        "delete_policy",
        "state_basis",
        "state_delta",
    ):
        assert removed not in text
