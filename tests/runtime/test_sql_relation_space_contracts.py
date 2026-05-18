from __future__ import annotations

import json

import duckdb
import pytest

import retl.backends.duckdb as duckdb_backend
from retl.backends.duckdb import (
    DuckDBRuntimeStore,
    DuckDBSourceAdapter,
    DuckDBSqlBackend,
)
from retl.backends.duckdb import (
    duckdb as duckdb_source,
)
from retl.errors import DeclarationValidationError
from retl.stores import SqlCollectPlacement as ExportedSqlCollectPlacement
from retl.stores import SqlRelationSpace as ExportedSqlRelationSpace
from retl.stores.contracts import (
    EventSourceWindowHandle,
    EventSourceWindowRequest,
    SqlCollectPlacement,
    SqlRelationSpace,
    StateSnapshotHandle,
    StateSnapshotRequest,
    sql_collect_placement_from_jsonable,
    sql_collect_placement_to_jsonable,
    sql_relation_space_from_jsonable,
    sql_relation_space_to_jsonable,
)
from retl.stores.sql_runtime.schema import runtime_table_names


def test_sql_relation_space_serializes_as_backend_identity_and_access_contract() -> None:
    space = SqlRelationSpace(
        backend_name="duckdb",
        database="runtime.duckdb",
        schema="source",
        access="read_only",
    )

    encoded = sql_relation_space_to_jsonable(space)
    decoded = sql_relation_space_from_jsonable(json.loads(json.dumps(encoded)))

    assert encoded == {
        "access": "read_only",
        "backend_name": "duckdb",
        "database": "runtime.duckdb",
        "schema": "source",
    }
    assert decoded == space


def test_sql_collect_placement_pairs_source_and_runtime_spaces() -> None:
    placement = SqlCollectPlacement(
        source=SqlRelationSpace(
            backend_name="duckdb",
            database="runtime.duckdb",
            schema="source",
            access="read_only",
        ),
        runtime=SqlRelationSpace(
            backend_name="duckdb",
            database="runtime.duckdb",
            schema="retl_runtime",
            access="read_write",
        ),
    )

    encoded = sql_collect_placement_to_jsonable(placement)
    decoded = sql_collect_placement_from_jsonable(json.loads(json.dumps(encoded)))

    assert encoded == {
        "runtime": {
            "access": "read_write",
            "backend_name": "duckdb",
            "database": "runtime.duckdb",
            "schema": "retl_runtime",
        },
        "source": {
            "access": "read_only",
            "backend_name": "duckdb",
            "database": "runtime.duckdb",
            "schema": "source",
        },
    }
    assert decoded == placement


@pytest.mark.parametrize("field_name", ["backend_name", "database", "schema"])
@pytest.mark.parametrize("blank_value", ["", "   ", "\t\n"])
def test_sql_relation_space_requires_non_blank_identity_fields(
    field_name: str,
    blank_value: str,
) -> None:
    values = {
        "access": "read_only",
        "backend_name": "duckdb",
        "database": "runtime.duckdb",
        "schema": "source",
    }
    values[field_name] = blank_value

    with pytest.raises(ValueError, match=f"{field_name} must be a non-empty string"):
        SqlRelationSpace(**values)  # type: ignore[arg-type]


def test_sql_relation_space_rejects_unknown_access() -> None:
    with pytest.raises(ValueError, match="access is not supported"):
        SqlRelationSpace(
            backend_name="duckdb",
            database="runtime.duckdb",
            schema="source",
            access="write_only",  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("source_access", "runtime_access", "message"),
    [
        ("read_write", "read_write", "source access must be read_only"),
        ("read_only", "read_only", "runtime access must be read_write"),
    ],
)
def test_sql_collect_placement_enforces_source_and_runtime_access_roles(
    source_access: str,
    runtime_access: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        SqlCollectPlacement(
            source=SqlRelationSpace(
                backend_name="duckdb",
                database="runtime.duckdb",
                schema="source",
                access=source_access,  # type: ignore[arg-type]
            ),
            runtime=SqlRelationSpace(
                backend_name="duckdb",
                database="runtime.duckdb",
                schema="retl_runtime",
                access=runtime_access,  # type: ignore[arg-type]
            ),
        )


def test_sql_collect_placement_requires_one_backend_name() -> None:
    with pytest.raises(ValueError, match="backend names must match"):
        SqlCollectPlacement(
            source=SqlRelationSpace(
                backend_name="duckdb",
                database="runtime.duckdb",
                schema="source",
                access="read_only",
            ),
            runtime=SqlRelationSpace(
                backend_name="snowflake",
                database="runtime.duckdb",
                schema="retl_runtime",
                access="read_write",
            ),
        )


def test_sql_relation_space_contracts_are_exported_from_stores_package() -> None:
    assert ExportedSqlRelationSpace is SqlRelationSpace
    assert ExportedSqlCollectPlacement is SqlCollectPlacement


def test_state_snapshot_handle_requires_typed_read_only_source_space() -> None:
    source_space = SqlRelationSpace(
        backend_name="duckdb",
        database="source.duckdb",
        schema="main",
        access="read_only",
    )

    handle = StateSnapshotHandle(
        backend="duckdb",
        source_name="customers",
        source_identity={"backend": "duckdb"},
        query="select * from customers",
        source_space=source_space,
    )

    assert handle.source_space == source_space


def test_state_snapshot_handle_rejects_old_mapping_shaped_source_space() -> None:
    with pytest.raises(ValueError, match="source_space must be a SqlRelationSpace"):
        StateSnapshotHandle(
            backend="duckdb",
            source_name="customers",
            source_identity={"backend": "duckdb"},
            query="select * from customers",
            source_space={  # type: ignore[arg-type]
                "database": "source.duckdb",
                "default_schema": "main",
                "read_only": True,
            },
        )


def test_state_snapshot_handle_rejects_backend_source_space_mismatch() -> None:
    with pytest.raises(ValueError, match="backend must match source_space.backend_name"):
        StateSnapshotHandle(
            backend="duckdb",
            source_name="customers",
            source_identity={"backend": "duckdb"},
            query="select * from customers",
            source_space=SqlRelationSpace(
                backend_name="snowflake",
                database="warehouse",
                schema="source",
                access="read_only",
            ),
        )


def test_event_source_window_handle_rejects_non_read_only_source_space() -> None:
    with pytest.raises(ValueError, match="source_space access must be read_only"):
        EventSourceWindowHandle(
            backend="duckdb",
            source_name="purchases",
            source_identity={"backend": "duckdb"},
            query="select * from purchases",
            cursor_column="occurred_at",
            primary_key_column="purchase_id",
            source_space=SqlRelationSpace(
                backend_name="duckdb",
                database="source.duckdb",
                schema="main",
                access="read_write",
            ),
        )


def test_event_source_window_handle_rejects_old_mapping_shaped_source_space() -> None:
    with pytest.raises(ValueError, match="source_space must be a SqlRelationSpace"):
        EventSourceWindowHandle(
            backend="duckdb",
            source_name="purchases",
            source_identity={"backend": "duckdb"},
            query="select * from purchases",
            cursor_column="occurred_at",
            primary_key_column="purchase_id",
            source_space={  # type: ignore[arg-type]
                "database": "source.duckdb",
                "default_schema": "main",
                "read_only": True,
            },
        )


def test_event_source_window_handle_rejects_backend_source_space_mismatch() -> None:
    with pytest.raises(ValueError, match="backend must match source_space.backend_name"):
        EventSourceWindowHandle(
            backend="duckdb",
            source_name="purchases",
            source_identity={"backend": "duckdb"},
            query="select * from purchases",
            cursor_column="occurred_at",
            primary_key_column="purchase_id",
            source_space=SqlRelationSpace(
                backend_name="snowflake",
                database="warehouse",
                schema="source",
                access="read_only",
            ),
        )


def test_duckdb_source_adapter_populates_typed_source_space() -> None:
    adapter = duckdb_source(database="warehouse.duckdb", default_schema="source").adapter()

    state_handle = adapter.prepare_state_snapshot(
        StateSnapshotRequest(source_name="customers", query="select * from customers")
    )
    event_handle = adapter.prepare_event_source_window(
        EventSourceWindowRequest(
            source_name="purchases",
            query="select * from purchases",
            cursor_column="occurred_at",
            primary_key_column="purchase_id",
        )
    )

    assert state_handle.source_space == SqlRelationSpace(
        backend_name="duckdb",
        database="warehouse.duckdb",
        schema="source",
        access="read_only",
    )
    assert event_handle.source_space == state_handle.source_space


def test_duckdb_sql_backend_constructs_relation_spaces_and_placement(tmp_path) -> None:
    backend = DuckDBSqlBackend(
        database=tmp_path / "warehouse.duckdb",
        source_schema="source",
        runtime_schema="retl_runtime",
    )

    assert backend.name == "duckdb"
    assert backend.source_space == SqlRelationSpace(
        backend_name="duckdb",
        database=str(tmp_path / "warehouse.duckdb"),
        schema="source",
        access="read_only",
    )
    assert backend.runtime_space == SqlRelationSpace(
        backend_name="duckdb",
        database=str(tmp_path / "warehouse.duckdb"),
        schema="retl_runtime",
        access="read_write",
    )
    assert backend.placement == SqlCollectPlacement(
        source=backend.source_space,
        runtime=backend.runtime_space,
    )


def test_duckdb_sql_backend_initializes_only_runtime_schema(tmp_path) -> None:
    database = tmp_path / "warehouse.duckdb"
    source_connection = duckdb.connect(str(database))
    source_connection.execute("create schema source")
    source_connection.close()
    backend = DuckDBSqlBackend(
        database=database,
        source_schema="source",
        runtime_schema="retl_runtime",
    )

    store = backend.runtime_store()
    try:
        assert store.database == str(database)
        assert store.schema == "retl_runtime"
    finally:
        store.close()

    connection = duckdb.connect(str(database))
    try:
        schemas = {
            row[0]
            for row in connection.execute(
                "select schema_name from information_schema.schemata"
            ).fetchall()
        }
        runtime_tables = {
            row[0]
            for row in connection.execute(
                """
                select table_name
                from information_schema.tables
                where table_schema = 'retl_runtime'
                """
            ).fetchall()
        }
        source_tables = connection.execute(
            """
            select table_name
            from information_schema.tables
            where table_schema = 'source'
            """
        ).fetchall()
    finally:
        connection.close()

    assert {"source", "retl_runtime"} <= schemas
    assert runtime_tables == runtime_table_names()
    assert source_tables == []


@pytest.mark.parametrize(
    "kwargs",
    [
        {"database": "", "source_schema": "source", "runtime_schema": "retl_runtime"},
        {"database": "   ", "source_schema": "source", "runtime_schema": "retl_runtime"},
        {"database": "warehouse.duckdb", "source_schema": "", "runtime_schema": "retl_runtime"},
        {
            "database": "warehouse.duckdb",
            "source_schema": "source",
            "runtime_schema": "\t\n",
        },
    ],
)
def test_duckdb_sql_backend_requires_non_blank_database_and_schemas(kwargs) -> None:
    with pytest.raises(DeclarationValidationError, match="must be non-empty"):
        DuckDBSqlBackend(**kwargs)


@pytest.mark.parametrize(
    "invalid_schema", ["source schema", "source.schema", '"source"', "1source"]
)
def test_duckdb_sql_backend_rejects_invalid_source_schema_identifiers(
    invalid_schema: str,
) -> None:
    with pytest.raises(
        DeclarationValidationError,
        match="`source_schema` must be a simple SQL identifier",
    ):
        DuckDBSqlBackend(
            database="warehouse.duckdb",
            source_schema=invalid_schema,
            runtime_schema="retl_runtime",
        )


@pytest.mark.parametrize(
    "invalid_schema", ["runtime schema", "runtime.schema", '"runtime"', "1runtime"]
)
def test_duckdb_sql_backend_rejects_invalid_runtime_schema_identifiers(
    invalid_schema: str,
) -> None:
    with pytest.raises(
        DeclarationValidationError,
        match="`runtime_schema` must be a simple SQL identifier",
    ):
        DuckDBSqlBackend(
            database="warehouse.duckdb",
            source_schema="source",
            runtime_schema=invalid_schema,
        )


def test_duckdb_sql_backend_rejects_equal_source_and_runtime_schemas() -> None:
    with pytest.raises(DeclarationValidationError, match="schemas must be distinct"):
        DuckDBSqlBackend(
            database="warehouse.duckdb",
            source_schema="retl",
            runtime_schema="retl",
        )


def test_duckdb_sql_backend_constructs_read_only_source_adapter_handles(tmp_path) -> None:
    backend = DuckDBSqlBackend(
        database=tmp_path / "warehouse.duckdb",
        source_schema="source",
        runtime_schema="retl_runtime",
    )

    source_backend = backend.source_backend()
    adapter = backend.source_adapter()
    state_handle = adapter.prepare_state_snapshot(
        StateSnapshotRequest(source_name="customers", query="select * from customers")
    )
    event_handle = adapter.prepare_event_source_window(
        EventSourceWindowRequest(
            source_name="purchases",
            query="select * from purchases",
            cursor_column="occurred_at",
            primary_key_column="purchase_id",
        )
    )

    assert source_backend.database == str(tmp_path / "warehouse.duckdb")
    assert source_backend.default_schema == "source"
    assert source_backend.read_only is True
    assert isinstance(adapter, DuckDBSourceAdapter)
    assert state_handle.source_space == backend.source_space
    assert event_handle.source_space == backend.source_space
    assert state_handle.source_space.access == "read_only"


def test_duckdb_sql_backend_constructs_runtime_store_from_runtime_space(tmp_path) -> None:
    backend = DuckDBSqlBackend(
        database=tmp_path / "warehouse.duckdb",
        source_schema="source",
        runtime_schema="retl_runtime",
    )

    store = backend.runtime_store()

    assert isinstance(store, DuckDBRuntimeStore)
    assert store.database == str(tmp_path / "warehouse.duckdb")
    assert store.schema == "retl_runtime"
    assert backend.runtime_space.access == "read_write"


def test_duckdb_sql_backend_is_exported_from_backend_surface() -> None:
    assert duckdb_backend.DuckDBSqlBackend is DuckDBSqlBackend
    assert duckdb_backend.DuckDBRuntimeStore is DuckDBRuntimeStore
    assert duckdb_backend.duckdb is duckdb_source
    assert "DuckDBSqlBackend" in duckdb_backend.__all__
