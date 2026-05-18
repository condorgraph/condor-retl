from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from retl.backends.bigquery import BIGQUERY_DIALECT
from retl.backends.duckdb import DUCKDB_DIALECT
from retl.backends.snowflake import SNOWFLAKE_DIALECT
from retl.errors import DeclarationValidationError
from retl.sql import SqlDialectCapabilities
from retl.stores.contracts import (
    CanonicalKeyScalar,
    DestinationBatchIdentity,
    DestinationBatchRecord,
    DestinationProgressScope,
    EventKeysetScanPosition,
    SqlRelationSpace,
    StateOrderedWorkScanPosition,
    destination_batch_id,
)
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.destination_batches import (
    compile_destination_batch_retry_candidates_read,
    compile_destination_batch_work_read,
    compile_destination_batches_by_id_read,
    compile_destination_batches_list_read,
)
from retl.stores.sql_runtime.ordered_work import (
    compile_first_pending_collect_id_read,
    compile_pending_work_read,
)
from retl.stores.sql_runtime.state_current import (
    compile_state_current_summary_read,
    compile_state_current_upserts_read,
)


class _Connection:
    def execute(self, sql: str, parameters: Sequence[object] = ()) -> Any:
        raise AssertionError("compile-only tests must not execute SQL")


def _context(
    dialect: SqlDialectCapabilities = DUCKDB_DIALECT,
    *,
    database: str = "runtime.duckdb",
    schema: str = "runtime",
) -> SqlRuntimeContext:
    return SqlRuntimeContext(
        connection=_Connection(),
        dialect=dialect,
        runtime_space=SqlRelationSpace(
            backend_name=dialect.name,
            database=database,
            schema=schema,
            access="read_write",
        ),
    )


def test_pending_work_read_sql_uses_runtime_relation_and_qmark_param_order() -> None:
    scope = DestinationProgressScope(
        sync_name="sync_a",
        destination_name="dest_a",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )

    compiled = compile_pending_work_read(
        _context(),
        scope=scope,
        lower_bound=StateOrderedWorkScanPosition(
            collect_id="00000000-0007-7000-8000-000000000000", sequence_order=3
        ),
        source_collect_id="00000000-0009-7000-8000-000000000000",
        limit=51,
    )

    assert '"runtime"."ordered_work"' in compiled.sql
    assert "customer_state" not in compiled.sql
    assert compiled.params == (
        "state",
        "customer_state",
        "00000000-0007-7000-8000-000000000000",
        "00000000-0007-7000-8000-000000000000",
        3,
        "00000000-0009-7000-8000-000000000000",
        51,
    )
    assert compiled.sql.count("?") == len(compiled.params)


def test_first_pending_collect_id_sql_uses_min_helper_and_params() -> None:
    scope = DestinationProgressScope(
        sync_name="sync_a",
        destination_name="dest_a",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )

    compiled = compile_first_pending_collect_id_read(
        _context(),
        scope=scope,
        lower_bound=StateOrderedWorkScanPosition(
            collect_id="00000000-0007-7000-8000-000000000000", sequence_order=3
        ),
    )

    assert '"runtime"."ordered_work"' in compiled.sql
    assert "MIN" in compiled.sql.upper()
    assert "customer_state" not in compiled.sql
    assert compiled.params == (
        "state",
        "customer_state",
        "00000000-0007-7000-8000-000000000000",
        "00000000-0007-7000-8000-000000000000",
        3,
    )
    assert compiled.sql.count("?") == len(compiled.params)


def test_ordered_work_read_rejects_event_keyset_lower_bound() -> None:
    scope = DestinationProgressScope(
        sync_name="sync_a",
        destination_name="dest_a",
        surface="events",
        family="event",
        declaration_name="purchase",
    )

    with pytest.raises(DeclarationValidationError, match="Event keyset.*Source SQL"):
        compile_first_pending_collect_id_read(
            _context(
                BIGQUERY_DIALECT,
                database="example-runtime-project",
                schema="retl_runtime",
            ),
            scope=scope,
            lower_bound=EventKeysetScanPosition(
                cursor_value=CanonicalKeyScalar.string("2026-04-15T20:47:18Z"),
                primary_key_value=CanonicalKeyScalar.string("purchase_1"),
            ),
        )


def test_state_current_summary_sql_uses_runtime_relation_and_params() -> None:
    compiled = compile_state_current_summary_read(
        _context(),
        declaration_name="customer_state",
        source_name="customers",
    )

    assert '"runtime"."state_current"' in compiled.sql
    assert "MAX" in compiled.sql.upper()
    assert "COUNT" in compiled.sql.upper()
    assert "customer_state" not in compiled.sql
    assert "customers" not in compiled.sql
    assert compiled.params == ("customer_state", "customers")
    assert compiled.sql.count("?") == len(compiled.params)


def test_state_current_upsert_read_sql_uses_runtime_relation_and_qmark_param_order() -> None:
    compiled = compile_state_current_upserts_read(
        _context(),
        declaration_name="customer_state",
        source_name="customers",
        lower_identity='{"key":{"customer":"cust_1"}}',
        limit=101,
    )

    assert '"runtime"."state_current"' in compiled.sql
    assert '\'state-current:\' || "identity_json" AS "work_id"' in compiled.sql
    assert 'ROW_NUMBER() OVER (ORDER BY "identity_json") - 1 AS "sequence_order"' in compiled.sql
    assert "'state' AS \"family\"" in compiled.sql
    assert "'upsert' AS \"kind\"" in compiled.sql
    for column in (
        "collect_id",
        "declaration_name",
        "declaration_version_id",
        "key_json",
        "target_json",
        "identifiers_json",
        "payload_json",
        "identity_json",
    ):
        assert f'"{column}"' in compiled.sql
    assert "customer_state" not in compiled.sql
    assert "customers" not in compiled.sql
    assert compiled.params == (
        "customer_state",
        "customers",
        '{"key":{"customer":"cust_1"}}',
        101,
    )
    assert compiled.sql.count("?") == len(compiled.params)


def test_destination_batches_by_id_sql_uses_runtime_relation_and_qmark_param_order() -> None:
    compiled = compile_destination_batches_by_id_read(
        _context(),
        batch_ids=("batch_a", "batch_b"),
    )

    assert '"runtime"."destination_batches"' in compiled.sql
    assert "batch_a" not in compiled.sql
    assert compiled.params == ("batch_a", "batch_b")
    assert compiled.sql.count("?") == len(compiled.params)


def test_destination_batches_list_sql_uses_runtime_relation_and_qmark_param_order() -> None:
    scope = DestinationProgressScope(
        sync_name="sync_a",
        destination_name="dest_a",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )

    compiled = compile_destination_batches_list_read(
        _context(),
        scope=scope,
        statuses=("pending", "failed"),
    )

    assert '"runtime"."destination_batches"' in compiled.sql
    assert "customer_state" not in compiled.sql
    assert compiled.params == (
        "sync_a",
        "dest_a",
        "profile",
        "state",
        "customer_state",
        "pending",
        "failed",
    )
    assert compiled.sql.count("?") == len(compiled.params)


def test_destination_batch_retry_candidate_sql_uses_runtime_relation_and_params() -> None:
    scope = DestinationProgressScope(
        sync_name="sync_a",
        destination_name="dest_a",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )

    compiled = compile_destination_batch_retry_candidates_read(
        _context(),
        scope=scope,
        retry_limit=3,
    )

    assert '"runtime"."destination_batches"' in compiled.sql
    assert "customer_state" not in compiled.sql
    assert compiled.params == (
        "sync_a",
        "dest_a",
        "profile",
        "state",
        "customer_state",
        "unresolved",
        "pending",
        "failed",
        True,
        3,
    )
    assert compiled.sql.count("?") == len(compiled.params)


def test_destination_batch_work_sql_uses_runtime_relation_and_qmark_param_order() -> None:
    scope = DestinationProgressScope(
        sync_name="sync_a",
        destination_name="dest_a",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )
    identity = DestinationBatchIdentity(
        scope=scope,
        declaration_version_id="decl_v1",
        source_page_index=0,
        first_collect_id="00000000-0007-7000-8000-000000000000",
        last_collect_id="00000000-0009-7000-8000-000000000000",
        first_sequence_order=4,
        last_sequence_order=12,
        payload_fingerprint="payload_fp",
        target_request_fingerprint="target_fp",
    )

    compiled = compile_destination_batch_work_read(
        _context(),
        batch=DestinationBatchRecord(
            batch_id=destination_batch_id(identity),
            identity=identity,
        ),
    )

    assert '"runtime"."ordered_work"' in compiled.sql
    assert "customer_state" not in compiled.sql
    assert compiled.params == (
        "state",
        "customer_state",
        "00000000-0007-7000-8000-000000000000",
        "00000000-0007-7000-8000-000000000000",
        4,
        "00000000-0009-7000-8000-000000000000",
        "00000000-0009-7000-8000-000000000000",
        12,
    )
    assert compiled.sql.count("?") == len(compiled.params)


def test_snowflake_pending_work_read_sql_uses_params_in_allocation_order() -> None:
    scope = DestinationProgressScope(
        sync_name="sync_a",
        destination_name="dest_a",
        surface="profile",
        family="state",
        declaration_name="customer_state",
    )

    compiled = compile_pending_work_read(
        _context(SNOWFLAKE_DIALECT, database="RETL_DB", schema="RETL_RUNTIME"),
        scope=scope,
        lower_bound=StateOrderedWorkScanPosition(
            collect_id="00000000-0007-7000-8000-000000000000", sequence_order=3
        ),
        source_collect_id="00000000-0009-7000-8000-000000000000",
        limit=51,
    )

    assert '"RETL_DB"."RETL_RUNTIME"."ordered_work"' in compiled.sql
    assert "customer_state" not in compiled.sql
    assert ":1" in compiled.sql
    assert ":7" in compiled.sql
    assert compiled.params == (
        "state",
        "customer_state",
        "00000000-0007-7000-8000-000000000000",
        "00000000-0007-7000-8000-000000000000",
        3,
        "00000000-0009-7000-8000-000000000000",
        51,
    )


def test_snowflake_state_current_upsert_read_sql_keeps_values_bound() -> None:
    compiled = compile_state_current_upserts_read(
        _context(SNOWFLAKE_DIALECT, database="RETL_DB", schema="RETL_RUNTIME"),
        declaration_name="customer_state",
        source_name="customers",
        lower_identity='{"key":{"customer":"cust_1"}}',
        limit=101,
    )

    assert '"RETL_DB"."RETL_RUNTIME"."state_current"' in compiled.sql
    assert "customer_state" not in compiled.sql
    assert "customers" not in compiled.sql
    assert "OBJECT_CONSTRUCT_KEEP_NULL" not in compiled.sql
    assert ":1" in compiled.sql
    assert ":4" in compiled.sql
    assert compiled.params == (
        "customer_state",
        "customers",
        '{"key":{"customer":"cust_1"}}',
        101,
    )
