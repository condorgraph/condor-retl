from __future__ import annotations

import re
from dataclasses import dataclass
from types import MappingProxyType

from retl.sql import (
    RelationPath,
    SqlConnection,
    SqlDialectCapabilities,
    list_read,
    render_relation_path,
    render_sql,
    validate_sql_identifier,
)


@dataclass(frozen=True)
class RuntimeTable:
    name: str
    definition_sql: str

    def __post_init__(self) -> None:
        validate_sql_identifier(self.name)


@dataclass(frozen=True)
class _RuntimeIndex:
    name: str
    table: str
    columns_sql: str

    def __post_init__(self) -> None:
        validate_sql_identifier(self.name)
        validate_sql_identifier(self.table)


_RUNTIME_TABLES = (
    RuntimeTable(
        name="runs",
        definition_sql="""
            run_id varchar primary key,
            runner_name varchar not null,
            status varchar not null,
            dry_run boolean not null,
            script_path varchar,
            script_content_hash varchar,
            started_at timestamp not null,
            completed_at timestamp,
            created_at timestamp not null default current_timestamp
        """,
    ),
    RuntimeTable(
        name="declarations",
        definition_sql="""
            declaration_version_id varchar not null,
            declaration_name varchar not null,
            declaration_kind varchar not null,
            source_name varchar not null,
            source_backend varchar,
            source_location_json varchar not null,
            source_query_hash varchar not null,
            declaration_json varchar not null,
            first_seen_at timestamp not null default current_timestamp,
            last_seen_at timestamp not null default current_timestamp,
            active boolean not null default true,
            primary key (declaration_name, declaration_version_id)
        """,
    ),
    RuntimeTable(
        name="target_registry",
        definition_sql="""
            binding_name varchar not null,
            destination_ref varchar not null,
            surface varchar not null,
            logical_target varchar not null,
            remote_id varchar not null,
            display_name varchar,
            metadata_json varchar not null,
            source varchar not null,
            created_at timestamp not null default current_timestamp,
            updated_at timestamp not null default current_timestamp,
            primary key (
                binding_name,
                destination_ref,
                surface,
                logical_target
            )
        """,
    ),
    RuntimeTable(
        name="ordered_work",
        definition_sql="""
            work_id varchar primary key,
            collect_id varchar not null,
            sequence_order bigint not null,
            family varchar not null,
            kind varchar not null,
            declaration_name varchar not null,
            declaration_version_id varchar,
            key_json varchar not null,
            target_json varchar,
            identifiers_json varchar not null,
            payload_json varchar not null,
            created_at timestamp not null default current_timestamp,
            unique (collect_id, sequence_order)
        """,
    ),
    RuntimeTable(
        name="destination_progress",
        definition_sql="""
            sync_name varchar not null,
            destination_name varchar not null,
            surface varchar not null,
            family varchar not null,
            declaration_name varchar not null,
            position_json varchar,
            updated_at timestamp not null default current_timestamp,
            primary key (
                sync_name,
                destination_name,
                surface,
                family,
                declaration_name
            )
        """,
    ),
    RuntimeTable(
        name="pending_work_cursors",
        definition_sql="""
            token varchar primary key,
            sync_name varchar not null,
            destination_name varchar not null,
            surface varchar not null,
            family varchar not null,
            declaration_name varchar not null,
            collect_id varchar not null,
            sequence_order bigint not null,
            created_at timestamp not null default current_timestamp
        """,
    ),
    RuntimeTable(
        name="state_current_cursors",
        definition_sql="""
            token varchar primary key,
            declaration_name varchar not null,
            source_name varchar not null,
            identity_json varchar not null,
            created_at timestamp not null default current_timestamp
        """,
    ),
    RuntimeTable(
        name="state_current",
        definition_sql="""
            declaration_name varchar not null,
            declaration_version_id varchar,
            source_name varchar not null,
            source_identity_json varchar not null,
            identity_json varchar not null,
            key_json varchar not null,
            target_json varchar,
            identifiers_json varchar not null,
            payload_json varchar not null,
            fingerprint varchar not null,
            collect_id varchar not null,
            updated_at timestamp not null default current_timestamp,
            primary key (declaration_name, source_name, identity_json)
        """,
    ),
    RuntimeTable(
        name="sync_reports",
        definition_sql="""
            report_id varchar primary key,
            report_ref varchar,
            run_id varchar not null,
            attempt_id varchar not null,
            runner_name varchar not null,
            sync_name varchar not null,
            declaration_name varchar not null,
            declaration_version_id varchar,
            declaration_kind varchar not null,
            destination_name varchar,
            surface varchar not null,
            status varchar not null,
            dry_run boolean not null,
            submitted_record_count bigint not null,
            succeeded_record_count bigint not null,
            accepted_record_count bigint not null,
            failed_record_count bigint not null,
            retryable_failure_count bigint not null,
            terminal_failure_count bigint not null,
            pre_acceptance_failure_count bigint not null,
            progress_advanced boolean not null,
            failure_category varchar,
            http_status integer,
            last_error_summary varchar,
            last_error_detail varchar,
            report_json varchar not null,
            created_at timestamp not null default current_timestamp
        """,
    ),
    RuntimeTable(
        name="destination_batches",
        definition_sql="""
            batch_id varchar primary key,
            run_id varchar,
            attempt_id varchar,
            sync_name varchar not null,
            destination_name varchar not null,
            surface varchar not null,
            family varchar not null,
            declaration_name varchar not null,
            declaration_version_id varchar not null,
            source_page_index bigint,
            reconcile_page_index bigint,
            first_collect_id varchar not null,
            last_collect_id varchar not null,
            first_sequence_order bigint not null,
            last_sequence_order bigint not null,
            has_source_range boolean not null,
            state_lower_collect_id varchar,
            state_lower_sequence_order bigint,
            state_first_identity_json varchar,
            state_last_identity_json varchar,
            state_upper_identity_json varchar,
            state_lower_identity_json varchar,
            event_lower_cursor_value varchar,
            event_lower_primary_key_value varchar,
            event_first_cursor_value varchar,
            event_first_primary_key_value varchar,
            event_last_cursor_value varchar,
            event_last_primary_key_value varchar,
            event_upper_cursor_value varchar,
            event_upper_primary_key_value varchar,
            event_cursor_kind varchar,
            event_primary_key_kind varchar,
            destination_batch_index bigint not null,
            record_count bigint not null,
            payload_fingerprint varchar not null,
            target_request_fingerprint varchar not null,
            status varchar not null,
            completion_state varchar not null,
            attempt_count bigint not null,
            last_error_summary varchar,
            last_error_detail varchar,
            last_failure_category varchar,
            http_status integer,
            retry_eligible boolean,
            first_submitted_at timestamp,
            last_attempted_at timestamp,
            completed_at timestamp,
            created_at timestamp not null default current_timestamp,
            updated_at timestamp not null default current_timestamp
        """,
    ),
)

RUNTIME_TABLE_CATALOG = MappingProxyType({table.name: table for table in _RUNTIME_TABLES})

_RUNTIME_INDEXES = (
    _RuntimeIndex(
        name="ordered_work_pending_idx",
        table="ordered_work",
        columns_sql="""
            family,
            declaration_name,
            collect_id,
            sequence_order
        """,
    ),
    _RuntimeIndex(
        name="state_current_collect_idx",
        table="state_current",
        columns_sql="""
            declaration_name,
            source_name,
            collect_id
        """,
    ),
    _RuntimeIndex(
        name="target_registry_destination_idx",
        table="target_registry",
        columns_sql="""
            destination_ref,
            surface,
            logical_target
        """,
    ),
    _RuntimeIndex(
        name="sync_reports_run_sync_idx",
        table="sync_reports",
        columns_sql="run_id, sync_name",
    ),
    _RuntimeIndex(
        name="sync_reports_failure_idx",
        table="sync_reports",
        columns_sql="status, failure_category, created_at",
    ),
    _RuntimeIndex(
        name="destination_batches_scope_idx",
        table="destination_batches",
        columns_sql="""
            sync_name,
            destination_name,
            surface,
            family,
            declaration_name,
            status,
            completion_state,
            first_collect_id,
            first_sequence_order,
            destination_batch_index
        """,
    ),
    _RuntimeIndex(
        name="destination_batches_identity_idx",
        table="destination_batches",
        columns_sql="""
            sync_name,
            destination_name,
            surface,
            family,
            declaration_name,
            declaration_version_id,
            source_page_index,
            reconcile_page_index,
            destination_batch_index,
            payload_fingerprint,
            target_request_fingerprint
        """,
    ),
    _RuntimeIndex(
        name="destination_batches_run_idx",
        table="destination_batches",
        columns_sql="run_id, attempt_id",
    ),
)


def runtime_table_names() -> frozenset[str]:
    return frozenset(RUNTIME_TABLE_CATALOG)


def restore_next_attempt_number(
    connection: SqlConnection,
    *,
    schema: str,
    dialect: SqlDialectCapabilities,
) -> int:
    reports_relation = RelationPath(
        "sync_reports",
        schema=validate_sql_identifier(schema),
    )
    query = list_read(reports_relation, "attempt_id")
    compiled = render_sql(query, dialect=dialect)
    attempt_ids = connection.execute(compiled.sql, compiled.params).fetchall()
    max_attempt = 0
    for (attempt_id,) in attempt_ids:
        match = re.search(r":attempt-(\d+)$", str(attempt_id))
        if match is not None:
            max_attempt = max(max_attempt, int(match.group(1)))
    return max_attempt + 1


def _create_table_sql(
    schema: str,
    table: RuntimeTable,
    dialect: SqlDialectCapabilities,
) -> str:
    return (
        f"create table if not exists {_runtime_relation(schema, table.name, dialect)} "
        f"({_normalize_sql_body(table.definition_sql)})"
    )


def _create_index_sql(
    schema: str,
    index: _RuntimeIndex,
    dialect: SqlDialectCapabilities,
) -> str:
    return (
        f"create index if not exists {_quote(index.name, dialect)} "
        f"on {_runtime_relation(schema, index.table, dialect)} "
        f"({_normalize_sql_body(index.columns_sql)})"
    )


def _runtime_relation(
    schema: str,
    relation: str,
    dialect: SqlDialectCapabilities,
) -> str:
    return render_relation_path(RelationPath(relation, schema=schema), dialect=dialect)


def _quote(identifier: str, dialect: SqlDialectCapabilities) -> str:
    return dialect.quote_identifier(validate_sql_identifier(identifier))


def _normalize_sql_body(sql: str) -> str:
    return " ".join(sql.split())
