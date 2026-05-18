from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Protocol, cast, runtime_checkable

from retl.declarations.provenance import DeclarationMetadata
from retl.errors import DeclarationValidationError
from retl.runtime.provenance import RunProvenance
from retl.sql import SqlParamAllocator
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.writes import execute_runtime_insert, execute_runtime_update


@runtime_checkable
class _ProvenanceDialect(Protocol):
    def upsert_declaration_sql(
        self,
        declarations_relation_sql: str,
        values: Mapping[str, str],
    ) -> str: ...


def register_run(context: SqlRuntimeContext, run: object) -> None:
    if not isinstance(run, RunProvenance):
        raise DeclarationValidationError("Run registration requires RunProvenance.")
    execute_runtime_insert(
        context,
        "runs",
        (
            ("run_id", run.run_id),
            ("runner_name", run.runner_name),
            ("status", "running"),
            ("dry_run", run.dry_run),
            ("script_path", run.script_path),
            ("script_content_hash", run.script_content_hash),
            ("started_at", datetime.fromisoformat(run.started_at)),
        ),
    )


def complete_run(context: SqlRuntimeContext, *, run_id: str, status: str) -> None:
    execute_runtime_update(
        context,
        "runs",
        (
            ("status", status),
            ("completed_at", datetime.now().astimezone()),
        ),
        where_values=(("run_id", run_id),),
    )


def register_declaration(context: SqlRuntimeContext, metadata: object) -> None:
    if not isinstance(metadata, DeclarationMetadata):
        raise DeclarationValidationError("Declaration registration requires DeclarationMetadata.")
    declarations = context.render_runtime_relation("declarations")
    params = context.new_params()
    values = {
        "declaration_version_id": _add_param(context, params, metadata.declaration_version_id),
        "declaration_name": _add_param(context, params, metadata.declaration_name),
        "declaration_kind": _add_param(context, params, metadata.declaration_kind),
        "source_name": _add_param(context, params, metadata.source_name),
        "source_backend": _add_param(context, params, metadata.source_backend),
        "source_location_json": _add_param(context, params, metadata.source_location_json),
        "source_query_hash": _add_param(context, params, metadata.source_query_hash),
        "declaration_json": _add_param(context, params, metadata.declaration_json),
    }
    context.connection.execute(
        provenance_dialect(context).upsert_declaration_sql(declarations, values),
        params.params,
    )


def _add_param(context: SqlRuntimeContext, params: SqlParamAllocator, value: object) -> str:
    params.add(value)
    return context.dialect.placeholder(len(params.params))


def provenance_dialect(context: SqlRuntimeContext) -> _ProvenanceDialect:
    if not isinstance(context.dialect, _ProvenanceDialect):
        raise DeclarationValidationError(
            "SQL runtime provenance requires declaration upsert dialect capabilities."
        )
    return cast(_ProvenanceDialect, context.dialect)


__all__ = [
    "complete_run",
    "register_declaration",
    "register_run",
]
