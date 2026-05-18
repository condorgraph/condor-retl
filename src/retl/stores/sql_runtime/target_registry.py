from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from sqlglot import exp

from retl.declarations import JSONValue
from retl.destinations.receipts import sanitize_partner_error_detail
from retl.destinations.targets import RemoteTarget, TargetRegistryKey, TargetRegistryRecord
from retl.errors import DeclarationValidationError
from retl.sql import render_sql, row_read, sql_and, sql_eq_param, upsert_assignment
from retl.stores.sql_runtime import json as json_helpers
from retl.stores.sql_runtime import validation as validation_helpers
from retl.stores.sql_runtime.context import SqlRuntimeContext
from retl.stores.sql_runtime.writes import execute_runtime_upsert


def get_target_registry_record(
    context: SqlRuntimeContext,
    key: TargetRegistryKey,
) -> TargetRegistryRecord | None:
    if not isinstance(key, TargetRegistryKey):
        raise DeclarationValidationError("Target registry lookup requires TargetRegistryKey.")
    params = context.new_params()
    query = row_read(
        context.runtime_relation("target_registry"),
        (
            "binding_name",
            "destination_ref",
            "surface",
            "logical_target",
            "remote_id",
            "display_name",
            "metadata_json",
            "source",
        ),
        where=sql_and(
            sql_eq_param("binding_name", key.binding_name, params=params),
            sql_eq_param("destination_ref", key.destination_ref, params=params),
            sql_eq_param("surface", key.surface, params=params),
            sql_eq_param("logical_target", key.logical_target, params=params),
        ),
    )
    compiled = render_sql(query, dialect=context.dialect, params=params)
    row = context.connection.execute(compiled.sql, compiled.params).fetchone()
    if row is None:
        return None
    return _target_registry_record_from_row(row)


def put_target_registry_record(
    context: SqlRuntimeContext,
    record: TargetRegistryRecord,
) -> None:
    if not isinstance(record, TargetRegistryRecord):
        raise DeclarationValidationError(
            "Target registry persistence requires TargetRegistryRecord."
        )
    _validate_target_registry_record(record)
    key = record.key
    execute_runtime_upsert(
        context,
        "target_registry",
        (
            ("binding_name", key.binding_name),
            ("destination_ref", key.destination_ref),
            ("surface", key.surface),
            ("logical_target", key.logical_target),
            ("remote_id", record.remote.remote_id),
            ("display_name", record.remote.display_name),
            ("metadata_json", _target_metadata_json(record.remote.metadata)),
            ("source", record.source),
        ),
        key_columns=("binding_name", "destination_ref", "surface", "logical_target"),
        update_columns=("remote_id", "display_name", "metadata_json", "source"),
        update_assignments=(upsert_assignment("updated_at", exp.Anonymous(this="NOW")),),
    )


def _target_registry_record_from_row(record: object) -> TargetRegistryRecord:
    values = cast(tuple[Any, ...], record)
    metadata = _target_metadata_from_json(cast(str, values[6]))
    source = cast(str, values[7])
    if source not in {"managed_existing", "managed_created"}:
        raise DeclarationValidationError("target registry source is not supported.")
    return TargetRegistryRecord(
        key=TargetRegistryKey(
            binding_name=cast(str, values[0]),
            destination_ref=cast(str, values[1]),
            surface=cast(str, values[2]),
            logical_target=cast(str, values[3]),
        ),
        remote=RemoteTarget(
            remote_id=cast(str, values[4]),
            display_name=cast(str | None, values[5]),
            metadata=metadata,
        ),
        source=cast(Any, source),
    )


def _validate_target_registry_record(record: TargetRegistryRecord) -> None:
    validation_helpers.validate_identity(record.key.binding_name, "binding_name")
    validation_helpers.validate_identity(record.key.destination_ref, "destination_ref")
    validation_helpers.validate_identity(record.key.surface, "surface")
    validation_helpers.validate_identity(record.key.logical_target, "logical_target")
    validation_helpers.validate_identity(record.remote.remote_id, "remote_id")
    if record.remote.display_name is not None:
        _validate_target_registry_string(record.remote.display_name, "display_name")
    if record.source not in {"managed_existing", "managed_created"}:
        raise DeclarationValidationError("target registry source is not supported.")
    _target_metadata_json(record.remote.metadata)


def _validate_target_registry_string(value: str, field_name: str) -> None:
    if sanitize_partner_error_detail(value) != value:
        raise DeclarationValidationError(
            f"target registry `{field_name}` must not contain raw secrets or auth-bearing values."
        )


def _target_metadata_json(value: Mapping[str, object]) -> str:
    metadata = dict(value)
    _validate_target_registry_metadata(metadata, "metadata")
    return json_helpers.to_json(metadata, "target registry metadata")


def _target_metadata_from_json(value: str) -> Mapping[str, JSONValue]:
    decoded = json_helpers.from_json(value)
    if not isinstance(decoded, Mapping):
        raise DeclarationValidationError("target registry metadata must be a mapping.")
    _validate_target_registry_metadata(decoded, "metadata")
    return cast(Mapping[str, JSONValue], decoded)


def _validate_target_registry_metadata(value: object, field_name: str) -> None:
    if isinstance(value, str):
        _validate_target_registry_string(value, field_name)
        return
    if value is None or isinstance(value, bool | int | float):
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise DeclarationValidationError("target registry metadata keys must be strings.")
            _validate_target_registry_string(key, "metadata key")
            _validate_target_registry_metadata(item, f"{field_name}.{key}")
        return
    if isinstance(value, list | tuple):
        for index, item in enumerate(value):
            _validate_target_registry_metadata(item, f"{field_name}[{index}]")
        return
    raise DeclarationValidationError("target registry metadata must be JSON-serializable.")


__all__ = [
    "get_target_registry_record",
    "put_target_registry_record",
]
