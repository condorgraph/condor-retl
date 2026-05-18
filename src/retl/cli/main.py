from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

from retl.cli.setup import install_user_skills
from retl.errors import DeclarationValidationError, RetlError
from retl.operations import OrderedWorkDeleteRange, OrderedWorkRange
from retl.runtime import Runner
from retl.runtime.redaction import redact_text
from retl.stores.contracts import (
    CanonicalKeyScalar,
    CanonicalKeyScalarKind,
    DestinationProgressScope,
    DestinationScanRange,
    EventKeysetScanPosition,
    RuntimeStore,
)

_SNOWFLAKE_DEFAULT_NAMESPACE = "backends.snowflake"
_BIGQUERY_DEFAULT_NAMESPACE = "backends.bigquery"
_DATABRICKS_DEFAULT_NAMESPACE = "backends.databricks"
_POSTGRESQL_DEFAULT_NAMESPACE = "backends.postgresql"
_DEFAULT_DUCKDB_DATABASE = ".retl/state.duckdb"
_DEFAULT_DUCKDB_SCHEMA = "retl"
_SECRET_KEY_PARTS = (
    "authorization",
    "cookie",
    "credential",
    "passphrase",
    "password",
    "private_key",
    "secret",
    "token",
)
_CANONICAL_KEY_SCALAR_KINDS = ("null", "boolean", "integer", "number", "string")


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "handler"):
        parser.print_help(sys.stderr)
        return 2
    try:
        result = args.handler(args)
    except (DeclarationValidationError, RetlError, ValueError, TypeError) as exc:
        print(_json({"error": _redact(str(exc))}, pretty=args.pretty), file=sys.stderr)
        return 2
    print(_json(result, pretty=args.pretty))
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="retl")
    subcommands = parser.add_subparsers(dest="command")
    install_skills = subcommands.add_parser(
        "install-skills",
        help="Install packaged user-facing RETL skills into a project.",
    )
    install_skills.add_argument("path", nargs="?", default=".", type=Path)
    install_skills.add_argument(
        "--destination",
        type=Path,
        default=None,
        help=(
            "Project-relative destination for installed skill directories. "
            "By default, installs to .agents/skills and .claude/skills."
        ),
    )
    install_skills.add_argument(
        "--force",
        action="store_true",
        help="Accepted for compatibility; install-skills overwrites changed skills by default.",
    )
    _add_output_flags(install_skills)
    install_skills.set_defaults(handler=_install_skills)

    operations = subcommands.add_parser(
        "operations",
        help="Inspect and repair RETL runtime-store state.",
    )
    operation_commands = operations.add_subparsers(dest="operation_command")
    _add_operation_commands(operation_commands)
    return parser


def _install_skills(args: argparse.Namespace) -> dict[str, object]:
    summary = install_user_skills(
        project_path=args.path,
        destination=args.destination,
        force=True,
    )
    destination = summary.destinations[0]
    by_destination = {
        str(destination_summary.destination): {
            "created": list(destination_summary.created),
            "overwritten": list(destination_summary.overwritten),
            "unchanged": list(destination_summary.unchanged),
        }
        for destination_summary in summary.by_destination
    }
    return {
        "kind": "install-skills",
        "path": str(args.path),
        "destination": str(destination),
        "destinations": [str(destination) for destination in summary.destinations],
        "created": list(summary.created),
        "overwritten": list(summary.overwritten),
        "unchanged": list(summary.unchanged),
        "by_destination": by_destination,
    }


def _add_operation_commands(commands: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    _command(commands, "inspect-runtime", _inspect_runtime)
    declaration = _command(commands, "inspect-declaration", _inspect_declaration)
    _add_declaration_name(declaration)
    scope = _command(commands, "inspect-destination-scope", _inspect_destination_scope)
    _add_scope_flags(scope, required=True)
    collect = _command(commands, "inspect-collect-id", _inspect_collect_id)
    _add_declaration_name(collect)
    collect.add_argument("--collect-id", required=True, type=_nonempty_string)
    registry = _command(commands, "inspect-target-registry", _inspect_target_registry)
    registry.add_argument("--destination-name")
    run = _command(commands, "inspect-run", _inspect_run)
    run.add_argument("--run-id", required=True)

    dismiss = _command(commands, "dismiss-unresolved", _dismiss_unresolved)
    _add_scope_flags(dismiss, required=True)
    skip = _command(commands, "skip-ordered-work-range", _skip_ordered_work_range)
    _add_scope_flags(skip, required=True)
    _add_range_flags(skip)
    event_skip = _command(commands, "skip-event-keyset-range", _skip_event_keyset_range)
    _add_scope_flags(event_skip, required=True)
    _add_event_keyset_range_flags(event_skip)

    _command(commands, "reset-runtime-store", _reset_runtime_store)
    reset_scope = _command(commands, "reset-destination-scope", _reset_destination_scope)
    _add_scope_flags(reset_scope, required=True)
    cleanup_ordered = _command(commands, "cleanup-ordered-work", _cleanup_ordered_work)
    _add_declaration_name(cleanup_ordered)
    cleanup_ordered.add_argument("--family", choices=("state", "event"), required=True)
    cleanup_ordered.add_argument("--through-collect-id", type=_nonempty_string)
    cleanup_ordered.add_argument("--older-than-seconds", type=_non_negative_int)
    cleanup_ordered.add_argument("--dry-run", action="store_true")
    delete_ordered = _command(commands, "delete-ordered-work", _delete_ordered_work)
    _add_declaration_name(delete_ordered)
    delete_ordered.add_argument("--family", choices=("state", "event"), required=True)
    delete_ordered.add_argument("--force", action="store_true")
    cleanup_cursors = _command(commands, "cleanup-cursors", _cleanup_cursors)
    cleanup_cursors.add_argument("--older-than-seconds", required=True, type=_non_negative_int)
    cleanup_cursors.add_argument("--dry-run", action="store_true")
    cleanup_evidence = _command(commands, "cleanup-evidence", _cleanup_evidence)
    cleanup_evidence.add_argument("--older-than-seconds", required=True, type=_non_negative_int)
    cleanup_evidence.add_argument("--run-id")
    cleanup_evidence.add_argument("--sync-name")
    cleanup_evidence.add_argument("--dry-run", action="store_true")
    delete_collect = _command(commands, "delete-collect-id", _delete_collect_id)
    _add_declaration_name(delete_collect)
    delete_collect.add_argument("--collect-id", required=True, type=_nonempty_string)
    delete_collect.add_argument("--force", action="store_true")
    delete_range = _command(commands, "delete-ordered-work-range", _delete_ordered_work_range)
    _add_declaration_name(delete_range)
    _add_range_flags(delete_range)
    delete_range.add_argument("--family", choices=("state", "event"), default="state")
    delete_range.add_argument("--force", action="store_true")
    rebaseline = _command(commands, "rebaseline-state", _rebaseline_state)
    _add_declaration_name(rebaseline)
    rebaseline.add_argument("--source-name", required=True)
    rebaseline.add_argument("--force", action="store_true")
    reset_registry = _command(commands, "reset-target-registry", _reset_target_registry)
    reset_registry.add_argument("--target")
    _add_scope_flags(reset_registry, required=False)
    delete_run = _command(commands, "delete-run-evidence", _delete_run_evidence)
    delete_run.add_argument("--run-id", required=True)
    delete_report = _command(commands, "delete-report-evidence", _delete_report_evidence)
    delete_report.add_argument("--run-id")
    delete_report.add_argument("--sync-name")


def _command(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
    name: str,
    handler: Any,
) -> argparse.ArgumentParser:
    parser = commands.add_parser(name)
    _add_backend_flags(parser)
    _add_output_flags(parser)
    parser.set_defaults(handler=handler)
    return parser


def _add_backend_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--backend",
        choices=("duckdb", "snowflake", "bigquery", "databricks", "postgresql"),
        default="duckdb",
    )
    parser.add_argument("--database", default=_DEFAULT_DUCKDB_DATABASE)
    parser.add_argument("--schema", default=_DEFAULT_DUCKDB_SCHEMA)
    parser.add_argument("--namespace", default=_SNOWFLAKE_DEFAULT_NAMESPACE)
    parser.add_argument("--bigquery-namespace", default=_BIGQUERY_DEFAULT_NAMESPACE)
    parser.add_argument("--databricks-namespace", default=_DATABRICKS_DEFAULT_NAMESPACE)
    parser.add_argument("--postgresql-namespace", default=_POSTGRESQL_DEFAULT_NAMESPACE)
    parser.add_argument("--auth-mode")
    parser.add_argument("--credential-namespace")


def _add_output_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pretty", action="store_true")


def _add_declaration_name(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--declaration-name", required=True)


def _add_scope_flags(parser: argparse.ArgumentParser, *, required: bool) -> None:
    parser.add_argument("--sync-name", required=required)
    parser.add_argument("--destination-name", required=required)
    parser.add_argument("--surface", required=required)
    parser.add_argument("--family", choices=("state", "event"), required=required)
    parser.add_argument("--declaration-name", required=required)


def _add_range_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--first-collect-id", required=True, type=_nonempty_string)
    parser.add_argument("--first-sequence-order", required=True, type=_non_negative_int)
    parser.add_argument("--last-collect-id", required=True, type=_nonempty_string)
    parser.add_argument("--last-sequence-order", required=True, type=_non_negative_int)


def _add_event_keyset_range_flags(parser: argparse.ArgumentParser) -> None:
    _add_event_keyset_position_flags(parser, "first", required=True)
    _add_event_keyset_position_flags(parser, "last", required=True)
    _add_event_keyset_position_flags(parser, "upper", required=True)
    _add_event_keyset_position_flags(parser, "lower", required=False)


def _add_event_keyset_position_flags(
    parser: argparse.ArgumentParser,
    name: str,
    *,
    required: bool,
) -> None:
    parser.add_argument(
        f"--{name}-cursor-kind",
        choices=_CANONICAL_KEY_SCALAR_KINDS,
        required=required,
    )
    parser.add_argument(f"--{name}-cursor-value", required=required)
    parser.add_argument(
        f"--{name}-primary-key-kind",
        choices=_CANONICAL_KEY_SCALAR_KINDS,
        required=required,
    )
    parser.add_argument(f"--{name}-primary-key-value", required=required)


def _inspect_runtime(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.inspect_runtime_store()


def _inspect_declaration(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.inspect_declaration(args.declaration_name)


def _inspect_destination_scope(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.inspect_destination_scope(_scope(args))


def _inspect_collect_id(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.inspect_collect_id(
        args.declaration_name,
        args.collect_id,
    )


def _inspect_target_registry(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.inspect_target_registry(destination_name=args.destination_name)


def _inspect_run(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.inspect_run(args.run_id)


def _dismiss_unresolved(args: argparse.Namespace) -> tuple[object, ...]:
    return _runner(args).operations.dismiss_unresolved(_scope(args))


def _skip_ordered_work_range(args: argparse.Namespace) -> dict[str, Any]:
    scope = _state_scope(args)
    scan_range = _ordered_range(args)
    return _runner(args).operations.skip_ordered_work_range(scope, scan_range)


def _skip_event_keyset_range(args: argparse.Namespace) -> dict[str, Any]:
    scope = _event_scope(args)
    scan_range = _event_keyset_range(args)
    return _runner(args).operations.skip_event_keyset_range(scope, scan_range)


def _reset_runtime_store(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.reset_runtime_store()


def _reset_destination_scope(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.reset_destination_scope(_scope(args))


def _cleanup_ordered_work(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.cleanup_ordered_work(
        family=args.family,
        declaration_name=args.declaration_name,
        through_collect_id=args.through_collect_id,
        older_than_seconds=args.older_than_seconds,
        dry_run=args.dry_run,
    )


def _delete_ordered_work(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.delete_ordered_work(
        family=args.family,
        declaration_name=args.declaration_name,
        force=args.force,
    )


def _cleanup_cursors(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.cleanup_cursors(
        older_than_seconds=args.older_than_seconds,
        dry_run=args.dry_run,
    )


def _cleanup_evidence(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.cleanup_evidence(
        older_than_seconds=args.older_than_seconds,
        run_id=args.run_id,
        sync_name=args.sync_name,
        dry_run=args.dry_run,
    )


def _delete_collect_id(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.delete_collect_id(
        args.declaration_name,
        args.collect_id,
        force=args.force,
    )


def _delete_ordered_work_range(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.delete_ordered_work_range(
        args.declaration_name,
        OrderedWorkDeleteRange(
            first_collect_id=args.first_collect_id,
            first_sequence_order=args.first_sequence_order,
            last_collect_id=args.last_collect_id,
            last_sequence_order=args.last_sequence_order,
            family=args.family,
        ),
        force=args.force,
    )


def _rebaseline_state(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.rebaseline_state(
        args.declaration_name,
        args.source_name,
        force=args.force,
    )


def _reset_target_registry(args: argparse.Namespace) -> dict[str, Any]:
    scope = _optional_scope(args)
    return _runner(args).operations.reset_target_registry(
        destination_name=args.destination_name,
        sync=scope,
        target=args.target,
    )


def _delete_run_evidence(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.delete_run_evidence(args.run_id)


def _delete_report_evidence(args: argparse.Namespace) -> dict[str, Any]:
    return _runner(args).operations.delete_report_evidence(
        run_id=args.run_id,
        sync_name=args.sync_name,
    )


def _runner(args: argparse.Namespace) -> Runner:
    return Runner(name="cli-operations", runtime_store=cast(RuntimeStore, _runtime_store(args)))


def _runtime_store(args: argparse.Namespace) -> object:
    if args.backend == "duckdb":
        from retl.backends.duckdb import DuckDBRuntimeStore

        return DuckDBRuntimeStore(database=args.database, schema=args.schema)
    if args.backend == "snowflake":
        from retl.backends.snowflake import SnowflakeSqlBackend

        snowflake_backend = SnowflakeSqlBackend.from_config(
            namespace=args.namespace,
            auth_mode=args.auth_mode or "password",
            credential_namespace=args.credential_namespace,
        )
        return snowflake_backend.runtime_store()
    if args.backend == "bigquery":
        from retl.backends.bigquery import BigQuerySqlBackend

        bigquery_backend = BigQuerySqlBackend.from_config(
            namespace=args.bigquery_namespace,
            auth_mode=args.auth_mode or "application_default",
            credential_namespace=args.credential_namespace,
        )
        return bigquery_backend.runtime_store()
    if args.backend == "databricks":
        from retl.backends.databricks import DatabricksSqlBackend

        databricks_backend = DatabricksSqlBackend.from_config(
            namespace=args.databricks_namespace,
            auth_mode=args.auth_mode or "pat",
            credential_namespace=args.credential_namespace,
        )
        return databricks_backend.runtime_store()
    if args.backend == "postgresql":
        from retl.backends.postgresql import PostgreSqlBackend

        postgresql_backend = PostgreSqlBackend.from_config(
            namespace=args.postgresql_namespace,
            auth_mode=args.auth_mode or "password",
            credential_namespace=args.credential_namespace,
        )
        return postgresql_backend.runtime_store()
    raise DeclarationValidationError(f"Unsupported operations backend `{args.backend}`.")


def _scope(args: argparse.Namespace) -> DestinationProgressScope:
    missing = [
        field
        for field in ("sync_name", "destination_name", "surface", "family", "declaration_name")
        if not getattr(args, field, None)
    ]
    if missing:
        joined = ", ".join(f"--{field.replace('_', '-')}" for field in missing)
        raise DeclarationValidationError(f"destination scope requires {joined}.")
    return DestinationProgressScope(
        sync_name=args.sync_name,
        destination_name=args.destination_name,
        surface=args.surface,
        family=args.family,
        declaration_name=args.declaration_name,
    )


def _event_scope(args: argparse.Namespace) -> DestinationProgressScope:
    scope = _scope(args)
    if scope.family != "event":
        raise DeclarationValidationError(
            "skip-event-keyset-range requires --family event; use "
            "skip-ordered-work-range for State ordered-work ranges."
        )
    return scope


def _state_scope(args: argparse.Namespace) -> DestinationProgressScope:
    scope = _scope(args)
    if scope.family != "state":
        raise DeclarationValidationError(
            "skip-ordered-work-range requires --family state; use "
            "skip-event-keyset-range for Event source keyset ranges."
        )
    return scope


def _optional_scope(args: argparse.Namespace) -> DestinationProgressScope | None:
    values = {
        field: getattr(args, field, None)
        for field in ("sync_name", "destination_name", "surface", "family", "declaration_name")
    }
    present = {field for field, value in values.items() if value}
    scope_specific = {"sync_name", "surface", "family", "declaration_name"}
    if present == {"destination_name"}:
        return None
    if not present:
        return None
    if not present.intersection(scope_specific):
        return None
    if present != set(values):
        missing = sorted(set(values) - present)
        joined = ", ".join(f"--{field.replace('_', '-')}" for field in missing)
        raise DeclarationValidationError(f"partial destination scope is invalid; missing {joined}.")
    return _scope(args)


def _ordered_range(args: argparse.Namespace) -> OrderedWorkRange:
    if (args.last_collect_id, args.last_sequence_order) < (
        args.first_collect_id,
        args.first_sequence_order,
    ):
        raise DeclarationValidationError(
            "ordered work range end must be greater than or equal to range start."
        )
    return OrderedWorkRange(
        first_collect_id=args.first_collect_id,
        first_sequence_order=args.first_sequence_order,
        last_collect_id=args.last_collect_id,
        last_sequence_order=args.last_sequence_order,
    )


def _event_keyset_range(args: argparse.Namespace) -> DestinationScanRange:
    _validate_optional_event_keyset_position_complete(args, "lower")
    _validate_event_keyset_range_kinds(args)
    return DestinationScanRange(
        lower_bound_exclusive=_optional_event_keyset_position(args, "lower"),
        first_record_position=_event_keyset_position(args, "first"),
        last_record_position=_event_keyset_position(args, "last"),
        upper_bound_inclusive=_event_keyset_position(args, "upper"),
    )


def _validate_event_keyset_range_kinds(args: argparse.Namespace) -> None:
    cursor_kinds = _supplied_event_keyset_kinds(args, "cursor")
    primary_key_kinds = _supplied_event_keyset_kinds(args, "primary_key")
    if len(set(cursor_kinds.values())) > 1:
        raise DeclarationValidationError(
            "Event keyset cursor scalar kind must be identical across all supplied bounds."
        )
    if len(set(primary_key_kinds.values())) > 1:
        raise DeclarationValidationError(
            "Event keyset primary-key scalar kind must be identical across all supplied bounds."
        )


def _supplied_event_keyset_kinds(args: argparse.Namespace, field: str) -> dict[str, str]:
    return {
        name: cast(str, kind)
        for name in ("lower", "first", "last", "upper")
        if (kind := getattr(args, f"{name}_{field}_kind", None)) is not None
    }


def _validate_optional_event_keyset_position_complete(
    args: argparse.Namespace,
    name: str,
) -> None:
    fields = _event_keyset_position_fields(name)
    present = {field for field in fields if getattr(args, field, None) is not None}
    if present and present != set(fields):
        missing = sorted(set(fields) - present)
        joined = ", ".join(f"--{field.replace('_', '-')}" for field in missing)
        raise DeclarationValidationError(
            f"{name} Event keyset bound is incomplete; missing {joined}."
        )


def _optional_event_keyset_position(
    args: argparse.Namespace,
    name: str,
) -> EventKeysetScanPosition | None:
    fields = _event_keyset_position_fields(name)
    present = {field for field in fields if getattr(args, field, None) is not None}
    if not present:
        return None
    _validate_optional_event_keyset_position_complete(args, name)
    return _event_keyset_position(args, name)


def _event_keyset_position_fields(name: str) -> tuple[str, str, str, str]:
    return (
        f"{name}_cursor_kind",
        f"{name}_cursor_value",
        f"{name}_primary_key_kind",
        f"{name}_primary_key_value",
    )


def _event_keyset_position(args: argparse.Namespace, name: str) -> EventKeysetScanPosition:
    cursor_kind = cast(CanonicalKeyScalarKind, getattr(args, f"{name}_cursor_kind"))
    primary_key_kind = cast(CanonicalKeyScalarKind, getattr(args, f"{name}_primary_key_kind"))
    return EventKeysetScanPosition(
        cursor_value=_canonical_key_scalar(
            kind=cursor_kind,
            raw_value=getattr(args, f"{name}_cursor_value"),
            label=f"{name} cursor",
        ),
        primary_key_value=_canonical_key_scalar(
            kind=primary_key_kind,
            raw_value=getattr(args, f"{name}_primary_key_value"),
            label=f"{name} primary key",
        ),
    )


def _canonical_key_scalar(
    *,
    kind: CanonicalKeyScalarKind,
    raw_value: str,
    label: str,
) -> CanonicalKeyScalar:
    if kind == "null":
        if raw_value != "null":
            raise DeclarationValidationError(
                f"{label} value must be `null` when scalar kind is null."
            )
        return CanonicalKeyScalar.null()
    if kind == "boolean":
        if raw_value == "true":
            return CanonicalKeyScalar.boolean(True)
        if raw_value == "false":
            return CanonicalKeyScalar.boolean(False)
        raise DeclarationValidationError(
            f"{label} boolean value must be exactly `true` or `false`."
        )
    if kind == "integer":
        try:
            return CanonicalKeyScalar.integer(int(raw_value))
        except ValueError as exc:
            raise DeclarationValidationError(
                f"{label} integer value must be a base-10 integer."
            ) from exc
    if kind == "number":
        try:
            return CanonicalKeyScalar.number(float(raw_value))
        except ValueError as exc:
            raise DeclarationValidationError(f"{label} number value must be finite.") from exc
    if kind == "string":
        return CanonicalKeyScalar.string(raw_value)
    raise DeclarationValidationError(f"{label} scalar kind `{kind}` is not supported.")


def _positive_int(value: str) -> int:
    parsed = _int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def _non_negative_int(value: str) -> int:
    parsed = _int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be greater than or equal to 0")
    return parsed


def _int(value: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc


def _nonempty_string(value: str) -> str:
    if not value.strip():
        raise argparse.ArgumentTypeError("must be non-empty")
    return value


def _json(value: object, *, pretty: bool = False) -> str:
    if pretty:
        return json.dumps(_redact(_jsonable(value)), indent=2, sort_keys=True)
    return json.dumps(_redact(_jsonable(value)), sort_keys=True, separators=(",", ":"))


def _jsonable(value: object) -> object:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _jsonable(getattr(value, field.name)) for field in dataclasses.fields(value)
        }
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _redact(value: object, *, key: str | None = None) -> object:
    if key is not None and _is_secret_key(key):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {
            item_key: _redact(item_value, key=item_key) for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, tuple):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


def _is_secret_key(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return any(part in normalized for part in _SECRET_KEY_PARTS)


__all__ = [
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
