from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from retl.backends.duckdb.source import DuckDBSourceAdapter, DuckDBSourceBackend
from retl.errors import DeclarationValidationError
from retl.stores.contracts import SqlCollectPlacement, SqlRelationSpace

if TYPE_CHECKING:
    from retl.backends.duckdb.store import DuckDBRuntimeStore

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class DuckDBSqlBackend:
    database: str | Path
    source_schema: str
    runtime_schema: str

    def __post_init__(self) -> None:
        database = str(self.database).strip()
        source_schema = self.source_schema.strip()
        runtime_schema = self.runtime_schema.strip()
        if not database:
            raise DeclarationValidationError("DuckDB SQL backend `database` must be non-empty.")
        if not source_schema:
            raise DeclarationValidationError(
                "DuckDB SQL backend `source_schema` must be non-empty."
            )
        if not runtime_schema:
            raise DeclarationValidationError(
                "DuckDB SQL backend `runtime_schema` must be non-empty."
            )
        _validate_schema_identifier(source_schema, "source_schema")
        _validate_schema_identifier(runtime_schema, "runtime_schema")
        if source_schema == runtime_schema:
            raise DeclarationValidationError(
                "DuckDB SQL backend source and runtime schemas must be distinct."
            )
        object.__setattr__(self, "database", database)
        object.__setattr__(self, "source_schema", source_schema)
        object.__setattr__(self, "runtime_schema", runtime_schema)

    @property
    def name(self) -> str:
        return "duckdb"

    @property
    def source_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="duckdb",
            database=str(self.database),
            schema=self.source_schema,
            access="read_only",
        )

    @property
    def runtime_space(self) -> SqlRelationSpace:
        return SqlRelationSpace(
            backend_name="duckdb",
            database=str(self.database),
            schema=self.runtime_schema,
            access="read_write",
        )

    @property
    def placement(self) -> SqlCollectPlacement:
        return SqlCollectPlacement(source=self.source_space, runtime=self.runtime_space)

    def source_backend(self) -> DuckDBSourceBackend:
        return DuckDBSourceBackend(
            database=str(self.database),
            read_only=True,
            default_schema=self.source_schema,
        )

    def source_adapter(self) -> DuckDBSourceAdapter:
        return self.source_backend().adapter()

    def runtime_store(self) -> DuckDBRuntimeStore:
        from retl.backends.duckdb.store import DuckDBRuntimeStore

        return DuckDBRuntimeStore(backend=self)


def _validate_schema_identifier(value: str, field_name: str) -> None:
    if _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise DeclarationValidationError(
            f"DuckDB SQL backend `{field_name}` must be a simple SQL identifier."
        )


__all__ = ["DuckDBSqlBackend"]
