from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from typing import Protocol, cast, runtime_checkable

from retl.sql import (
    RelationName,
    RelationPath,
    SqlConnection,
    SqlDialectCapabilities,
    SqlParamAllocator,
    render_relation_path,
)
from retl.stores.contracts import SqlCollectPlacement, SqlRelationSpace


@runtime_checkable
class SqlRuntimeAppendWriter(Protocol):
    def supports(self, relation: str) -> bool: ...

    def append_rows(self, relation: str, rows: Sequence[object]) -> None: ...


@runtime_checkable
class _RuntimeDialect(SqlDialectCapabilities, Protocol):
    def runtime_relation(
        self,
        runtime_space: SqlRelationSpace,
        relation: RelationName | str,
    ) -> RelationPath: ...

    def render_runtime_relation(self, runtime_space: SqlRelationSpace, relation: str) -> str: ...

    def temp_relation(self, name: RelationName | str) -> RelationPath: ...

    def render_temp_relation(self, name: str) -> str: ...

    def create_temp_table_as_sql(self, name: str, query_sql: str) -> str: ...

    def drop_temp_table_sql(self, name: str) -> str: ...

    def begin_transaction(self, connection: SqlConnection) -> None: ...

    def commit(self, connection: SqlConnection) -> None: ...

    def rollback(self, connection: SqlConnection) -> None: ...

    def runtime_reset_uses_transaction(self) -> bool: ...

    def delete_all_rows_sql(self, relation_sql: str) -> str | None: ...


@runtime_checkable
class _CollectDialect(_RuntimeDialect, Protocol):
    def source_relation(
        self,
        source_space: SqlRelationSpace,
        relation: RelationName | str,
    ) -> RelationPath: ...

    def render_source_relation(self, source_space: SqlRelationSpace, relation: str) -> str: ...


@dataclass(frozen=True)
class SqlRuntimeContext:
    """Shared SQL runtime state for a single backend execution context."""

    connection: SqlConnection
    dialect: SqlDialectCapabilities
    runtime_space: SqlRelationSpace
    collect_placement: SqlCollectPlacement | None = None
    append_writer: SqlRuntimeAppendWriter | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.connection, SqlConnection):
            raise ValueError("SQL runtime context connection must implement SqlConnection.")
        if not isinstance(self.dialect, SqlDialectCapabilities):
            raise ValueError("SQL runtime context dialect must implement SqlDialectCapabilities.")
        if not isinstance(self.dialect, _RuntimeDialect):
            raise ValueError(
                "SQL runtime context dialect must expose runtime relation, transaction, "
                "and temp-table capability methods."
            )
        if not isinstance(self.runtime_space, SqlRelationSpace):
            raise ValueError("SQL runtime context runtime_space must be a SqlRelationSpace.")
        if self.runtime_space.backend_name != self.dialect.name:
            raise ValueError(
                "SQL runtime context runtime_space backend must match the dialect name."
            )
        if self.runtime_space.access != "read_write":
            raise ValueError("SQL runtime context runtime_space access must be read_write.")
        self._runtime_dialect().runtime_relation(
            self.runtime_space,
            "__retl_runtime_context_validation",
        )
        if self.collect_placement is not None:
            if not isinstance(self.collect_placement, SqlCollectPlacement):
                raise ValueError(
                    "SQL runtime context collect_placement must be a SqlCollectPlacement."
                )
            if self.collect_placement.runtime != self.runtime_space:
                raise ValueError(
                    "SQL runtime context collect_placement runtime must match runtime_space."
                )
            if not isinstance(self.dialect, _CollectDialect):
                raise ValueError(
                    "SQL runtime context dialect must expose source relation capability "
                    "methods when collect placement is configured."
                )
            self._collect_dialect().source_relation(
                self.collect_placement.source,
                "__retl_source_context_validation",
            )
        if self.append_writer is not None and not isinstance(
            self.append_writer, SqlRuntimeAppendWriter
        ):
            raise ValueError(
                "SQL runtime context append_writer must implement SqlRuntimeAppendWriter."
            )

    @property
    def sqlglot_dialect(self) -> str:
        return self.dialect.sqlglot_dialect

    def new_params(self) -> SqlParamAllocator:
        return SqlParamAllocator(self.dialect.parameter_style)

    def runtime_relation(self, relation: RelationName | str) -> RelationPath:
        return self._runtime_dialect().runtime_relation(self.runtime_space, relation)

    def render_runtime_relation(self, relation: str) -> str:
        return render_relation_path(self.runtime_relation(relation), dialect=self.dialect)

    def source_relation(self, relation: RelationName | str) -> RelationPath:
        return self._collect_dialect().source_relation(self._source_space(), relation)

    def render_source_relation(self, relation: str) -> str:
        return render_relation_path(self.source_relation(relation), dialect=self.dialect)

    def temp_relation(self, name: RelationName | str) -> RelationPath:
        return self._runtime_dialect().temp_relation(name)

    def render_temp_relation(self, name: str) -> str:
        return render_relation_path(self.temp_relation(name), dialect=self.dialect)

    def create_temp_table_as_sql(self, name: str, query_sql: str) -> str:
        return self._runtime_dialect().create_temp_table_as_sql(name, query_sql)

    def drop_temp_table_sql(self, name: str) -> str:
        return self._runtime_dialect().drop_temp_table_sql(name)

    def create_temp_table_as(self, name: str, query_sql: str) -> None:
        self.connection.execute(self.create_temp_table_as_sql(name, query_sql))

    def drop_temp_table(self, name: str) -> None:
        self.connection.execute(self.drop_temp_table_sql(name))

    def cleanup_temp_tables(self, names: Sequence[str]) -> None:
        for name in names:
            self.drop_temp_table(name)

    def begin_transaction(self) -> None:
        self._runtime_dialect().begin_transaction(self.connection)

    def commit(self) -> None:
        self._runtime_dialect().commit(self.connection)

    def rollback(self) -> None:
        self._runtime_dialect().rollback(self.connection)

    def runtime_reset_uses_transaction(self) -> bool:
        return self._runtime_dialect().runtime_reset_uses_transaction()

    def delete_all_runtime_rows_sql(self, relation: str) -> str | None:
        return self._runtime_dialect().delete_all_rows_sql(self.render_runtime_relation(relation))

    @contextmanager
    def transaction(self) -> Iterator[SqlRuntimeContext]:
        self.begin_transaction()
        try:
            yield self
        except BaseException:
            with suppress(Exception):
                self.rollback()
            raise
        else:
            self.commit()

    def _source_space(self) -> SqlRelationSpace:
        if self.collect_placement is None:
            raise ValueError("SQL runtime context source relations require collect placement.")
        return self.collect_placement.source

    def _runtime_dialect(self) -> _RuntimeDialect:
        return cast(_RuntimeDialect, self.dialect)

    def _collect_dialect(self) -> _CollectDialect:
        if not isinstance(self.dialect, _CollectDialect):
            raise ValueError(
                "SQL runtime context dialect does not expose source relation capability methods."
            )
        return cast(_CollectDialect, self.dialect)
