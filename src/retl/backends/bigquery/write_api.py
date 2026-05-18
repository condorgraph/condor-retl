from __future__ import annotations

import hashlib
import importlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast

from retl.errors import RetlError
from retl.sql import ColumnName
from retl.sql.contracts import validate_sql_identifier
from retl.stores.sql_runtime.schema import RUNTIME_TABLE_CATALOG

RowWriteValues = Mapping[ColumnName | str, object] | Sequence[tuple[ColumnName | str, object]]

_APPEND_TABLES = frozenset(
    {
        "runs",
        "sync_reports",
    }
)


class BigQueryStorageWriteError(RetlError):
    """Raised when BigQuery Storage Write API append fails."""


@dataclass
class BigQueryRuntimeAppendWriter:
    project: str
    dataset: str
    client_kwargs: Mapping[str, object] = field(default_factory=dict)
    bigquery_storage_module: Any | None = field(default=None, repr=False)
    _client: Any | None = field(default=None, init=False, repr=False)
    _message_classes: dict[tuple[str, tuple[str, ...]], type[Any]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )

    def supports(self, relation: str) -> bool:
        return relation in _APPEND_TABLES

    def append_rows(self, relation: str, rows: Sequence[object]) -> None:
        if not rows:
            return
        relation = validate_sql_identifier(relation)
        if not self.supports(relation):
            raise BigQueryStorageWriteError(
                f"BigQuery Storage Write API append is not configured for `{relation}`."
            )
        normalized = tuple(_normalize_row(cast(RowWriteValues, row)) for row in rows)
        columns = tuple(normalized[0])
        for row in normalized[1:]:
            if tuple(row) != columns:
                raise BigQueryStorageWriteError(
                    "BigQuery Storage Write API batch rows require identical columns."
                )
        message_class = self._message_class(relation, columns)
        serialized_rows = [
            message_class(**_proto_values(relation, row)).SerializeToString() for row in normalized
        ]
        module = self._storage_module()
        types = module.types
        from google.protobuf import descriptor_pb2  # type: ignore[import-untyped]

        descriptor = descriptor_pb2.DescriptorProto()
        message_class.DESCRIPTOR.CopyToProto(descriptor)
        request = types.AppendRowsRequest(
            write_stream=self._write_stream(relation),
            proto_rows=types.AppendRowsRequest.ProtoData(
                writer_schema=types.ProtoSchema(proto_descriptor=descriptor),
                rows=types.ProtoRows(serialized_rows=serialized_rows),
            ),
            default_missing_value_interpretation=(
                types.AppendRowsRequest.MissingValueInterpretation.NULL_VALUE
            ),
        )
        responses = self._client_or_open().append_rows(iter((request,)))
        for response in responses:
            self._raise_for_response(relation, response)

    def close(self) -> None:
        if self._client is not None and hasattr(self._client, "close"):
            self._client.close()
        self._client = None

    def _message_class(self, relation: str, columns: tuple[str, ...]) -> type[Any]:
        key = (relation, columns)
        if key not in self._message_classes:
            self._message_classes[key] = _build_message_class(relation, columns)
        return self._message_classes[key]

    def _client_or_open(self) -> Any:
        if self._client is None:
            self._client = self._storage_module().BigQueryWriteClient(**dict(self.client_kwargs))
        return self._client

    def _storage_module(self) -> Any:
        if self.bigquery_storage_module is not None:
            return self.bigquery_storage_module

        return importlib.import_module("google.cloud.bigquery_storage_v1")

    def _write_stream(self, relation: str) -> str:
        return f"projects/{self.project}/datasets/{self.dataset}/tables/{relation}/_default"

    def _raise_for_response(self, relation: str, response: Any) -> None:
        error = getattr(response, "error", None)
        row_errors = tuple(getattr(response, "row_errors", ()) or ())
        if error is not None and getattr(error, "code", False):
            details = _row_error_details(row_errors)
            suffix = f" Row errors: {details}" if details else ""
            raise BigQueryStorageWriteError(
                f"BigQuery Storage Write API append to `{relation}` failed: {error.message}{suffix}"
            )
        if row_errors:
            raise BigQueryStorageWriteError(
                f"BigQuery Storage Write API append to `{relation}` rejected rows: "
                f"{_row_error_details(row_errors)}"
            )


def _normalize_row(row: RowWriteValues) -> dict[str, object]:
    items = row.items() if isinstance(row, Mapping) else row
    normalized: dict[str, object] = {}
    for raw_column, value in items:
        column = raw_column.value if isinstance(raw_column, ColumnName) else str(raw_column)
        normalized[validate_sql_identifier(column)] = value
    return normalized


def _proto_values(relation: str, row: Mapping[str, object]) -> dict[str, object]:
    values: dict[str, object] = {}
    for column, value in row.items():
        if value is None:
            continue
        if _column_type(relation, column) == "timestamp":
            if not isinstance(value, datetime):
                raise BigQueryStorageWriteError(
                    f"BigQuery Storage Write API timestamp column `{relation}.{column}` "
                    "requires a datetime value."
                )
            values[column] = _timestamp_micros(value)
        else:
            values[column] = value
    return values


def _build_message_class(relation: str, columns: tuple[str, ...]) -> type[Any]:
    from google.protobuf import (  # type: ignore[import-untyped]
        descriptor_pb2,
        descriptor_pool,
        message_factory,
    )

    file_proto = descriptor_pb2.FileDescriptorProto()
    digest = hashlib.sha256(f"{relation}:{','.join(columns)}".encode("utf-8")).hexdigest()[:16]
    file_proto.name = f"retl_bigquery_write_{relation}_{digest}.proto"
    file_proto.package = "retl.bigquery_write"
    file_proto.syntax = "proto2"
    message = file_proto.message_type.add()
    message.name = "RetlRuntimeRow"
    for index, column in enumerate(columns, start=1):
        field = message.field.add()
        field.name = column
        field.number = index
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        column_type = _column_type(relation, column)
        if column_type in {"varchar", "string"}:
            field.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
        elif column_type in {"bigint", "integer", "int"}:
            field.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT64
        elif column_type == "boolean":
            field.type = descriptor_pb2.FieldDescriptorProto.TYPE_BOOL
        elif column_type == "timestamp":
            field.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT64
        else:
            raise BigQueryStorageWriteError(
                f"BigQuery Storage Write API does not support column `{relation}.{column}` "
                f"with runtime type `{column_type}`."
            )
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_proto)
    descriptor = pool.FindMessageTypeByName("retl.bigquery_write.RetlRuntimeRow")
    return message_factory.GetMessageClass(descriptor)


def _timestamp_micros(value: datetime) -> int:
    if value.tzinfo is None:
        timestamp = value.replace(tzinfo=None).timestamp()
    else:
        timestamp = value.timestamp()
    return int(timestamp * 1_000_000)


def _row_error_details(row_errors: Sequence[object]) -> str:
    return "; ".join(
        f"row {getattr(row_error, 'index', '?')}: {getattr(row_error, 'message', '')}"
        for row_error in row_errors[:3]
    )


def _column_type(relation: str, column: str) -> str:
    table = RUNTIME_TABLE_CATALOG.get(relation)
    if table is None:
        raise BigQueryStorageWriteError(f"Unknown runtime table `{relation}`.")
    for raw_line in table.definition_sql.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line:
            continue
        parts = line.split()
        if len(parts) >= 2 and parts[0] == column:
            return parts[1].casefold()
    raise BigQueryStorageWriteError(f"Unknown runtime column `{relation}.{column}`.")


__all__ = [
    "BigQueryRuntimeAppendWriter",
    "BigQueryStorageWriteError",
]
