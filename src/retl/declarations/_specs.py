from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, TypeAlias, TypeVar

from retl.errors import DeclarationValidationError

if TYPE_CHECKING:
    from retl.destinations.targets import ManagedTargetClient, TargetMapping, TargetRegistry

SourceMode: TypeAlias = Literal["snapshot", "checkpointed"]
StateOperation: TypeAlias = Literal["upsert", "remove"]
CheckpointScalarKind: TypeAlias = Literal["boolean", "integer", "number", "string"]
FailureHandlingMode: TypeAlias = Literal[
    "stop_on_terminal",
    "stop_on_any",
    "continue_on_any",
]
VALID_STATE_OPERATIONS: frozenset[str] = frozenset(("upsert", "remove"))
VALID_FAILURE_HANDLING_MODES: frozenset[str] = frozenset(
    ("stop_on_terminal", "stop_on_any", "continue_on_any")
)


@dataclass(frozen=True)
class SecretRef:
    name: str

    def __post_init__(self) -> None:
        _validate_name(self.name, field_name="secret")


@dataclass(frozen=True, repr=False, eq=False)
class SecretLiteral:
    value: str = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.value, str):
            raise DeclarationValidationError("SecretLiteral value must be a string.")

    def __repr__(self) -> str:
        return "SecretLiteral(<redacted>)"


class SecretRegistry:
    def __getitem__(self, name: str) -> SecretRef:
        return SecretRef(name=name)

    def literal(self, value: str) -> SecretLiteral:
        return SecretLiteral(value=value)


JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | Mapping[str, "JSONValue"] | Sequence["JSONValue"]
CredentialValue: TypeAlias = SecretRef | SecretLiteral
FieldMapping: TypeAlias = Mapping[str, str]
Identifier: TypeAlias = Mapping[str, str]
Checkpoint: TypeAlias = Mapping[str, str]
VALID_CHECKPOINT_SCALAR_KINDS: frozenset[str] = frozenset(
    ("boolean", "integer", "number", "string")
)

_T = TypeVar("_T")
_IDENTIFIER_MAPPING_KEYS = frozenset(("type", "value", "values"))


def _validate_name(value: str, *, field_name: str = "name") -> None:
    if not value.strip():
        raise DeclarationValidationError(f"`{field_name}` must be a non-empty string.")


def _immutable_mapping(value: Mapping[str, _T], *, field_name: str) -> Mapping[str, _T]:
    if not value:
        raise DeclarationValidationError(f"`{field_name}` must not be empty.")
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip():
            raise DeclarationValidationError(f"`{field_name}` keys must be non-empty strings.")
        if isinstance(item, str) and not item.strip():
            raise DeclarationValidationError(f"`{field_name}` values must be non-empty strings.")
    return MappingProxyType(dict(value))


def _immutable_credentials(
    value: Mapping[str, CredentialValue],
    *,
    field_name: str = "credentials",
) -> Mapping[str, CredentialValue]:
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip():
            raise DeclarationValidationError(f"`{field_name}` keys must be non-empty strings.")
        if isinstance(item, SecretRef | SecretLiteral):
            continue
        raise DeclarationValidationError(
            f"`{field_name}` value `{key}` must be a SecretRef or SecretLiteral."
        )
    return MappingProxyType(dict(value))


def _immutable_public_config(
    value: Mapping[str, JSONValue],
    *,
    field_name: str = "config",
) -> Mapping[str, JSONValue]:
    _reject_secret_shaped_public_config(value, field_name=field_name)
    return MappingProxyType(dict(value))


def _reject_secret_shaped_public_config(value: object, *, field_name: str) -> None:
    if isinstance(value, SecretRef | SecretLiteral):
        raise DeclarationValidationError(
            f"`{field_name}` must use public values, not SecretRef or SecretLiteral."
        )
    if isinstance(value, Mapping):
        for item in value.values():
            _reject_secret_shaped_public_config(item, field_name=field_name)
        return
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for item in value:
            _reject_secret_shaped_public_config(item, field_name=field_name)


def _immutable_optional_mapping(
    value: Mapping[str, _T] | None,
    *,
    field_name: str,
) -> Mapping[str, _T] | None:
    if value is None:
        return None
    return _immutable_mapping(value, field_name=field_name)


def _immutable_identifier_mapping(
    value: Mapping[str, str],
    *,
    field_name: str,
) -> Mapping[str, str]:
    if not isinstance(value, Mapping):
        raise DeclarationValidationError(f"`{field_name}` entries must be mappings.")
    for key in value:
        if not isinstance(key, str):
            raise DeclarationValidationError(
                f"`{field_name}` entries only support keys: type, value, values."
            )
    unsupported_keys = sorted(set(value) - _IDENTIFIER_MAPPING_KEYS)
    if unsupported_keys:
        unsupported = ", ".join(unsupported_keys)
        raise DeclarationValidationError(
            f"`{field_name}` entries contain unsupported key(s): {unsupported}."
        )

    identifier_type = value.get("type")
    if not isinstance(identifier_type, str) or not identifier_type.strip():
        raise DeclarationValidationError(f"`{field_name}` entries require non-empty string `type`.")

    has_value = "value" in value
    has_values = "values" in value
    if has_value == has_values:
        raise DeclarationValidationError(
            f"`{field_name}` entries require exactly one of `value` or `values`."
        )

    source_key = "value" if has_value else "values"
    source_column = value[source_key]
    if not isinstance(source_column, str) or not source_column.strip():
        raise DeclarationValidationError(
            f"`{field_name}` entry `{source_key}` must be a non-empty string source column name."
        )
    return MappingProxyType(dict(value))


def _immutable_identifier_sequence(
    value: Sequence[Mapping[str, str]],
    *,
    field_name: str,
) -> FrozenIdentifierSequence:
    return FrozenIdentifierSequence(
        tuple(_immutable_identifier_mapping(entry, field_name=field_name) for entry in value)
    )


class FrozenIdentifierSequence(tuple[Mapping[str, str], ...]):
    def __new__(
        cls,
        values: Sequence[Mapping[str, str]],
    ) -> FrozenIdentifierSequence:
        return super().__new__(cls, values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Sequence) and not isinstance(other, str):
            return tuple(self) == tuple(other)
        return super().__eq__(other)


@dataclass(frozen=True)
class StaticTarget:
    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not self.value.strip():
            raise DeclarationValidationError("Static target value must be a non-empty string.")


StateTarget: TypeAlias = str | StaticTarget


def _immutable_state_target(value: StateTarget | None) -> StateTarget | None:
    if value is None:
        return None
    if isinstance(value, StaticTarget):
        return value
    if isinstance(value, str):
        if not value.strip():
            raise DeclarationValidationError("`target` must be non-empty when provided.")
        return value
    raise DeclarationValidationError("`target` must be a source column string or StaticTarget.")


def _immutable_state_operations(
    value: Sequence[StateOperation] | None,
    *,
    declaration: object,
) -> tuple[StateOperation, ...] | None:
    if isinstance(declaration, Event):
        if value is not None:
            raise DeclarationValidationError("Event Sync does not support `operations`.")
        return None
    if not isinstance(declaration, State):
        return None
    raw_operations = ("upsert", "remove") if value is None else tuple(value)
    if not raw_operations:
        raise DeclarationValidationError("State Sync `operations` must not be empty.")
    operations: list[StateOperation] = []
    for operation in raw_operations:
        if operation not in VALID_STATE_OPERATIONS:
            allowed = ", ".join(sorted(VALID_STATE_OPERATIONS))
            raise DeclarationValidationError(f"State Sync `operations` must be one of: {allowed}.")
        if operation not in operations:
            operations.append(operation)
    return tuple(operations)


@dataclass(frozen=True)
class Source:
    name: str
    query: str
    mode: SourceMode = "snapshot"
    checkpoint: Checkpoint | None = None
    backend: object | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        _validate_name(self.name)
        if self.mode not in ("snapshot", "checkpointed"):
            raise DeclarationValidationError("`mode` must be either 'snapshot' or 'checkpointed'.")
        if not self.query.strip():
            raise DeclarationValidationError("`query` must be a non-empty string.")
        if self.mode == "snapshot" and self.checkpoint is not None:
            raise DeclarationValidationError("Snapshot Source cannot define `checkpoint`.")
        if self.mode == "checkpointed":
            if self.checkpoint is None:
                raise DeclarationValidationError("Checkpointed Source requires `checkpoint`.")
            missing = {"cursor", "primary_key", "cursor_type", "primary_key_type"} - set(
                self.checkpoint
            )
            if missing:
                missing_fields = ", ".join(sorted(missing))
                raise DeclarationValidationError(
                    f"Checkpointed Source `checkpoint` missing: {missing_fields}."
                )
            for field_name in ("cursor_type", "primary_key_type"):
                scalar_kind = self.checkpoint[field_name]
                if scalar_kind not in VALID_CHECKPOINT_SCALAR_KINDS:
                    allowed = ", ".join(sorted(VALID_CHECKPOINT_SCALAR_KINDS))
                    raise DeclarationValidationError(
                        f"Checkpointed Source `checkpoint.{field_name}` must be one of: {allowed}."
                    )
        object.__setattr__(
            self,
            "checkpoint",
            _immutable_optional_mapping(self.checkpoint, field_name="checkpoint"),
        )


@dataclass(frozen=True)
class State:
    name: str
    source: Source
    key: FieldMapping
    identifiers: Sequence[Identifier] = field(default_factory=tuple)
    payload: FieldMapping = field(default_factory=dict)
    target: StateTarget | None = None

    def __post_init__(self) -> None:
        _validate_name(self.name)
        if not isinstance(self.source, Source):
            raise DeclarationValidationError("State declaration `source` must be a Source.")
        if self.source.mode != "snapshot":
            raise DeclarationValidationError("State declaration requires a snapshot Source.")
        object.__setattr__(self, "key", _immutable_mapping(self.key, field_name="key"))
        object.__setattr__(
            self,
            "identifiers",
            _immutable_identifier_sequence(self.identifiers, field_name="identifiers"),
        )
        object.__setattr__(
            self,
            "payload",
            MappingProxyType(dict(self.payload)),
        )
        object.__setattr__(self, "target", _immutable_state_target(self.target))


@dataclass(frozen=True)
class Event:
    name: str
    source: Source
    key: FieldMapping
    occurred_at: str
    identifiers: Sequence[Identifier] = field(default_factory=tuple)
    payload: FieldMapping = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_name(self.name)
        if not isinstance(self.source, Source):
            raise DeclarationValidationError("Event declaration `source` must be a Source.")
        if self.source.mode != "checkpointed":
            raise DeclarationValidationError("Event declaration requires a checkpointed Source.")
        if not self.occurred_at.strip():
            raise DeclarationValidationError("Event declaration requires `occurred_at`.")
        object.__setattr__(self, "key", _immutable_mapping(self.key, field_name="key"))
        object.__setattr__(
            self,
            "identifiers",
            _immutable_identifier_sequence(self.identifiers, field_name="identifiers"),
        )
        object.__setattr__(
            self,
            "payload",
            MappingProxyType(dict(self.payload)),
        )


Declaration: TypeAlias = State | Event


@dataclass(frozen=True)
class DestinationBinding:
    binding_name: str
    destination_ref: str
    config: Mapping[str, JSONValue] = field(default_factory=dict, repr=False)
    auth_mode: str | None = None
    credentials: Mapping[str, CredentialValue] = field(
        default_factory=dict, repr=False, compare=False
    )
    connector: object | None = field(default=None, repr=False, compare=False)
    target_mappings: Sequence[TargetMapping] = field(
        default_factory=tuple, repr=False, compare=False
    )
    target_registry: TargetRegistry | None = field(default=None, repr=False, compare=False)
    managed_target_client: ManagedTargetClient | None = field(
        default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        _validate_name(self.binding_name, field_name="binding_name")
        if not self.destination_ref.strip():
            raise DeclarationValidationError("`destination_ref` must be a non-empty string.")
        if self.auth_mode is not None:
            _validate_name(self.auth_mode, field_name="auth_mode")
        object.__setattr__(self, "config", _immutable_public_config(self.config))
        object.__setattr__(self, "credentials", _immutable_credentials(self.credentials))
        object.__setattr__(self, "target_mappings", tuple(self.target_mappings))

    @property
    def credential_presence(self) -> Mapping[str, bool]:
        return MappingProxyType({field_name: True for field_name in self.credentials})

    @property
    def surfaces(self) -> Mapping[str, object]:
        if self.connector is None:
            return MappingProxyType({})
        surfaces = getattr(self.connector, "surfaces", None)
        if isinstance(surfaces, Mapping):
            return surfaces
        return MappingProxyType({})

    def surface(self, name: str) -> object:
        if self.connector is None:
            raise KeyError(name)
        lookup = getattr(self.connector, "surface", None)
        if callable(lookup):
            return lookup(name)
        return self.surfaces[name]


@dataclass(frozen=True)
class Sync:
    name: str
    declaration: Declaration
    destination: object
    surface: str
    operations: Sequence[StateOperation] | None = None
    on_failure: FailureHandlingMode = "continue_on_any"

    def __post_init__(self) -> None:
        _validate_name(self.name)
        if self.destination is None:
            raise DeclarationValidationError("Sync `destination` is required.")
        if not self.surface.strip():
            raise DeclarationValidationError("Sync `surface` must be a non-empty string.")
        if not isinstance(self.declaration, State | Event):
            raise DeclarationValidationError("Sync `declaration` must be a State or Event.")
        object.__setattr__(
            self,
            "operations",
            _immutable_state_operations(self.operations, declaration=self.declaration),
        )
        if self.on_failure not in VALID_FAILURE_HANDLING_MODES:
            allowed = ", ".join(sorted(VALID_FAILURE_HANDLING_MODES))
            raise DeclarationValidationError(f"`on_failure` must be one of: {allowed}.")


def source(
    *,
    name: str,
    query: str,
    mode: SourceMode = "snapshot",
    checkpoint: Checkpoint | None = None,
    backend: object | None = None,
) -> Source:
    return Source(name=name, query=query, mode=mode, checkpoint=checkpoint, backend=backend)


def state(
    *,
    name: str,
    source: Source,
    key: FieldMapping,
    identifiers: Sequence[Identifier] = (),
    payload: FieldMapping | None = None,
    target: StateTarget | None = None,
) -> State:
    return State(
        name=name,
        source=source,
        key=key,
        identifiers=identifiers,
        payload=payload or {},
        target=target,
    )


def target(value: str) -> StaticTarget:
    return StaticTarget(value=value)


def event(
    *,
    name: str,
    source: Source,
    key: FieldMapping,
    occurred_at: str,
    identifiers: Sequence[Identifier] = (),
    payload: FieldMapping | None = None,
) -> Event:
    return Event(
        name=name,
        source=source,
        key=key,
        occurred_at=occurred_at,
        identifiers=identifiers,
        payload=payload or {},
    )


def sync(
    *,
    name: str,
    declaration: Declaration,
    destination: object,
    surface: str,
    operations: Sequence[StateOperation] | None = None,
    on_failure: FailureHandlingMode = "continue_on_any",
) -> Sync:
    return Sync(
        name=name,
        declaration=declaration,
        destination=destination,
        surface=surface,
        operations=operations,
        on_failure=on_failure,
    )


__all__ = [
    "Checkpoint",
    "CheckpointScalarKind",
    "CredentialValue",
    "Declaration",
    "DestinationBinding",
    "Event",
    "FieldMapping",
    "FailureHandlingMode",
    "Identifier",
    "JSONValue",
    "SecretRef",
    "SecretLiteral",
    "SecretRegistry",
    "Source",
    "SourceMode",
    "State",
    "StateOperation",
    "StateTarget",
    "StaticTarget",
    "Sync",
    "event",
    "source",
    "state",
    "sync",
    "target",
]
