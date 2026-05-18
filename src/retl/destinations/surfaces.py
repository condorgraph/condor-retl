from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal, TypeAlias, cast

from retl.auth import AuthMode, JwtSigner, TokenTransport
from retl.declarations import JSONValue
from retl.errors import DeclarationValidationError

DeclarationFamily: TypeAlias = Literal["state", "event"]
StateOperation: TypeAlias = Literal["upsert", "remove"]
EventOperation: TypeAlias = Literal["import"]
SurfaceOperation: TypeAlias = StateOperation | EventOperation
TargetMode: TypeAlias = Literal["required", "optional", "unsupported"]
SurfaceExecutionMode: TypeAlias = Literal["synchronous", "asynchronous"]
DeliveryOutcome: TypeAlias = Literal["accepted", "succeeded"]
IdentifierRequirementMatch: TypeAlias = Literal["any_of", "all_of"]
DestinationConnectorVisibility: TypeAlias = Literal["public", "internal"]
DestinationSubmissionHook: TypeAlias = Callable[..., object]
DestinationBatchPlanningHook: TypeAlias = Callable[..., object]
DestinationManagedTargetClientHook: TypeAlias = Callable[..., object]


def _non_empty(value: str, *, field_name: str) -> str:
    if not value.strip():
        raise DeclarationValidationError(f"`{field_name}` must be a non-empty string.")
    return value


def _unique_strings(values: Sequence[str], *, field_name: str) -> tuple[str, ...]:
    normalized = tuple(_non_empty(value, field_name=field_name) for value in values)
    if len(set(normalized)) != len(normalized):
        raise DeclarationValidationError(f"`{field_name}` must not contain duplicates.")
    return normalized


@dataclass(frozen=True)
class IdentifierRequirement:
    match: IdentifierRequirementMatch
    identifier_types: Sequence[str]

    def __post_init__(self) -> None:
        if self.match not in ("any_of", "all_of"):
            raise DeclarationValidationError(
                "`IdentifierRequirement.match` must be 'any_of' or 'all_of'."
            )
        identifier_types = _unique_strings(
            self.identifier_types,
            field_name="IdentifierRequirement.identifier_types",
        )
        if not identifier_types:
            raise DeclarationValidationError(
                "`IdentifierRequirement.identifier_types` must not be empty."
            )
        object.__setattr__(self, "identifier_types", identifier_types)


@dataclass(frozen=True)
class DestinationSurface:
    name: str
    declaration_family: DeclarationFamily
    supported_operations: Sequence[str]
    target_mode: TargetMode = "unsupported"
    supports_managed_targets: bool = False
    accepted_identifier_types: Sequence[str] = field(default_factory=tuple)
    identifier_requirements: Sequence[IdentifierRequirement] = field(default_factory=tuple)
    required_payload_fields: Sequence[str] = field(default_factory=tuple)
    required_key_fields: Sequence[str] = field(default_factory=tuple)
    delivery_outcome: DeliveryOutcome = "succeeded"
    execution_mode: SurfaceExecutionMode = "synchronous"
    request_template: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _non_empty(self.name, field_name="surface name")
        if self.declaration_family not in ("state", "event"):
            raise DeclarationValidationError(
                "`declaration_family` must be either 'state' or 'event'."
            )
        if self.target_mode not in ("required", "optional", "unsupported"):
            raise DeclarationValidationError(
                "`target_mode` must be 'required', 'optional', or 'unsupported'."
            )
        if self.execution_mode not in ("synchronous", "asynchronous"):
            raise DeclarationValidationError(
                "`execution_mode` must be either 'synchronous' or 'asynchronous'."
            )
        operations = _unique_strings(self.supported_operations, field_name="supported_operations")
        allowed_operations: set[str]
        if self.declaration_family == "state":
            allowed_operations = {"upsert", "remove"}
        else:
            allowed_operations = {"import"}
            if self.target_mode != "unsupported":
                raise DeclarationValidationError("Event surfaces cannot accept Target.")
            if self.supports_managed_targets:
                raise DeclarationValidationError("Event surfaces cannot support managed targets.")
        unsupported = sorted(set(operations) - allowed_operations)
        if unsupported:
            unsupported_text = ", ".join(unsupported)
            raise DeclarationValidationError(
                f"Surface `{self.name}` declares unsupported operations: {unsupported_text}."
            )
        if not operations:
            raise DeclarationValidationError("`supported_operations` must not be empty.")
        if self.delivery_outcome not in ("accepted", "succeeded"):
            raise DeclarationValidationError(
                "`delivery_outcome` must be either 'accepted' or 'succeeded'."
            )
        object.__setattr__(self, "supported_operations", operations)
        accepted_identifier_types = _unique_strings(
            self.accepted_identifier_types,
            field_name="accepted_identifier_types",
        )
        identifier_requirements = tuple(self.identifier_requirements)
        if any(
            not isinstance(requirement, IdentifierRequirement)
            for requirement in identifier_requirements
        ):
            raise DeclarationValidationError(
                "`identifier_requirements` must contain IdentifierRequirement values."
            )
        unsupported_requirement_identifiers = sorted(
            {
                identifier_type
                for requirement in identifier_requirements
                for identifier_type in requirement.identifier_types
                if identifier_type not in accepted_identifier_types
            }
        )
        if unsupported_requirement_identifiers:
            unsupported_text = ", ".join(unsupported_requirement_identifiers)
            raise DeclarationValidationError(
                f"Surface `{self.name}` has identifier requirement(s) outside "
                f"`accepted_identifier_types`: {unsupported_text}."
            )
        object.__setattr__(
            self,
            "accepted_identifier_types",
            accepted_identifier_types,
        )
        object.__setattr__(self, "identifier_requirements", identifier_requirements)
        object.__setattr__(
            self,
            "required_payload_fields",
            _unique_strings(self.required_payload_fields, field_name="required_payload_fields"),
        )
        object.__setattr__(
            self,
            "required_key_fields",
            _unique_strings(self.required_key_fields, field_name="required_key_fields"),
        )
        object.__setattr__(self, "request_template", MappingProxyType(dict(self.request_template)))

    @property
    def accepts_state(self) -> bool:
        return self.declaration_family == "state"

    @property
    def accepts_event(self) -> bool:
        return self.declaration_family == "event"


@dataclass(frozen=True)
class DestinationConnector:
    """Declarative connector contract exposed by a destination package."""

    name: str
    surfaces: Mapping[str, DestinationSurface] | Sequence[DestinationSurface]
    version: str | None = None
    package: str | None = None
    ref: str = ""
    display_name: str = ""
    visibility: DestinationConnectorVisibility = "public"
    aliases: Sequence[str] = field(default_factory=tuple)
    auth_modes: Sequence[AuthMode] = field(default_factory=tuple)
    config_namespace_fields: Sequence[str] = field(default_factory=tuple)
    auth_token_transport: TokenTransport | None = field(default=None, repr=False, compare=False)
    auth_jwt_signer: JwtSigner | None = field(default=None, repr=False, compare=False)
    batch_planning_hook: DestinationBatchPlanningHook | None = field(
        default=None, repr=False, compare=False
    )
    submission_hook: DestinationSubmissionHook | None = field(
        default=None, repr=False, compare=False
    )
    managed_target_client_hook: DestinationManagedTargetClientHook | None = field(
        default=None, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        _non_empty(self.name, field_name="connector name")
        if self.ref:
            _non_empty(self.ref, field_name="connector ref")
        if self.display_name:
            _non_empty(self.display_name, field_name="connector display_name")
        if self.visibility not in ("public", "internal"):
            raise DeclarationValidationError(
                "Destination Connector visibility must be 'public' or 'internal'."
            )
        aliases = _unique_strings(self.aliases, field_name="connector aliases")
        surfaces = _surface_mapping(self.surfaces)
        if not surfaces:
            raise DeclarationValidationError(
                "Destination Connector must expose at least one surface."
            )
        auth_modes = tuple(self.auth_modes)
        if not auth_modes:
            raise DeclarationValidationError(
                "Destination Connector must declare explicit auth_modes; use retl.auth.none() "
                "for public or mock destinations."
            )
        if any(not isinstance(mode, AuthMode) for mode in auth_modes):
            raise DeclarationValidationError("Destination Connector auth_modes must be AuthMode.")
        if len({mode.name for mode in auth_modes}) != len(auth_modes):
            raise DeclarationValidationError("Destination Connector auth_modes must be unique.")
        if any(mode.kind in {"oauth2_client_credentials", "oauth_jwt"} for mode in auth_modes):
            if not callable(self.auth_token_transport):
                raise DeclarationValidationError(
                    "OAuth Auth Modes require callable connector auth_token_transport."
                )
        if any(mode.kind == "oauth_jwt" for mode in auth_modes) and not callable(
            self.auth_jwt_signer
        ):
            raise DeclarationValidationError(
                "OAuth JWT Auth Modes require callable connector auth_jwt_signer."
            )
        object.__setattr__(self, "surfaces", MappingProxyType(surfaces))
        object.__setattr__(self, "aliases", aliases)
        object.__setattr__(self, "auth_modes", auth_modes)
        object.__setattr__(
            self,
            "config_namespace_fields",
            _unique_field_paths(
                self.config_namespace_fields,
                field_name="connector config_namespace_fields",
            ),
        )

    @property
    def connector_ref(self) -> str:
        return self.ref or self.name

    @property
    def surface_names(self) -> tuple[str, ...]:
        surfaces = cast(Mapping[str, DestinationSurface], self.surfaces)
        return tuple(surfaces)

    def surface(self, name: str) -> DestinationSurface:
        surfaces = cast(Mapping[str, DestinationSurface], self.surfaces)
        try:
            return surfaces[name]
        except KeyError as exc:
            available = ", ".join(sorted(surfaces))
            raise KeyError(
                f"Destination connector `{self.name}` does not expose surface `{name}`. "
                f"Available surfaces: {available}."
            ) from exc


def _surface_mapping(
    surfaces: Mapping[str, DestinationSurface] | Sequence[DestinationSurface],
) -> dict[str, DestinationSurface]:
    if isinstance(surfaces, Mapping):
        mapped = dict(surfaces)
        for name, surface in mapped.items():
            if name != surface.name:
                raise DeclarationValidationError(
                    "Destination Connector surface map keys must match surface names."
                )
        return mapped

    sequenced: dict[str, DestinationSurface] = {}
    for surface in surfaces:
        if surface.name in sequenced:
            raise DeclarationValidationError(f"Duplicate Destination Surface name: {surface.name}.")
        sequenced[surface.name] = surface
    return sequenced


def _unique_field_paths(values: Sequence[str], *, field_name: str) -> tuple[str, ...]:
    field_paths = _unique_strings(values, field_name=field_name)
    for field_path in field_paths:
        for segment in field_path.split("."):
            if not segment.strip():
                raise DeclarationValidationError(
                    f"`{field_name}` values must use non-empty dotted path segments."
                )
    return field_paths


__all__ = [
    "DeclarationFamily",
    "DestinationBatchPlanningHook",
    "DestinationConnector",
    "DestinationManagedTargetClientHook",
    "DestinationSubmissionHook",
    "DestinationSurface",
    "EventOperation",
    "IdentifierRequirement",
    "IdentifierRequirementMatch",
    "StateOperation",
    "SurfaceExecutionMode",
    "SurfaceOperation",
    "TargetMode",
]
