from __future__ import annotations

import re
from collections.abc import Mapping

from retl.auth import select_auth_mode, validate_required_credentials
from retl.config import configured_config_resolver
from retl.declarations import CredentialValue, DestinationBinding, JSONValue, SecretRef
from retl.destinations.registry import DestinationRegistry
from retl.destinations.resolver import resolve_connector
from retl.destinations.targets import ManagedTargetClient, TargetMapping, TargetRegistry
from retl.errors import DeclarationValidationError

_NAMESPACE_SEGMENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def load(
    destination_ref: str,
    *,
    binding_name: str,
    credentials: Mapping[str, CredentialValue] | None = None,
    credential_namespace: str | None = None,
    config: Mapping[str, JSONValue] | None = None,
    config_namespace: str | None = None,
    auth_mode: str | None = None,
    target_mappings: tuple[TargetMapping, ...] = (),
    target_registry: TargetRegistry | None = None,
    managed_target_client: ManagedTargetClient | None = None,
    registry: DestinationRegistry | None = None,
) -> DestinationBinding:
    connector = resolve_connector(destination_ref, registry=registry)
    selected_auth = select_auth_mode(connector.auth_modes, auth_mode)
    binding_credentials = _credentials_from_namespace(selected_auth, credential_namespace)
    if credentials is not None:
        binding_credentials.update(credentials)
    binding_config = _config_from_namespace(connector, config_namespace)
    validate_required_credentials(selected_auth, binding_credentials)
    if config is not None:
        binding_config = _merge_config(binding_config, dict(config))
    _reject_unsupported_base_url_config(connector, binding_config)
    return DestinationBinding(
        binding_name=binding_name,
        destination_ref=connector.connector_ref,
        config=binding_config,
        auth_mode=selected_auth.name,
        credentials=binding_credentials,
        connector=connector,
        target_mappings=target_mappings,
        target_registry=target_registry,
        managed_target_client=managed_target_client,
    )


def _credentials_from_namespace(
    selected_auth: object,
    namespace: str | None,
) -> dict[str, CredentialValue]:
    if namespace is None:
        return {}
    namespace = _validate_namespace(namespace, field_name="credential_namespace")
    return {
        field_name: SecretRef(f"{namespace}.{field_name}")
        for field_name in getattr(selected_auth, "required_fields", ())
    }


def _config_from_namespace(connector: object, namespace: str | None) -> dict[str, JSONValue]:
    if namespace is None:
        return {}
    namespace = _validate_namespace(namespace, field_name="config_namespace")
    fields = tuple(getattr(connector, "config_namespace_fields", ()))
    if not fields:
        raise DeclarationValidationError(
            f"Destination connector `{getattr(connector, 'connector_ref', '<unknown>')}` does not "
            "declare config fields that may be loaded from a namespace."
        )
    config: dict[str, JSONValue] = {}
    resolver = configured_config_resolver()
    for field_path in fields:
        _validate_field_path(field_path, field_name="config_namespace_fields")
        config_name = f"{namespace}.{field_path}"
        value = resolver.resolve(config_name)
        if value is None:
            continue
        _assign_config_path(config, field_path, value)
    return config


def _merge_config(
    base: Mapping[str, JSONValue],
    override: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    merged = dict(base)
    for key, value in override.items():
        existing = merged.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            merged[key] = _merge_config(existing, value)
            continue
        merged[key] = value
    return merged


def _reject_unsupported_base_url_config(connector: object, config: Mapping[str, JSONValue]) -> None:
    if "base_url" not in config:
        return
    fields = tuple(getattr(connector, "config_namespace_fields", ()))
    if "base_url" in fields:
        return
    raise DeclarationValidationError(
        f"Destination connector `{getattr(connector, 'connector_ref', '<unknown>')}` does not "
        "support `base_url` config. Partner connector API origins are connector-owned; use "
        "an injected transport for tests or a generic/private HTTP connector that explicitly "
        "declares `base_url`."
    )


def _assign_config_path(config: dict[str, JSONValue], field_path: str, value: str) -> None:
    segments = field_path.split(".")
    cursor: dict[str, JSONValue] = config
    for segment in segments[:-1]:
        next_value = cursor.get(segment)
        if not isinstance(next_value, dict):
            next_value = {}
            cursor[segment] = next_value
        cursor = next_value
    cursor[segments[-1]] = value


def _validate_namespace(value: str, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(f"`{field_name}` must be a non-empty dotted namespace.")
    namespace = value.strip()
    for segment in namespace.split("."):
        if not _NAMESPACE_SEGMENT_RE.fullmatch(segment):
            raise DeclarationValidationError(
                f"`{field_name}` must contain only dotted identifier segments."
            )
    return namespace


def _validate_field_path(value: str, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(f"`{field_name}` values must be non-empty field paths.")
    for segment in value.split("."):
        if not _NAMESPACE_SEGMENT_RE.fullmatch(segment):
            raise DeclarationValidationError(
                f"`{field_name}` values must contain only dotted identifier segments."
            )
    return value


__all__ = ["load"]
