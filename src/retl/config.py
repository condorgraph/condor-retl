from __future__ import annotations

import os
import re
import tomllib
from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol, TypeVar, cast, overload

from retl.declarations import SecretRef
from retl.errors import DeclarationValidationError

if TYPE_CHECKING:
    from retl.stores.contracts import RuntimeStore

_UNSET = object()
_T = TypeVar("_T")


class ConfigResolutionError(DeclarationValidationError):
    """Raised when a public config value cannot be resolved."""


class ConfigResolver(Protocol):
    def resolve(self, name: str) -> str | None: ...


class SecretResolver(Protocol):
    def resolve(self, ref: SecretRef) -> str: ...


class EnvironmentConfigResolver:
    def resolve(self, name: str) -> str | None:
        return os.environ.get(self.env_name(name))

    def env_name(self, name: str) -> str:
        return _config_env_name(name)


class MappingConfigResolver:
    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = dict(values)

    def resolve(self, name: str) -> str | None:
        return self._values.get(name)


class TomlConfigResolver:
    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = os.fspath(path)
        self._values = _load_toml_file(self._path, value_kind="public config")

    def resolve(self, name: str) -> str | None:
        name = _validate_config_name(name)
        found, value = _lookup_toml_value(
            self._values,
            name,
            path=self._path,
            value_kind="public config",
            error_cls=ConfigResolutionError,
        )
        if not found:
            return None
        return _toml_public_config_value(value, name=name, path=self._path)


class TomlSecretResolver:
    provider_kind = "toml"

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = os.fspath(path)
        self._values = _load_toml_file(self._path, value_kind="secret")

    def resolve(self, ref: SecretRef) -> str:
        from retl.auth import AuthResolutionError, MissingSecretError

        found, value = _lookup_toml_value(
            self._values,
            ref.name,
            path=self._path,
            value_kind="secret",
            error_cls=AuthResolutionError,
        )
        if not found:
            raise MissingSecretError(
                f"Missing secret `{ref.name}` from provider `toml` at TOML path "
                f"`{ref.name}` in file `{self._path}`."
            )
        if not isinstance(value, str):
            raise AuthResolutionError(
                f"TOML secret `{ref.name}` in file `{self._path}` must be a string."
            )
        if not value:
            raise AuthResolutionError(
                f"TOML secret `{ref.name}` in file `{self._path}` must be a non-empty string."
            )
        return value


class ChainedConfigResolver:
    def __init__(self, *resolvers: ConfigResolver) -> None:
        self._resolvers = tuple(resolvers)

    def resolve(self, name: str) -> str | None:
        for resolver in self._resolvers:
            value = resolver.resolve(name)
            if value is not None:
                return value
        return None


class ChainedSecretResolver:
    def __init__(self, *resolvers: SecretResolver) -> None:
        self._resolvers = tuple(resolvers)

    def resolve(self, ref: SecretRef) -> str:
        from retl.auth import AuthResolutionError, MissingSecretError

        failures: list[str] = []
        for resolver in self._resolvers:
            try:
                return resolver.resolve(ref)
            except MissingSecretError as exc:
                failures.append(str(exc))
        detail = " ".join(failures) if failures else "No secret providers are configured."
        raise AuthResolutionError(f"Missing secret `{ref.name}`. {detail}")


class ConfigRegistry:
    def __getitem__(self, name: str) -> str:
        value = configured_config_resolver().resolve(_validate_config_name(name))
        if value is None:
            raise ConfigResolutionError(
                f"Missing public config `{name}`. Configure a resolver value or set "
                f"environment variable `{_config_env_name(name)}` when using "
                "EnvironmentConfigResolver."
            )
        return value

    @overload
    def get(self, name: str) -> str | None: ...

    @overload
    def get(self, name: str, default: _T) -> str | _T: ...

    def get(self, name: str, default: _T | None = None) -> str | _T | None:
        value = configured_config_resolver().resolve(_validate_config_name(name))
        if value is None:
            return default
        return value


_runtime_store: RuntimeStore | None = None
_config_resolver: ConfigResolver = EnvironmentConfigResolver()
_secret_resolver: SecretResolver | None = None


def configure(
    *,
    runtime_store: RuntimeStore | None | object = _UNSET,
    config_resolver: ConfigResolver | None | object = _UNSET,
    secret_resolver: SecretResolver | None | object = _UNSET,
) -> None:
    global _runtime_store, _config_resolver, _secret_resolver
    if runtime_store is _UNSET and config_resolver is _UNSET and secret_resolver is _UNSET:
        _runtime_store = None
        return
    if runtime_store is not _UNSET:
        _runtime_store = cast("RuntimeStore | None", runtime_store)
    if config_resolver is not _UNSET:
        if config_resolver is None:
            _config_resolver = EnvironmentConfigResolver()
        else:
            _config_resolver = cast(ConfigResolver, config_resolver)
    if secret_resolver is _UNSET:
        return
    if secret_resolver is None:
        _secret_resolver = None
        return
    _secret_resolver = cast(SecretResolver, secret_resolver)


def configured_runtime_store() -> RuntimeStore | None:
    return _runtime_store


def configured_config_resolver() -> ConfigResolver:
    return _config_resolver


def configured_secret_resolver() -> SecretResolver:
    from retl.auth import EnvironmentSecretResolver

    environment_resolver = EnvironmentSecretResolver()
    if _secret_resolver is None:
        return environment_resolver
    return ChainedSecretResolver(_secret_resolver, environment_resolver)


def _config_env_name(name: str) -> str:
    segments = (_config_env_segment(segment) for segment in _validate_config_name(name).split("."))
    return "__".join(segments).upper()


def _config_env_segment(segment: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", segment)


def _validate_config_name(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise DeclarationValidationError("Public config name must be a non-empty string.")
    return name


def _load_toml_file(path: str, *, value_kind: str) -> Mapping[str, object]:
    try:
        with open(path, "rb") as file:
            data = tomllib.load(file)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigResolutionError(
            f"Unable to load TOML {value_kind} file `{path}`: {exc}"
        ) from exc
    return data


def _toml_public_config_value(value: object, *, name: str, path: str) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    raise ConfigResolutionError(
        f"TOML public config `{name}` in file `{path}` must be a string, integer, "
        "number, or boolean."
    )


def _lookup_toml_value(
    values: Mapping[str, object],
    name: str,
    *,
    path: str,
    value_kind: str,
    error_cls: type[DeclarationValidationError],
) -> tuple[bool, object]:
    current: object = values
    traversed: list[str] = []
    for segment in name.split("."):
        traversed.append(segment)
        if not isinstance(current, Mapping):
            toml_path = ".".join(traversed[:-1])
            raise error_cls(
                f"TOML {value_kind} path `{name}` in file `{path}` crosses non-table "
                f"value at `{toml_path}`."
            )
        if segment not in current:
            return False, None
        current = current[segment]
    return True, current


__all__ = [
    "ChainedConfigResolver",
    "ChainedSecretResolver",
    "ConfigRegistry",
    "ConfigResolutionError",
    "ConfigResolver",
    "EnvironmentConfigResolver",
    "MappingConfigResolver",
    "SecretResolver",
    "TomlConfigResolver",
    "TomlSecretResolver",
    "configure",
    "configured_config_resolver",
    "configured_runtime_store",
    "configured_secret_resolver",
]
