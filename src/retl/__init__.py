"""Active RETL public API for the State/Event rewrite."""

# ruff: noqa: I001

from __future__ import annotations

from retl.logging import configure_logging, install_null_handler as _install_null_handler

from retl import console
from retl import destinations
from retl.declarations import (
    Checkpoint,
    CredentialValue,
    Declaration,
    DestinationBinding,
    Event,
    FailureHandlingMode,
    FieldMapping,
    Identifier,
    SecretLiteral,
    SecretRef,
    SecretRegistry as _SecretRegistry,
    Source,
    SourceMode,
    State,
    StateOperation,
    StateTarget,
    StaticTarget,
    Sync,
)
from retl.declarations import (
    event as _event_constructor,
)
from retl.declarations import (
    source as _source_constructor,
)
from retl.declarations import (
    state as _state_constructor,
)
from retl.declarations import (
    sync as _sync_constructor,
)
from retl.declarations import (
    target as _target_constructor,
)
from retl.runtime.runner import Runner, runner
import retl.auth as auth
from retl.errors import DeclarationValidationError, RetlError, RetlRuntimeNotImplementedError
from retl.runtime.results import (
    RunResult,
    RunStatus,
)
from retl import sources
from retl.config import (
    ChainedConfigResolver,
    ChainedSecretResolver,
    ConfigRegistry as _ConfigRegistry,
    ConfigResolutionError,
    EnvironmentConfigResolver,
    configured_secret_resolver,
    MappingConfigResolver,
    TomlConfigResolver,
    TomlSecretResolver,
    configure,
)

_install_null_handler()

config = _ConfigRegistry()
secrets = _SecretRegistry()
event = _event_constructor
source = _source_constructor
state = _state_constructor
sync = _sync_constructor
target = _target_constructor

__all__ = [
    "ChainedConfigResolver",
    "ChainedSecretResolver",
    "Checkpoint",
    "ConfigResolutionError",
    "CredentialValue",
    "Declaration",
    "DeclarationValidationError",
    "Runner",
    "DestinationBinding",
    "EnvironmentConfigResolver",
    "Event",
    "FailureHandlingMode",
    "FieldMapping",
    "Identifier",
    "MappingConfigResolver",
    "RetlError",
    "RetlRuntimeNotImplementedError",
    "RunResult",
    "RunStatus",
    "SecretLiteral",
    "SecretRef",
    "Source",
    "SourceMode",
    "State",
    "StateOperation",
    "StateTarget",
    "StaticTarget",
    "Sync",
    "TomlConfigResolver",
    "TomlSecretResolver",
    "auth",
    "config",
    "configure",
    "configure_logging",
    "configured_secret_resolver",
    "console",
    "runner",
    "destinations",
    "event",
    "secrets",
    "source",
    "sources",
    "state",
    "sync",
    "target",
]
