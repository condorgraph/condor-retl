from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any, cast

import pytest

import retl
from retl.destinations import (
    DestinationRegistry,
    DestinationSurface,
    IdentifierRequirement,
    UnknownDestinationConnectorError,
    UnknownDestinationSurfaceError,
    declarative_connector,
    resolve_surface,
)


@pytest.fixture(autouse=True)
def reset_retl_config() -> Iterator[None]:
    retl.configure(config_resolver=None, secret_resolver=None)
    yield
    retl.configure(config_resolver=None, secret_resolver=None)


def test_loads_builtin_mock_connector_and_exposes_named_surfaces() -> None:
    destination = retl.destinations.load(
        "retl/mock",
        binding_name="mock_primary",
    )

    assert isinstance(destination, retl.DestinationBinding)
    assert destination.destination_ref == "retl/mock"
    assert destination.binding_name == "mock_primary"
    assert "profile_properties" in destination.surfaces
    assert "purchase_event" in destination.surfaces
    assert destination.auth_mode == "none"
    assert destination.config == {}
    assert destination.credentials == {}


def test_load_unknown_connector_fails_with_clear_diagnostic() -> None:
    with pytest.raises(UnknownDestinationConnectorError, match="retl/does_not_exist"):
        retl.destinations.load("retl/does_not_exist", binding_name="missing")


def test_unknown_surface_fails_with_available_surface_names() -> None:
    destination = retl.destinations.load("retl/mock", binding_name="mock_primary")

    with pytest.raises(UnknownDestinationSurfaceError, match="profile_properties"):
        resolve_surface(destination, "does_not_exist")


def test_minimal_declarative_connector_authoring_resolves_from_registry() -> None:
    surface = DestinationSurface(
        name="minimal_profile",
        declaration_family="state",
        supported_operations=("upsert",),
        accepted_identifier_types=("email",),
        identifier_requirements=(
            IdentifierRequirement(match="all_of", identifier_types=("email",)),
        ),
        required_payload_fields=("tier",),
    )
    connector = declarative_connector(
        ref="example/minimal",
        display_name="Minimal Example",
        surfaces=(surface,),
        auth_modes=(retl.auth.none(),),
    )
    registry = DestinationRegistry()
    registry.register(connector)

    destination = retl.destinations.load(
        "example/minimal",
        binding_name="minimal_primary",
        config={"workspace": "test"},
        registry=registry,
    )

    resolved = resolve_surface(destination, "minimal_profile")
    assert resolved is surface
    assert resolved.supported_operations == ("upsert",)
    assert resolved.accepted_identifier_types == ("email",)
    assert resolved.identifier_requirements == (
        IdentifierRequirement(match="all_of", identifier_types=("email",)),
    )
    assert resolved.required_payload_fields == ("tier",)
    assert destination.config == {"workspace": "test"}


def test_unknown_connector_diagnostic_lists_only_public_connector_refs() -> None:
    public_surface = DestinationSurface(
        name="profile",
        declaration_family="state",
        supported_operations=("upsert",),
    )
    public_connector = declarative_connector(
        ref="example/public",
        aliases=("example/public-alias",),
        surfaces=(public_surface,),
        auth_modes=(retl.auth.none(),),
    )
    internal_connector = declarative_connector(
        ref="example/internal",
        aliases=("example/internal-alias",),
        visibility="internal",
        surfaces=(public_surface,),
        auth_modes=(retl.auth.none(),),
    )
    registry = DestinationRegistry()
    registry.register(public_connector)
    registry.register(internal_connector)

    assert registry.resolve("example/internal") is internal_connector
    assert registry.resolve("example/internal-alias") is internal_connector
    assert registry.available_connector_refs() == ("example/public", "example/public-alias")
    assert registry.available_connector_refs(include_internal=True) == (
        "example/internal",
        "example/internal-alias",
        "example/public",
        "example/public-alias",
    )

    with pytest.raises(UnknownDestinationConnectorError) as error:
        registry.resolve("example/missing")

    message = str(error.value)
    assert "Available connectors: example/public, example/public-alias." in message
    assert "example/internal" not in message
    assert "example/internal-alias" not in message


def test_load_rejects_credentials_outside_selected_auth_mode() -> None:
    surface = DestinationSurface(
        name="public_profile",
        declaration_family="state",
        supported_operations=("upsert",),
    )
    connector = declarative_connector(
        ref="example/public",
        display_name="Public Example",
        surfaces=(surface,),
        auth_modes=(retl.auth.none(),),
    )
    registry = DestinationRegistry()
    registry.register(connector)

    with pytest.raises(retl.DeclarationValidationError, match="undeclared credential"):
        retl.destinations.load(
            "example/public",
            binding_name="public_primary",
            credentials={"api_key": cast(Any, "secret")},
            registry=registry,
        )


def test_load_rejects_bare_string_credentials_for_selected_auth_mode() -> None:
    surface = DestinationSurface(
        name="profile",
        declaration_family="state",
        supported_operations=("upsert",),
    )
    connector = declarative_connector(
        ref="example/private",
        display_name="Private Example",
        surfaces=(surface,),
        auth_modes=(retl.auth.bearer_token(field="api_key"),),
    )
    registry = DestinationRegistry()
    registry.register(connector)

    with pytest.raises(retl.DeclarationValidationError, match="SecretRef or SecretLiteral"):
        retl.destinations.load(
            "example/private",
            binding_name="private_primary",
            credentials={"api_key": cast(Any, "secret")},
            registry=registry,
        )


def test_direct_destination_binding_rejects_bare_string_credentials() -> None:
    with pytest.raises(retl.DeclarationValidationError, match="SecretRef or SecretLiteral"):
        retl.DestinationBinding(
            binding_name="private_primary",
            destination_ref="example/private",
            credentials={"api_key": cast(Any, "secret")},
        )


def test_direct_destination_binding_allows_secret_shaped_credentials_and_public_config() -> None:
    binding = retl.DestinationBinding(
        binding_name="private_primary",
        destination_ref="example/private",
        config={"workspace": "public-string"},
        credentials={
            "api_key": retl.secrets["destinations.private.api_key"],
            "fallback": retl.secrets.literal("process-secret"),
        },
    )

    assert binding.config == {"workspace": "public-string"}
    assert binding.credential_presence == {"api_key": True, "fallback": True}
    assert "process-secret" not in repr(binding)


def test_direct_destination_binding_rejects_secret_shaped_public_config() -> None:
    with pytest.raises(retl.DeclarationValidationError, match="public values"):
        retl.DestinationBinding(
            binding_name="private_primary",
            destination_ref="example/private",
            config={
                "workspace": "public-string",
                "nested": {"token": cast(Any, retl.secrets["destinations.private.token"])},
            },
        )

    with pytest.raises(retl.DeclarationValidationError, match="public values"):
        retl.DestinationBinding(
            binding_name="private_primary",
            destination_ref="example/private",
            config={"tokens": [cast(Any, retl.secrets.literal("process-secret"))]},
        )


def test_load_rejects_secret_shaped_public_config() -> None:
    surface = DestinationSurface(
        name="profile",
        declaration_family="state",
        supported_operations=("upsert",),
    )
    connector = declarative_connector(
        ref="example/private-config",
        display_name="Private Config Example",
        surfaces=(surface,),
        auth_modes=(retl.auth.none(),),
    )
    registry = DestinationRegistry()
    registry.register(connector)

    with pytest.raises(retl.DeclarationValidationError, match="public values"):
        retl.destinations.load(
            "example/private-config",
            binding_name="private_config_primary",
            config={"nested": {"token": cast(Any, retl.secrets["destinations.private.token"])}},
            registry=registry,
        )


def test_load_expands_credential_namespace_from_selected_auth_mode() -> None:
    surface = DestinationSurface(
        name="profile",
        declaration_family="state",
        supported_operations=("upsert",),
    )
    connector = declarative_connector(
        ref="example/oauth",
        display_name="OAuth Example",
        surfaces=(surface,),
        auth_modes=(
            retl.auth.custom(
                name="service_account",
                required_fields=("project_id", "client_email", "private_key"),
                hook=lambda values, context: retl.auth.ResolvedAuth(mode=str(context["mode"])),
            ),
        ),
    )
    registry = DestinationRegistry()
    registry.register(connector)

    destination = retl.destinations.load(
        "example/oauth",
        binding_name="oauth_primary",
        credential_namespace="destinations.example.service_account",
        registry=registry,
    )

    assert destination.credentials == {
        "project_id": retl.secrets["destinations.example.service_account.project_id"],
        "client_email": retl.secrets["destinations.example.service_account.client_email"],
        "private_key": retl.secrets["destinations.example.service_account.private_key"],
    }
    assert "project-secret" not in repr(destination)


def test_load_explicit_credentials_override_namespace_fields() -> None:
    surface = DestinationSurface(
        name="profile",
        declaration_family="state",
        supported_operations=("upsert",),
    )
    connector = declarative_connector(
        ref="example/bearer",
        surfaces=(surface,),
        auth_modes=(retl.auth.bearer_token(field="access_token"),),
    )
    registry = DestinationRegistry()
    registry.register(connector)

    destination = retl.destinations.load(
        "example/bearer",
        binding_name="bearer_primary",
        credential_namespace="destinations.example",
        credentials={"access_token": retl.secrets["destinations.override.access_token"]},
        registry=registry,
    )

    assert destination.credentials == {
        "access_token": retl.secrets["destinations.override.access_token"]
    }


def test_namespace_derived_missing_secret_fails_through_auth_resolution_path() -> None:
    surface = DestinationSurface(
        name="profile",
        declaration_family="state",
        supported_operations=("upsert",),
    )
    connector = declarative_connector(
        ref="example/secret-resolution",
        surfaces=(surface,),
        auth_modes=(retl.auth.bearer_token(field="access_token"),),
    )
    registry = DestinationRegistry()
    registry.register(connector)
    destination = retl.destinations.load(
        "example/secret-resolution",
        binding_name="secret_resolution_primary",
        credential_namespace="destinations.example",
        registry=registry,
    )

    with pytest.raises(
        retl.auth.AuthResolutionError,
        match="access_token.*destinations.example.access_token",
    ):
        retl.auth.resolve_auth(
            mode=connector.auth_modes[0],
            credentials=destination.credentials,
            resolver=retl.auth.MappingSecretResolver({}),
        )


def test_load_expands_config_namespace_from_connector_declared_fields() -> None:
    retl.configure(
        config_resolver=retl.MappingConfigResolver(
            {
                "destinations.example.base_url": "https://api.example.test",
                "destinations.example.events.event_routes.purchase": "pixel_123",
                "destinations.example.ignored": "ignored",
            }
        )
    )
    surface = DestinationSurface(
        name="events",
        declaration_family="event",
        supported_operations=("import",),
    )
    connector = declarative_connector(
        ref="example/config-namespace",
        surfaces=(surface,),
        auth_modes=(retl.auth.none(),),
        config_namespace_fields=("base_url", "events.event_routes.purchase"),
    )
    registry = DestinationRegistry()
    registry.register(connector)

    destination = retl.destinations.load(
        "example/config-namespace",
        binding_name="config_namespace_primary",
        config_namespace="destinations.example",
        registry=registry,
    )

    assert destination.config == {
        "base_url": "https://api.example.test",
        "events": {"event_routes": {"purchase": "pixel_123"}},
    }


def test_load_explicit_config_overrides_namespace_fields() -> None:
    retl.configure(
        config_resolver=retl.MappingConfigResolver(
            {
                "destinations.example.base_url": "https://api.example.test",
                "destinations.example.events.event_routes.purchase": "pixel_123",
            }
        )
    )
    surface = DestinationSurface(
        name="events",
        declaration_family="event",
        supported_operations=("import",),
    )
    connector = declarative_connector(
        ref="example/config-override",
        surfaces=(surface,),
        auth_modes=(retl.auth.none(),),
        config_namespace_fields=("base_url", "events.event_routes.purchase"),
    )
    registry = DestinationRegistry()
    registry.register(connector)

    destination = retl.destinations.load(
        "example/config-override",
        binding_name="config_override_primary",
        config_namespace="destinations.example",
        config={
            "base_url": "https://override.example.test",
            "events": {"event_routes": {"purchase": "pixel_override"}},
        },
        registry=registry,
    )

    assert destination.config == {
        "base_url": "https://override.example.test",
        "events": {"event_routes": {"purchase": "pixel_override"}},
    }


def test_load_rejects_invalid_namespace_names() -> None:
    surface = DestinationSurface(
        name="profile",
        declaration_family="state",
        supported_operations=("upsert",),
    )
    connector = declarative_connector(
        ref="example/namespaces",
        surfaces=(surface,),
        auth_modes=(retl.auth.bearer_token(field="access_token"),),
        config_namespace_fields=("base_url",),
    )
    registry = DestinationRegistry()
    registry.register(connector)

    with pytest.raises(retl.DeclarationValidationError, match="credential_namespace"):
        retl.destinations.load(
            "example/namespaces",
            binding_name="bad_credential_namespace",
            credential_namespace="destinations..example",
            registry=registry,
        )

    with pytest.raises(retl.DeclarationValidationError, match="config_namespace"):
        retl.destinations.load(
            "example/namespaces",
            binding_name="bad_config_namespace",
            config_namespace="destinations.example.*",
            registry=registry,
        )


def test_load_rejects_base_url_config_unless_connector_declares_it() -> None:
    surface = DestinationSurface(
        name="profile",
        declaration_family="state",
        supported_operations=("upsert",),
    )
    connector = declarative_connector(
        ref="example/partner",
        surfaces=(surface,),
        auth_modes=(retl.auth.none(),),
        config_namespace_fields=("api_version",),
    )
    registry = DestinationRegistry()
    registry.register(connector)

    with pytest.raises(retl.DeclarationValidationError, match="does not support `base_url`"):
        retl.destinations.load(
            "example/partner",
            binding_name="partner",
            config={"base_url": "https://api.example.test"},
            registry=registry,
        )


def test_load_allows_base_url_config_when_connector_declares_it() -> None:
    surface = DestinationSurface(
        name="profile",
        declaration_family="state",
        supported_operations=("upsert",),
    )
    connector = declarative_connector(
        ref="example/private-http",
        surfaces=(surface,),
        auth_modes=(retl.auth.none(),),
        config_namespace_fields=("base_url",),
    )
    registry = DestinationRegistry()
    registry.register(connector)

    destination = retl.destinations.load(
        "example/private-http",
        binding_name="private_http",
        config={"base_url": "https://api.example.test"},
        registry=registry,
    )

    assert destination.config["base_url"] == "https://api.example.test"


def test_oauth2_connector_requires_token_transport() -> None:
    surface = DestinationSurface(
        name="profile",
        declaration_family="state",
        supported_operations=("upsert",),
    )

    with pytest.raises(retl.DeclarationValidationError, match="auth_token_transport"):
        declarative_connector(
            ref="example/oauth2",
            surfaces=(surface,),
            auth_modes=(
                retl.auth.oauth2_client_credentials(token_url="https://auth.example.test/token"),
            ),
        )

    with pytest.raises(retl.DeclarationValidationError, match="callable.*auth_token_transport"):
        declarative_connector(
            ref="example/oauth2-bad-transport",
            surfaces=(surface,),
            auth_modes=(
                retl.auth.oauth2_client_credentials(token_url="https://auth.example.test/token"),
            ),
            auth_token_transport="not-callable",  # type: ignore[arg-type]
        )


def test_oauth_jwt_connector_requires_token_transport_and_signer() -> None:
    surface = DestinationSurface(
        name="profile",
        declaration_family="state",
        supported_operations=("upsert",),
    )

    with pytest.raises(retl.DeclarationValidationError, match="auth_token_transport"):
        declarative_connector(
            ref="example/oauth-jwt-no-transport",
            surfaces=(surface,),
            auth_modes=(retl.auth.oauth_jwt(token_url="https://auth.example.test/token"),),
        )

    def token_transport(request: Mapping[str, object]) -> Mapping[str, object]:
        return {"access_token": "token"}

    with pytest.raises(retl.DeclarationValidationError, match="auth_jwt_signer"):
        declarative_connector(
            ref="example/oauth-jwt-no-signer",
            surfaces=(surface,),
            auth_modes=(retl.auth.oauth_jwt(token_url="https://auth.example.test/token"),),
            auth_token_transport=token_transport,
        )

    with pytest.raises(retl.DeclarationValidationError, match="callable.*auth_jwt_signer"):
        declarative_connector(
            ref="example/oauth-jwt-bad-signer",
            surfaces=(surface,),
            auth_modes=(retl.auth.oauth_jwt(token_url="https://auth.example.test/token"),),
            auth_token_transport=token_transport,
            auth_jwt_signer="not-callable",  # type: ignore[arg-type]
        )
