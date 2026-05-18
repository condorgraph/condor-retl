from __future__ import annotations

import base64
from collections.abc import Mapping
from typing import Any, cast

import pytest

import retl
from retl.declarations import CredentialValue


def test_none_auth_produces_no_http_auth() -> None:
    resolved = retl.auth.apply_http_auth(retl.auth.none(), {})

    assert resolved.mode == "none"
    assert resolved.headers == {}
    assert resolved.query == {}
    assert resolved.cookies == {}


def test_bearer_token_resolves_secret_ref_at_runtime() -> None:
    resolved = retl.auth.apply_http_auth(
        retl.auth.bearer_token(field="token"),
        {"token": retl.secrets["destinations.auth.token"]},
        resolver=retl.auth.MappingSecretResolver({"destinations.auth.token": "runtime-token"}),
    )

    assert resolved.headers == {"Authorization": "Bearer runtime-token"}
    assert "runtime-token" not in repr(resolved)


def test_secret_ref_requires_explicit_resolver_in_low_level_helper() -> None:
    with pytest.raises(retl.auth.AuthResolutionError) as exc_info:
        retl.auth.apply_http_auth(
            retl.auth.bearer_token(field="token"),
            {"token": retl.secrets["destinations.auth.token"]},
        )

    message = str(exc_info.value)
    assert "token" in message
    assert "destinations.auth.token" in message
    assert "no SecretResolver" in message


def test_resolve_auth_no_longer_accepts_secret_resolver_alias() -> None:
    with pytest.raises(TypeError, match="secret_resolver"):
        retl.auth.resolve_auth(
            mode=retl.auth.bearer_token(field="token"),
            credentials={"token": retl.secrets["destinations.auth.token"]},
            secret_resolver=retl.auth.MappingSecretResolver(  # type: ignore[call-arg]
                {"destinations.auth.token": "runtime-token"}
            ),
        )


def test_environment_secret_resolver_uses_unprefixed_double_underscore_hierarchy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = retl.auth.EnvironmentSecretResolver()
    monkeypatch.setenv("DESTINATIONS__ADS_API__ACCESS_TOKEN", "ads-api-token")
    monkeypatch.setenv("DESTINATIONS__ADS__API__ACCESS_TOKEN", "ads-api-nested-token")

    assert resolver.resolve(retl.secrets["destinations.ads_api.access_token"]) == "ads-api-token"
    assert (
        resolver.resolve(retl.secrets["destinations.ads.api.access_token"])
        == "ads-api-nested-token"
    )


def test_environment_secret_resolver_uses_explicit_logical_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = retl.auth.EnvironmentSecretResolver()
    monkeypatch.setenv("RETL__DESTINATIONS__ADS_API__ACCESS_TOKEN", "namespaced-token")

    assert (
        resolver.resolve(retl.secrets["retl.destinations.ads_api.access_token"])
        == "namespaced-token"
    )


def test_environment_secret_resolver_does_not_try_env_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = retl.auth.EnvironmentSecretResolver()
    monkeypatch.setenv("ACCESS_TOKEN", "alias-token")

    with pytest.raises(retl.auth.MissingSecretError) as exc_info:
        resolver.resolve(retl.secrets["destinations.ads_api.access_token"])

    message = str(exc_info.value)
    assert "DESTINATIONS__ADS_API__ACCESS_TOKEN" in message
    assert "environment variable `ACCESS_TOKEN`" not in message
    assert "alias-token" not in message


def test_configured_secret_resolver_precedes_environment_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DESTINATIONS__ADS_API__ACCESS_TOKEN", "env-token")
    monkeypatch.setenv("DESTINATIONS__OTHER__ACCESS_TOKEN", "fallback-token")
    resolver = retl.ChainedSecretResolver(
        retl.auth.MappingSecretResolver({"destinations.ads_api.access_token": "mapped-token"}),
        retl.auth.EnvironmentSecretResolver(),
    )

    assert resolver.resolve(retl.secrets["destinations.ads_api.access_token"]) == "mapped-token"
    assert resolver.resolve(retl.secrets["destinations.other.access_token"]) == "fallback-token"


@pytest.mark.parametrize(
    ("location", "expected_attr"),
    (
        ("header", "headers"),
        ("query", "query"),
        ("cookie", "cookies"),
    ),
)
def test_api_key_supports_header_query_and_cookie_placement(
    location: retl.auth.ApiKeyLocation,
    expected_attr: str,
) -> None:
    resolved = retl.auth.apply_http_auth(
        retl.auth.api_key(
            field="private_api_key",
            location=location,
            key="Authorization",
            prefix="Klaviyo-API-Key ",
        ),
        {"private_api_key": retl.secrets.literal("private-key")},
    )

    assert getattr(resolved, expected_attr) == {"Authorization": "Klaviyo-API-Key private-key"}


def test_basic_auth_produces_basic_authorization_header() -> None:
    resolved = retl.auth.apply_http_auth(
        retl.auth.basic(),
        {
            "username": retl.secrets.literal("user"),
            "password": retl.secrets.literal("pass"),
        },
    )

    encoded = base64.b64encode(b"user:pass").decode("ascii")
    assert resolved.headers == {"Authorization": f"Basic {encoded}"}


def test_native_auth_resolves_backend_credentials_without_http_placement() -> None:
    resolved = retl.auth.resolve_auth(
        mode=retl.auth.native(
            "key_pair",
            required_fields=("user", "private_key"),
            optional_fields=("private_key_passphrase",),
        ),
        credentials={
            "user": retl.secrets["destinations.snowflake.key_pair.user"],
            "private_key": retl.secrets.literal("private-key-material"),
        },
        resolver=retl.auth.MappingSecretResolver(
            {"destinations.snowflake.key_pair.user": "loader"}
        ),
    )

    assert resolved.mode == "key_pair"
    assert resolved.headers == {}
    assert resolved.query == {}
    assert resolved.cookies == {}
    assert resolved.sdk_context["credentials"] == {
        "user": "loader",
        "private_key": "private-key-material",
    }
    assert resolved.evidence == {
        "mode": "key_pair",
        "required_fields": {"user": True, "private_key": True},
        "resolved": True,
    }
    assert "private-key-material" not in repr(resolved)


def test_missing_secret_diagnostic_uses_field_and_ref_names_without_value() -> None:
    with pytest.raises(retl.auth.AuthResolutionError) as exc_info:
        retl.auth.apply_http_auth(
            retl.auth.bearer_token(field="token"),
            {"token": retl.secrets["destinations.auth.token"]},
            resolver=retl.auth.MappingSecretResolver({}),
        )

    message = str(exc_info.value)
    assert "token" in message
    assert "destinations.auth.token" in message
    assert "runtime-token" not in message


def test_oauth2_client_credentials_uses_injected_token_transport() -> None:
    requests: list[Mapping[str, object]] = []

    def token_transport(request: Mapping[str, object]) -> Mapping[str, object]:
        requests.append(request)
        return {"access_token": "oauth-token", "expires_in": 120}

    resolved = retl.auth.apply_http_auth(
        retl.auth.oauth2_client_credentials(
            token_url="https://auth.example.test/token",
            scopes=("profile:write", "event:write"),
            audience="https://api.example.test",
        ),
        {
            "client_id": retl.secrets.literal("client"),
            "client_secret": retl.secrets.literal("secret"),
        },
        token_transport=token_transport,
    )

    assert resolved.headers == {"Authorization": "Bearer oauth-token"}
    assert resolved.expires_in == 120
    assert requests == [
        {
            "grant_type": "client_credentials",
            "token_url": "https://auth.example.test/token",
            "client_id": "client",
            "client_secret": "secret",
            "scope": "profile:write event:write",
            "audience": "https://api.example.test",
        }
    ]
    assert "oauth-token" not in repr(resolved)


def test_oauth2_requires_injected_transport_to_keep_tests_network_free() -> None:
    with pytest.raises(retl.DeclarationValidationError, match="token_transport"):
        retl.auth.apply_http_auth(
            retl.auth.oauth2_client_credentials(token_url="https://auth.example.test/token"),
            {
                "client_id": retl.secrets.literal("client"),
                "client_secret": retl.secrets.literal("secret"),
            },
        )


def test_oauth_jwt_uses_injected_signer_and_token_transport() -> None:
    signed: list[tuple[Mapping[str, object], str]] = []
    requests: list[Mapping[str, object]] = []

    def jwt_signer(claims: Mapping[str, object], private_key: str) -> str:
        signed.append((claims, private_key))
        return "signed-assertion"

    def token_transport(request: Mapping[str, object]) -> Mapping[str, object]:
        requests.append(request)
        return {"access_token": "jwt-token", "expires_in": 60}

    resolved = retl.auth.apply_http_auth(
        retl.auth.oauth_jwt(
            token_url="https://oauth2.example.test/token",
            scopes=("https://api.example.test/scope",),
            subject="delegated@example.test",
            key_id_field="private_key_id",
        ),
        {
            "client_email": retl.secrets.literal("service@example.test"),
            "private_key": retl.secrets.literal("private-key-material"),
            "private_key_id": retl.secrets.literal("kid-1"),
        },
        token_transport=token_transport,
        jwt_signer=jwt_signer,
    )

    assert resolved.headers == {"Authorization": "Bearer jwt-token"}
    assert signed == [
        (
            {
                "iss": "service@example.test",
                "iat": 0,
                "exp": 3600,
                "scope": "https://api.example.test/scope",
                "aud": "https://oauth2.example.test/token",
                "sub": "delegated@example.test",
            },
            "private-key-material",
        )
    ]
    assert requests == [
        {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "token_url": "https://oauth2.example.test/token",
            "assertion": "signed-assertion",
            "key_id": "kid-1",
        }
    ]
    assert "jwt-token" not in repr(resolved)


def test_custom_hook_can_return_connector_owned_auth_output() -> None:
    def hook(
        values: Mapping[str, str],
        context: Mapping[str, object],
    ) -> retl.auth.ResolvedAuth:
        return retl.auth.ResolvedAuth(
            mode=str(context["mode"]),
            headers={"X-Signature": f"signed:{values['secret']}"},
            query={"version": "1"},
            context={"tenant": values["tenant"]},
        )

    resolved = retl.auth.apply_http_auth(
        retl.auth.custom(required_fields=("secret", "tenant"), hook=hook),
        {
            "secret": retl.secrets.literal("hook-secret"),
            "tenant": retl.secrets.literal("tenant-a"),
        },
    )

    assert resolved.headers == {"X-Signature": "signed:hook-secret"}
    assert resolved.query == {"version": "1"}
    assert resolved.context["tenant"] == "tenant-a"


def test_credential_fingerprint_and_evidence_are_redacted() -> None:
    mode = retl.auth.oauth_jwt(token_url="https://oauth2.example.test/token")
    credentials: dict[str, CredentialValue] = {
        "client_email": retl.secrets["destinations.jwt.client_email"],
        "private_key": retl.secrets.literal("private-key-material"),
    }

    assert retl.auth.credential_presence_fingerprint(mode, credentials) == {
        "client_email": True,
        "private_key": True,
    }
    evidence = retl.auth.auth_evidence(mode=mode, credentials=credentials, resolved=True)
    assert evidence.required_fields == {"client_email": True, "private_key": True}
    assert evidence.resolved is True
    assert "private-key-material" not in repr(evidence)


def test_secret_literal_repr_and_identity_do_not_expose_value() -> None:
    literal = retl.secrets.literal("process-token")

    assert repr(literal) == "SecretLiteral(<redacted>)"
    assert "process-token" not in repr(literal)
    assert literal != retl.secrets.literal("process-token")


def test_bare_string_credentials_are_rejected_before_resolution() -> None:
    with pytest.raises(retl.DeclarationValidationError, match="SecretRef or SecretLiteral"):
        retl.auth.apply_http_auth(
            retl.auth.bearer_token(field="token"),
            {"token": cast(Any, "bare-token")},
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    (
        ({"name": "bad_bearer", "kind": "bearer_token"}, "Bearer-token"),
        (
            {"name": "bad_basic", "kind": "basic", "required_fields": ("username",)},
            "Basic auth",
        ),
        (
            {"name": "bad_api", "kind": "api_key", "required_fields": ()},
            "API-key auth",
        ),
        (
            {
                "name": "bad_oauth2",
                "kind": "oauth2_client_credentials",
                "required_fields": ("client_id",),
                "token_url": "https://auth.example.test/token",
            },
            "OAuth2 Client Credentials",
        ),
        (
            {
                "name": "bad_jwt",
                "kind": "oauth_jwt",
                "required_fields": ("client_email",),
                "token_url": "https://auth.example.test/token",
            },
            "OAuth JWT",
        ),
        ({"name": "bad_native", "kind": "native"}, "Native auth"),
        ({"name": "bad_custom", "kind": "custom"}, "Custom auth"),
    ),
)
def test_auth_mode_rejects_invalid_per_kind_shapes(
    kwargs: Mapping[str, object],
    message: str,
) -> None:
    with pytest.raises(retl.DeclarationValidationError, match=message):
        retl.auth.AuthMode(**kwargs)  # type: ignore[arg-type]
