from __future__ import annotations

import base64
import os
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal, Protocol, TypeAlias

from retl.declarations import CredentialValue, SecretLiteral, SecretRef
from retl.errors import DeclarationValidationError

AuthModeKind: TypeAlias = Literal[
    "none",
    "bearer_token",
    "api_key",
    "basic",
    "oauth2_client_credentials",
    "oauth_jwt",
    "native",
    "custom",
]
ApiKeyLocation: TypeAlias = Literal["header", "query", "cookie"]
AuthLocation: TypeAlias = ApiKeyLocation


class AuthResolutionError(DeclarationValidationError):
    """Raised when auth resolution fails at the runtime secret boundary."""


class MissingSecretError(AuthResolutionError):
    """Raised when a SecretRef has no runtime value."""


class SecretResolver(Protocol):
    def resolve(self, ref: SecretRef) -> str: ...


TokenTransport: TypeAlias = Callable[[Mapping[str, object]], Mapping[str, object]]
JwtSigner: TypeAlias = Callable[[Mapping[str, object], str], str]
CustomAuthHook: TypeAlias = Callable[
    [Mapping[str, str], Mapping[str, object]],
    "ResolvedAuth",
]


@dataclass(frozen=True)
class TokenRequest:
    url: str
    form: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "form", MappingProxyType(dict(self.form)))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Mapping):
            return dict(other) == {"token_url": self.url, **dict(self.form)}
        return super().__eq__(other)


@dataclass(frozen=True)
class AuthMode:
    name: str
    kind: AuthModeKind
    required_fields: Sequence[str] = field(default_factory=tuple)
    optional_fields: Sequence[str] = field(default_factory=tuple)
    location: ApiKeyLocation | None = None
    key: str | None = None
    prefix: str = ""
    token_url: str | None = None
    scopes: Sequence[str] = field(default_factory=tuple)
    audience: str | None = None
    access_token_field: str = "access_token"
    expires_in_field: str = "expires_in"
    subject: str | None = None
    key_id_field: str | None = None
    custom_hook: CustomAuthHook | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        _non_empty(self.name, field_name="auth mode name")
        if self.kind not in {
            "none",
            "bearer_token",
            "api_key",
            "basic",
            "oauth2_client_credentials",
            "oauth_jwt",
            "native",
            "custom",
        }:
            raise DeclarationValidationError(f"Unsupported auth mode kind `{self.kind}`.")
        required = _unique_strings(self.required_fields, field_name="auth required_fields")
        optional = _unique_strings(self.optional_fields, field_name="auth optional_fields")
        duplicated = sorted(set(required) & set(optional))
        if duplicated:
            raise DeclarationValidationError(
                f"Auth mode `{self.name}` repeats field(s): {', '.join(duplicated)}."
            )
        if self.kind == "none" and (required or optional):
            raise DeclarationValidationError("No-auth mode must not declare credential fields.")
        if self.kind == "bearer_token" and len(required) != 1:
            raise DeclarationValidationError("Bearer-token auth requires exactly one field.")
        if self.kind == "api_key":
            if len(required) != 1:
                raise DeclarationValidationError("API-key auth requires exactly one field.")
            if self.location not in ("header", "query", "cookie"):
                raise DeclarationValidationError("API-key auth requires header, query, or cookie.")
            if not self.key:
                raise DeclarationValidationError("API-key auth requires a placement key.")
        if self.kind == "basic" and len(required) != 2:
            raise DeclarationValidationError("Basic auth requires username and password fields.")
        if self.kind == "oauth2_client_credentials" and len(required) != 2:
            raise DeclarationValidationError(
                "OAuth2 Client Credentials auth requires client id and client secret fields."
            )
        if self.kind == "oauth_jwt" and len(required) != 2:
            raise DeclarationValidationError(
                "OAuth JWT auth requires issuer and private key fields."
            )
        if self.kind == "native" and not required:
            raise DeclarationValidationError("Native auth requires at least one credential field.")
        if self.kind == "custom" and not required:
            raise DeclarationValidationError("Custom auth requires at least one credential field.")
        if self.kind in {"oauth2_client_credentials", "oauth_jwt"} and not self.token_url:
            raise DeclarationValidationError(f"Auth mode `{self.name}` requires token_url.")
        object.__setattr__(self, "required_fields", required)
        object.__setattr__(self, "optional_fields", optional)
        object.__setattr__(self, "scopes", _unique_strings(self.scopes, field_name="scopes"))

    @property
    def field_names(self) -> tuple[str, ...]:
        return (*self.required_fields, *self.optional_fields)


@dataclass(frozen=True)
class ResolvedAuth:
    mode: str
    headers: Mapping[str, str] = field(default_factory=dict, repr=False)
    query: Mapping[str, str] = field(default_factory=dict, repr=False)
    cookies: Mapping[str, str] = field(default_factory=dict, repr=False)
    context: Mapping[str, object] = field(default_factory=dict, repr=False)
    expires_in: int | None = None
    token_expires_at: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "headers", MappingProxyType(dict(self.headers)))
        object.__setattr__(self, "query", MappingProxyType(dict(self.query)))
        object.__setattr__(self, "cookies", MappingProxyType(dict(self.cookies)))
        object.__setattr__(self, "context", MappingProxyType(dict(self.context)))

    def __repr__(self) -> str:
        return (
            f"ResolvedAuth(mode={self.mode!r}, headers=<redacted>, query=<redacted>, "
            "cookies=<redacted>, context=<redacted>)"
        )

    @property
    def evidence(self) -> Mapping[str, object]:
        evidence = self.context.get("auth_evidence")
        if isinstance(evidence, Mapping):
            return evidence
        return {"mode": self.mode, "required_fields": {}, "resolved": True}

    @property
    def sdk_context(self) -> Mapping[str, object]:
        return self.context


@dataclass(frozen=True)
class AuthEvidence:
    mode: str
    required_fields: Mapping[str, bool]
    resolved: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "required_fields", MappingProxyType(dict(self.required_fields)))


class MappingSecretResolver:
    provider_kind = "mapping"

    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = dict(values)

    def resolve(self, ref: SecretRef) -> str:
        try:
            return self._values[ref.name]
        except KeyError as exc:
            raise MissingSecretError(
                f"Missing secret `{ref.name}` from provider `mapping` at key `{ref.name}`."
            ) from exc


class EnvironmentSecretResolver:
    provider_kind = "environment"

    def resolve(self, ref: SecretRef) -> str:
        env_name = self.env_name(ref.name)
        try:
            return os.environ[env_name]
        except KeyError as exc:
            raise MissingSecretError(
                f"Missing secret `{ref.name}` from provider `environment` at "
                f"environment variable `{env_name}`."
            ) from exc

    def env_name(self, name: str) -> str:
        return _secret_env_name(name)


def none(name: str = "none") -> AuthMode:
    return AuthMode(name=name, kind="none")


def bearer_token(name: str = "bearer_token", *, field: str = "access_token") -> AuthMode:
    return AuthMode(name=name, kind="bearer_token", required_fields=(field,))


def api_key(
    name: str = "api_key",
    *,
    field: str = "api_key",
    location: ApiKeyLocation,
    key: str,
    prefix: str = "",
) -> AuthMode:
    return AuthMode(
        name=name,
        kind="api_key",
        required_fields=(field,),
        location=location,
        key=key,
        prefix=prefix,
    )


def basic(
    name: str = "basic",
    *,
    username_field: str = "username",
    password_field: str = "password",
) -> AuthMode:
    return AuthMode(
        name=name,
        kind="basic",
        required_fields=(username_field, password_field),
    )


def oauth2_client_credentials(
    name: str = "oauth2_client_credentials",
    *,
    token_url: str,
    client_id_field: str = "client_id",
    client_secret_field: str = "client_secret",
    scopes: Sequence[str] = (),
    audience: str | None = None,
    access_token_field: str = "access_token",
    expires_in_field: str = "expires_in",
) -> AuthMode:
    return AuthMode(
        name=name,
        kind="oauth2_client_credentials",
        required_fields=(client_id_field, client_secret_field),
        token_url=token_url,
        scopes=tuple(scopes),
        audience=audience,
        access_token_field=access_token_field,
        expires_in_field=expires_in_field,
    )


def oauth_jwt(
    name: str = "oauth_jwt",
    *,
    token_url: str,
    issuer_field: str = "client_email",
    private_key_field: str = "private_key",
    scopes: Sequence[str] = (),
    audience: str | None = None,
    subject: str | None = None,
    key_id_field: str | None = "private_key_id",
    access_token_field: str = "access_token",
    expires_in_field: str = "expires_in",
) -> AuthMode:
    optional = (key_id_field,) if key_id_field else ()
    return AuthMode(
        name=name,
        kind="oauth_jwt",
        required_fields=(issuer_field, private_key_field),
        optional_fields=optional,
        token_url=token_url,
        scopes=tuple(scopes),
        audience=audience,
        subject=subject,
        key_id_field=key_id_field,
        access_token_field=access_token_field,
        expires_in_field=expires_in_field,
    )


def native(
    name: str,
    *,
    required_fields: Sequence[str],
    optional_fields: Sequence[str] = (),
) -> AuthMode:
    return AuthMode(
        name=name,
        kind="native",
        required_fields=tuple(required_fields),
        optional_fields=tuple(optional_fields),
    )


def custom(
    name: str = "custom",
    *,
    required_fields: Sequence[str],
    optional_fields: Sequence[str] = (),
    hook: CustomAuthHook,
) -> AuthMode:
    return AuthMode(
        name=name,
        kind="custom",
        required_fields=tuple(required_fields),
        optional_fields=tuple(optional_fields),
        custom_hook=hook,
    )


def select_auth_mode(
    modes: Sequence[AuthMode] | None = None,
    selected: str | None = None,
    *,
    auth_modes: Sequence[AuthMode] | None = None,
    connector_ref: str | None = None,
) -> AuthMode:
    if modes is None:
        modes = auth_modes or ()
    if not modes:
        raise DeclarationValidationError("Destination Connector must declare explicit auth_modes.")
    by_name = {mode.name: mode for mode in modes}
    if len(by_name) != len(tuple(modes)):
        raise DeclarationValidationError("Destination Connector auth mode names must be unique.")
    if selected is not None:
        try:
            return by_name[selected]
        except KeyError as exc:
            available = ", ".join(sorted(by_name))
            raise DeclarationValidationError(
                f"Unknown auth_mode `{selected}`. Available auth modes: {available}."
            ) from exc
    non_custom = tuple(mode for mode in modes if mode.kind != "custom")
    if len(modes) == 1:
        return tuple(modes)[0]
    if len(non_custom) == 1:
        return non_custom[0]
    available = ", ".join(sorted(by_name))
    connector_text = f" `{connector_ref}`" if connector_ref else ""
    raise DeclarationValidationError(
        f"Destination connector{connector_text} declares multiple Auth Modes; "
        f"pass auth_mode explicitly. Available auth modes: {available}."
    )


def validate_credential_fields(mode: AuthMode, credentials: Mapping[str, object]) -> None:
    missing = tuple(field for field in mode.required_fields if field not in credentials)
    if missing:
        rendered = ", ".join(sorted(missing))
        raise DeclarationValidationError(
            f"Auth mode `{mode.name}` missing required credential field(s): {rendered}."
        )


def validate_required_credentials(
    mode: AuthMode, credentials: Mapping[str, CredentialValue]
) -> None:
    validate_credential_fields(mode, credentials)
    unknown = tuple(sorted(set(credentials) - set(mode.field_names)))
    if unknown:
        rendered = ", ".join(unknown)
        raise DeclarationValidationError(
            f"Auth mode `{mode.name}` received undeclared credential field(s): {rendered}."
        )
    _validate_secret_shaped_credentials(mode, credentials)


def credential_presence_fingerprint(
    mode: AuthMode,
    credentials: Mapping[str, CredentialValue],
) -> Mapping[str, bool]:
    return MappingProxyType({field: bool(credentials.get(field)) for field in mode.required_fields})


def resolve_auth(
    *,
    mode: AuthMode,
    credentials: Mapping[str, CredentialValue],
    resolver: SecretResolver | None = None,
    token_transport: TokenTransport | None = None,
    jwt_signer: JwtSigner | None = None,
) -> ResolvedAuth:
    values = resolve_credentials(mode=mode, credentials=credentials, resolver=resolver)
    resolved = apply_auth(
        mode=mode,
        values=values,
        token_transport=token_transport,
        jwt_signer=jwt_signer,
    )
    return _with_evidence(resolved, mode=mode, credentials=credentials, resolved=True)


def apply_http_auth(
    mode: AuthMode,
    credentials: Mapping[str, CredentialValue],
    *,
    resolver: SecretResolver | None = None,
    token_transport: Callable[[Mapping[str, object]], Mapping[str, object]] | None = None,
    jwt_signer: Callable[..., str] | None = None,
    request_context: Mapping[str, object] | None = None,
    now: Callable[[], float] | None = None,
) -> ResolvedAuth:
    values = resolve_credentials(mode=mode, credentials=credentials, resolver=resolver)
    resolved = _apply_http_auth_values(
        mode=mode,
        values=values,
        token_transport=token_transport,
        jwt_signer=jwt_signer,
        request_context=request_context or {},
        now=now or (lambda: 0.0),
    )
    return _with_evidence(resolved, mode=mode, credentials=credentials, resolved=True)


def resolve_credentials(
    *,
    mode: AuthMode,
    credentials: Mapping[str, CredentialValue],
    resolver: SecretResolver | None,
) -> Mapping[str, str]:
    validate_credential_fields(mode, credentials)
    resolved: dict[str, str] = {}
    for field_name in mode.field_names:
        if field_name not in credentials:
            continue
        value = credentials[field_name]
        if isinstance(value, SecretRef):
            if resolver is None:
                raise AuthResolutionError(
                    f"Auth credential `{field_name}` references SecretRef `{value.name}` "
                    "but no SecretResolver was provided."
                )
            try:
                resolved[field_name] = resolver.resolve(value)
            except AuthResolutionError as exc:
                raise AuthResolutionError(
                    f"Auth credential `{field_name}` references SecretRef `{value.name}`. {exc}"
                ) from exc
        elif isinstance(value, SecretLiteral):
            resolved[field_name] = value.value
        else:
            raise DeclarationValidationError(
                f"Auth credential `{field_name}` must be a SecretRef or SecretLiteral."
            )
        if resolved[field_name] == "":
            raise DeclarationValidationError(
                f"Auth mode `{mode.name}` has empty credential field `{field_name}`."
            )
    return MappingProxyType(resolved)


def apply_auth(
    *,
    mode: AuthMode,
    values: Mapping[str, str],
    token_transport: TokenTransport | None = None,
    jwt_signer: JwtSigner | None = None,
) -> ResolvedAuth:
    if mode.kind == "none":
        return ResolvedAuth(mode=mode.name)
    validate_credential_fields(mode, values)
    if mode.kind == "bearer_token":
        return ResolvedAuth(
            mode=mode.name,
            headers={"Authorization": f"Bearer {values[mode.required_fields[0]]}"},
        )
    if mode.kind == "api_key":
        value = f"{mode.prefix}{values[mode.required_fields[0]]}"
        return _placed_api_key(mode=mode, value=value)
    if mode.kind == "basic":
        username, password = mode.required_fields
        encoded = base64.b64encode(f"{values[username]}:{values[password]}".encode()).decode(
            "ascii"
        )
        return ResolvedAuth(mode=mode.name, headers={"Authorization": f"Basic {encoded}"})
    if mode.kind == "oauth2_client_credentials":
        if token_transport is None:
            raise DeclarationValidationError(
                f"Auth mode `{mode.name}` requires an injected token transport."
            )
        request = {
            "grant_type": "client_credentials",
            "token_url": mode.token_url,
            "client_id": values[mode.required_fields[0]],
            "client_secret": values[mode.required_fields[1]],
            "scope": " ".join(mode.scopes),
            "audience": mode.audience,
        }
        return _bearer_from_token_response(mode=mode, response=token_transport(request))
    if mode.kind == "oauth_jwt":
        if token_transport is None or jwt_signer is None:
            raise DeclarationValidationError(
                f"Auth mode `{mode.name}` requires injected JWT signer and token transport."
            )
        issuer_field, key_field = mode.required_fields
        claims = {
            "iss": values[issuer_field],
            "scope": " ".join(mode.scopes),
            "aud": mode.audience or mode.token_url,
            "sub": mode.subject,
        }
        assertion = jwt_signer(claims, values[key_field])
        request = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "token_url": mode.token_url,
            "assertion": assertion,
        }
        if mode.key_id_field and mode.key_id_field in values:
            request["key_id"] = values[mode.key_id_field]
        return _bearer_from_token_response(mode=mode, response=token_transport(request))
    if mode.kind == "native":
        return ResolvedAuth(mode=mode.name, context={"credentials": values})
    if mode.kind == "custom":
        if mode.custom_hook is None:
            raise DeclarationValidationError(f"Auth mode `{mode.name}` requires a custom hook.")
        return mode.custom_hook(values, {"mode": mode.name})
    raise DeclarationValidationError(f"Unsupported auth mode `{mode.kind}`.")


def _apply_http_auth_values(
    *,
    mode: AuthMode,
    values: Mapping[str, str],
    token_transport: Callable[[Mapping[str, object]], Mapping[str, object]] | None,
    jwt_signer: Callable[..., str] | None,
    request_context: Mapping[str, object],
    now: Callable[[], float],
) -> ResolvedAuth:
    if mode.kind in {"none", "bearer_token", "api_key", "basic", "native"}:
        return apply_auth(mode=mode, values=values)
    if mode.kind == "oauth2_client_credentials":
        if token_transport is None:
            raise AuthResolutionError(f"Auth mode `{mode.name}` requires token_transport.")
        form = {
            "grant_type": "client_credentials",
            "token_url": str(mode.token_url),
            "client_id": values[mode.required_fields[0]],
            "client_secret": values[mode.required_fields[1]],
        }
        if mode.scopes:
            form["scope"] = " ".join(mode.scopes)
        if mode.audience:
            form["audience"] = mode.audience
        return _bearer_from_token_response(
            mode=mode,
            response=token_transport(form),  # type: ignore[arg-type]
            now=now,
        )
    if mode.kind == "oauth_jwt":
        if token_transport is None or jwt_signer is None:
            raise AuthResolutionError(
                f"Auth mode `{mode.name}` requires jwt_signer and token_transport."
            )
        issued_at = int(now())
        claims: dict[str, object] = {
            "iss": values[mode.required_fields[0]],
            "iat": issued_at,
            "exp": issued_at + 3600,
            "scope": " ".join(mode.scopes),
            "aud": mode.audience or mode.token_url,
        }
        if mode.subject:
            claims["sub"] = mode.subject
        headers: dict[str, str] = {}
        if mode.key_id_field and mode.key_id_field in values:
            headers["kid"] = values[mode.key_id_field]
        try:
            assertion = jwt_signer(claims=claims, credentials=values, headers=headers)
        except TypeError:
            assertion = jwt_signer(claims, values[mode.required_fields[1]])
        return _bearer_from_token_response(
            mode=mode,
            response=token_transport(
                {
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "token_url": str(mode.token_url),
                    "assertion": assertion,
                    **({"key_id": headers["kid"]} if "kid" in headers else {}),
                }
            ),  # type: ignore[arg-type]
            now=now,
        )
    if mode.kind == "custom":
        if mode.custom_hook is None:
            raise AuthResolutionError(f"Auth mode `{mode.name}` requires a custom hook.")
        return mode.custom_hook(values, {"mode": mode.name, **request_context})
    raise AuthResolutionError(f"Unsupported auth mode `{mode.kind}`.")


def auth_evidence(
    *,
    mode: AuthMode,
    credentials: Mapping[str, CredentialValue],
    resolved: bool,
) -> AuthEvidence:
    return AuthEvidence(
        mode=mode.name,
        required_fields=credential_presence_fingerprint(mode, credentials),
        resolved=resolved,
    )


def redacted_auth_evidence(
    mode: AuthMode,
    credentials: Mapping[str, CredentialValue],
    *,
    resolved: bool,
) -> Mapping[str, object]:
    evidence = auth_evidence(mode=mode, credentials=credentials, resolved=resolved)
    return {
        "mode": evidence.mode,
        "required_fields": dict(evidence.required_fields),
        "resolved": evidence.resolved,
    }


def _placed_api_key(*, mode: AuthMode, value: str) -> ResolvedAuth:
    if mode.key is None or mode.location is None:
        raise DeclarationValidationError(f"Auth mode `{mode.name}` has invalid API-key placement.")
    if mode.location == "header":
        return ResolvedAuth(mode=mode.name, headers={mode.key: value})
    if mode.location == "query":
        return ResolvedAuth(mode=mode.name, query={mode.key: value})
    return ResolvedAuth(mode=mode.name, cookies={mode.key: value})


def _bearer_from_token_response(
    *,
    mode: AuthMode,
    response: Mapping[str, object],
    now: Callable[[], float] | None = None,
) -> ResolvedAuth:
    token = response.get(mode.access_token_field)
    if not isinstance(token, str) or not token:
        raise DeclarationValidationError(
            f"Auth mode `{mode.name}` token response missing `{mode.access_token_field}`."
        )
    expires_value = response.get(mode.expires_in_field)
    expires_in = int(expires_value) if isinstance(expires_value, int | str) else None
    token_expires_at = (now() + expires_in) if now is not None and expires_in is not None else None
    return ResolvedAuth(
        mode=mode.name,
        headers={"Authorization": f"Bearer {token}"},
        expires_in=expires_in,
        token_expires_at=token_expires_at,
    )


def _with_evidence(
    resolved_auth: ResolvedAuth,
    *,
    mode: AuthMode,
    credentials: Mapping[str, CredentialValue],
    resolved: bool,
) -> ResolvedAuth:
    return ResolvedAuth(
        mode=resolved_auth.mode,
        headers=resolved_auth.headers,
        query=resolved_auth.query,
        cookies=resolved_auth.cookies,
        context={
            **resolved_auth.context,
            "auth_evidence": redacted_auth_evidence(mode, credentials, resolved=resolved),
        },
        expires_in=resolved_auth.expires_in,
        token_expires_at=resolved_auth.token_expires_at,
    )


def _secret_env_name(name: str) -> str:
    segments = (_secret_env_segment(segment) for segment in name.split("."))
    return "__".join(segments).upper()


def _secret_env_segment(segment: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", segment)


def _non_empty(value: str, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeclarationValidationError(f"`{field_name}` must be a non-empty string.")
    return value


def _unique_strings(values: Sequence[str], *, field_name: str) -> tuple[str, ...]:
    normalized = tuple(_non_empty(value, field_name=field_name) for value in values)
    if len(set(normalized)) != len(normalized):
        raise DeclarationValidationError(f"`{field_name}` must not contain duplicates.")
    return normalized


def _validate_secret_shaped_credentials(
    mode: AuthMode,
    credentials: Mapping[str, CredentialValue],
) -> None:
    for field_name, value in credentials.items():
        if isinstance(value, SecretRef | SecretLiteral):
            continue
        raise DeclarationValidationError(
            f"Auth credential `{field_name}` for mode `{mode.name}` must be a SecretRef "
            "or SecretLiteral."
        )


__all__ = [
    "ApiKeyLocation",
    "AuthLocation",
    "AuthEvidence",
    "AuthMode",
    "AuthModeKind",
    "AuthResolutionError",
    "EnvironmentSecretResolver",
    "JwtSigner",
    "MappingSecretResolver",
    "MissingSecretError",
    "ResolvedAuth",
    "SecretResolver",
    "TokenRequest",
    "TokenTransport",
    "api_key",
    "apply_auth",
    "apply_http_auth",
    "auth_evidence",
    "basic",
    "bearer_token",
    "credential_presence_fingerprint",
    "custom",
    "native",
    "none",
    "oauth2_client_credentials",
    "oauth_jwt",
    "resolve_auth",
    "resolve_credentials",
    "redacted_auth_evidence",
    "select_auth_mode",
    "validate_credential_fields",
    "validate_required_credentials",
]
