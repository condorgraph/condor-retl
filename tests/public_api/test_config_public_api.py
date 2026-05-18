from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

import retl
from retl.config import ConfigRegistry


@pytest.fixture(autouse=True)
def reset_config_resolver() -> Iterator[None]:
    retl.configure(runtime_store=None, config_resolver=None, secret_resolver=None)
    yield
    retl.configure(runtime_store=None, config_resolver=None, secret_resolver=None)


def test_public_config_resolves_environment_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DESTINATIONS__ADS_API__ACCOUNT_ID", "acct_123")

    assert retl.config["destinations.ads_api.account_id"] == "acct_123"


def test_environment_config_resolver_does_not_accept_prefix() -> None:
    with pytest.raises(TypeError):
        retl.EnvironmentConfigResolver(prefix="RETL_CONFIG__")  # type: ignore[call-arg]


def test_public_config_uses_double_underscore_hierarchy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DESTINATIONS__ADS_API__ACCOUNT_ID", "ads-api-account")
    monkeypatch.setenv("DESTINATIONS__ADS__API__ACCOUNT_ID", "nested-account")

    assert retl.config["destinations.ads_api.account_id"] == "ads-api-account"
    assert retl.config["destinations.ads.api.account_id"] == "nested-account"


def test_public_config_get_returns_none_or_default_when_missing() -> None:
    assert retl.config.get("destinations.ads_api.account_id") is None
    assert retl.config.get("destinations.ads_api.account_id", "default-account") == (
        "default-account"
    )


def test_public_config_required_lookup_names_key_and_env_var_when_missing() -> None:
    with pytest.raises(retl.ConfigResolutionError) as exc_info:
        retl.config["destinations.ads_api.account_id"]

    message = str(exc_info.value)
    assert "destinations.ads_api.account_id" in message
    assert "DESTINATIONS__ADS_API__ACCOUNT_ID" in message
    assert "EnvironmentConfigResolver" in message


def test_public_config_can_use_configured_mapping_resolver() -> None:
    retl.configure(
        config_resolver=retl.MappingConfigResolver(
            {"destinations.ads_api.account_id": "acct_from_mapping"}
        )
    )

    assert retl.config["destinations.ads_api.account_id"] == "acct_from_mapping"
    assert retl.config.get("destinations.ads_api.missing") is None


def test_configure_accepts_runtime_store_without_runtime_type_import() -> None:
    from retl.config import configured_runtime_store

    store = object()

    retl.configure(runtime_store=store)

    assert configured_runtime_store() is store


def test_public_config_can_use_toml_resolver(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [destinations.ads_api]
        account_id = "acct_from_toml"
        """,
    )
    retl.configure(config_resolver=retl.TomlConfigResolver(path))

    assert retl.config["destinations.ads_api.account_id"] == "acct_from_toml"


def test_toml_config_get_returns_none_when_missing(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [destinations.ads_api]
        account_id = "acct_from_toml"
        """,
    )
    retl.configure(config_resolver=retl.TomlConfigResolver(path))

    assert retl.config.get("destinations.ads_api.missing") is None


def test_toml_config_required_lookup_uses_resolver_neutral_missing_message(
    tmp_path: Path,
) -> None:
    path = _write_toml(
        tmp_path,
        """
        [destinations.ads_api]
        account_id = "acct_from_toml"
        """,
    )
    retl.configure(config_resolver=retl.TomlConfigResolver(path))

    with pytest.raises(retl.ConfigResolutionError) as exc_info:
        retl.config["destinations.ads_api.missing"]

    message = str(exc_info.value)
    assert "destinations.ads_api.missing" in message
    assert "DESTINATIONS__ADS_API__MISSING" in message
    assert "for environment variable" not in message


def test_chained_config_resolver_falls_back_to_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _write_toml(
        tmp_path,
        """
        [destinations.ads_api]
        account_id = "acct_from_toml"
        """,
    )
    monkeypatch.setenv("DESTINATIONS__ADS_API__API_VERSION", "stable-api-version")
    retl.configure(
        config_resolver=retl.ChainedConfigResolver(
            retl.TomlConfigResolver(path),
            retl.EnvironmentConfigResolver(),
        )
    )

    assert retl.config["destinations.ads_api.account_id"] == "acct_from_toml"
    assert retl.config["destinations.ads_api.api_version"] == "stable-api-version"


def test_toml_config_scalar_values_are_returned_as_strings(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [destinations.ads_api]
        account_id = 123
        enabled = true
        poll_interval = 0.5
        """,
    )
    retl.configure(config_resolver=retl.TomlConfigResolver(path))

    assert retl.config["destinations.ads_api.account_id"] == "123"
    assert retl.config["destinations.ads_api.enabled"] == "true"
    assert retl.config["destinations.ads_api.poll_interval"] == "0.5"


def test_toml_config_non_scalar_value_fails(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [destinations.ads_api]
        account_ids = ["123"]
        """,
    )
    retl.configure(config_resolver=retl.TomlConfigResolver(path))

    with pytest.raises(retl.ConfigResolutionError, match="string, integer, number, or boolean"):
        retl.config["destinations.ads_api.account_ids"]


def test_toml_config_path_through_non_table_fails(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [destinations]
        ads_api = "not a table"
        """,
    )
    retl.configure(config_resolver=retl.TomlConfigResolver(path))

    with pytest.raises(retl.ConfigResolutionError, match="crosses non-table"):
        retl.config["destinations.ads_api.account_id"]


def test_toml_resolver_fails_for_malformed_or_missing_file(tmp_path: Path) -> None:
    malformed_path = tmp_path / "malformed.toml"
    malformed_path.write_text("[destinations.ads_api\naccount_id = 'acct'\n")

    with pytest.raises(retl.ConfigResolutionError, match="Unable to load TOML public config file"):
        retl.TomlConfigResolver(malformed_path)
    with pytest.raises(retl.ConfigResolutionError, match="Unable to load TOML secret file"):
        retl.TomlSecretResolver(tmp_path / "missing.toml")


def test_config_submodule_import_does_not_replace_root_registry() -> None:
    from retl.config import ChainedConfigResolver, MappingConfigResolver, TomlConfigResolver

    assert MappingConfigResolver is retl.MappingConfigResolver
    assert ChainedConfigResolver is retl.ChainedConfigResolver
    assert TomlConfigResolver is retl.TomlConfigResolver
    assert isinstance(retl.config, ConfigRegistry)


def test_configured_secret_resolver_uses_mapping_before_environment_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DESTINATIONS__PRIVATE__ACCESS_TOKEN", "env-token")
    monkeypatch.setenv("DESTINATIONS__FALLBACK__ACCESS_TOKEN", "fallback-token")
    retl.configure(
        secret_resolver=retl.auth.MappingSecretResolver(
            {"destinations.private.access_token": "configured-token"}
        )
    )

    resolver = retl.configured_secret_resolver()

    assert resolver.resolve(retl.secrets["destinations.private.access_token"]) == (
        "configured-token"
    )
    assert resolver.resolve(retl.secrets["destinations.fallback.access_token"]) == (
        "fallback-token"
    )


def test_toml_secret_resolver_resolves_secret_refs(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [destinations.ads_api]
        access_token = "toml-token"
        """,
    )
    retl.configure(secret_resolver=retl.TomlSecretResolver(path))

    resolver = retl.configured_secret_resolver()

    assert resolver.resolve(retl.secrets["destinations.ads_api.access_token"]) == "toml-token"


def test_toml_secret_resolver_falls_back_to_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _write_toml(
        tmp_path,
        """
        [destinations.ads_api]
        access_token = "toml-token"
        """,
    )
    monkeypatch.setenv("DESTINATIONS__FALLBACK__ACCESS_TOKEN", "env-token")
    retl.configure(secret_resolver=retl.TomlSecretResolver(path))

    resolver = retl.configured_secret_resolver()

    assert resolver.resolve(retl.secrets["destinations.fallback.access_token"]) == "env-token"


def test_toml_secret_resolver_reports_missing_when_all_providers_miss(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [destinations.ads_api]
        access_token = "toml-token"
        """,
    )
    retl.configure(secret_resolver=retl.TomlSecretResolver(path))

    with pytest.raises(retl.auth.AuthResolutionError, match="Missing secret"):
        retl.configured_secret_resolver().resolve(retl.secrets["destinations.missing.access_token"])


@pytest.mark.parametrize(
    ("toml_value", "message"),
    [
        ("123", "must be a string"),
        ('""', "must be a non-empty string"),
    ],
)
def test_toml_secret_resolver_rejects_invalid_values(
    tmp_path: Path,
    toml_value: str,
    message: str,
) -> None:
    path = _write_toml(
        tmp_path,
        f"""
        [destinations.ads_api]
        access_token = {toml_value}
        """,
    )
    retl.configure(secret_resolver=retl.TomlSecretResolver(path))

    with pytest.raises(retl.auth.AuthResolutionError, match=message):
        retl.configured_secret_resolver().resolve(retl.secrets["destinations.ads_api.access_token"])


def test_toml_secret_resolver_is_exported_from_config_submodule() -> None:
    from retl.config import TomlSecretResolver

    assert TomlSecretResolver is retl.TomlSecretResolver


def _write_toml(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "retl.toml"
    path.write_text(content)
    return path
