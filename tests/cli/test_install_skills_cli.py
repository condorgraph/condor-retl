from __future__ import annotations

import json
from importlib import resources
from pathlib import Path

import pytest

from retl.cli import main as cli_main
from retl.cli.setup import USER_SKILL_NAMES


def test_install_skills_copies_all_packaged_user_skills(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    unrelated = project / ".agents" / "notes.md"
    unrelated.parent.mkdir()
    unrelated.write_text("keep me\n")

    code = cli_main(["install-skills", str(project)])

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["kind"] == "install-skills"
    assert payload["destination"] == str(project / ".agents" / "skills")
    assert payload["destinations"] == [
        str(project / ".agents" / "skills"),
        str(project / ".claude" / "skills"),
    ]
    assert set(_installed_skill_names(project / ".agents" / "skills")) == set(USER_SKILL_NAMES)
    assert set(_installed_skill_names(project / ".claude" / "skills")) == set(USER_SKILL_NAMES)
    assert unrelated.read_text() == "keep me\n"


def test_install_skills_is_idempotent_for_unchanged_files(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"

    assert cli_main(["install-skills", str(project)]) == 0
    capsys.readouterr()
    assert cli_main(["install-skills", str(project)]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["created"] == []
    assert payload["overwritten"] == []
    assert "retl-create-sync/SKILL.md" in payload["unchanged"]
    assert payload["by_destination"][str(project / ".claude" / "skills")]["created"] == []
    assert (
        "retl-create-sync/SKILL.md"
        in payload["by_destination"][str(project / ".claude" / "skills")]["unchanged"]
    )


def test_install_skills_overwrites_changed_skill_by_default(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    assert cli_main(["install-skills", str(project)]) == 0
    capsys.readouterr()
    changed = project / ".agents" / "skills" / "retl-create-sync" / "SKILL.md"
    changed.write_text("local change\n")

    code = cli_main(["install-skills", str(project)])

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert "retl-create-sync/SKILL.md" in payload["overwritten"]
    assert "name: retl-create-sync" in changed.read_text()
    claude_changed = project / ".claude" / "skills" / "retl-create-sync" / "SKILL.md"
    assert (
        "retl-create-sync/SKILL.md"
        in payload["by_destination"][str(claude_changed.parents[1])]["unchanged"]
    )
    assert "name: retl-create-sync" in claude_changed.read_text()


def test_install_skills_force_is_accepted_for_compatibility(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    assert cli_main(["install-skills", str(project)]) == 0
    capsys.readouterr()
    changed = project / ".agents" / "skills" / "retl-create-sync" / "SKILL.md"
    changed.write_text("local change\n")

    code = cli_main(["install-skills", str(project), "--force"])

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert "retl-create-sync/SKILL.md" in payload["overwritten"]
    assert "name: retl-create-sync" in changed.read_text()


def test_install_skills_supports_explicit_project_local_destination(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"

    code = cli_main(["install-skills", str(project), "--destination", ".retl/user-skills"])

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["destination"] == str(project / ".retl" / "user-skills")
    assert payload["destinations"] == [str(project / ".retl" / "user-skills")]
    assert set(_installed_skill_names(project / ".retl" / "user-skills")) == set(USER_SKILL_NAMES)
    assert not (project / ".agents" / "skills").exists()
    assert not (project / ".claude" / "skills").exists()


def test_install_skills_writes_through_claude_skills_symlink(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    (project / ".agents").mkdir(parents=True)
    (project / ".claude").mkdir()
    (project / ".claude" / "skills").symlink_to("../.agents/skills")

    code = cli_main(["install-skills", str(project)])

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["by_destination"][str(project / ".claude" / "skills")]["created"] == []
    assert set(_installed_skill_names(project / ".agents" / "skills")) == set(USER_SKILL_NAMES)
    assert set(_installed_skill_names(project / ".claude" / "skills")) == set(USER_SKILL_NAMES)


def test_packaged_user_skill_resources_are_available() -> None:
    skill_root = resources.files("retl.skills.user")

    for skill_name in USER_SKILL_NAMES:
        skill_file = skill_root / skill_name / "SKILL.md"
        assert skill_file.is_file()
        assert f"name: {skill_name}" in skill_file.read_text()


def test_installed_destination_skills_include_local_config_wiring_guidance() -> None:
    skill_root = resources.files("retl.skills.user")

    for skill_name in ("retl-start-project", "retl-use-destinations"):
        text = (skill_root / skill_name / "SKILL.md").read_text()

        assert "retl.toml" in text
        assert ".env" in text
        assert "root `.gitignore`" in text
        assert "local/" not in text
        assert "retl.local.toml" not in text
        assert ".env.example" not in text
        assert "retl.example.toml" not in text
        assert "retl.ChainedConfigResolver" in text
        assert "retl.TomlConfigResolver" in text
        assert "credential_namespace=" in text
        assert "config_namespace=" in text
        assert "Placeholder generation" in text
        assert "export DESTINATIONS__<DESTINATION_NAME>__<FIELD>=REPLACE_ME" in text
        assert "[destinations.<destination_name>]" in text


def test_installed_destination_skills_keep_destination_guidance_generic() -> None:
    skill_root = resources.files("retl.skills.user")

    for skill_name in ("retl-start-project", "retl-use-destinations"):
        text = (skill_root / skill_name / "SKILL.md").read_text()

        assert "Only include public config keys" in text
        assert "retl/meta" not in text
        assert "https://graph.facebook.com" not in text


def test_configure_backend_skill_defines_backend_defaults() -> None:
    skill_root = resources.files("retl.skills.user")
    text = (skill_root / "retl-configure-backend" / "SKILL.md").read_text()

    assert "name: retl-configure-backend" in text
    assert "[backends.duckdb]" in text
    assert 'database = "data/warehouse.duckdb"' in text
    assert 'source_schema = "main"' in text
    assert 'runtime_schema = "retl"' in text
    assert "one physical `.duckdb` file" in text
    assert "write the selected backend's `[backends.<backend>]`" in text
    assert "Do not leave backend config as comments" in text
    assert "[sources.<backend>]" in text
    assert "[runtime.<backend>]" in text
    assert "[sources.duckdb]" not in text
    assert "[runtime.duckdb]" not in text
    assert "[backends.postgresql]" in text
    assert 'sslmode = "require"' in text
    assert 'sslmode = "verify-full"' in text
    assert "[backends.snowflake]" in text
    assert "[backends.bigquery]" in text
    assert 'project = "REPLACE_ME"' in text
    assert "[backends.databricks]" in text


def test_installable_skills_reference_configure_backend_where_backend_placement_matters() -> None:
    skill_root = resources.files("retl.skills.user")

    for skill_name in (
        "retl-start-project",
        "retl-create-sync",
        "retl-use-destinations",
        "retl-runtime-operations",
    ):
        text = (skill_root / skill_name / "SKILL.md").read_text()
        assert "retl-configure-backend" in text


def _installed_skill_names(path: Path) -> list[str]:
    return sorted(child.name for child in path.iterdir() if (child / "SKILL.md").is_file())
