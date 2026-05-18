from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path

from retl.errors import DeclarationValidationError

USER_SKILL_NAMES = (
    "retl-start-project",
    "retl-configure-backend",
    "retl-create-sync",
    "retl-use-destinations",
    "retl-create-local-destination",
    "retl-debug-sync",
    "retl-runtime-operations",
    "retl-organize-project",
)
DEFAULT_SKILL_DESTINATION = Path(".agents") / "skills"
DEFAULT_CLAUDE_SKILL_DESTINATION = Path(".claude") / "skills"
DEFAULT_SKILL_DESTINATIONS = (
    DEFAULT_SKILL_DESTINATION,
    DEFAULT_CLAUDE_SKILL_DESTINATION,
)

_USER_SKILL_PACKAGE = "retl.skills.user"


@dataclass(frozen=True)
class CopySummary:
    created: tuple[str, ...]
    overwritten: tuple[str, ...]
    unchanged: tuple[str, ...]

    @property
    def changed(self) -> tuple[str, ...]:
        return (*self.created, *self.overwritten)


@dataclass(frozen=True)
class DestinationCopySummary(CopySummary):
    destination: Path


@dataclass(frozen=True)
class InstallSkillsSummary:
    destinations: tuple[Path, ...]
    by_destination: tuple[DestinationCopySummary, ...]

    @property
    def primary(self) -> DestinationCopySummary:
        return self.by_destination[0]

    @property
    def created(self) -> tuple[str, ...]:
        return self.primary.created

    @property
    def overwritten(self) -> tuple[str, ...]:
        return self.primary.overwritten

    @property
    def unchanged(self) -> tuple[str, ...]:
        return self.primary.unchanged

    @property
    def changed(self) -> tuple[str, ...]:
        return self.primary.changed


def install_user_skills(
    *,
    project_path: Path,
    destination: Path | None = None,
    force: bool = True,
) -> InstallSkillsSummary:
    requested_destinations = (
        (destination,) if destination is not None else DEFAULT_SKILL_DESTINATIONS
    )
    sources = []
    skill_root = resources.files(_USER_SKILL_PACKAGE)
    for skill_name in USER_SKILL_NAMES:
        skill_dir = skill_root / skill_name
        if not skill_dir.is_dir():
            raise DeclarationValidationError(f"packaged user skill `{skill_name}` is missing.")
        sources.extend(_resource_files(skill_dir, prefix=Path(skill_name)))
    summaries: list[DestinationCopySummary] = []
    for requested_destination in requested_destinations:
        skill_destination = _resolve_project_path(project_path, requested_destination)
        copy_summary = _copy_resources(sources, skill_destination, force=force)
        summaries.append(
            DestinationCopySummary(
                created=copy_summary.created,
                overwritten=copy_summary.overwritten,
                unchanged=copy_summary.unchanged,
                destination=skill_destination,
            )
        )
    return InstallSkillsSummary(
        destinations=tuple(summary.destination for summary in summaries),
        by_destination=tuple(summaries),
    )


def _resolve_project_path(project_path: Path, destination: Path) -> Path:
    if destination.is_absolute():
        return destination
    return project_path / destination


def _resource_files(root: Traversable, *, prefix: Path) -> list[tuple[Path, bytes]]:
    files: list[tuple[Path, bytes]] = []
    for child in sorted(root.iterdir(), key=lambda item: item.name):
        child_prefix = prefix / child.name
        if child.is_dir():
            files.extend(_resource_files(child, prefix=child_prefix))
            continue
        files.append((child_prefix, child.read_bytes()))
    return files


def _copy_resources(
    sources: list[tuple[Path, bytes]],
    destination_root: Path,
    *,
    force: bool,
) -> CopySummary:
    created: list[str] = []
    overwritten: list[str] = []
    unchanged: list[str] = []
    conflicts: list[str] = []

    for relative_path, data in sources:
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise DeclarationValidationError(
                f"packaged resource path `{relative_path.as_posix()}` is not project-relative."
            )
        target = destination_root / relative_path
        rendered = relative_path.as_posix()
        if target.exists():
            if target.is_dir():
                conflicts.append(f"{rendered} exists as a directory")
                continue
            if target.read_bytes() == data:
                unchanged.append(rendered)
                continue
            overwritten.append(rendered)
        else:
            created.append(rendered)

    if conflicts:
        joined = ", ".join(conflicts)
        raise DeclarationValidationError(
            f"refusing to overwrite existing project path(s): {joined}."
        )

    for relative_path, data in sources:
        target = destination_root / relative_path
        if target.exists() and target.is_file() and target.read_bytes() == data:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    return CopySummary(
        created=tuple(created),
        overwritten=tuple(overwritten),
        unchanged=tuple(unchanged),
    )
