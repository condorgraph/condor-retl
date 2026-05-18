"""Source contracts used by runtime orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from retl.errors import DeclarationValidationError

if TYPE_CHECKING:
    from retl.declarations import Source


@dataclass(frozen=True)
class SourceCapabilities:
    snapshots: bool = False
    checkpointed_windows: bool = False

    @property
    def supports_snapshot(self) -> bool:
        return self.snapshots

    @property
    def supports_checkpointed(self) -> bool:
        return self.checkpointed_windows


def source_identity(source: "Source") -> str:
    from retl.sources.fixtures import source_identity as fixture_source_identity

    return fixture_source_identity(source)


def _checkpoint_columns(source: "Source") -> tuple[str, str]:
    if source.checkpoint is None:
        raise DeclarationValidationError("Checkpointed Source requires `checkpoint`.")
    return source.checkpoint["cursor"], source.checkpoint["primary_key"]


__all__ = [
    "SourceCapabilities",
    "source_identity",
]
