from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Protocol

AttemptStatus = Literal["active", "blocked", "completed", "failed"]


@dataclass(frozen=True)
class AttemptIdentity:
    runner_name: str
    sync_name: str
    attempt_id: str


@dataclass(frozen=True)
class AttemptRecord:
    identity: AttemptIdentity
    status: AttemptStatus
    dry_run: bool


@dataclass(frozen=True)
class CommitDecisionRecord:
    attempt_id: str
    sync_name: str
    progress_advanced: bool
    reason: str


class AttemptRecoveryStore(Protocol):
    def begin_attempt(
        self,
        *,
        runner_name: str,
        sync_name: str,
        dry_run: bool,
    ) -> AttemptIdentity: ...

    def record_commit_decision(self, decision: CommitDecisionRecord) -> None: ...

    def complete_attempt(self, *, attempt_id: str, status: AttemptStatus) -> None: ...


@dataclass
class InMemoryAttemptRecoveryStore:
    """Deterministic test store for runtime attempts and recovery evidence."""

    attempts: list[AttemptRecord] = field(default_factory=list)
    commit_decisions: list[CommitDecisionRecord] = field(default_factory=list)
    _next_attempt_number: int = field(default=1, init=False, repr=False)

    def begin_attempt(
        self,
        *,
        runner_name: str,
        sync_name: str,
        dry_run: bool,
    ) -> AttemptIdentity:
        attempt_id = f"{runner_name}:{sync_name}:attempt-{self._next_attempt_number}"
        self._next_attempt_number += 1
        identity = AttemptIdentity(
            runner_name=runner_name,
            sync_name=sync_name,
            attempt_id=attempt_id,
        )
        self.attempts.append(
            AttemptRecord(
                identity=identity,
                status="active",
                dry_run=dry_run,
            )
        )
        return identity

    def record_commit_decision(self, decision: CommitDecisionRecord) -> None:
        self.commit_decisions.append(decision)

    def complete_attempt(self, *, attempt_id: str, status: AttemptStatus) -> None:
        self.attempts = [
            AttemptRecord(
                identity=record.identity,
                status=status if record.identity.attempt_id == attempt_id else record.status,
                dry_run=record.dry_run,
            )
            for record in self.attempts
        ]


__all__ = [
    "AttemptIdentity",
    "AttemptRecoveryStore",
    "AttemptRecord",
    "AttemptStatus",
    "CommitDecisionRecord",
    "InMemoryAttemptRecoveryStore",
]
