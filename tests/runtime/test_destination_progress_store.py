from __future__ import annotations

from pathlib import Path

import pytest

from retl.backends.duckdb import DuckDBRuntimeStore
from retl.errors import DeclarationValidationError
from retl.stores.contracts import (
    CanonicalKey,
    CanonicalKeyScalar,
    DestinationProgressScope,
    EventKeysetScanPosition,
    StateCurrentSnapshotScanPosition,
    StateOrderedWorkScanPosition,
)


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return DuckDBRuntimeStore(database=tmp_path / "runtime.duckdb")


def _scope(
    *,
    destination_name: str = "crm",
    surface: str = "profile_properties",
    declaration_name: str = "customer_state",
) -> DestinationProgressScope:
    return DestinationProgressScope(
        sync_name="customer_sync",
        destination_name=destination_name,
        surface=surface,
        family="state",
        declaration_name=declaration_name,
    )


def _collect_id(value: int | str) -> str:
    if isinstance(value, int):
        return f"00000000-{value:04x}-7000-8000-000000000000"
    return value


def _position(collect_id: int | str, sequence_order: int = 0) -> StateOrderedWorkScanPosition:
    return StateOrderedWorkScanPosition(
        collect_id=_collect_id(collect_id),
        sequence_order=sequence_order,
    )


def test_destination_progress_defaults_to_none_for_missing_and_registered_scope(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    scope = _scope()

    missing = store.get_destination_progress(scope)
    registered = store.register_destination_progress(scope)
    row_count = store._connection.execute(
        "select count(*) from retl.destination_progress"
    ).fetchone()[0]

    assert missing.position is None
    assert registered.position is None
    assert store.get_destination_progress(scope).position is None
    assert row_count == 0


def test_state_position_update_persists_and_round_trips_through_duckdb(
    tmp_path: Path,
) -> None:
    database = tmp_path / "runtime.duckdb"
    store = DuckDBRuntimeStore(database=database)
    scope = _scope()
    position = _position(collect_id="00000000-002a-7000-8000-000000000000", sequence_order=918)

    update = store.update_destination_progress(scope=scope, position=position)
    store.close()

    reopened = DuckDBRuntimeStore(database=database)

    assert update.before is None
    assert update.after == position
    assert update.advanced is True
    assert reopened.get_destination_progress(scope).position == position


def test_state_position_update_advances_legacy_null_progress_row(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    scope = _scope()
    position = _position(collect_id=9, sequence_order=12)
    store._connection.execute(
        """
        insert into retl.destination_progress (
            sync_name,
            destination_name,
            surface,
            family,
            declaration_name,
            position_json
        ) values (?, ?, ?, ?, ?, null)
        """,
        [
            scope.sync_name,
            scope.destination_name,
            scope.surface,
            scope.family,
            scope.declaration_name,
        ],
    )

    update = store.update_destination_progress(scope=scope, position=position)
    rows = store._connection.execute(
        """
        select position_json
        from retl.destination_progress
        where sync_name = ?
          and destination_name = ?
          and surface = ?
          and family = ?
          and declaration_name = ?
        """,
        [
            scope.sync_name,
            scope.destination_name,
            scope.surface,
            scope.family,
            scope.declaration_name,
        ],
    ).fetchall()

    assert update.before is None
    assert update.after == position
    assert update.advanced is True
    assert len(rows) == 1
    assert store.get_destination_progress(scope).position == position


def test_destination_progress_scopes_are_isolated(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first_scope = _scope(destination_name="crm")
    second_scope = _scope(destination_name="ads")

    store.update_destination_progress(scope=first_scope, position=_position(3, 7))

    assert store.get_destination_progress(first_scope).position == _position(3, 7)
    assert store.get_destination_progress(second_scope).position is None


def test_destination_progress_rejects_rollback(tmp_path: Path) -> None:
    store = _store(tmp_path)
    scope = _scope()

    store.update_destination_progress(scope=scope, position=_position(5, 10))

    with pytest.raises(DeclarationValidationError, match="cannot move behind"):
        store.update_destination_progress(scope=scope, position=_position(5, 9))

    assert store.get_destination_progress(scope).position == _position(5, 10)


def test_destination_progress_rejects_scope_family_mismatch(tmp_path: Path) -> None:
    store = _store(tmp_path)
    event_position = EventKeysetScanPosition(
        cursor_value=CanonicalKeyScalar.integer(5),
        primary_key_value=CanonicalKeyScalar.string("evt_5"),
    )

    with pytest.raises(DeclarationValidationError, match="family must match"):
        store.update_destination_progress(scope=_scope(), position=event_position)


def test_destination_progress_rejects_mode_mismatch(tmp_path: Path) -> None:
    store = _store(tmp_path)
    scope = _scope()

    store.update_destination_progress(scope=scope, position=_position(5, 0))

    with pytest.raises(DeclarationValidationError, match="modes cannot be mixed"):
        store.update_destination_progress(
            scope=scope,
            position=StateCurrentSnapshotScanPosition(
                key=CanonicalKey.of(CanonicalKeyScalar.string("cust_5"))
            ),
        )

    assert store.get_destination_progress(scope).position == _position(5, 0)


def test_pending_work_read_accepts_committed_state_ordered_position(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    scope = _scope()
    store.update_destination_progress(scope=scope, position=_position(5, 10))

    page = store.read_pending_work(scope=scope, max_rows=10)

    assert page.row_count == 0
