from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

import retl
from retl.backends.duckdb import DuckDBRuntimeStore, DuckDBSqlBackend
from retl.runtime.progress import destination_progress_scope
from retl.runtime.provenance import RunProvenance


def _store(tmp_path: Path) -> DuckDBRuntimeStore:
    return _backend(tmp_path).runtime_store()


def _backend(tmp_path: Path, *, source_schema: str = "main") -> DuckDBSqlBackend:
    return DuckDBSqlBackend(
        database=_warehouse_database(tmp_path),
        source_schema=source_schema,
        runtime_schema="retl",
    )


def _warehouse_database(tmp_path: Path) -> Path:
    return tmp_path / "warehouse.duckdb"


def _source(
    backend: DuckDBSqlBackend,
    *,
    rows: list[tuple[str, str, str]] | None = None,
) -> retl.Source:
    database = Path(backend.database)
    source_schema = backend.source_schema
    connection = duckdb.connect(str(database))
    if source_schema != "main":
        connection.execute(f"create schema if not exists {source_schema}")
    connection.execute(
        f"create table {source_schema}.customers (customer_id varchar, email varchar, plan varchar)"
    )
    connection.executemany(
        f"insert into {source_schema}.customers values (?, ?, ?)",
        rows or [("cust_1", "one@example.com", "pro")],
    )
    connection.close()
    query = (
        "select customer_id, email, plan from customers"
        if source_schema == "main"
        else f"select customer_id, email, plan from {source_schema}.customers"
    )
    return retl.source(
        name="customers",
        query=query,
        backend=backend.source_backend(),
    )


def _replace_source_rows(tmp_path: Path, rows: list[tuple[str, str, str]]) -> None:
    connection = duckdb.connect(str(_warehouse_database(tmp_path)))
    connection.execute("delete from customers")
    connection.executemany("insert into customers values (?, ?, ?)", rows)
    connection.close()


def _sync(declaration: retl.State, *, name: str = "customer_profiles") -> retl.Sync:
    return retl.sync(
        name=name,
        declaration=declaration,
        destination=retl.destinations.load(
            "retl/mock",
            binding_name="mock_profiles",
        ),
        surface="profile_properties",
    )


def test_runner_registers_one_run_and_declaration_metadata_durably(tmp_path: Path) -> None:
    database = _warehouse_database(tmp_path)
    backend = _backend(tmp_path)
    declaration = retl.state(
        name="customer_state",
        source=_source(backend),
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )
    store = backend.runtime_store()

    result = retl.runner(name="crm_to_lifecycle", runtime_store=store).run(_sync(declaration))
    run_id = result.run_id
    store.close()

    reopened = DuckDBRuntimeStore(database=database)
    run_rows = reopened._connection.execute(
        "select run_id, runner_name, status, dry_run from retl.runs"
    ).fetchall()
    declaration_rows = reopened._connection.execute(
        """
        select declaration_version_id, declaration_name, declaration_kind, declaration_json
        from retl.declarations
        """
    ).fetchall()

    assert run_rows == [(run_id, "crm_to_lifecycle", "succeeded", False)]
    assert declaration_rows[0][1:3] == ("customer_state", "state")
    declaration_json = json.loads(declaration_rows[0][3])
    assert declaration_json["name"] == "customer_state"


def test_runner_marks_registered_run_failed_when_later_step_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _backend(tmp_path)
    declaration = retl.state(
        name="customer_state",
        source=_source(backend),
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )
    store = backend.runtime_store()

    def fail_produce_state_collect(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("boom after run registration")

    monkeypatch.setattr(store, "produce_state_collect", fail_produce_state_collect)

    with pytest.raises(RuntimeError, match="boom after run registration"):
        retl.runner(name="crm_to_lifecycle", runtime_store=store).run(_sync(declaration))

    run_rows = store._connection.execute(
        "select runner_name, status, completed_at from retl.runs"
    ).fetchall()
    assert len(run_rows) == 1
    assert run_rows[0][0:2] == ("crm_to_lifecycle", "failed")
    assert run_rows[0][2] is not None


def test_duplicate_run_evidence_surfaces_primary_key_failure(tmp_path: Path) -> None:
    store = _store(tmp_path)
    run = RunProvenance(
        run_id="run-duplicate",
        runner_name="runner",
        dry_run=False,
        script_path=None,
        script_content_hash=None,
        started_at="2026-05-09T12:00:00+00:00",
    )

    store.register_run(run)

    with pytest.raises(Exception, match="Duplicate key|constraint|primary key|unique"):
        store.register_run(run)

    rows = store._connection.execute(
        "select run_id, runner_name from retl.runs where run_id = ?",
        [run.run_id],
    ).fetchall()
    assert rows == [("run-duplicate", "runner")]


def test_declaration_version_change_does_not_reset_destination_progress(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    source = _source(backend)
    first = retl.state(
        name="customer_state",
        source=source,
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )
    second = retl.state(
        name="customer_state",
        source=source,
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan", "email": "email"},
    )
    store = backend.runtime_store()
    first_sync = _sync(first)
    second_sync = _sync(second)

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(first_sync)
    before = store.get_destination_progress(destination_progress_scope(first_sync)).position
    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(second_sync)
    after = store.get_destination_progress(destination_progress_scope(second_sync)).position
    versions = store._connection.execute(
        "select count(*) from retl.declarations where declaration_name = ?",
        ["customer_state"],
    ).fetchone()[0]

    assert before is not None
    assert after is not None
    assert getattr(after, "collect_id", 0) >= getattr(before, "collect_id", 0)
    assert versions == 2


def test_state_collect_diff_uses_declaration_name(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    source = _source(
        backend,
        rows=[
            ("cust_1", "one@example.com", "pro"),
            ("cust_2", "two@example.com", "free"),
        ],
    )
    first = retl.state(
        name="customer_state",
        source=source,
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )
    second = retl.state(
        name="customer_state",
        source=source,
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )
    store = backend.runtime_store()

    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(_sync(first))
    _replace_source_rows(tmp_path, [("cust_1", "one@example.com", "pro")])
    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(_sync(second))

    work = store._connection.execute(
        """
        select kind, declaration_name
        from retl.ordered_work
        where kind = 'remove'
        order by sequence_order
        """
    ).fetchall()
    current = store._connection.execute(
        """
        select declaration_name, count(*)
        from retl.state_current
        group by declaration_name
        """
    ).fetchall()

    assert work == [("remove", "customer_state")]
    assert current == [("customer_state", 1)]


def test_source_location_change_creates_new_declaration_version(tmp_path: Path) -> None:
    first_backend = _backend(tmp_path, source_schema="source_a")
    second_backend = _backend(tmp_path, source_schema="source_b")
    first = retl.state(
        name="customer_state",
        source=_source(first_backend),
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )
    second = retl.state(
        name="customer_state",
        source=_source(second_backend),
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )
    first_store = first_backend.runtime_store()

    retl.runner(name="crm_to_lifecycle", runtime_store=first_store).run(_sync(first))
    first_store.close()

    store = second_backend.runtime_store()
    retl.runner(name="crm_to_lifecycle", runtime_store=store).run(_sync(second))

    versions = store._connection.execute(
        """
        select count(distinct declaration_version_id), count(distinct source_location_json)
        from retl.declarations
        where declaration_name = ?
        """,
        ["customer_state"],
    ).fetchone()
    assert versions == (2, 2)
