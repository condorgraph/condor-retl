from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "checks"))

from _control_plane import (  # noqa: E402
    Report,
    format_report,
    markdown_links,
    resolve_link,
    validate_columnar_data_plane_runtime,
    validate_no_concrete_source_backend_leakage,
    validate_package_layering,
    validate_pyproject_surface,
    validate_root_retl_inactive,
    validate_sql_backed_collect_stage_boundary,
    validate_sql_backend_contracts,
    validate_sql_runtime_raw_sql_boundary,
)


def test_agents_markdown_links_resolve() -> None:
    agents = ROOT / "AGENTS.md"
    broken_links: list[str] = []

    for target in markdown_links(agents.read_text(encoding="utf-8")):
        resolved = resolve_link(agents, target)
        if resolved is not None and not resolved.exists():
            broken_links.append(target)

    assert broken_links == []


def test_root_pyproject_names_only_active_package_inputs() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"] == ["src/retl"]
    assert pyproject["tool"]["mypy"]["mypy_path"] == ["src", "tools/checks"]
    assert pyproject["tool"]["ruff"]["src"] == ["src", "tests", "tools"]
    assert pyproject["tool"]["pytest"]["ini_options"]["pythonpath"] == ["src"]


def test_connector_build_target_builds_active_package() -> None:
    result = subprocess.run(
        ["make", "build-destination-connector", "PACKAGE=meta"],
        cwd=ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert result.returncode == 0
    output = result.stdout + result.stderr
    assert "Successfully built" in output


def test_connector_publish_target_requires_package_argument() -> None:
    result = subprocess.run(
        ["make", "publish-destination-connector"],
        cwd=ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert result.returncode == 2
    assert "Usage: make publish-destination-connector PACKAGE=<connector-directory>" in (
        result.stdout
    )


def test_reference_http_connector_is_not_a_publish_target() -> None:
    result = subprocess.run(
        ["make", "publish-destination-connector", "PACKAGE=reference_http"],
        cwd=ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert result.returncode == 2
    assert "reference_http is repo-local and is not published to PyPI" in result.stdout


def test_architecture_validation_has_no_unavailable_surfaces() -> None:
    report = validate_package_layering(ROOT)

    assert report.unavailable == [], format_report(report)


def test_pyproject_validator_rejects_root_retl_package_discovery(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
[project]
name = "retl"
version = "0.1.0"
requires-python = ">=3.12,<3.15"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["retl"]

[tool.mypy]
mypy_path = ["tools/checks"]

[tool.ruff]
src = ["src", "tests", "tools"]

""",
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_pyproject_surface(report, pyproject)

    assert any("src/retl" in finding.rule for finding in report.failures)


def test_architecture_validator_rejects_active_root_retl_package(tmp_path: Path) -> None:
    retl_dir = tmp_path / "retl"
    retl_dir.mkdir()
    (retl_dir / "__init__.py").write_text("", encoding="utf-8")
    report = Report(title="test")

    validate_root_retl_inactive(report, retl_dir, tmp_path)

    assert any(
        "Root `retl/` must not contain active Python package files" in finding.rule
        for finding in report.failures
    )


def test_architecture_validator_scans_src_retl_columnar_runtime(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "src" / "retl" / "artifacts"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "fake_arrow.py").write_text('SENTINEL = b"ARROW\\n"\n', encoding="utf-8")
    report = Report(title="test")

    validate_columnar_data_plane_runtime(report, tmp_path)

    assert any("fake Arrow sentinel" in finding.problem for finding in report.failures)


def test_columnar_guardrail_rejects_row_object_page_contracts(tmp_path: Path) -> None:
    stores_dir = tmp_path / "src" / "retl" / "stores"
    stores_dir.mkdir(parents=True)
    (stores_dir / "contracts.py").write_text(
        """
from dataclasses import dataclass

@dataclass(frozen=True)
class PendingWorkPage:
    rows: tuple[object, ...]

@dataclass(frozen=True)
class StateCurrentPage:
    row_records: tuple[object, ...]
""",
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_columnar_data_plane_runtime(report, tmp_path)

    assert any("PendingWorkPage.rows" in finding.problem for finding in report.failures)
    assert any("StateCurrentPage.row_records" in finding.problem for finding in report.failures)


def test_columnar_guardrail_rejects_row_object_page_aliases(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "src" / "retl" / "runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "staging.py").write_text(
        """
class StageWorkPage:
    records: tuple[object, ...]

    def to_rows(self):
        return self.records

    def iter_rows(self):
        return iter(self.records)

    def as_rows(self):
        return tuple(self.records)
""",
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_columnar_data_plane_runtime(report, tmp_path)

    assert any("StageWorkPage.records" in finding.problem for finding in report.failures)
    assert any("StageWorkPage.to_rows" in finding.problem for finding in report.failures)
    assert any("StageWorkPage.iter_rows" in finding.problem for finding in report.failures)
    assert any("StageWorkPage.as_rows" in finding.problem for finding in report.failures)


def test_columnar_guardrail_rejects_page_row_iteration_and_construction(
    tmp_path: Path,
) -> None:
    runtime_dir = tmp_path / "src" / "retl" / "runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "reconcile.py").write_text(
        """
def reconcile_state(page):
    for row in page.rows:
        pass
    for row in page.records:
        pass
    for row in page.to_rows():
        pass
    return StateOperationPage(rows=(), records=())
""",
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_columnar_data_plane_runtime(report, tmp_path)

    assert any(
        "runtime iterates row-object page payloads" in finding.problem
        for finding in report.failures
    )
    assert any("to_rows" in finding.problem for finding in report.failures)
    assert any("StateOperationPage(rows=...)" in finding.problem for finding in report.failures)
    assert any("StateOperationPage(records=...)" in finding.problem for finding in report.failures)


def test_columnar_guardrail_rejects_row_operation_import_exports(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "src" / "retl" / "runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "__init__.py").write_text(
        """
from retl.runtime.reconcile import EventImportRow, StateOperationRow

__all__ = ["EventImportRow", "StateOperationRow"]
""",
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_columnar_data_plane_runtime(report, tmp_path)

    assert any(
        "row operation/import handoff export" in finding.problem for finding in report.failures
    )


def test_columnar_guardrail_rejects_duckdb_fetchmany_ordered_work_expansion(
    tmp_path: Path,
) -> None:
    stores_dir = tmp_path / "src" / "retl" / "backends" / "duckdb"
    stores_dir.mkdir(parents=True)
    (stores_dir / "store.py").write_text(
        """
def read_pending_work(result, max_rows):
    return tuple(_ordered_work_row_from_record(row) for row in result.fetchmany(max_rows + 1))

def read_current_state(result, max_rows):
    fetched = tuple(result.fetchmany(max_rows + 1))
    return tuple(OrderedWorkRow(record) for record in fetched)
""",
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_columnar_data_plane_runtime(report, tmp_path)

    assert any(
        "DuckDB staged read expands fetched pages into `OrderedWorkRow`" in finding.problem
        for finding in report.failures
    )


def test_architecture_validator_scans_src_retl_backend_leakage(tmp_path: Path) -> None:
    state_dir = tmp_path / "src" / "retl" / "state_runtime"
    state_dir.mkdir(parents=True)
    (state_dir / "leak.py").write_text('BACKEND = "snowflake"\n', encoding="utf-8")
    report = Report(title="test")

    validate_no_concrete_source_backend_leakage(report, tmp_path)

    assert any("concrete source backend leaked" in finding.problem for finding in report.failures)


def test_backend_leakage_validator_allows_backend_package_modules(tmp_path: Path) -> None:
    backend_dir = tmp_path / "src" / "retl" / "backends" / "duckdb"
    backend_dir.mkdir(parents=True)
    (backend_dir / "store.py").write_text('BACKEND = "duckdb"\n', encoding="utf-8")
    (backend_dir / "connection.py").write_text("import duckdb\n", encoding="utf-8")
    report = Report(title="test")

    validate_no_concrete_source_backend_leakage(report, tmp_path)

    assert not report.failures


def test_backend_driver_import_validator_rejects_duckdb_imports_outside_backend_package(
    tmp_path: Path,
) -> None:
    runtime_dir = tmp_path / "src" / "retl" / "runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "bad.py").write_text("import duckdb\n", encoding="utf-8")
    report = Report(title="test")

    validate_no_concrete_source_backend_leakage(report, tmp_path)

    assert any(
        "backend-specific driver import outside backend package boundary" in finding.problem
        and "`duckdb`" in finding.rule
        for finding in report.failures
    )


def test_backend_leakage_validator_allows_duckdb_backend_package_boundary(
    tmp_path: Path,
) -> None:
    connection_dir = tmp_path / "src" / "retl" / "backends" / "duckdb"
    connection_dir.mkdir(parents=True)
    (connection_dir / "connection.py").write_text("import duckdb\n", encoding="utf-8")
    report = Report(title="test")

    validate_no_concrete_source_backend_leakage(report, tmp_path)

    assert not report.failures


def test_backend_driver_import_validator_rejects_shared_runtime_driver_import(
    tmp_path: Path,
) -> None:
    runtime_dir = tmp_path / "src" / "retl" / "stores" / "sql_runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "bad_driver.py").write_text("import psycopg\n", encoding="utf-8")
    report = Report(title="test")

    validate_no_concrete_source_backend_leakage(report, tmp_path)

    assert any(
        "backend-specific driver import outside backend package boundary" in finding.problem
        for finding in report.failures
    )


def test_backend_driver_import_validator_rejects_wrong_backend_package_driver_import(
    tmp_path: Path,
) -> None:
    duckdb_backend_dir = tmp_path / "src" / "retl" / "backends" / "duckdb"
    duckdb_backend_dir.mkdir(parents=True)
    (duckdb_backend_dir / "bad_driver.py").write_text(
        'from importlib import import_module\n\nimport_module("snowflake.connector")\n',
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_no_concrete_source_backend_leakage(report, tmp_path)

    assert any(
        "backend-specific driver import outside backend package boundary" in finding.problem
        and "`snowflake`" in finding.rule
        for finding in report.failures
    )


def test_backend_package_boundary_rejects_concrete_backend_imports_from_shared_sql(
    tmp_path: Path,
) -> None:
    sql_dir = tmp_path / "src" / "retl" / "sql"
    sql_dir.mkdir(parents=True)
    (sql_dir / "bad_backend.py").write_text(
        "from retl.backends.duckdb import DUCKDB_DIALECT\n",
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_no_concrete_source_backend_leakage(report, tmp_path)

    assert any(
        "concrete backend package import in shared runtime module" in finding.problem
        for finding in report.failures
    )


def test_sqlglot_import_validator_rejects_import_outside_sql_generation_layer(
    tmp_path: Path,
) -> None:
    runtime_dir = tmp_path / "src" / "retl" / "runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "bad_sqlglot.py").write_text("from sqlglot import exp\n", encoding="utf-8")
    report = Report(title="test")

    validate_no_concrete_source_backend_leakage(report, tmp_path)

    assert any(
        "SQLGlot import outside shared SQL generation layer" in finding.problem
        for finding in report.failures
    )


def test_sqlglot_import_validator_allows_shared_sql_generation_layer(
    tmp_path: Path,
) -> None:
    sql_dir = tmp_path / "src" / "retl" / "sql"
    runtime_sql_dir = tmp_path / "src" / "retl" / "stores" / "sql_runtime"
    source_dir = tmp_path / "src" / "retl" / "sources"
    sql_dir.mkdir(parents=True)
    runtime_sql_dir.mkdir(parents=True)
    source_dir.mkdir(parents=True)
    (sql_dir / "contracts.py").write_text("from sqlglot import exp\n", encoding="utf-8")
    (runtime_sql_dir / "reads.py").write_text("from sqlglot import select\n", encoding="utf-8")
    (source_dir / "sql.py").write_text("from sqlglot import parse_one\n", encoding="utf-8")
    report = Report(title="test")

    validate_no_concrete_source_backend_leakage(report, tmp_path)

    assert not report.failures


def test_sql_runtime_raw_sql_boundary_rejects_transferable_execute_sql(
    tmp_path: Path,
) -> None:
    sql_runtime_dir = tmp_path / "src" / "retl" / "stores" / "sql_runtime"
    sql_runtime_dir.mkdir(parents=True)
    (sql_runtime_dir / "bad_read.py").write_text(
        '''
def get_progress(context):
    return context.connection.execute("""
        select position_json
        from {context.render_runtime_relation("destination_progress")}
        where declaration_name = ?
    """).fetchone()
''',
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_runtime_raw_sql_boundary(report, tmp_path)

    assert any(
        "raw transferable SQL" in finding.problem and finding.inspect_next == "docs/runtime.md"
        for finding in report.failures
    )


def test_sql_runtime_raw_sql_boundary_rejects_transferable_executemany_sql(
    tmp_path: Path,
) -> None:
    sql_runtime_dir = tmp_path / "src" / "retl" / "stores" / "sql_runtime"
    sql_runtime_dir.mkdir(parents=True)
    (sql_runtime_dir / "bad_batch_write.py").write_text(
        '''
def write_progress_rows(context, rows):
    context.connection.executemany("""
        insert into destination_progress (declaration_name, position_json)
        values (?, ?)
    """, rows)
''',
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_runtime_raw_sql_boundary(report, tmp_path)

    assert any(
        "raw transferable SQL" in finding.problem and finding.inspect_next == "docs/runtime.md"
        for finding in report.failures
    )


def test_sql_runtime_raw_sql_boundary_points_parse_failures_to_completed_plan(
    tmp_path: Path,
) -> None:
    sql_runtime_dir = tmp_path / "src" / "retl" / "stores" / "sql_runtime"
    sql_runtime_dir.mkdir(parents=True)
    (sql_runtime_dir / "bad_syntax.py").write_text("def broken(:\n", encoding="utf-8")
    report = Report(title="test")

    validate_sql_runtime_raw_sql_boundary(report, tmp_path)

    assert any(
        "Python syntax error prevents SQL runtime raw-SQL guardrail scan" in finding.problem
        and finding.inspect_next == "docs/runtime.md"
        for finding in report.failures
    )


def test_sql_runtime_raw_sql_boundary_allows_documented_backend_owned_surfaces(
    tmp_path: Path,
) -> None:
    sql_runtime_dir = tmp_path / "src" / "retl" / "stores" / "sql_runtime"
    sql_runtime_dir.mkdir(parents=True)
    (sql_runtime_dir / "collect.py").write_text(
        '''
def insert_state_upsert_work(context):
    ordered_work = context.render_runtime_relation("ordered_work")
    context.connection.execute(f"""
        insert into {ordered_work} (work_id, payload_json)
        select sha256(identity_json), json_object('source_name', source_name)
        from temp_rows
    """)
''',
        encoding="utf-8",
    )
    (sql_runtime_dir / "schema.py").write_text(
        """
def _run_additive_migrations(connection):
    connection.execute("alter table retl.ordered_work add column if not exists source_json text")
""",
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_runtime_raw_sql_boundary(report, tmp_path)

    assert not report.failures


def test_sql_collect_stage_validator_scans_src_retl_state_runtime(tmp_path: Path) -> None:
    state_dir = tmp_path / "src" / "retl" / "state_runtime"
    state_dir.mkdir(parents=True)
    (state_dir / "bad.py").write_text(
        """
def state_style_stage() -> None:
    read_source_stage_rows()
""",
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_backed_collect_stage_boundary(report, tmp_path)

    assert any(
        "state-style stage uses superseded" in finding.problem for finding in report.failures
    )


def test_duckdb_sql_backend_contract_rejects_attach_and_unqualified_runtime_writes(
    tmp_path: Path,
) -> None:
    store_dir = tmp_path / "src" / "retl" / "backends" / "duckdb"
    store_dir.mkdir(parents=True)
    (store_dir / "store.py").write_text(
        '''
class DuckDBRuntimeStore:
    def produce_state_collect(self):
        self._connection.execute("attach 'source.duckdb' as source_db")
        self._connection.execute("""
            insert into ordered_work (work_id)
            values ('work')
        """)
''',
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_backend_contracts(report, tmp_path)

    assert any("contains `ATTACH`" in finding.problem for finding in report.failures)
    assert any("not Runtime-relation-qualified" in finding.problem for finding in report.failures)


def test_duckdb_sql_backend_contract_rejects_shared_behavior_in_duckdb_facade(
    tmp_path: Path,
) -> None:
    store_dir = tmp_path / "src" / "retl" / "backends" / "duckdb"
    store_dir.mkdir(parents=True)
    (store_dir / "store.py").write_text(
        """
class DuckDBRuntimeStore:
    def produce_state_collect(self):
        return None
""",
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_backend_contracts(report, tmp_path)

    assert any(
        "DuckDB store facade reintroduces shared runtime behavior" in finding.problem
        for finding in report.failures
    )


def test_duckdb_sql_backend_contract_rejects_top_level_shared_helper_in_duckdb_facade(
    tmp_path: Path,
) -> None:
    store_dir = tmp_path / "src" / "retl" / "backends" / "duckdb"
    store_dir.mkdir(parents=True)
    (store_dir / "store.py").write_text(
        """
def duckdb(**kwargs):
    return DuckDBRuntimeStore(**kwargs)

def _persist_sync_report(context, report):
    return None
""",
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_backend_contracts(report, tmp_path)

    assert any(
        "DuckDB store facade reintroduces shared runtime behavior" in finding.problem
        and "_persist_sync_report" in finding.problem
        for finding in report.failures
    )


def test_sql_backend_contract_rejects_unqualified_quoted_runtime_writes(
    tmp_path: Path,
) -> None:
    store_dir = tmp_path / "src" / "retl" / "backends" / "duckdb"
    store_dir.mkdir(parents=True)
    (store_dir / "store.py").write_text(
        '''
class DuckDBRuntimeStore:
    def produce_state_collect(self):
        self._connection.execute("""
            delete from "state_current"
            where declaration_name = ?
        """)
''',
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_backend_contracts(report, tmp_path)

    assert any("not Runtime-relation-qualified" in finding.problem for finding in report.failures)


def test_sql_backend_contract_rejects_shared_runtime_unqualified_writes(
    tmp_path: Path,
) -> None:
    sql_runtime_dir = tmp_path / "src" / "retl" / "stores" / "sql_runtime"
    sql_runtime_dir.mkdir(parents=True)
    (sql_runtime_dir / "bad_write.py").write_text(
        '''
def write_ordered_work(context):
    context.connection.execute("""
        insert into ordered_work (work_id)
        values ('work')
    """)
''',
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_backend_contracts(report, tmp_path)

    assert any("not Runtime-relation-qualified" in finding.problem for finding in report.failures)


def test_sql_backend_contract_allows_shared_runtime_rendered_writes(
    tmp_path: Path,
) -> None:
    sql_runtime_dir = tmp_path / "src" / "retl" / "stores" / "sql_runtime"
    sql_runtime_dir.mkdir(parents=True)
    (sql_runtime_dir / "good_write.py").write_text(
        '''
def write_ordered_work(context):
    ordered_work_relation = context.render_runtime_relation("ordered_work")
    context.connection.execute(f"""
        insert into {ordered_work_relation} (work_id)
        values ('work')
    """)
''',
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_backend_contracts(report, tmp_path)

    assert not report.failures


def test_sql_backend_contract_rejects_wrong_schema_runtime_writes(
    tmp_path: Path,
) -> None:
    store_dir = tmp_path / "src" / "retl" / "backends" / "duckdb"
    store_dir.mkdir(parents=True)
    (store_dir / "store.py").write_text(
        '''
class DuckDBRuntimeStore:
    def produce_state_collect(self):
        self._connection.execute("""
            insert into source.ordered_work (work_id)
            values ('work')
        """)
''',
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_backend_contracts(report, tmp_path)

    assert any("not Runtime-relation-qualified" in finding.problem for finding in report.failures)


def test_sql_backend_contract_allows_schema_qualified_runtime_writes(
    tmp_path: Path,
) -> None:
    store_dir = tmp_path / "src" / "retl" / "backends" / "duckdb"
    store_dir.mkdir(parents=True)
    (store_dir / "store.py").write_text(
        '''
class DuckDBRuntimeStore:
    def _initialize_ordered_work_store(self):
        self._connection.execute(f"""
            insert into {self.schema}."ordered_work" (work_id)
            values ('work')
        """)
        self._connection.execute("drop table if exists temp.retl_state_collect_snapshot")
''',
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_backend_contracts(report, tmp_path)

    assert not report.failures


def test_sql_backend_contract_rejects_source_adapter_runtime_writes(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "src" / "retl" / "backends" / "duckdb"
    source_dir.mkdir(parents=True)
    (source_dir / "source.py").write_text(
        '''
class DuckDBSourceAdapter:
    def prepare_state_snapshot(self):
        self._connection.execute("""
            insert into retl.ordered_work (work_id)
            values ('work')
        """)
''',
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_backend_contracts(report, tmp_path)

    assert any(
        "Source adapter writes Runtime relations" in finding.problem for finding in report.failures
    )


def test_sql_backend_contract_rejects_non_duckdb_source_adapter_runtime_writes(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "src" / "retl" / "backends" / "snowflake"
    source_dir.mkdir(parents=True)
    (source_dir / "source.py").write_text(
        '''
class SnowflakeSourceAdapter:
    def prepare_state_snapshot(self):
        self._connection.execute("""
            insert into retl.ordered_work (work_id)
            values ('work')
        """)
''',
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_backend_contracts(report, tmp_path)

    assert any(
        "Snowflake Source adapter writes Runtime relations" in finding.problem
        for finding in report.failures
    )


def test_duckdb_sql_backend_contract_rejects_source_access_outside_collect(
    tmp_path: Path,
) -> None:
    store_dir = tmp_path / "src" / "retl" / "backends" / "duckdb"
    store_dir.mkdir(parents=True)
    (store_dir / "store.py").write_text(
        """
class DuckDBRuntimeStore:
    def read_pending_work(self, source_space):
        self._use_duckdb_source_schema(source_space)

    def _use_duckdb_source_schema(self, source_space):
        self._connection.execute("set schema 'source'")
""",
        encoding="utf-8",
    )
    report = Report(title="test")

    validate_sql_backend_contracts(report, tmp_path)

    assert any(
        "Source relation access appears outside collect" in finding.problem
        for finding in report.failures
    )
