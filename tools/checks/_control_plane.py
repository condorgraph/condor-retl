from __future__ import annotations

import ast
import hashlib
import re
import tomllib
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

DOCS_RELATIVE_ROOT = Path("docs")

ROOT_NAVIGATION_EXPECTATIONS = {
    "index.md": [
        "control-plane.md",
        "core-beliefs.md",
        "product.md",
        "runtime.md",
        "recovery.md",
        "data-plane-types.md",
        "canonical-model.md",
        "destinations.md",
        "examples.md",
        "appendices.md",
        "reference-mapping.md",
        "plans/index.md",
    ],
    "plans/index.md": [
        "deferred-work.md",
        "roadmap.md",
    ],
}

REQUIRED_COMPACT_DOCS = tuple(
    Path(path)
    for path in (
        "index.md",
        "control-plane.md",
        "core-beliefs.md",
        "product.md",
        "runtime.md",
        "recovery.md",
        "data-plane-types.md",
        "canonical-model.md",
        "destinations.md",
        "examples.md",
        "appendices.md",
        "reference-mapping.md",
        "plans/index.md",
    )
)

TEMPORARY_DOCS_EXCLUDED_ROOTS = (Path("plans/active"),)

REFERENCE_BOUNDARY_MARKERS = (
    "support material",
    "support-only",
    "stable policy lives in",
    "normative policy lives in",
    "durable rules live in",
    "normative rules live in",
    "not a second policy home",
    "policy source of truth",
    "stable docs",
    "stable design docs",
)

DESIGN_CONTRACT_PHRASES = {
    "control-plane.md": [
        "core runtime code belongs only under `src/retl/`",
        "first-party publishable destination-package code belongs only under `destination_connectors/`",
        "core runtime must not import concrete source or destination packages",
        "destination_connectors/",
        "package manager:",
        "uv.lock",
        "makefile",
        "hatchling",
        "semantic versioning",
        "canonical trunk branch is `main`",
        "short-lived branches that target `main`",
        "recommended branch prefixes are `feat/`, `fix/`, `chore/`, and `docs/`",
        "feature toggles",
        "`docs/index.md` is the durable docs entrypoint",
        "compact root pages are the primary source-of-truth surface",
        "repository-local, reproducible, machine-checkable validation paths",
        "dry-run runner execution",
        "destination-definition loading",
        "simulators",
        "logs and inspection artifacts",
    ],
}

REQUIRED_REPO_CONTRACT_SURFACES = [
    "pyproject.toml",
    "uv.lock",
    "Makefile",
    "CONTRIBUTING.md",
    "LICENSE.txt",
    ".github/workflows",
    ".agents/skills",
    ".claude/skills",
    "src/retl",
    "destination_connectors",
]

DESTINATION_CONNECTOR_APACHE_LICENSE_SHA256 = (
    "7e102f4f47573046438924041e360f96c6da1d6826ac09b35bb272607146c32b"
)

VALIDATION_PATH_SURFACES = (
    {
        "subject": "tests/common",
        "path": "tests/common",
        "description": "repository-local implementation test path",
    },
    {
        "subject": "tests/fixtures",
        "path": "tests/fixtures",
        "description": "deterministic fixture path",
    },
)

SCAFFOLD_TRIGGER_SURFACES = (
    "pyproject.toml",
    "uv.lock",
    "Makefile",
    ".github/workflows",
    ".agents/skills",
    ".claude/skills",
    "src/retl",
    "destination_connectors",
)

REQUIRED_REPO_SKILL_NAME = "retl-create-destination"

REQUIRED_USER_SKILL_NAMES = (
    "retl-start-project",
    "retl-configure-backend",
    "retl-create-sync",
    "retl-use-destinations",
    "retl-create-local-destination",
    "retl-debug-sync",
    "retl-runtime-operations",
    "retl-organize-project",
)

REPO_SKILL_SYMLINKS = {
    ".claude/skills": "../.agents/skills",
}

VALIDATION_SCAFFOLD_TRIGGER_SURFACES = (
    "pyproject.toml",
    "src/retl",
    "destination_connectors",
    "tests/common",
    "tests/fixtures",
)

SYNC_PROOF_ARTIFACTS = (
    "resolved_targets.json",
    "submissions.jsonl",
    "receipts/summary.json",
    "receipts/succeeded.jsonl",
    "receipts/accepted.jsonl",
    "receipts/failed.jsonl",
    "receipts/pending.jsonl",
)

DESTINATION_VERSIONING_DOC_SURFACES = (
    "docs/product.md",
    "docs/runtime.md",
    "docs/destinations.md",
    "docs/examples.md",
)

DESTINATION_VERSIONING_FIXTURE_SURFACES = ("tests/fixtures/cli/destinations.json",)

DESTINATION_COMPATIBILITY_REQUIRED_FIELDS = (
    "supported_retl_versions",
    "destination_definition_fingerprint",
)

SUPPORTED_SURFACE_LANGUAGE_ROOTS = (
    "docs",
    "src",
    "destination_connectors",
    "tests",
    "tools",
    "README.md",
    "ARCHITECTURE.md",
)

SUPPORTED_SURFACE_LANGUAGE_EXCLUDED_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".uv-cache",
    ".venv",
    "__pycache__",
    "docs_v2",
    "dist",
    "reference",
}

SUPPORTED_SURFACE_LANGUAGE_ALLOWED_PREFIXES: tuple[Path, ...] = ()

SUPPORTED_SURFACE_LANGUAGE_SUFFIXES = {
    ".md",
    ".py",
    ".json",
    ".toml",
    ".yaml",
    ".yml",
}

GENERIC_ROADMAP_VERSION_PATTERN = re.compile(r"\bv[0-9]\b")

REQUIRED_MAKE_TARGETS = (
    "dev",
    "format",
    "format-check",
    "lint",
    "typecheck",
    "lint-lock",
    "test",
    "test-common",
    "check",
    "build-library",
    "build-destination-connector",
    "publish-library",
    "publish-destination-connector",
)

REQUIRED_WORKFLOW_FILES = (
    ".github/workflows/main.yml",
    ".github/workflows/lint.yml",
    ".github/workflows/test_common.yml",
)

SUPPORTED_PYTHON_VERSIONS = ("3.12", "3.13", "3.14")

TRUNK_BRANCH = "main"

CLEANUP_SCAN_ROOTS = (
    "src",
    "destination_connectors",
    "tests",
    "docs/control-plane.md",
    "docs/product.md",
    "docs/runtime.md",
    "docs/recovery.md",
    "docs/data-plane-types.md",
    "docs/canonical-model.md",
    "docs/destinations.md",
    "docs/examples.md",
    "docs/appendices.md",
    "docs/plans",
    "README.md",
    "AGENTS.md",
    "CONTRIBUTING.md",
)

CLEANUP_SCAN_SUFFIXES = {
    ".md",
    ".py",
    ".toml",
    ".yaml",
    ".yml",
}

CLEANUP_EXCLUDED_PARTS = SUPPORTED_SURFACE_LANGUAGE_EXCLUDED_PARTS | {
    "fixtures",
}

DEFERRED_OBLIGATION_PATTERN = re.compile(r"\b(?:TODO|FIXME|XXX)\b")

FEATURE_TOGGLE_MARKERS = (
    "feature toggle",
    "feature-toggle",
    "feature_toggle",
    "compatibility bridge",
    "compatibility-bridge",
    "compatibility_bridge",
)

FEATURE_TOGGLE_SCAN_ROOTS = (
    "src/retl",
    "destination_connectors",
)

RUNNER_VOCABULARY_ALLOWED_PATHS = {
    "docs/reference-mapping.md",
}

RUNNER_VOCABULARY_SCAN_SUFFIXES = {
    ".md",
    ".py",
    ".json",
    ".toml",
    ".yaml",
    ".yml",
}

RUNNER_VOCABULARY_EXCLUDED_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".uv-cache",
    ".venv",
    "__pycache__",
    "dist",
    "ref",
    "reference",
}

_OLD_EXECUTION_TERM = "de" + "ployment"
RUNNER_VOCABULARY_PATTERN = re.compile(re.escape(_OLD_EXECUTION_TERM), re.I)

AUTHORITATIVE_ARROW_RUNTIME_DIRS = (
    "src/retl/artifacts",
    "src/retl/sources",
    "src/retl/stores",
    "src/retl/backends",
    "src/retl/state_runtime",
    "src/retl/sync_runtime",
    "src/retl/runtime",
    "src/retl/destinations",
)

FAKE_ARROW_SENTINELS = (
    'b"ARROW\\n"',
    "b'ARROW\\n'",
)

ROW_OBJECT_PAGE_CONTRACT_RUNTIME_ROOTS = (
    "src/retl/stores",
    "src/retl/backends",
    "src/retl/runtime",
    "src/retl/state_runtime",
    "src/retl/events",
)

ROW_OBJECT_PAGE_CONTRACT_CLASSES = frozenset(
    {
        "PendingWorkPage",
        "StateCurrentPage",
        "StageWorkPage",
        "StateOperationPage",
        "EventImportPage",
    }
)

ROW_OBJECT_COMPATIBILITY_API_NAMES = frozenset(
    {"rows", "to_rows", "iter_rows", "as_rows", "row_records", "records"}
)

ROW_OBJECT_HANDOFF_EXPORT_NAMES = frozenset({"StateOperationRow", "EventImportRow"})

STORE_STAGE_RECONCILE_MODULE_TERMS = frozenset({"store", "stores", "staging", "stage", "reconcile"})

CALLBACK_FREE_RUNTIME_BANNED_IDENTIFIERS = {
    "CollectedRow",
    "CollectedValue",
    "LegacyMembershipSelection",
    "collect_rows",
    "entity_fn",
    "event_fn",
    "consent_fn",
    "membership_fn",
}

CALLBACK_FREE_CONNECTOR_PACKAGE_MARKERS = {
    "retl_meta",
    "retl_reference_http",
}

DESTINATION_ACCUMULATION_MARKERS = (
    "canonical_mutation_chunks_from_arrow_chunks(mutation_chunks)",
    "iter_canonical_mutations_from_arrow_chunks(mutation_chunks)",
)

DESTINATION_CANONICAL_MUTATION_PAYLOAD_PATTERNS = (
    r'["\']mutations["\']\s*:',
    r'\.get\(\s*["\']mutations["\']',
    r"\[[\"\']mutations[\"\']\]",
)

PUBLIC_RESULT_SURFACE_FILES = (
    "src/retl/runtime/runner.py",
    "src/retl/runtime/__init__.py",
    "src/retl/runtime/results.py",
)

PUBLIC_RESULT_DATACLASS_NAMES = frozenset(
    {
        "RunResult",
        "RunManyResult",
        "SyncRunResult",
        "CollectPhaseResult",
        "StagePhaseResult",
        "ReconcileBranchResult",
        "ReconcileManyPhaseResult",
        "SyncPhaseResult",
        "HydratedSyncResult",
        "ReceiptDiagnosticSample",
        "RemoteTargetSummary",
        "RetryHint",
        "SyncArtifactSummary",
        "SyncDiagnostics",
        "SyncReceiptArtifact",
        "SyncReceiptSummary",
    }
)

PUBLIC_RESULT_FORBIDDEN_RECEIPT_FIELD_NAMES = frozenset(
    {
        "full_receipts",
        "raw_receipts",
        "receipt_file_contents",
        "receipt_jsonl",
        "receipt_lines",
        "receipt_payloads",
        "receipt_records",
        "receipts",
    }
)

PUBLIC_RESULT_COLLECTION_TYPE_NAMES = frozenset(
    {
        "Sequence",
        "Iterable",
        "Iterator",
        "Mapping",
        "MutableMapping",
        "dict",
        "frozenset",
        "list",
        "set",
        "tuple",
    }
)

PUBLIC_RESULT_SAFE_RECEIPT_METADATA_ANNOTATION_TERMS = frozenset(
    {
        "Artifact",
        "Count",
        "Diagnostic",
        "Path",
        "Sample",
        "Summary",
    }
)

SYNC_PRODUCTION_ROW_MATERIALIZATION_METHODS = frozenset(
    {
        "to_pydict",
        "to_pylist",
    }
)

BOUNDED_SYNC_ROW_MATERIALIZATION_ALLOWLIST = frozenset(
    {
        ("src/retl/destinations/base.py", "canonical_mutation_chunks_from_arrow_chunks"),
        ("src/retl/destinations/request_batch.py", "_records_from_page"),
        (
            "src/retl/destinations/toolkit/payload_chunks.py",
            "canonical_mutation_chunks_from_arrow_chunks",
        ),
    }
)

REQUEST_BATCH_HTTP_OLD_AUTHORING_METHODS = frozenset(
    {
        "validate_sync_request",
        "resolve_targets",
        "build_submission",
        "execute_submission",
        "poll_submission",
    }
)

REQUEST_BATCH_HTTP_PACKAGE_LOCAL_FRAMEWORK_HELPERS = frozenset(
    {
        "RequestBatchHttpReceiptPolicy",
        "RequestBatchHttpSubmissionSpec",
        "build_request_batch_dry_run_receipt",
        "build_request_batch_http_chunk_payload",
        "build_request_batch_payload_record",
        "build_request_batch_submission_plan",
    }
)

REQUEST_BATCH_HTTP_LIFECYCLE_MARKERS = REQUEST_BATCH_HTTP_PACKAGE_LOCAL_FRAMEWORK_HELPERS | {
    "request_batch",
    "RequestBatchHttp",
}

DESTINATION_CONNECTOR_CONFIG_BASE_URL_EXCEPTIONS = frozenset(
    {
        "destination_connectors/reference_http",
    }
)

PUBLIC_RUNNER_VOCABULARY_SURFACES = (
    "README.md",
    "AGENTS.md",
    "ARCHITECTURE.md",
    "CONTRIBUTING.md",
    "docs/control-plane.md",
    "docs/product.md",
    "docs/runtime.md",
    "docs/recovery.md",
    "docs/data-plane-types.md",
    "docs/canonical-model.md",
    "docs/destinations.md",
    "docs/examples.md",
    "docs/appendices.md",
    "src/retl/__init__.py",
    "src/retl/cli",
)

SQL_SOURCE_STATE_PRODUCTION_ROOTS = (
    "src/retl/runtime",
    "src/retl/state_runtime",
    "src/retl/sources",
    "src/retl/stores",
)


@dataclass(frozen=True)
class Finding:
    subject: str
    problem: str
    why: str
    rule: str
    inspect_next: str


@dataclass
class Report:
    title: str
    failures: list[Finding] = field(default_factory=list)
    unavailable: list[Finding] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def docs_root(root: Path | None = None) -> Path:
    return (root or repo_root()) / DOCS_RELATIVE_ROOT


def reference_archive_mode(root: Path) -> bool:
    archive = root / "reference" / "legacy-retl"
    return (
        archive.is_dir()
        and (archive / "retl").is_dir()
        and (archive / "destination_connectors").is_dir()
        and (archive / "tests").is_dir()
    )


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def exists_or_symlink(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def markdown_links(text: str) -> list[str]:
    links: list[str] = []
    for match in re.finditer(r"(?<!!)\[([^\]]+)\]\(([^)]+)\)", text):
        target = match.group(2).strip()
        if target.startswith("<") and target.endswith(">"):
            target = target[1:-1].strip()
        target = target.split()[0]
        links.append(target)
    return links


def split_link_target(target: str) -> str:
    clean = target.strip()
    if clean.startswith("<") and clean.endswith(">"):
        clean = clean[1:-1].strip()
    clean = clean.split("#", 1)[0]
    clean = clean.split("?", 1)[0]
    return clean


def is_external_link(target: str) -> bool:
    clean = target.strip().lower()
    return clean.startswith(("http://", "https://", "mailto:", "ftp://", "javascript:"))


def resolve_link(source: Path, target: str) -> Path | None:
    clean = split_link_target(target)
    if not clean or clean.startswith("#") or is_external_link(clean):
        return None
    return (source.parent / clean).resolve()


def resolve_docs_link(source: Path, target: str, docs_root_dir: Path) -> Path | None:
    resolved = resolve_link(source, target)
    if resolved is None:
        return None
    try:
        resolved.relative_to(docs_root_dir.resolve())
    except ValueError:
        return None

    if resolved.is_dir():
        index = resolved / "index.md"
        return index if index.exists() else None

    return resolved if resolved.suffix == ".md" else None


def relative_docs_path(path: Path, root: Path | None = None) -> str:
    docs = docs_root(root)
    try:
        return str(path.resolve().relative_to(docs.resolve()))
    except ValueError:
        return str(path.resolve())


def relative_repo_path(path: Path, root: Path | None = None) -> str:
    base = (root or repo_root()).resolve()
    try:
        return str(path.resolve().relative_to(base))
    except ValueError:
        return str(path.resolve())


def add_failure(
    report: Report, subject: str, problem: str, why: str, rule: str, inspect_next: str
) -> None:
    report.failures.append(
        Finding(
            subject=subject,
            problem=problem,
            why=why,
            rule=rule,
            inspect_next=inspect_next,
        )
    )


def add_unavailable(
    report: Report, subject: str, problem: str, why: str, rule: str, inspect_next: str
) -> None:
    report.unavailable.append(
        Finding(
            subject=subject,
            problem=problem,
            why=why,
            rule=rule,
            inspect_next=inspect_next,
        )
    )


def section_body(text: str, heading: str) -> str | None:
    lines = text.splitlines()
    start = None
    heading_level = None
    heading_re = re.compile(rf"^(#+)\s+{re.escape(heading)}\s*$")
    for index, line in enumerate(lines):
        match = heading_re.match(line)
        if match:
            start = index + 1
            heading_level = len(match.group(1))
            break
    if start is None:
        return None
    body: list[str] = []
    for line in lines[start:]:
        match = re.match(r"^(#+)\s+", line)
        if match and heading_level is not None and len(match.group(1)) <= heading_level:
            break
        body.append(line)
    return "\n".join(body).strip()


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def make_target_recipes(text: str) -> dict[str, str]:
    targets: dict[str, str] = {}
    current_targets: list[str] = []
    current_recipe: list[str] = []

    def flush() -> None:
        if not current_targets:
            return
        recipe_text = "\n".join(current_recipe)
        for name in current_targets:
            if name == ".PHONY":
                continue
            targets[name] = recipe_text

    target_re = re.compile(r"^([A-Za-z0-9_.-]+(?:\s+[A-Za-z0-9_.-]+)*)\s*:(?:\s|$)")
    for line in text.splitlines():
        target_match = target_re.match(line)
        if target_match and not line.startswith("\t"):
            flush()
            current_targets = [name for name in target_match.group(1).split() if name != ".PHONY"]
            current_recipe = []
            continue
        if current_targets and (line.startswith("\t") or line.startswith(" ")):
            current_recipe.append(line.lstrip())
            continue
        if current_targets and not line.strip():
            current_recipe.append(line)
            continue
        flush()
        current_targets = []
        current_recipe = []

    flush()
    return {target: recipe for target, recipe in targets.items() if target}


def recipe_mentions_uv_tool(text: str) -> bool:
    return re.search(r"(^|[\s(&|;])@?(?:uv\b|\$\([^)]+\)|\${[^}]+})", text) is not None


def recipe_contains_all(text: str, required_bits: Iterable[str]) -> bool:
    return all(bit in text for bit in required_bits)


def recipe_mentions_connector_pyproject_path(text: str) -> bool:
    connector_patterns = (
        r"destination_connectors/\$\(\s*package\s*\)/pyproject\.toml",
        r"destination_connectors/\$\{\s*package\s*\}/pyproject\.toml",
        r"destination_connectors/\$package/pyproject\.toml",
    )
    return any(re.search(pattern, text) is not None for pattern in connector_patterns)


def recipe_matches_connector_dir_token(text: str) -> bool:
    connector_patterns = (
        r"destination_connectors/\$\(\s*package\s*\)",
        r"destination_connectors/\$\{\s*package\s*\}",
        r"destination_connectors/\$package",
    )
    return any(re.search(rf"{pattern}(?=$|\s)", text) is not None for pattern in connector_patterns)


def recipe_matches_connector_dist_path(text: str) -> bool:
    connector_patterns = (
        r"destination_connectors/\$\(\s*package\s*\)/dist",
        r"destination_connectors/\$\{\s*package\s*\}/dist",
        r"destination_connectors/\$package/dist",
    )
    return any(
        re.search(rf"{pattern}(?:/\*)?(?=$|\s)", text) is not None for pattern in connector_patterns
    )


def recipe_satisfies_target(target: str, recipe: str, *, archive_mode: bool = False) -> bool:
    normalized = normalize_whitespace(recipe)
    if target == "check":
        return recipe_contains_all(
            normalized,
            (
                "format-check",
                "lint",
                "typecheck",
                "test",
                "uv run python",
                "validate_repo_skeleton.py",
                "validate_architecture.py",
            ),
        )
    if target in {"build-destination-connector", "publish-destination-connector"}:
        if not recipe_mentions_uv_tool(normalized):
            return False
        if target == "build-destination-connector":
            return (
                recipe_contains_all(normalized, ("build", "--out-dir"))
                and not recipe_mentions_connector_pyproject_path(normalized)
                and recipe_matches_connector_dir_token(normalized)
                and recipe_matches_connector_dist_path(normalized)
            )
        return (
            recipe_contains_all(normalized, ("publish",))
            and not recipe_mentions_connector_pyproject_path(normalized)
            and recipe_matches_connector_dist_path(normalized)
        )

    requirements: dict[str, tuple[str, ...]] = {
        "dev": ("sync", "--all-extras", "--group", "dev"),
        "format": ("ruff", "format", "."),
        "format-check": ("ruff", "format", "--check", "."),
        "lint": ("ruff", "check", "."),
        "typecheck": ("mypy", "src", "tests"),
        "lint-lock": ("lock", "--check"),
        "test": ("pytest", "tests", "-q"),
        "test-common": ("pytest", "tests/architecture", "-q"),
        "build-library": ("build",),
        "publish-library": ("publish",),
    }
    if target in requirements:
        if not recipe_mentions_uv_tool(normalized):
            return False
        return recipe_contains_all(normalized, requirements[target])
    return True


def has_bounded_retl_dependency(text: str) -> bool:
    for line in text.splitlines():
        lowered = line.lower()
        if "retl" in lowered and any(marker in lowered for marker in (">", "<", "~=", "==", "!=")):
            return True
    return False


def expression_mentions_name(node: ast.AST, names: set[str]) -> bool:
    for child in ast.walk(node):
        if isinstance(child, ast.Name) and child.id in names:
            return True
    return False


def json_loads_payload_line_numbers(tree: ast.AST) -> list[int]:
    line_numbers: list[int] = []
    payload_like_names = {
        "arrow_payload",
        "artifact_payload",
        "data_plane_payload",
        "payload",
        "raw_payload",
        "serialized_payload",
    }
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (
            isinstance(func, ast.Attribute)
            and func.attr == "loads"
            and isinstance(func.value, ast.Name)
            and func.value.id == "json"
        ):
            continue
        if node.args and expression_mentions_name(node.args[0], payload_like_names):
            line_numbers.append(node.lineno)
    return line_numbers


def python_files_under(path: Path) -> list[Path]:
    if not path.exists():
        return []
    return sorted(
        candidate
        for candidate in path.rglob("*.py")
        if candidate.is_file() and "__pycache__" not in candidate.parts
    )


def callback_free_runtime_files(root: Path) -> list[Path]:
    files = python_files_under(root / "src" / "retl")
    destination_connectors_dir = root / "destination_connectors"
    if destination_connectors_dir.is_dir():
        for destination_connector_root, package_roots in destination_connector_package_roots(
            destination_connectors_dir
        ).items():
            for package_root in package_roots:
                files.extend(python_files_under(destination_connector_root / package_root))
    return sorted(set(files))


def identifier_occurrence_lines(tree: ast.AST, identifiers: set[str]) -> dict[str, set[int]]:
    lines: dict[str, set[int]] = {identifier: set() for identifier in identifiers}
    for node in ast.walk(tree):
        found: str | None = None
        if isinstance(node, ast.Name):
            found = node.id
        elif isinstance(node, ast.Attribute):
            found = node.attr
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            found = node.name
        elif isinstance(node, ast.arg):
            found = node.arg
        elif isinstance(node, ast.keyword):
            found = node.arg
        elif isinstance(node, ast.alias):
            found = node.asname or node.name.rsplit(".", 1)[-1]
        if found in identifiers:
            lines[found].add(getattr(node, "lineno", 0))
    return {identifier: found_lines for identifier, found_lines in lines.items() if found_lines}


def source_segment(text: str, node: ast.AST) -> str:
    return ast.get_source_segment(text, node) or ""


def expression_mentions_source_row_collection(text: str, node: ast.AST) -> bool:
    segment = source_segment(text, node)
    return any(
        marker in segment
        for marker in (
            ".collect_rows(",
            "collect_rows(",
            "_rows_from_record_batches(",
            ".collect_batches(",
        )
    )


def source_row_tuple_materialization_lines(tree: ast.AST, text: str) -> list[int]:
    lines: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        if not isinstance(node.func, ast.Name) or node.func.id not in {"list", "tuple"}:
            continue
        if expression_mentions_source_row_collection(text, node.args[0]):
            lines.append(node.lineno)
    return lines


COLLECT_ARROW_ROW_MATERIALIZATION_METHODS = frozenset(
    {
        "iter_rows",
        "to_pydict",
        "to_pylist",
    }
)


def collect_arrow_row_materialization_lines(
    path: Path, root: Path, tree: ast.AST, text: str
) -> list[int]:
    rel_path = relative_repo_path(path, root)
    if not rel_path.startswith(
        (
            "src/retl/sources/",
            "src/retl/backends/",
            "src/retl/artifacts/",
            "src/retl/runtime/",
            "src/retl/stores/sql_runtime/",
        )
    ):
        return []
    lines: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        function_source = source_segment(text, node)
        normalized_name = node.name.lower()
        is_collect_boundary = (
            rel_path.startswith(("src/retl/sources/", "src/retl/backends/", "src/retl/artifacts/"))
            or "collect" in normalized_name
            or "collect_batches" in function_source
            or "commit_source_state_collect" in function_source
        )
        if not is_collect_boundary:
            continue
        for child in ast.walk(node):
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Attribute)
                and child.func.attr in COLLECT_ARROW_ROW_MATERIALIZATION_METHODS
            ):
                lines.append(child.lineno)
    return lines


def collect_to_arrow_materialization_lines(path: Path, root: Path, tree: ast.AST) -> list[int]:
    rel_path = relative_repo_path(path, root)
    if not rel_path.startswith(
        (
            "src/retl/sources/",
            "src/retl/backends/",
            "src/retl/artifacts/",
            "src/retl/runtime/",
            "src/retl/stores/sql_runtime/",
        )
    ):
        return []
    lines: list[int] = []
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"to_arrow", "to_arrow_table"}
            and isinstance(node.func.value, ast.Call)
        ):
            continue
        called = node.func.value.func
        if isinstance(called, ast.Name) and "collect" in called.id.lower():
            lines.append(node.lineno)
        elif isinstance(called, ast.Attribute) and "collect" in called.attr.lower():
            lines.append(node.lineno)
    return lines


def full_package_to_pylist_lines(path: Path, root: Path, tree: ast.AST) -> list[int]:
    rel_path = relative_repo_path(path, root)
    if not rel_path.startswith(("src/retl/state_runtime/", "src/retl/sync_runtime/")):
        return []
    lines: list[int] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "to_pylist"
        ):
            lines.append(node.lineno)
    return lines


STATE_BASIS_PYTHON_OBJECT_MARKERS = (
    "BasisRecord",
    "StateBasisRecord",
    "StateBasisSnapshot",
    "basis_objects",
    "basis_rows",
    "basis_dicts",
    "basis_python_rows",
)

REMOVED_STATE_BASIS_RUNTIME_CONTRACTS = frozenset(
    {
        "InMemoryStateBasisStore",
        "StateBasisSnapshot",
    }
)

ORDERED_WORK_RESET_REMOVED_RUNTIME_NAMES = frozenset(
    {
        "StateBasisStore",
        "basis_state",
        "collect_state_snapshot",
        "commit_collect_as_basis",
        "commit_reconciled_basis",
        "delete_policy",
        "delta_state",
        "ignore_missing",
        "remove_missing",
        "send_snapshot",
        "state_basis",
        "state_basis_table_name",
        "state_delta",
        "state_strategy",
    }
)

ORDERED_WORK_RESET_REMOVED_RUNTIME_TEXT_PATTERNS = tuple(
    re.compile(rf"\b{re.escape(name)}\b") for name in ORDERED_WORK_RESET_REMOVED_RUNTIME_NAMES
)


def operation_page_table_materialization_lines(
    path: Path, root: Path, tree: ast.AST, text: str
) -> list[int]:
    rel_path = relative_repo_path(path, root)
    if not rel_path.startswith("src/retl/state_runtime/"):
        return []
    lines: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != "OperationPageSet":
            continue
        class_source = source_segment(text, node)
        if "pa.Table.from_batches" in class_source:
            lines.add(node.lineno)
        for child in node.body:
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) and child.name == "table":
                lines.add(child.lineno)
    return sorted(lines)


def destination_full_operation_table_plan_lines(
    path: Path, root: Path, tree: ast.AST, text: str
) -> list[int]:
    rel_path = relative_repo_path(path, root)
    if not rel_path.startswith(("src/retl/destinations/", "src/retl/sync_runtime/")) and not any(
        marker in path.parts for marker in CALLBACK_FREE_CONNECTOR_PACKAGE_MARKERS
    ):
        return []
    lines: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        normalized_name = node.name.lower()
        function_source = source_segment(text, node)
        is_planner = any(term in normalized_name for term in ("plan", "request", "submission"))
        if not is_planner:
            continue
        if re.search(r"\bfull_operation_table\b|\boperation_table\b", function_source):
            lines.add(node.lineno)
        for child in ast.walk(node):
            if (
                isinstance(child, ast.Attribute)
                and child.attr == "table"
                and isinstance(child.value, ast.Name)
                and child.value.id in {"operation_batches", "operation_pages", "operations"}
            ):
                lines.add(getattr(child, "lineno", node.lineno))
    return sorted(lines)


def is_sync_production_path(path: Path, root: Path) -> bool:
    rel_path = relative_repo_path(path, root)
    return rel_path.startswith(("src/retl/sync_runtime/", "src/retl/destinations/")) or any(
        marker in path.parts for marker in CALLBACK_FREE_CONNECTOR_PACKAGE_MARKERS
    )


def sync_row_materialization_lines(path: Path, root: Path, tree: ast.AST, text: str) -> list[int]:
    if not is_sync_production_path(path, root):
        return []

    rel_path = relative_repo_path(path, root)
    lines: list[int] = []
    covered_calls: set[ast.Call] = set()

    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        function_key = (rel_path, node.name)
        for child in ast.walk(node):
            if not (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Attribute)
                and child.func.attr in SYNC_PRODUCTION_ROW_MATERIALIZATION_METHODS
            ):
                continue
            covered_calls.add(child)
            if function_key in BOUNDED_SYNC_ROW_MATERIALIZATION_ALLOWLIST:
                continue
            lines.append(child.lineno)

    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in SYNC_PRODUCTION_ROW_MATERIALIZATION_METHODS
            and node not in covered_calls
        ):
            continue
        lines.append(node.lineno)

    return lines


def event_stage_to_pylist_lines(path: Path, root: Path, tree: ast.AST, text: str) -> list[int]:
    rel_path = relative_repo_path(path, root)
    if not rel_path.startswith(
        ("src/retl/runtime/", "src/retl/state_runtime/", "src/retl/events/")
    ):
        return []
    lines: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        function_source = source_segment(text, node)
        if "collected_events" not in function_source and "Event staging" not in function_source:
            continue
        for child in ast.walk(node):
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Attribute)
                and child.func.attr == "to_pylist"
            ):
                lines.append(child.lineno)
    return lines


def uses_fake_arrow_duck_typing(text: str) -> bool:
    return (
        ('"to_pylist"' in text or "'to_pylist'" in text)
        and "getattr(" in text
        and "pyarrow" not in text.lower()
    )


def is_destination_runtime_file(path: Path, root: Path) -> bool:
    rel_path = relative_repo_path(path, root)
    if rel_path.startswith("src/retl/destinations/"):
        return True
    return any(marker in path.parts for marker in CALLBACK_FREE_CONNECTOR_PACKAGE_MARKERS)


def build_submission_function_sources(tree: ast.AST, text: str) -> list[tuple[int, str]]:
    sources: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "build_submission"
        ):
            sources.append((node.lineno, source_segment(text, node)))
    return sources


def destination_accumulation_lines(path: Path, root: Path, tree: ast.AST, text: str) -> list[int]:
    if not is_destination_runtime_file(path, root):
        return []
    lines: list[int] = []
    for line_number, function_source in build_submission_function_sources(tree, text):
        if any(marker in function_source for marker in DESTINATION_ACCUMULATION_MARKERS) and (
            ".append(" in function_source
            or ".extend(" in function_source
            or re.search(
                r"\b(?:list|tuple)\(\s*(?:iter_)?canonical_mutation[a-z_]*_from_arrow_chunks",
                function_source,
            )
        ):
            lines.append(line_number)
    return lines


def destination_payload_mutation_lines(path: Path, root: Path, text: str) -> list[int]:
    if not is_destination_runtime_file(path, root):
        return []
    lines: list[int] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if any(
            re.search(pattern, line) for pattern in DESTINATION_CANONICAL_MUTATION_PAYLOAD_PATTERNS
        ):
            lines.append(line_number)
    return lines


def annotation_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        return annotation_name(node.value)
    return None


def annotation_contains_direct_receipt_collection(node: ast.AST) -> bool:
    for child in ast.walk(node):
        if not isinstance(child, ast.Subscript):
            continue
        if annotation_name(child.value) not in PUBLIC_RESULT_COLLECTION_TYPE_NAMES:
            continue
        for nested in ast.walk(child.slice):
            if annotation_name(nested) == "Receipt":
                return True
    return False


def annotation_is_safe_receipt_metadata(node: ast.AST) -> bool:
    annotation_text = ast.unparse(node)
    return any(
        term in annotation_text for term in PUBLIC_RESULT_SAFE_RECEIPT_METADATA_ANNOTATION_TERMS
    )


def public_result_large_receipt_payload_fields(
    path: Path, root: Path, tree: ast.AST
) -> list[tuple[int, str, str]]:
    rel_path = relative_repo_path(path, root)
    if rel_path not in PUBLIC_RESULT_SURFACE_FILES:
        return []

    violations: list[tuple[int, str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name not in PUBLIC_RESULT_DATACLASS_NAMES:
            continue
        for statement in node.body:
            if not isinstance(statement, ast.AnnAssign) or not isinstance(
                statement.target, ast.Name
            ):
                continue
            field_name = statement.target.id
            if (
                field_name in PUBLIC_RESULT_FORBIDDEN_RECEIPT_FIELD_NAMES
                and not annotation_is_safe_receipt_metadata(statement.annotation)
            ):
                violations.append(
                    (
                        statement.lineno,
                        f"{node.name}.{field_name}",
                        "field name implies embedded receipt payload contents",
                    )
                )
                continue
            if annotation_contains_direct_receipt_collection(statement.annotation):
                violations.append(
                    (
                        statement.lineno,
                        f"{node.name}.{field_name}",
                        "annotation directly exposes a collection of `Receipt` records",
                    )
                )
    return violations


def render_line_numbers(lines: Iterable[int], limit: int = 8) -> str:
    rendered = ", ".join(str(line) for line in sorted(set(lines))[:limit] if line)
    return rendered or "unknown"


STATE_STYLE_COLLECT_BANNED_MARKERS = (
    "collect_batches",
    "_rows_from_record_batches",
    "_spool_collected_source_batches",
    "source_current_state.arrow",
    "source_run_delta.arrow",
    "collected_source_input",
)

STATE_STYLE_STAGE_BANNED_MARKERS = (
    "read_source_stage_rows",
    "_read_source_stage_rows",
    "_write_staged_source_payload",
    "write_staged_source_payload",
)

DUCKDB_DELTA_STAGE_BANNED_MARKERS = (
    "fetchmany",
    "dict(zip",
    "source_run_delta_rows",
)

CONCRETE_SOURCE_BACKEND_NAMES = (
    "duckdb",
    "snowflake",
    "bigquery",
    "databricks",
    "postgresql",
    "synapse",
    "sqlalchemy",
)

SOURCE_BACKEND_LEAKAGE_ROOTS = (
    "src/retl/artifacts",
    "src/retl/state_runtime",
    "src/retl/sync_runtime",
    "src/retl/runtime",
    "src/retl/destinations",
)

OLD_CONCRETE_BACKEND_IMPLEMENTATION_PATHS = (
    Path("src/retl/sources/duckdb.py"),
    Path("src/retl/stores/duckdb.py"),
    Path("src/retl/sql/dialects/duckdb.py"),
    Path("src/retl/sql/connections/duckdb.py"),
)

OLD_CONCRETE_BACKEND_IMPLEMENTATION_ROOTS = (
    Path("src/retl/sources"),
    Path("src/retl/stores"),
    Path("src/retl/sql/dialects"),
    Path("src/retl/sql/connections"),
)

OLD_BACKEND_IMPLEMENTATION_SYMBOL_PATTERNS = (
    re.compile(r"\bclass\s+\w*(?:Backend|Adapter|Store|Dialect|Connection)\b"),
    re.compile(r"\bdef\s+\w*(?:backend|adapter|store|dialect|connection)\w*\b"),
    re.compile(r"\bimport\s+(?:duckdb|snowflake|psycopg|psycopg2|sqlalchemy|databricks)\b"),
    re.compile(r"\bfrom\s+(?:duckdb|snowflake|psycopg|psycopg2|sqlalchemy|databricks)\b"),
)

SHARED_RUNTIME_CONCRETE_BACKEND_IMPORT_ROOTS = (
    Path("src/retl/artifacts"),
    Path("src/retl/state_runtime"),
    Path("src/retl/sync_runtime"),
    Path("src/retl/runtime"),
    Path("src/retl/destinations"),
    Path("src/retl/sources"),
    Path("src/retl/sql"),
    Path("src/retl/stores/sql_runtime"),
)


@dataclass(frozen=True)
class BackendCheckSpec:
    name: str
    display_name: str
    package_root: Path
    concrete_import_prefix: str
    driver_import_prefixes: tuple[str, ...]
    runtime_store_file: Path | None = None
    runtime_store_class: str | None = None
    allowed_store_methods: frozenset[str] = frozenset()
    allowed_top_level_functions: frozenset[str] = frozenset()
    banned_sql_patterns: tuple[tuple[str, re.Pattern[str]], ...] = ()
    collect_source_access_allowed_functions: frozenset[str] = frozenset()
    source_access_markers: tuple[str, ...] = ()
    legacy_compatibility_identifiers: frozenset[str] = frozenset()


BACKEND_CHECK_SPECS = (
    BackendCheckSpec(
        name="duckdb",
        display_name="DuckDB",
        package_root=Path("src/retl/backends/duckdb"),
        concrete_import_prefix="retl.backends.duckdb",
        driver_import_prefixes=("duckdb",),
        runtime_store_file=Path("src/retl/backends/duckdb/store.py"),
        runtime_store_class="DuckDBRuntimeStore",
        allowed_store_methods=frozenset(
            {
                "__post_init__",
                "_initialize_ordered_work_store",
                "_validate_duckdb_collect_source_space",
            }
        ),
        allowed_top_level_functions=frozenset({"duckdb"}),
        banned_sql_patterns=(("ATTACH", re.compile(r"\battach\b", re.I)),),
        collect_source_access_allowed_functions=frozenset(
            {
                "produce_event_collect",
                "produce_state_collect",
                "_restore_duckdb_source_schema",
                "_use_duckdb_source_schema",
                "_validate_duckdb_collect_source_space",
            }
        ),
        source_access_markers=("_use_duckdb_source_schema(",),
        legacy_compatibility_identifiers=frozenset(
            {"runtime_connection_for_backend", "runtime_database"}
        ),
    ),
    BackendCheckSpec(
        name="snowflake",
        display_name="Snowflake",
        package_root=Path("src/retl/backends/snowflake"),
        concrete_import_prefix="retl.backends.snowflake",
        driver_import_prefixes=("snowflake",),
        runtime_store_file=Path("src/retl/backends/snowflake/store.py"),
        runtime_store_class="SnowflakeRuntimeStore",
    ),
    BackendCheckSpec(
        name="bigquery",
        display_name="BigQuery",
        package_root=Path("src/retl/backends/bigquery"),
        concrete_import_prefix="retl.backends.bigquery",
        driver_import_prefixes=("google.cloud.bigquery",),
        runtime_store_file=Path("src/retl/backends/bigquery/store.py"),
        runtime_store_class="BigQueryRuntimeStore",
    ),
    BackendCheckSpec(
        name="databricks",
        display_name="Databricks",
        package_root=Path("src/retl/backends/databricks"),
        concrete_import_prefix="retl.backends.databricks",
        driver_import_prefixes=("databricks",),
        runtime_store_file=Path("src/retl/backends/databricks/store.py"),
        runtime_store_class="DatabricksRuntimeStore",
    ),
    BackendCheckSpec(
        name="postgresql",
        display_name="PostgreSQL",
        package_root=Path("src/retl/backends/postgresql"),
        concrete_import_prefix="retl.backends.postgresql",
        driver_import_prefixes=("psycopg", "psycopg2"),
        runtime_store_file=Path("src/retl/backends/postgresql/store.py"),
        runtime_store_class="PostgreSqlRuntimeStore",
    ),
    BackendCheckSpec(
        name="synapse",
        display_name="Synapse",
        package_root=Path("src/retl/backends/synapse"),
        concrete_import_prefix="retl.backends.synapse",
        driver_import_prefixes=("pyodbc",),
    ),
    BackendCheckSpec(
        name="sqlalchemy",
        display_name="SQLAlchemy",
        package_root=Path("src/retl/backends/sqlalchemy"),
        concrete_import_prefix="retl.backends.sqlalchemy",
        driver_import_prefixes=("sqlalchemy",),
    ),
    BackendCheckSpec(
        name="mysql",
        display_name="MySQL",
        package_root=Path("src/retl/backends/mysql"),
        concrete_import_prefix="retl.backends.mysql",
        driver_import_prefixes=("mysql", "pymysql"),
    ),
    BackendCheckSpec(
        name="redshift",
        display_name="Redshift",
        package_root=Path("src/retl/backends/redshift"),
        concrete_import_prefix="retl.backends.redshift",
        driver_import_prefixes=("redshift_connector",),
    ),
)

CONCRETE_BACKEND_IMPORT_MODULE_PREFIXES = tuple(
    spec.concrete_import_prefix for spec in BACKEND_CHECK_SPECS
)

BACKEND_DRIVER_IMPORT_ALLOWED_ROOTS_BY_PREFIX = {
    driver_prefix: (spec.package_root,)
    for spec in BACKEND_CHECK_SPECS
    for driver_prefix in spec.driver_import_prefixes
}

BACKEND_DRIVER_IMPORT_BOUNDARY_ALLOWED_FILES: frozenset[Path] = frozenset()

BACKEND_DRIVER_IMPORT_MODULE_PREFIXES = tuple(
    driver_prefix for spec in BACKEND_CHECK_SPECS for driver_prefix in spec.driver_import_prefixes
)

SQLGLOT_IMPORT_ALLOWED_ROOTS = (
    # RETL-owned SQL AST/rendering contracts and dialect capabilities.
    Path("src/retl/sql"),
    # Shared runtime SQL generation modules build generated SQL against those contracts.
    Path("src/retl/stores/sql_runtime"),
)

SQLGLOT_IMPORT_ALLOWED_FILES = frozenset(
    {
        # Source SQL wrapping/keyset compilation is a shared SQL generation boundary.
        Path("src/retl/sources/sql.py"),
    }
)

RUNTIME_RELATION_NAMES = frozenset(
    {
        "declarations",
        "destination_batches",
        "destination_progress",
        "ordered_work",
        "pending_work_cursors",
        "runs",
        "state_current",
        "state_current_cursors",
        "sync_reports",
        "target_registry",
    }
)

SQL_RUNTIME_RAW_SQL_ALLOWED_FUNCTIONS = frozenset(
    {
        # Runtime schema DDL and additive migrations are backend-owned SQL.
        "_run_additive_migrations",
        # Source collect wraps user/source SQL and dialect-owned JSON, hashing,
        # canonical-key, temp-table, and insert-select bodies.
        "produce_state_collect",
        "produce_event_collect",
        "_event_source_window_payload",
        "insert_state_upsert_work",
        "insert_state_remove_work",
        "replace_state_current",
        # Declaration persistence still delegates the final wrapper to the dialect.
        "register_declaration",
    }
)

SQL_RUNTIME_TRANSFERABLE_RAW_SQL_PATTERN = re.compile(
    r"\b(select|insert|update|delete)\b",
    re.I,
)

SQL_WRITE_TARGET_PATTERN = re.compile(
    r"\b(?:insert\s+into|delete\s+from|update|create\s+(?:table|sequence)"
    r"(?:\s+if\s+not\s+exists)?|drop\s+table(?:\s+if\s+exists)?|alter\s+table)"
    r"\s+(?P<target>(?:[A-Za-z_][A-Za-z0-9_]*|\"[^\"]+\"|\{[A-Za-z_][A-Za-z0-9_\\.]*\})"
    r"(?:\.(?:[A-Za-z_][A-Za-z0-9_]*|\"[^\"]+\"|\{[A-Za-z_][A-Za-z0-9_\\.]*\}))?)",
    re.I,
)

TOOLKIT_AUTH_FALLBACK_FILES = (
    "src/retl/destinations/toolkit/auth.py",
    "src/retl/destinations/toolkit/surfaces.py",
)

RERUN_POLICY_FILES = (
    "src/retl/state_runtime/models.py",
    "src/retl/runtime/_internal/executor.py",
)


def toolkit_auth_mode_fallback_lines(path: Path, root: Path, text: str, tree: ast.AST) -> list[int]:
    rel_path = relative_repo_path(path, root)
    if rel_path not in TOOLKIT_AUTH_FALLBACK_FILES:
        return []
    lines: list[int] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "_default_auth_modes"
        ):
            lines.append(node.lineno)
            continue
        if isinstance(node, ast.Name) and node.id == "_default_auth_modes":
            lines.append(node.lineno)
            continue
        if (
            isinstance(node, ast.Call)
            and call_name(node) == "AuthMode"
            and any(
                keyword.arg == "name"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value == "default"
                for keyword in node.keywords
            )
        ):
            lines.append(node.lineno)
    if "or _default_auth_modes(" in text:
        lines.append(text[: text.index("or _default_auth_modes(")].count("\n") + 1)
    return sorted(set(lines))


def resume_rerun_policy_acceptance_lines(path: Path, root: Path, tree: ast.AST) -> list[int]:
    rel_path = relative_repo_path(path, root)
    if rel_path not in RERUN_POLICY_FILES:
        return []
    lines: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and node.value == "resume":
            parent_line = getattr(node, "lineno", 0)
            lines.append(parent_line)
    return sorted(set(lines))


def destination_connector_code_files(root: Path) -> list[Path]:
    connectors_root = root / "destination_connectors"
    if not connectors_root.exists():
        return []
    files: list[Path] = []
    for path in connectors_root.rglob("*"):
        if not path.is_file() or path.suffix not in {".py", ".toml"}:
            continue
        rel_path = Path(relative_repo_path(path, root))
        if any(part in SUPPORTED_SURFACE_LANGUAGE_EXCLUDED_PARTS for part in rel_path.parts):
            continue
        files.append(path)
    return sorted(files)


def is_mock_connector_path(path: Path, root: Path) -> bool:
    rel_path = Path(relative_repo_path(path, root))
    return len(rel_path.parts) >= 2 and rel_path.parts[:2] == (
        "destination_connectors",
        "mock",
    )


def copied_request_batch_lifecycle_loop_lines(text: str, tree: ast.AST) -> list[int]:
    lines: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        methods = {
            child.name
            for child in node.body
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        if len(methods & REQUEST_BATCH_HTTP_OLD_AUTHORING_METHODS) < 3:
            continue
        class_source = source_segment(text, node)
        if not any(marker in class_source for marker in REQUEST_BATCH_HTTP_LIFECYCLE_MARKERS):
            continue
        lines.extend(
            child.lineno
            for child in node.body
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
            and child.name in REQUEST_BATCH_HTTP_OLD_AUTHORING_METHODS
        )
    return lines


def package_local_request_batch_framework_helper_lines(tree: ast.AST) -> list[int]:
    lines: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.alias):
            name = node.name.rsplit(".", 1)[-1]
            if name in REQUEST_BATCH_HTTP_PACKAGE_LOCAL_FRAMEWORK_HELPERS:
                lines.append(getattr(node, "lineno", 0))
            continue
        if (
            isinstance(node, ast.Name)
            and node.id in REQUEST_BATCH_HTTP_PACKAGE_LOCAL_FRAMEWORK_HELPERS
        ):
            lines.append(node.lineno)
            continue
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and node.value in REQUEST_BATCH_HTTP_PACKAGE_LOCAL_FRAMEWORK_HELPERS
        ):
            lines.append(node.lineno)
    return lines


def literal_string_sequence(node: ast.AST) -> tuple[str, ...]:
    try:
        value = ast.literal_eval(node)
    except (SyntaxError, ValueError):
        return ()
    if isinstance(value, str):
        return (value,)
    if not isinstance(value, (list, tuple, set)):
        return ()
    fields: list[str] = []
    for item in value:
        if not isinstance(item, str):
            return ()
        fields.append(item)
    return tuple(fields)


def call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def partner_base_url_config_namespace_lines(path: Path, root: Path, tree: ast.AST) -> list[int]:
    rel_path = relative_repo_path(path, root)
    if any(
        rel_path.startswith(prefix) for prefix in DESTINATION_CONNECTOR_CONFIG_BASE_URL_EXCEPTIONS
    ):
        return []
    lines: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if call_name(node) not in {"declarative_connector", "DestinationConnector"}:
            continue
        for keyword in node.keywords:
            if keyword.arg != "config_namespace_fields":
                continue
            if "base_url" in literal_string_sequence(keyword.value):
                lines.append(keyword.value.lineno)
    return lines


def validate_current_toolkit_auth_surface(report: Report, root: Path) -> None:
    for rel_path in TOOLKIT_AUTH_FALLBACK_FILES:
        path = root / rel_path
        if not path.exists():
            continue
        text = read_text(path)
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError:
            continue
        lines = toolkit_auth_mode_fallback_lines(path, root, text, tree)
        if not lines:
            continue
        add_failure(
            report,
            subject=rel_path,
            problem=(
                "toolkit auth-mode fallback synthesis is present at line(s): "
                f"{render_line_numbers(lines)}"
            ),
            why=(
                "destination toolkit auth policies must declare explicit auth modes so "
                "generated definitions do not infer a credential shape from flat fields."
            ),
            rule=(
                "Do not synthesize `AuthMode` values from `credential_fields` in toolkit "
                "auth or surface helpers; connector surfaces must provide explicit modes."
            ),
            inspect_next="docs/destinations.md",
        )


def validate_public_rerun_policy_surface(report: Report, root: Path) -> None:
    for rel_path in RERUN_POLICY_FILES:
        path = root / rel_path
        if not path.exists():
            continue
        try:
            tree = ast.parse(read_text(path), filename=str(path))
        except SyntaxError:
            continue
        lines = resume_rerun_policy_acceptance_lines(path, root, tree)
        if not lines:
            continue
        add_failure(
            report,
            subject=rel_path,
            problem=(
                '`rerun_policy="resume"` public acceptance is present at line(s): '
                f"{render_line_numbers(lines)}"
            ),
            why=(
                "normal fresh-after-failure startup resume and explicit replay are the "
                "supported operator paths; `resume` must not become a public run mode."
            ),
            rule=(
                '`rerun_policy` must not accept the literal `"resume"` in public runtime '
                "validation or state model types."
            ),
            inspect_next="docs/product.md",
        )


def validate_current_destination_surface_guardrails(report: Report, root: Path) -> None:
    for path in destination_connector_code_files(root):
        rel_path = relative_repo_path(path, root)
        text = read_text(path)
        tree: ast.AST | None = None
        if path.suffix == ".py":
            try:
                tree = ast.parse(text, filename=str(path))
            except SyntaxError:
                tree = None

        if tree is None:
            continue

        base_url_config_lines = partner_base_url_config_namespace_lines(path, root, tree)
        if base_url_config_lines:
            add_failure(
                report,
                subject=rel_path,
                problem=(
                    "partner connector exposes `base_url` through config namespace fields "
                    f"at line(s): {render_line_numbers(base_url_config_lines)}"
                ),
                why=(
                    "first-party partner connectors own their production API origin in "
                    "package code; arbitrary HTTP endpoint config belongs to the generic "
                    "`retl/reference-http` connector or a documented exception."
                ),
                rule=(
                    "Do not include `base_url` in first-party partner connector "
                    "`config_namespace_fields`. Use connector-owned production origin "
                    "constants and injected transports for tests."
                ),
                inspect_next="docs/destinations.md",
            )

        if is_mock_connector_path(path, root):
            continue

        lifecycle_lines = copied_request_batch_lifecycle_loop_lines(text, tree)
        if lifecycle_lines:
            add_failure(
                report,
                subject=rel_path,
                problem=(
                    "copied request-batch destination client lifecycle loop is present "
                    f"at line(s): {render_line_numbers(lifecycle_lines)}"
                ),
                why=(
                    "ordinary HTTP request-batch destination clients are generated from "
                    "`RequestBatchHttpSurfaceSpec`; package code should contain partner "
                    "hooks, not copied runtime lifecycle methods."
                ),
                rule=(
                    "Destination connector packages must not define package-local "
                    "request-batch clients that copy `validate_sync_request`, "
                    "`resolve_targets`, `build_submission`, `execute_submission`, or "
                    "`poll_submission` around toolkit request-batch helpers. The mock "
                    "simulator remains the repo-owned conformance implementation."
                ),
                inspect_next="docs/destinations.md",
            )

        helper_lines = package_local_request_batch_framework_helper_lines(tree)
        if helper_lines:
            add_failure(
                report,
                subject=rel_path,
                problem=(
                    "package-local request-batch framework helper is present at line(s): "
                    f"{render_line_numbers(helper_lines)}"
                ),
                why=(
                    "submission planning, chunk payload assembly, dry-run receipts, and "
                    "default request-batch receipt classification are generated-client "
                    "responsibilities in the declarative surface-spec model."
                ),
                rule=(
                    "Connector packages may use surface-spec builders and partner hooks, "
                    "but must not import or call toolkit-owned request-batch framework "
                    "helpers directly. The mock simulator remains the repo-owned "
                    "conformance implementation."
                ),
                inspect_next="docs/destinations.md",
            )


def validate_public_runner_vocabulary(report: Report, root: Path) -> None:
    for path in _existing_public_vocabulary_files(root):
        rel_path = relative_repo_path(path, root)
        if rel_path in RUNNER_VOCABULARY_ALLOWED_PATHS:
            continue
        name_match = RUNNER_VOCABULARY_PATTERN.search(rel_path)
        if name_match is not None:
            add_failure(
                report,
                subject=rel_path,
                problem="retired execution vocabulary appears in a public path",
                why=(
                    "Runner is the canonical public execution term, so public docs, "
                    "entrypoints, and CLI paths must not keep the retired term."
                ),
                rule=(
                    "Public first-party surfaces may use the retired execution term only "
                    "in `docs/reference-mapping.md`."
                ),
                inspect_next="docs/reference-mapping.md",
            )
            continue
        text = read_text(path)
        match = RUNNER_VOCABULARY_PATTERN.search(text)
        if match is None:
            continue
        line_number = text[: match.start()].count("\n") + 1
        add_failure(
            report,
            subject=f"{rel_path}:{line_number}",
            problem="retired execution vocabulary appears in public content",
            why=(
                "Runner is the canonical public execution term, so public docs and "
                "entrypoints must not keep the retired term."
            ),
            rule=(
                "Public first-party surfaces may use the retired execution term only in "
                "`docs/reference-mapping.md`."
            ),
            inspect_next="docs/reference-mapping.md",
        )


def _existing_public_vocabulary_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for rel_surface in PUBLIC_RUNNER_VOCABULARY_SURFACES:
        surface = root / rel_surface
        if surface.is_file() and surface.suffix in RUNNER_VOCABULARY_SCAN_SUFFIXES:
            files.append(surface)
            continue
        if not surface.is_dir():
            continue
        for path in surface.rglob("*"):
            if not path.is_file() or path.suffix not in RUNNER_VOCABULARY_SCAN_SUFFIXES:
                continue
            rel_path = Path(relative_repo_path(path, root))
            if any(part in RUNNER_VOCABULARY_EXCLUDED_PARTS for part in rel_path.parts):
                continue
            files.append(path)
    return sorted(set(files))


def _python_files_under_roots(root: Path, rel_roots: tuple[str, ...]) -> list[Path]:
    files: list[Path] = []
    for rel_root in rel_roots:
        candidate = root / rel_root
        if not candidate.exists():
            continue
        if candidate.is_file() and candidate.suffix == ".py":
            files.append(candidate)
            continue
        if candidate.is_dir():
            files.extend(python_files_under(candidate))
    return sorted(set(files))


def concrete_source_backend_occurrence_lines(text: str) -> dict[str, list[int]]:
    occurrences: dict[str, list[int]] = {}
    lowered_lines = text.lower().splitlines()
    for backend_name in CONCRETE_SOURCE_BACKEND_NAMES:
        lines = [
            line_number
            for line_number, line in enumerate(lowered_lines, start=1)
            if backend_name in line
        ]
        if lines:
            occurrences[backend_name] = lines
    return occurrences


def old_backend_file_contains_concrete_implementation(path: Path) -> bool:
    text = read_text(path)
    return any(pattern.search(text) for pattern in OLD_BACKEND_IMPLEMENTATION_SYMBOL_PATTERNS)


def old_concrete_backend_implementation_paths(root: Path) -> list[Path]:
    old_paths = set(OLD_CONCRETE_BACKEND_IMPLEMENTATION_PATHS)
    backend_module_names = frozenset(CONCRETE_SOURCE_BACKEND_NAMES)
    for old_root in OLD_CONCRETE_BACKEND_IMPLEMENTATION_ROOTS:
        absolute_root = root / old_root
        if not absolute_root.exists():
            continue
        for path in sorted(absolute_root.glob("*.py")):
            rel_path = Path(relative_repo_path(path, root))
            if rel_path in OLD_CONCRETE_BACKEND_IMPLEMENTATION_PATHS:
                continue
            if (
                path.stem in backend_module_names
                and old_backend_file_contains_concrete_implementation(path)
            ):
                old_paths.add(Path(relative_repo_path(path, root)))
    return sorted(old_paths)


def validate_no_concrete_source_backend_leakage(report: Report, root: Path) -> None:
    validate_backend_package_boundary(report, root)
    validate_backend_driver_import_boundary(report, root)
    validate_sqlglot_import_boundary(report, root)
    for rel_root in SOURCE_BACKEND_LEAKAGE_ROOTS:
        runtime_root = root / rel_root
        if not runtime_root.exists():
            continue
        for path in sorted(runtime_root.rglob("*.py")):
            text = read_text(path)
            occurrences = concrete_source_backend_occurrence_lines(text)
            if not occurrences:
                continue
            details = ", ".join(
                f"`{backend}` line(s) {render_line_numbers(lines)}"
                for backend, lines in sorted(occurrences.items())
            )
            add_failure(
                report,
                subject=relative_repo_path(path, root),
                problem=f"concrete source backend leaked into shared runtime/state code: {details}",
                why=(
                    "source-state dispatch and phase orchestration must remain backend-generic; "
                    "connection setup, SQL dialects, cursor APIs, and backend-local failures "
                    "belong behind source adapter ownership."
                ),
                rule=(
                    "`src/retl/state_runtime` and core runtime phase packages must not name concrete "
                    "source backends such as DuckDB, Snowflake, or future SQL adapters."
                ),
                inspect_next="docs/control-plane.md",
            )


def validate_backend_package_boundary(report: Report, root: Path) -> None:
    for rel_path in old_concrete_backend_implementation_paths(root):
        if not (root / rel_path).exists():
            continue
        add_failure(
            report,
            subject=rel_path.as_posix(),
            problem="old concrete backend implementation path is present",
            why=(
                "DuckDB and future SQL backend implementation must live behind a "
                "backend-owned package instead of the old source/store/dialect/connection "
                "layer paths."
            ),
            rule=(
                "Concrete SQL backend implementation belongs under "
                "`src/retl/backends/<backend>/`; do not reintroduce "
                "`src/retl/sources/duckdb.py`, `src/retl/stores/duckdb.py`, "
                "`src/retl/sql/dialects/duckdb.py`, "
                "`src/retl/sql/connections/duckdb.py`, or equivalent old "
                "layer-oriented files for other concrete backend names."
            ),
            inspect_next="docs/control-plane.md",
        )

    for rel_root in SHARED_RUNTIME_CONCRETE_BACKEND_IMPORT_ROOTS:
        shared_root = root / rel_root
        if not shared_root.exists():
            continue
        for path in python_files_under(shared_root):
            text = read_text(path)
            try:
                tree = ast.parse(text)
            except SyntaxError:
                continue
            lines = concrete_backend_import_lines(tree)
            if not lines:
                continue
            add_failure(
                report,
                subject=relative_repo_path(path, root),
                problem=(
                    "concrete backend package import in shared runtime module "
                    f"at line(s): {render_line_numbers(lines)}"
                ),
                why=(
                    "shared runtime, source, store, and SQL modules must remain "
                    "backend-neutral and depend on RETL-owned contracts instead of "
                    "concrete backend packages."
                ),
                rule=(
                    "`src/retl/stores/sql_runtime/`, `src/retl/runtime/`, "
                    "`src/retl/sources/`, `src/retl/sql/`, and other shared runtime "
                    "packages must not import `retl.backends.<backend>`."
                ),
                inspect_next="docs/control-plane.md",
            )


def validate_backend_driver_import_boundary(report: Report, root: Path) -> None:
    source_root = root / "src" / "retl"
    if not source_root.exists():
        return
    for path in python_files_under(source_root):
        rel_path = Path(relative_repo_path(path, root))
        if rel_path in BACKEND_DRIVER_IMPORT_BOUNDARY_ALLOWED_FILES:
            continue
        text = read_text(path)
        try:
            tree = ast.parse(text)
        except SyntaxError:
            continue
        import_occurrences = [
            (prefix, line)
            for prefix, line in backend_driver_import_occurrences(tree)
            if not backend_driver_import_allowed(rel_path, prefix)
        ]
        if not import_occurrences:
            continue
        lines = sorted({line for _, line in import_occurrences})
        drivers = sorted({prefix for prefix, _ in import_occurrences})
        add_failure(
            report,
            subject=rel_path.as_posix(),
            problem=(
                "backend-specific driver import outside backend package boundary "
                f"at line(s): {render_line_numbers(lines)}"
            ),
            why=(
                "backend-native Python drivers must stay behind their matching "
                "backend-owned package so shared runtime code and other backend packages "
                "remain backend-neutral."
            ),
            rule=(
                "Driver imports must stay under their corresponding backend package; "
                f"rejected driver prefix(es): {', '.join(f'`{driver}`' for driver in drivers)}."
            ),
            inspect_next="docs/control-plane.md",
        )


def backend_driver_import_allowed(rel_path: Path, driver_prefix: str) -> bool:
    allowed_roots = BACKEND_DRIVER_IMPORT_ALLOWED_ROOTS_BY_PREFIX.get(driver_prefix, ())
    return any(_is_relative_to(rel_path, allowed) for allowed in allowed_roots)


def validate_sqlglot_import_boundary(report: Report, root: Path) -> None:
    source_root = root / "src" / "retl"
    if not source_root.exists():
        return
    for path in python_files_under(source_root):
        rel_path = Path(relative_repo_path(path, root))
        if rel_path in SQLGLOT_IMPORT_ALLOWED_FILES or any(
            _is_relative_to(rel_path, allowed) for allowed in SQLGLOT_IMPORT_ALLOWED_ROOTS
        ):
            continue
        text = read_text(path)
        try:
            tree = ast.parse(text)
        except SyntaxError:
            continue
        lines = sqlglot_import_lines(tree)
        if not lines:
            continue
        add_failure(
            report,
            subject=rel_path.as_posix(),
            problem=(
                "SQLGlot import outside shared SQL generation layer at line(s): "
                f"{render_line_numbers(lines)}"
            ),
            why=(
                "SQLGlot is RETL's generated-SQL AST/rendering layer; runtime semantics, "
                "connection behavior, and product code should depend on RETL-owned SQL "
                "contracts instead of importing SQLGlot directly."
            ),
            rule=(
                "SQLGlot imports are allowed only in `src/retl/sql/`, "
                "`src/retl/stores/sql_runtime/`, `src/retl/sources/sql.py`, and tests."
            ),
            inspect_next="docs/runtime.md",
        )


def import_module_matches(module: str, prefixes: tuple[str, ...]) -> bool:
    return any(module == prefix or module.startswith(f"{prefix}.") for prefix in prefixes)


def matching_driver_prefix(module: str) -> str | None:
    matches = [
        prefix
        for prefix in BACKEND_DRIVER_IMPORT_MODULE_PREFIXES
        if module == prefix or module.startswith(f"{prefix}.")
    ]
    if not matches:
        return None
    return max(matches, key=len)


def backend_driver_import_occurrences(tree: ast.AST) -> list[tuple[str, int]]:
    occurrences: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                prefix = matching_driver_prefix(alias.name)
                if prefix is not None:
                    occurrences.append((prefix, node.lineno))
                    break
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            prefix = matching_driver_prefix(module)
            if prefix is not None:
                occurrences.append((prefix, node.lineno))
        elif isinstance(node, ast.Call) and is_dynamic_import_module_call(node):
            dynamic_module = dynamic_import_module_name(node)
            if dynamic_module is None:
                continue
            prefix = matching_driver_prefix(dynamic_module)
            if prefix is not None:
                occurrences.append((prefix, node.lineno))
    return sorted(set(occurrences), key=lambda occurrence: (occurrence[1], occurrence[0]))


def is_dynamic_import_module_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call) or not node.args:
        return False
    if isinstance(node.func, ast.Name):
        return node.func.id == "import_module"
    if isinstance(node.func, ast.Attribute):
        return node.func.attr == "import_module"
    return False


def dynamic_import_module_name(node: ast.AST) -> str | None:
    if not isinstance(node, ast.Call) or not node.args:
        return None
    first_arg = node.args[0]
    if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
        return first_arg.value
    return None


def sqlglot_import_lines(tree: ast.AST) -> list[int]:
    lines: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "sqlglot" or alias.name.startswith("sqlglot."):
                    lines.append(node.lineno)
                    break
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "sqlglot" or module.startswith("sqlglot."):
                lines.append(node.lineno)
    return sorted(set(lines))


def concrete_backend_import_lines(tree: ast.AST) -> list[int]:
    lines: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if import_module_matches(alias.name, CONCRETE_BACKEND_IMPORT_MODULE_PREFIXES):
                    lines.append(node.lineno)
                    break
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if import_module_matches(module, CONCRETE_BACKEND_IMPORT_MODULE_PREFIXES):
                lines.append(node.lineno)
    return sorted(set(lines))


def execute_sql_segments(tree: ast.AST, text: str) -> list[tuple[int, str]]:
    segments: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        if not isinstance(node.func, ast.Attribute) or node.func.attr != "execute":
            continue
        segment = source_segment(text, node.args[0])
        if segment:
            segments.append((node.lineno, segment))
    return segments


def enclosing_function_name(parent_map: dict[ast.AST, ast.AST], node: ast.AST) -> str | None:
    current = node
    while current in parent_map:
        current = parent_map[current]
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current.name
    return None


def direct_raw_execute_segments(tree: ast.AST, text: str) -> list[tuple[int, str, str | None]]:
    parent_map = {
        child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)
    }
    segments: list[tuple[int, str, str | None]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        if not isinstance(node.func, ast.Attribute) or node.func.attr not in {
            "execute",
            "executemany",
        }:
            continue
        sql_arg = node.args[0]
        if not isinstance(sql_arg, (ast.Constant, ast.JoinedStr)):
            continue
        segment = source_segment(text, sql_arg)
        if segment:
            segments.append((node.lineno, segment, enclosing_function_name(parent_map, node)))
    return segments


def validate_sql_runtime_raw_sql_boundary(report: Report, root: Path) -> None:
    sql_runtime_root = root / "src" / "retl" / "stores" / "sql_runtime"
    if not sql_runtime_root.exists():
        return
    for path in python_files_under(sql_runtime_root):
        rel_path = Path(relative_repo_path(path, root))
        text = read_text(path)
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError as exc:
            add_failure(
                report,
                subject=rel_path.as_posix(),
                problem=f"Python syntax error prevents SQL runtime raw-SQL guardrail scan: {exc}",
                why=(
                    "raw SQL regression checks cannot prove migrated Runtime SQL stays behind "
                    "SQLGlot helpers unless shared runtime modules parse."
                ),
                rule="SQL runtime Python files must be parseable by the architecture validator.",
                inspect_next="docs/runtime.md",
            )
            continue
        violations = [
            (line, function_name)
            for line, sql_source, function_name in direct_raw_execute_segments(tree, text)
            if function_name not in SQL_RUNTIME_RAW_SQL_ALLOWED_FUNCTIONS
            and SQL_RUNTIME_TRANSFERABLE_RAW_SQL_PATTERN.search(sql_source)
        ]
        if not violations:
            continue
        details = ", ".join(
            f"`{function_name or '<module>'}` line {line}" for line, function_name in violations[:8]
        )
        add_failure(
            report,
            subject=rel_path.as_posix(),
            problem=f"raw transferable SQL in shared SQL runtime module: {details}",
            why=(
                "migrated Runtime reads, deletes, row inserts, and row upserts must keep "
                "table, column, and placeholder structure in RETL SQLGlot helpers so "
                "parameter order and backend rendering stay mechanically provable."
            ),
            rule=(
                "Direct raw `select`, `insert`, `update`, or `delete` execute/executemany "
                "calls in `src/retl/stores/sql_runtime` are allowed only for documented "
                "backend-owned or dialect-wrapper surfaces such as DDL/schema migration, "
                "collect insert-select/JSON/hash/canonical-key bodies, and declaration "
                "upsert wrappers."
            ),
            inspect_next="docs/runtime.md",
        )


def sql_write_targets(sql_source: str) -> list[str]:
    normalized = normalize_whitespace(sql_source)
    return [match.group("target") for match in SQL_WRITE_TARGET_PATTERN.finditer(normalized)]


def sql_identifier_name(identifier: str) -> str:
    identifier = identifier.strip()
    if len(identifier) >= 2 and identifier[0] == '"' and identifier[-1] == '"':
        return identifier[1:-1].replace('""', '"')
    return identifier


def sql_target_relation_name(target: str) -> str:
    relation_token = target.rsplit(".", 1)[-1]
    if relation_token.startswith("{") and relation_token.endswith("}"):
        return relation_token[1:-1]
    return sql_identifier_name(relation_token)


def sql_target_uses_runtime_schema(target: str) -> bool:
    return target.startswith("{self.schema}.")


def runtime_relation_assignment_names(function: ast.AST) -> dict[str, str]:
    assignments: dict[str, str] = {}
    for node in ast.walk(function):
        if not isinstance(node, ast.Assign):
            continue
        if not (
            isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Attribute)
            and node.value.func.attr == "render_runtime_relation"
            and node.value.args
            and isinstance(node.value.args[0], ast.Constant)
            and isinstance(node.value.args[0].value, str)
        ):
            continue
        relation_name = node.value.args[0].value
        if relation_name not in RUNTIME_RELATION_NAMES:
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                assignments[target.id] = relation_name
    return assignments


def runtime_write_target_is_schema_qualified(
    target: str, runtime_relation_vars: dict[str, str], relation_name: str
) -> bool:
    if sql_target_uses_runtime_schema(target):
        return True
    if target.startswith("{") and target.endswith("}"):
        variable_name = target[1:-1]
        return runtime_relation_vars.get(variable_name) == relation_name
    return False


def unqualified_runtime_write_lines(tree: ast.AST, text: str) -> list[tuple[int, str]]:
    violations: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        runtime_relation_vars = runtime_relation_assignment_names(node)
        for line, sql_source in execute_sql_segments(node, text):
            for target in sql_write_targets(sql_source):
                variable_name = (
                    target[1:-1] if target.startswith("{") and target.endswith("}") else ""
                )
                relation_name = runtime_relation_vars.get(
                    variable_name, sql_target_relation_name(target)
                )
                if relation_name not in RUNTIME_RELATION_NAMES:
                    continue
                if runtime_write_target_is_schema_qualified(
                    target, runtime_relation_vars, relation_name
                ):
                    continue
                violations.append((line, target))
    return violations


def banned_sql_pattern_lines(tree: ast.AST, text: str, pattern: re.Pattern[str]) -> list[int]:
    lines = [
        line for line, sql_source in execute_sql_segments(tree, text) if pattern.search(sql_source)
    ]
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if pattern.search(node.value):
                lines.append(getattr(node, "lineno", 0))
    return sorted({line for line in lines if line})


def backend_runtime_store_shared_behavior_lines(
    tree: ast.AST, spec: BackendCheckSpec
) -> list[tuple[int, str]]:
    if spec.runtime_store_class is None:
        return []
    lines: list[tuple[int, str]] = []
    for node in getattr(tree, "body", []):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name not in spec.allowed_top_level_functions:
            lines.append((node.lineno, node.name))
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != spec.runtime_store_class:
            continue
        for child in node.body:
            if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if child.name not in spec.allowed_store_methods:
                lines.append((child.lineno, child.name))
    return sorted(lines)


def source_adapter_runtime_write_lines(tree: ast.AST, text: str) -> list[tuple[int, str]]:
    violations: list[tuple[int, str]] = []
    for line, sql_source in execute_sql_segments(tree, text):
        for target in sql_write_targets(sql_source):
            if sql_target_relation_name(target) in RUNTIME_RELATION_NAMES:
                violations.append((line, target))
    return violations


def source_access_outside_collect_lines(
    tree: ast.AST, text: str, spec: BackendCheckSpec
) -> list[tuple[int, str]]:
    if not spec.source_access_markers:
        return []
    violations: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name in spec.collect_source_access_allowed_functions:
            continue
        function_source = source_segment(text, node)
        if any(marker in function_source for marker in spec.source_access_markers):
            violations.append((node.lineno, node.name))
            continue
        for child in ast.walk(node):
            if not isinstance(child, ast.Call) or not child.args:
                continue
            if not isinstance(child.func, ast.Attribute) or child.func.attr != "execute":
                continue
            sql_source = source_segment(text, child.args[0])
            if re.search(r"\bset\s+schema\b", sql_source, re.I):
                violations.append((child.lineno, node.name))
    return violations


def backend_legacy_compatibility_lines(
    tree: ast.AST, spec: BackendCheckSpec
) -> dict[str, list[int]]:
    lines: dict[str, set[int]] = {
        identifier: set() for identifier in spec.legacy_compatibility_identifiers
    }
    if not lines:
        return {}
    for node in ast.walk(tree):
        found: str | None = None
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            found = node.name
        elif isinstance(node, ast.Name):
            found = node.id
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            found = node.value
        if found in lines:
            lines[found].add(getattr(node, "lineno", 0))

    return {name: sorted(line for line in found if line) for name, found in lines.items() if found}


def validate_sql_backend_contracts(report: Report, root: Path) -> None:
    sql_runtime_root = root / "src" / "retl" / "stores" / "sql_runtime"
    validate_shared_sql_runtime_backend_contract(report, root, sql_runtime_root)
    for spec in BACKEND_CHECK_SPECS:
        validate_single_sql_backend_contract(report, root, spec)


def validate_shared_sql_runtime_backend_contract(
    report: Report, root: Path, sql_runtime_root: Path
) -> None:
    for path in python_files_under(sql_runtime_root):
        text = read_text(path)
        rel_path = relative_repo_path(path, root)
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError as exc:
            add_failure(
                report,
                subject=rel_path,
                problem=f"Python syntax error prevents SQL-backend guardrail scan: {exc}",
                why=(
                    "the SQL backend contract guardrails cannot prove SQL collect "
                    "placement, Runtime writes, or compatibility cleanup unless source files parse."
                ),
                rule="SQL backend and shared SQL runtime Python files must be parseable by the architecture validator.",
                inspect_next="docs/runtime.md",
            )
            continue
        runtime_write_lines = unqualified_runtime_write_lines(tree, text)
        if runtime_write_lines:
            details = ", ".join(
                f"`{target}` line {line}" for line, target in runtime_write_lines[:8]
            )
            add_failure(
                report,
                subject=rel_path,
                problem=f"SQL Runtime write target is not Runtime-relation-qualified: {details}",
                why=(
                    "Runtime relations are RETL-owned and must be addressed through the "
                    "backend-owned Runtime relation space, not the current/default relation space."
                ),
                rule=(
                    "Shared SQL runtime writes must use `context.render_runtime_relation(...)` "
                    "for Runtime table targets."
                ),
                inspect_next="docs/runtime.md",
            )


def validate_single_sql_backend_contract(
    report: Report, root: Path, spec: BackendCheckSpec
) -> None:
    backend_root = root / spec.package_root
    store_path = root / spec.runtime_store_file if spec.runtime_store_file is not None else None
    checked_paths = python_files_under(backend_root)
    for path in sorted({candidate for candidate in checked_paths if candidate.exists()}):
        text = read_text(path)
        rel_path = relative_repo_path(path, root)
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError as exc:
            add_failure(
                report,
                subject=rel_path,
                problem=(
                    f"Python syntax error prevents {spec.display_name} SQL-backend guardrail scan: {exc}"
                ),
                why=(
                    "the SQL backend contract guardrails cannot prove collect placement, "
                    "Runtime writes, or compatibility cleanup unless source files parse."
                ),
                rule=f"{spec.display_name} backend Python files must be parseable by the architecture validator.",
                inspect_next="docs/runtime.md",
            )
            continue

        for label, pattern in spec.banned_sql_patterns:
            banned_lines = banned_sql_pattern_lines(tree, text, pattern)
            if banned_lines:
                add_failure(
                    report,
                    subject=rel_path,
                    problem=(
                        f"{spec.display_name} executable collect SQL contains `{label}` at line(s): "
                        f"{render_line_numbers(banned_lines)}"
                    ),
                    why=(
                        f"{spec.display_name} executable collect has backend-specific relation-space "
                        "rules; this SQL pattern preserves an unsupported legacy source-read path."
                    ),
                    rule=f"{spec.display_name} executable collect must not use `{label}` for Source reads.",
                    inspect_next="docs/runtime.md",
                )

        if store_path is not None and path == store_path and spec.allowed_store_methods:
            shared_behavior_lines = backend_runtime_store_shared_behavior_lines(tree, spec)
            if shared_behavior_lines:
                details = ", ".join(
                    f"`{name}` line {line}" for line, name in shared_behavior_lines[:8]
                )
                add_failure(
                    report,
                    subject=rel_path,
                    problem=(
                        f"{spec.display_name} store facade reintroduces shared runtime behavior: {details}"
                    ),
                    why=(
                        f"`{spec.runtime_store_file}` is the {spec.display_name} runtime-store "
                        "wiring module; runtime domains belong in shared SQL runtime modules."
                    ),
                    rule=(
                        f"{spec.runtime_store_class} may own construction, backend validation, "
                        "and initialization only; shared runtime methods must stay out of "
                        f"`{spec.runtime_store_file}`."
                    ),
                    inspect_next="docs/runtime.md",
                )

        if store_path is not None and path == store_path:
            runtime_write_lines = unqualified_runtime_write_lines(tree, text)
            if runtime_write_lines:
                details = ", ".join(
                    f"`{target}` line {line}" for line, target in runtime_write_lines[:8]
                )
                add_failure(
                    report,
                    subject=rel_path,
                    problem=f"{spec.display_name} Runtime write target is not Runtime-relation-qualified: {details}",
                    why=(
                        "Runtime relations are RETL-owned and must be addressed through the "
                        "backend-owned Runtime relation space, not the current/default relation space."
                    ),
                    rule=(
                        "Runtime writes must use a backend-owned Runtime relation qualifier: "
                        "`{self.schema}.<runtime_table>` in backend store modules or "
                        "`context.render_runtime_relation(...)` in shared SQL runtime modules."
                    ),
                    inspect_next="docs/runtime.md",
                )

        if store_path is not None and path == store_path:
            source_access_lines = source_access_outside_collect_lines(tree, text, spec)
            if source_access_lines:
                details = ", ".join(
                    f"`{function_name}` line {line}"
                    for line, function_name in source_access_lines[:8]
                )
                add_failure(
                    report,
                    subject=rel_path,
                    problem=f"{spec.display_name} Source relation access appears outside collect scope: {details}",
                    why=(
                        "collect is the only runtime phase that may read Source relations; "
                        "stage, reconcile, sync, reporting, and progress paths must stay on "
                        "Runtime relations."
                    ),
                    rule=(
                        f"{spec.display_name} source-space switching and source-space reads may occur only "
                        "inside executable collect helpers."
                    ),
                    inspect_next="docs/runtime.md",
                )
            continue

        source_write_lines = source_adapter_runtime_write_lines(tree, text)
        if source_write_lines:
            details = ", ".join(
                f"`{target}` line {line}" for line, target in source_write_lines[:8]
            )
            add_failure(
                report,
                subject=rel_path,
                problem=f"{spec.display_name} Source adapter writes Runtime relations: {details}",
                why=(
                    "Source adapters prepare read-only Source handles; Runtime table mutation "
                    "belongs to the Runtime store."
                ),
                rule="Source adapter modules must not write Runtime tables.",
                inspect_next="docs/runtime.md",
            )

        compat_lines = backend_legacy_compatibility_lines(tree, spec)
        if compat_lines:
            details = ", ".join(
                f"`{name}` line(s) {render_line_numbers(lines)}"
                for name, lines in sorted(compat_lines.items())
            )
            add_failure(
                report,
                subject=rel_path,
                problem=f"{spec.display_name} Source/runtime compatibility shim remains: {details}",
                why=(
                    "legacy Source/runtime pairing helpers preserve a deleted backend-specific "
                    "configuration model instead of the backend-owned relation-space contract."
                ),
                rule=(
                    f"{spec.display_name} executable collect must be configured through its backend "
                    "package relation-space contract, without old Source/runtime pairing shims."
                ),
                inspect_next="docs/runtime.md",
            )


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def validate_sql_backed_collect_stage_boundary(report: Report, root: Path) -> None:
    runtime_roots = (
        root / "src" / "retl" / "sources",
        root / "src" / "retl" / "artifacts",
        root / "src" / "retl" / "state_runtime",
        root / "src" / "retl" / "runtime",
    )
    for runtime_root in runtime_roots:
        if not runtime_root.exists():
            continue
        for path in runtime_root.rglob("*.py"):
            text = read_text(path)
            rel_path = relative_repo_path(path, root)
            try:
                tree = ast.parse(text, filename=str(path))
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                function_source = source_segment(text, node)
                normalized_name = node.name.lower()
                if "state_style" in normalized_name or "state-style collect" in function_source:
                    banned_collect = [
                        marker
                        for marker in STATE_STYLE_COLLECT_BANNED_MARKERS
                        if marker in function_source
                    ]
                    if banned_collect:
                        add_failure(
                            report,
                            subject=rel_path,
                            problem=(
                                "state-style collect uses superseded source-row authority: "
                                + ", ".join(banned_collect)
                            ),
                            why=(
                                "state-style collect must commit SQL-backed source-state handles "
                                "without source-row batch streaming or collect-owned source payload files."
                            ),
                            rule=(
                                "State-style collect must not call `collect_batches`, row conversion, "
                                "source-batch spooling, or source-state payload file writers."
                            ),
                            inspect_next="docs/runtime.md",
                        )
                if "state_style" in normalized_name or "state-style stage" in function_source:
                    banned_stage = [
                        marker
                        for marker in STATE_STYLE_STAGE_BANNED_MARKERS
                        if marker in function_source
                    ]
                    if banned_stage:
                        add_failure(
                            report,
                            subject=rel_path,
                            problem=(
                                "state-style stage uses superseded row-list materialization: "
                                + ", ".join(banned_stage)
                            ),
                            why=(
                                "stage must materialize destination-bound payloads from committed "
                                "source-state handles, not from full Python row-list fallbacks."
                            ),
                            rule=(
                                "State-style stage must use the SQL-backed materialization seam and "
                                "must not reintroduce row-list source-state handoffs."
                            ),
                            inspect_next="docs/runtime.md",
                        )
                if (
                    "duckdb" in normalized_name
                    and "delta" in normalized_name
                    and "stage" in normalized_name
                ):
                    banned_duckdb_delta = [
                        marker
                        for marker in DUCKDB_DELTA_STAGE_BANNED_MARKERS
                        if marker in function_source
                    ]
                    if banned_duckdb_delta:
                        add_failure(
                            report,
                            subject=rel_path,
                            problem=(
                                "DuckDB retained-delta stage uses row-dict materialization: "
                                + ", ".join(banned_duckdb_delta)
                            ),
                            why=(
                                "DuckDB retained deltas must stream committed payload relations "
                                "through `to_arrow_reader(...)`, not rebuild staged rows from "
                                "`source_run_delta_rows`."
                            ),
                            rule=(
                                "DuckDB retained-delta stage must use committed payload tables "
                                "and Arrow batch streaming."
                            ),
                            inspect_next="docs/runtime.md",
                        )

            event_to_pylist_lines = event_stage_to_pylist_lines(path, root, tree, text)
            if event_to_pylist_lines:
                add_failure(
                    report,
                    subject=rel_path,
                    problem=(
                        "event stage converts collected event Arrow to Python rows at line(s): "
                        f"{render_line_numbers(event_to_pylist_lines)}"
                    ),
                    why=(
                        "event stage must stream committed collected event Arrow batches into "
                        "staged Arrow without full-artifact row materialization."
                    ),
                    rule="Event stage must not call `.to_pylist()` on collected event payloads.",
                    inspect_next="docs/runtime.md",
                )


def is_store_stage_reconcile_runtime_path(path: Path, root: Path) -> bool:
    rel_path = relative_repo_path(path, root)
    if not rel_path.startswith(ROW_OBJECT_PAGE_CONTRACT_RUNTIME_ROOTS):
        return False
    lowered_parts = {part.lower() for part in Path(rel_path).parts}
    lowered_name = path.stem.lower()
    return bool(
        lowered_parts & STORE_STAGE_RECONCILE_MODULE_TERMS
        or lowered_name in STORE_STAGE_RECONCILE_MODULE_TERMS
        or lowered_name in {"contracts", "__init__"}
    )


def _class_field_lines(class_node: ast.ClassDef, names: frozenset[str]) -> list[tuple[int, str]]:
    lines: list[tuple[int, str]] = []
    for child in class_node.body:
        field_name: str | None = None
        if isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
            field_name = child.target.id
        elif isinstance(child, ast.Assign):
            targets = [target.id for target in child.targets if isinstance(target, ast.Name)]
            field_name = targets[0] if len(targets) == 1 else None
        elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            field_name = child.name
        if field_name in names:
            lines.append((child.lineno, field_name))
    return lines


def row_object_page_contract_api_lines(tree: ast.AST) -> list[tuple[int, str, str]]:
    lines: list[tuple[int, str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        if node.name not in ROW_OBJECT_PAGE_CONTRACT_CLASSES:
            continue
        for line, api_name in _class_field_lines(node, ROW_OBJECT_COMPATIBILITY_API_NAMES):
            lines.append((line, node.name, api_name))
    return lines


def row_object_page_constructor_lines(tree: ast.AST) -> list[tuple[int, str, str]]:
    lines: list[tuple[int, str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        page_name = call_name(node)
        if page_name not in ROW_OBJECT_PAGE_CONTRACT_CLASSES:
            continue
        for keyword in node.keywords:
            if keyword.arg in ROW_OBJECT_COMPATIBILITY_API_NAMES:
                lines.append((node.lineno, page_name, keyword.arg))
    return lines


def row_object_page_iteration_lines(tree: ast.AST) -> list[tuple[int, str]]:
    lines: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.For, ast.comprehension)):
            continue
        iterator = node.iter
        if isinstance(iterator, ast.Call) and isinstance(iterator.func, ast.Attribute):
            iterator = iterator.func
        if (
            isinstance(iterator, ast.Attribute)
            and iterator.attr in ROW_OBJECT_COMPATIBILITY_API_NAMES
            and isinstance(iterator.value, ast.Name)
            and iterator.value.id in {"page", "staged"}
        ):
            lineno = getattr(node, "lineno", getattr(iterator, "lineno", 0))
            lines.append((lineno, iterator.attr))
    return lines


def row_object_handoff_export_lines(tree: ast.AST) -> list[tuple[int, str]]:
    lines: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        found: str | None = None
        if isinstance(node, ast.ClassDef):
            found = node.name
        elif isinstance(node, ast.Name):
            found = node.id
        elif isinstance(node, ast.Attribute):
            found = node.attr
        elif isinstance(node, ast.alias):
            found = node.name.rsplit(".", 1)[-1]
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            found = node.value
        if found in ROW_OBJECT_HANDOFF_EXPORT_NAMES:
            lines.append((getattr(node, "lineno", 0), found))
    return lines


def duckdb_fetchmany_ordered_work_expansion_lines(
    path: Path, root: Path, tree: ast.AST, text: str
) -> list[int]:
    rel_path = relative_repo_path(path, root)
    if rel_path != "src/retl/backends/duckdb/store.py":
        return []
    lines: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        function_source = source_segment(text, node)
        if "fetchmany" not in function_source:
            continue
        if "OrderedWorkRow" in function_source or "_ordered_work_row" in function_source:
            lines.append(node.lineno)
    return lines


def validate_row_object_page_contract_guardrails(
    report: Report, root: Path, path: Path, tree: ast.AST, text: str
) -> None:
    if not is_store_stage_reconcile_runtime_path(path, root):
        return
    rel_path = relative_repo_path(path, root)

    page_api_lines = row_object_page_contract_api_lines(tree)
    if page_api_lines:
        details = ", ".join(
            f"{page_name}.{api_name} line {line}"
            for line, page_name, api_name in page_api_lines[:8]
        )
        add_failure(
            report,
            subject=rel_path,
            problem=f"row-object page compatibility API appears: {details}",
            why=(
                "Stage and Reconcile must hand off bounded Arrow-compatible pages; "
                "row-object page APIs keep the deleted store/Stage/Reconcile contract alive."
            ),
            rule=(
                "`PendingWorkPage`, `StateCurrentPage`, `StageWorkPage`, "
                "`StateOperationPage`, and `EventImportPage` must not expose `rows`, "
                "`to_rows`, `iter_rows`, `as_rows`, `row_records`, or `records`."
            ),
            inspect_next="docs/data-plane-types.md",
        )

    constructor_lines = row_object_page_constructor_lines(tree)
    if constructor_lines:
        details = ", ".join(
            f"{page_name}({api_name}=...) line {line}"
            for line, page_name, api_name in constructor_lines[:8]
        )
        add_failure(
            report,
            subject=rel_path,
            problem=f"row-object page constructor handoff appears: {details}",
            why=(
                "Constructing store, Stage, or Reconcile pages from row-object aliases "
                "preserves the forbidden row API even if the field is renamed."
            ),
            rule=(
                "Active store/Stage/Reconcile pages must be constructed from columnar "
                "payloads and metadata, not row containers or row compatibility aliases."
            ),
            inspect_next="docs/data-plane-types.md",
        )

    iteration_lines = row_object_page_iteration_lines(tree)
    if iteration_lines:
        details = ", ".join(f"{api_name} line {line}" for line, api_name in iteration_lines[:8])
        add_failure(
            report,
            subject=rel_path,
            problem=f"runtime iterates row-object page payloads: {details}",
            why=(
                "Reconcile and Stage must use columnar operations over bounded pages; "
                "`for row in page.<alias>` rebuilds the row-object runtime path."
            ),
            rule=(
                "Active store/Stage/Reconcile runtime paths must not iterate "
                "`page.rows`, `page.to_rows`, `page.iter_rows`, `page.as_rows`, "
                "`page.row_records`, or `page.records`."
            ),
            inspect_next="docs/data-plane-types.md",
        )

    row_export_lines = row_object_handoff_export_lines(tree)
    if row_export_lines:
        details = ", ".join(f"{name} line {line}" for line, name in row_export_lines[:8])
        add_failure(
            report,
            subject=rel_path,
            problem=f"row operation/import handoff export appears: {details}",
            why=(
                "`StateOperationRow` and `EventImportRow` make row objects part of the "
                "public Reconcile handoff instead of keeping operations/imports columnar."
            ),
            rule=(
                "Active runtime exports must not expose row operation/import handoff "
                "types such as `StateOperationRow` or `EventImportRow`."
            ),
            inspect_next="docs/data-plane-types.md",
        )

    duckdb_lines = duckdb_fetchmany_ordered_work_expansion_lines(path, root, tree, text)
    if duckdb_lines:
        add_failure(
            report,
            subject=rel_path,
            problem=(
                "DuckDB staged read expands fetched pages into `OrderedWorkRow` at line(s): "
                f"{render_line_numbers(duckdb_lines)}"
            ),
            why=(
                "Store-to-stage pending-work and resend-all reads must return bounded "
                "Arrow-compatible pages, not Python dataclass rows built from "
                "`fetchmany()`."
            ),
            rule=(
                "DuckDB store/Stage reads must not combine `fetchmany()` with "
                "`OrderedWorkRow` or `_ordered_work_row...` expansion."
            ),
            inspect_next="docs/data-plane-types.md",
        )


def validate_columnar_data_plane_runtime(report: Report, root: Path) -> None:
    runtime_files: list[Path] = []
    for rel_dir in AUTHORITATIVE_ARROW_RUNTIME_DIRS:
        runtime_dir = root / rel_dir
        if runtime_dir.exists():
            runtime_files.extend(sorted(runtime_dir.rglob("*.py")))

    for path in runtime_files:
        text = read_text(path)
        rel_path = relative_repo_path(path, root)
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError:
            tree = None

        if tree is not None:
            validate_row_object_page_contract_guardrails(report, root, path, tree, text)

        if any(sentinel in text for sentinel in FAKE_ARROW_SENTINELS):
            add_failure(
                report,
                subject=rel_path,
                problem="runtime code contains a fake Arrow sentinel",
                why=(
                    "authoritative data-plane payloads named `*.arrow` must be real Arrow IPC, "
                    "not JSON bytes prefixed with `ARROW\\n`."
                ),
                rule=(
                    "Columnar data-plane runtime paths must not write, strip, or recognize "
                    '`b"ARROW\\n"` fake Arrow payloads.'
                ),
                inspect_next="docs/control-plane.md",
            )

        if ".arrow" not in text or "json.loads" not in text:
            continue

        if tree is None:
            continue
        json_payload_lines = json_loads_payload_line_numbers(tree)
        if json_payload_lines:
            rendered_lines = ", ".join(str(line) for line in json_payload_lines[:8])
            add_failure(
                report,
                subject=rel_path,
                problem=(
                    "runtime code directly JSON-decodes payload-like bytes in an Arrow-aware module"
                    f" at line(s): {rendered_lines}"
                ),
                why=(
                    "authoritative `.arrow` data-plane artifacts must be reopened through Arrow IPC "
                    "readers; direct JSON decoding is allowed for control-plane metadata only."
                ),
                rule=(
                    "Runtime code that reads authoritative `.arrow` payloads must not call "
                    "`json.loads(...)` on payload-like variables."
                ),
                inspect_next="docs/runtime.md",
            )


def validate_links_resolve(report: Report, file_path: Path) -> None:
    text = read_text(file_path)
    for target in markdown_links(text):
        resolved = resolve_link(file_path, target)
        if resolved is None:
            continue
        if not resolved.exists():
            add_failure(
                report,
                subject=relative_docs_path(file_path),
                problem=f"broken local link to `{target}`",
                why="root navigation and subtree navigation must reach the declared child pages mechanically.",
                rule="All local markdown links must resolve to existing files or directories.",
                inspect_next=relative_docs_path(file_path),
            )


def validate_expected_links(
    report: Report, file_path: Path, expected_targets: Iterable[Path]
) -> None:
    text = read_text(file_path)
    actual_targets = {
        resolved
        for target in markdown_links(text)
        if (resolved := resolve_link(file_path, target)) is not None
    }
    missing = [target for target in expected_targets if target.resolve() not in actual_targets]
    if missing:
        expected_text = ", ".join(relative_docs_path(target) for target in missing)
        add_failure(
            report,
            subject=relative_docs_path(file_path),
            problem=f"missing required navigation targets: {expected_text}",
            why="the docs tree must stay navigable from the maintained index pages without guessing the next file.",
            rule="Each maintained index page must link to its current maintained children.",
            inspect_next=relative_docs_path(file_path),
        )


def validate_docs_reachability(report: Report, docs_root_dir: Path) -> None:
    for path in docs_root_dir.rglob("*.md"):
        if not is_maintained_docs_file(path, docs_root_dir):
            continue
        validate_links_resolve(report, path)


def validate_docs_navigation_coverage(report: Report, docs_root_dir: Path) -> None:
    root_index = docs_root_dir / "index.md"
    if not root_index.exists():
        return

    all_docs = {
        path.resolve()
        for path in docs_root_dir.rglob("*.md")
        if is_maintained_docs_file(path, docs_root_dir)
    }
    visited: set[Path] = set()
    queue: list[Path] = [root_index.resolve()]

    while queue:
        current = queue.pop()
        if current in visited or current not in all_docs:
            continue
        visited.add(current)
        text = read_text(current)
        for target in markdown_links(text):
            resolved = resolve_docs_link(current, target, docs_root_dir)
            if resolved is not None and resolved.resolve() in all_docs:
                queue.append(resolved.resolve())

    orphans = sorted(all_docs - visited)
    if not orphans:
        return

    rendered = ", ".join(relative_docs_path(path) for path in orphans[:10])
    if len(orphans) > 10:
        rendered += ", ..."
    add_failure(
        report,
        subject=relative_docs_path(root_index),
        problem=f"docs tree contains orphaned markdown pages not reachable from `docs/index.md`: {rendered}",
        why="the maintained docs tree must stay navigable from the durable root entrypoint without relying on filesystem guessing.",
        rule="Every maintained markdown page under `docs/` must be reachable by local links starting from `docs/index.md`.",
        inspect_next=relative_docs_path(root_index),
    )


def is_maintained_docs_file(path: Path, docs_root_dir: Path) -> bool:
    if not path.is_file() or path.name.startswith("."):
        return False
    try:
        rel_path = path.resolve().relative_to(docs_root_dir.resolve())
    except ValueError:
        return False
    return not any(rel_path.is_relative_to(root) for root in TEMPORARY_DOCS_EXCLUDED_ROOTS)


def validate_repo_skeleton(root: Path | None = None) -> Report:
    root = root or repo_root()
    docs = docs_root(root)
    report = Report(title="Repository skeleton validation")

    index = docs / "index.md"
    if not index.exists():
        add_failure(
            report,
            subject=relative_docs_path(index, root),
            problem="missing docs root entrypoint",
            why="the control plane starts at `docs/index.md` and the docs tree cannot be navigated without it.",
            rule="`docs/index.md` is the durable docs entrypoint.",
            inspect_next=relative_docs_path(docs, root),
        )
        return report

    for rel_path in REQUIRED_COMPACT_DOCS:
        path = docs / rel_path
        if path.exists():
            continue
        add_failure(
            report,
            subject=relative_docs_path(path, root),
            problem="missing required compact docs page",
            why="the compact docs structure is now the maintained docs control surface.",
            rule="The required compact docs pages must exist at the docs root plus `docs/plans/index.md`.",
            inspect_next=relative_docs_path(docs, root),
        )

    validate_expected_links(
        report,
        index,
        [docs / target for target in ROOT_NAVIGATION_EXPECTATIONS["index.md"]],
    )

    for rel_index, targets in ROOT_NAVIGATION_EXPECTATIONS.items():
        if rel_index == "index.md":
            continue
        index_path = docs / rel_index
        if not index_path.exists():
            add_failure(
                report,
                subject=relative_docs_path(index_path, root),
                problem="missing maintained subtree index",
                why="every maintained subtree needs one local index page that exposes its children.",
                rule="Maintained doc subtrees must remain reachable from their index page.",
                inspect_next=relative_docs_path(index_path.parent, root),
            )
            continue
        validate_expected_links(report, index_path, [index_path.parent / rel for rel in targets])

    validate_docs_reachability(report, docs)
    validate_docs_navigation_coverage(report, docs)
    return report


def validate_package_layering(root: Path | None = None) -> Report:
    root = root or repo_root()
    report = Report(title="Package layering validation")
    docs = docs_root(root)

    for rel_path, phrases in DESIGN_CONTRACT_PHRASES.items():
        path = docs / rel_path
        if not path.exists():
            add_failure(
                report,
                subject=relative_docs_path(path, root),
                problem="missing durable design doc",
                why="package-layering rules must be owned by stable docs before code surfaces can be validated.",
                rule="Durable package and toolchain rules must live in the design docs.",
                inspect_next=relative_docs_path(docs / "design", root),
            )
            continue
        text = read_text(path).lower()
        missing = [phrase for phrase in phrases if phrase.lower() not in text]
        if missing:
            add_failure(
                report,
                subject=relative_docs_path(path, root),
                problem=f"missing required contract language: {', '.join(missing)}",
                why="the package shape docs need to agree on ownership and dependency direction before runtime code lands.",
                rule="Design docs must state the same repository, toolchain, and layering contract.",
                inspect_next=relative_docs_path(path, root),
            )

    validate_trunk_branch_policy(report, root)

    scaffold_started = any(
        exists_or_symlink(root / surface) for surface in SCAFFOLD_TRIGGER_SURFACES
    )
    for surface in REQUIRED_REPO_CONTRACT_SURFACES:
        candidate = root / surface
        if exists_or_symlink(candidate):
            continue
        if scaffold_started:
            add_failure(
                report,
                subject=surface,
                problem="required repo-contract surface is missing",
                why="once the repository scaffold exists, required root and package surfaces must not silently downgrade to optional gaps.",
                rule="The documented repository contract requires the reserved root artifacts, workflow surface, and package roots to exist once scaffolding has started.",
                inspect_next="docs/control-plane.md",
            )
            continue
        add_unavailable(
            report,
            subject=surface,
            problem="surface is not present in this checkout",
            why="this repository is still docs-only, so code/package enforcement cannot yet bind to the final repo-contract file or directory.",
            rule="Missing future repo-contract surfaces must be reported as unavailable rather than validated.",
            inspect_next="docs/control-plane.md",
        )

    pyproject = root / "pyproject.toml"
    if pyproject.exists():
        validate_pyproject_surface(report, pyproject)

    uv_lock = root / "uv.lock"
    if uv_lock.exists():
        validate_lockfile_surface(report, uv_lock)

    makefile = root / "Makefile"
    if makefile.exists():
        validate_makefile_surface(report, makefile)

    workflows_dir = root / ".github" / "workflows"
    if workflows_dir.exists():
        validate_workflow_surface(report, workflows_dir)

    skills_dir = root / ".agents" / "skills"
    if exists_or_symlink(skills_dir):
        validate_repo_skill_surface(report, skills_dir, root)

    validate_packaged_user_setup_resources(report, root)

    for rel_path in REPO_SKILL_SYMLINKS:
        candidate = root / rel_path
        if exists_or_symlink(candidate):
            validate_repo_skill_symlink(report, candidate, root)

    validate_cleanup_proof_surfaces(report, root)
    validate_public_runner_vocabulary(report, root)

    archive_mode = reference_archive_mode(root)

    root_retl_dir = root / "retl"
    if root_retl_dir.exists():
        validate_root_retl_inactive(report, root_retl_dir, root)

    src_retl_dir = root / "src" / "retl"
    if src_retl_dir.exists():
        validate_src_retl_surface(report, src_retl_dir, root)
        validate_columnar_data_plane_runtime(report, root)
        if not archive_mode:
            validate_public_api_alignment(report, root)
            validate_destination_versioning_surfaces(report, root)
            validate_supported_surface_language(report, root)
            validate_current_toolkit_auth_surface(report, root)
            validate_public_rerun_policy_surface(report, root)
            validate_no_concrete_source_backend_leakage(report, root)
            validate_sql_backed_collect_stage_boundary(report, root)
            validate_sql_backend_contracts(report, root)
            validate_sql_runtime_raw_sql_boundary(report, root)

    destination_connectors_dir = root / "destination_connectors"
    if destination_connectors_dir.exists():
        if not archive_mode:
            validate_current_destination_surface_guardrails(report, root)
        validate_destination_connectors_surface(report, destination_connectors_dir, root)

    validate_validation_surfaces(report, root)

    return report


def validate_validation_surfaces(report: Report, root: Path) -> None:
    archive_mode = reference_archive_mode(root)
    validation_scaffold_started = any(
        (root / surface).exists() for surface in VALIDATION_SCAFFOLD_TRIGGER_SURFACES
    )
    for surface in VALIDATION_PATH_SURFACES:
        candidate = root / surface["path"]
        if candidate.exists():
            validate_validation_surface_contents(report, candidate, surface, root)
            continue
        if validation_scaffold_started and not archive_mode:
            add_failure(
                report,
                subject=surface["subject"],
                problem=f"required validation-path surface is missing: {surface['description']}",
                why=(
                    "once runtime or connector scaffolding starts, the repository must expose concrete "
                    "repo-local proof paths instead of relying on plan prose or manual QA."
                ),
                rule=(
                    "The repository must expose repository-local tests and fixtures once runtime or "
                    "connector scaffolding exists."
                ),
                inspect_next="docs/control-plane.md",
            )
            continue
        add_unavailable(
            report,
            subject=surface["subject"],
            problem=f"validation-path surface is not present in this checkout: {surface['description']}",
            why=(
                "this checkout does not yet expose the documented runtime/connector scaffolding, so the "
                "validation-path proof surface cannot be exercised yet."
            ),
            rule="Missing proof surfaces must be reported as unavailable rather than validated.",
            inspect_next="docs/control-plane.md",
        )


def validate_repo_skill_surface(report: Report, skills_dir: Path, root: Path) -> None:
    rel_skills_dir = skills_dir.relative_to(root).as_posix()
    if skills_dir.is_symlink():
        add_failure(
            report,
            subject=rel_skills_dir,
            problem="canonical skill surface is a symlink",
            why="Codex discovers repo skills directly from `.agents/skills`, so this repository keeps the real shared skill files there instead of behind another root-level indirection.",
            rule="`.agents/skills` must be the canonical directory for repo-owned agent skills.",
            inspect_next="docs/control-plane.md",
        )
        return
    if not skills_dir.is_dir():
        add_failure(
            report,
            subject=rel_skills_dir,
            problem="canonical skill surface exists but is not a directory",
            why="repo-owned skills must be maintained as canonical source directories, not placeholder files.",
            rule="`.agents/skills` must be the canonical directory for repo-owned agent skills.",
            inspect_next="docs/control-plane.md",
        )
        return

    skill_dir = skills_dir / REQUIRED_REPO_SKILL_NAME
    skill_entrypoint = skill_dir / "SKILL.md"
    if not skill_entrypoint.exists():
        add_failure(
            report,
            subject=relative_repo_path(skill_entrypoint, root),
            problem="required repo-owned skill entrypoint is missing",
            why="Codex discovers the shared skill from `.agents/skills`, and Claude reaches the same entrypoint through its project-skill symlink.",
            rule="`.agents/skills/retl-create-destination/SKILL.md` must exist.",
            inspect_next="docs/control-plane.md",
        )
        return
    if not skill_entrypoint.is_file():
        add_failure(
            report,
            subject=relative_repo_path(skill_entrypoint, root),
            problem="required repo-owned skill entrypoint is not a file",
            why="skill discovery requires a concrete `SKILL.md` entrypoint.",
            rule="`.agents/skills/retl-create-destination/SKILL.md` must be a file.",
            inspect_next=relative_repo_path(skill_entrypoint, root),
        )
        return

    frontmatter = parse_skill_frontmatter(read_text(skill_entrypoint))
    if frontmatter is None:
        add_failure(
            report,
            subject=relative_repo_path(skill_entrypoint, root),
            problem="skill entrypoint is missing YAML-style frontmatter",
            why="Codex requires `name` and `description` frontmatter, and Claude accepts the same shared metadata.",
            rule="Repo-owned skills must start with frontmatter containing compatible `name` and `description` fields.",
            inspect_next=relative_repo_path(skill_entrypoint, root),
        )
        return

    missing_or_invalid: list[str] = []
    if frontmatter.get("name") != REQUIRED_REPO_SKILL_NAME:
        missing_or_invalid.append(f"`name` must be `{REQUIRED_REPO_SKILL_NAME}`")
    description = frontmatter.get("description", "")
    description_lower = description.lower()
    if not description.strip():
        missing_or_invalid.append("`description` must be non-empty")
    elif "destination" not in description_lower or "connector" not in description_lower:
        missing_or_invalid.append("`description` must explicitly target destination connector work")

    if missing_or_invalid:
        add_failure(
            report,
            subject=relative_repo_path(skill_entrypoint, root),
            problem="skill frontmatter is missing compatible metadata: "
            + ", ".join(missing_or_invalid),
            why="the shared skill must be selectable by both agent tools without relying on duplicated tool-specific copies.",
            rule="`retl-create-destination` skill frontmatter must contain compatible `name` and `description` fields.",
            inspect_next=relative_repo_path(skill_entrypoint, root),
        )


def validate_packaged_user_setup_resources(report: Report, root: Path) -> None:
    packaged_skill_root = root / "src" / "retl" / "skills" / "user"

    for skill_name in REQUIRED_USER_SKILL_NAMES:
        contributor_skill = root / ".agents" / "skills" / skill_name / "SKILL.md"
        if contributor_skill.exists():
            add_failure(
                report,
                subject=relative_repo_path(contributor_skill, root),
                problem="packaged user skill is present on the contributor skill surface",
                why=(
                    "repo contributor skills and installable end-user skills have separate source "
                    "surfaces so repository-development workflows do not drift into user projects."
                ),
                rule=(
                    "End-user skill sources must live under `src/retl/skills/user/`, not "
                    "under `.agents/skills/`."
                ),
                inspect_next="docs/control-plane.md",
            )

        skill_entrypoint = packaged_skill_root / skill_name / "SKILL.md"
        if not skill_entrypoint.exists():
            add_failure(
                report,
                subject=relative_repo_path(skill_entrypoint, root),
                problem="required packaged user skill entrypoint is missing",
                why=(
                    "the `retl install-skills` command must install a complete approved user "
                    "skill set from the wheel."
                ),
                rule=(
                    "Each approved user skill must exist at "
                    "`src/retl/skills/user/<skill>/SKILL.md`."
                ),
                inspect_next="docs/control-plane.md",
            )
            continue

        frontmatter = parse_skill_frontmatter(read_text(skill_entrypoint))
        if frontmatter is None or frontmatter.get("name") != skill_name:
            add_failure(
                report,
                subject=relative_repo_path(skill_entrypoint, root),
                problem="packaged user skill frontmatter is missing compatible metadata",
                why=(
                    "installed skills must be selectable by agent tools after "
                    "`retl install-skills` copies them into a project."
                ),
                rule="Packaged user skill frontmatter must include the matching `name` field.",
                inspect_next=relative_repo_path(skill_entrypoint, root),
            )


def parse_skill_frontmatter(text: str) -> dict[str, str] | None:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return None
    closing_index = None
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            closing_index = index
            break
    if closing_index is None:
        return None

    frontmatter: dict[str, str] = {}
    for line in lines[1:closing_index]:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, separator, value = stripped.partition(":")
        if not separator:
            continue
        clean_value = value.strip()
        if (
            len(clean_value) >= 2
            and clean_value[0] == clean_value[-1]
            and clean_value[0] in {"'", '"'}
        ):
            clean_value = clean_value[1:-1]
        frontmatter[key.strip()] = clean_value
    return frontmatter


def validate_repo_skill_symlink(report: Report, candidate: Path, root: Path) -> None:
    rel_path = candidate.relative_to(root).as_posix()
    if not candidate.is_symlink():
        add_failure(
            report,
            subject=rel_path,
            problem="agent skill discovery surface is not a symlink",
            why="Claude must resolve the same skill files that Codex reads from `.agents/skills` without carrying a duplicated workflow copy.",
            rule="`.claude/skills` must be a symlink to the canonical `.agents/skills` tree.",
            inspect_next="docs/control-plane.md",
        )
        return

    raw_target = candidate.readlink()
    expected_raw_target = REPO_SKILL_SYMLINKS[rel_path]
    expected_resolved = (root / ".agents" / "skills").resolve()
    target_resolved = (
        raw_target.resolve()
        if raw_target.is_absolute()
        else (candidate.parent / raw_target).resolve()
    )
    if raw_target.as_posix() != expected_raw_target or target_resolved != expected_resolved:
        add_failure(
            report,
            subject=rel_path,
            problem=(
                f"agent skill discovery symlink points to the wrong target: {raw_target.as_posix()}"
            ),
            why="Codex and Claude must resolve the same canonical skill tree so skill instructions do not drift by tool.",
            rule="`.claude/skills` must point to `../.agents/skills`.",
            inspect_next="docs/control-plane.md",
        )


def validate_validation_surface_contents(
    report: Report,
    candidate: Path,
    surface: dict[str, str],
    root: Path,
) -> None:
    subject = surface["subject"]
    if not candidate.is_dir():
        add_failure(
            report,
            subject=subject,
            problem="validation-path surface exists but is not a directory",
            why="proof surfaces must resolve to concrete repository locations, not placeholder files.",
            rule="Validation-path surfaces must be concrete repository directories when present.",
            inspect_next=relative_repo_path(candidate, root),
        )
        return

    if subject == "tests/common":
        has_test_files = any(candidate.rglob("test_*.py"))
        if not has_test_files:
            add_failure(
                report,
                subject=subject,
                problem="validation-path surface has no executable test files",
                why="an empty tests directory does not provide a real repository-local proof path.",
                rule="`tests/common/` must contain executable test files when it is the implementation-test surface.",
                inspect_next=relative_repo_path(candidate, root),
            )
        return

    has_contents = any(not child.name.startswith(".") for child in candidate.iterdir())
    if not has_contents:
        add_failure(
            report,
            subject=subject,
            problem="validation-path surface is empty",
            why="placeholder directories do not satisfy the validation policy's mechanical proof requirement.",
            rule="Validation-path directories must contain concrete fixtures or package contents.",
            inspect_next=relative_repo_path(candidate, root),
        )


def validate_pyproject_surface(report: Report, pyproject: Path) -> None:
    text = read_text(pyproject)
    missing_bits = []
    if "[project]" not in text:
        missing_bits.append("`[project]` table")
    if "[build-system]" not in text:
        missing_bits.append("`[build-system]` table")
    if "requires-python" not in text:
        missing_bits.append("`requires-python` declaration")
    elif re.search(r'requires-python\s*=\s*["\']>=3\.12,<3\.15["\']', text) is None:
        missing_bits.append('documented `requires-python = ">=3.12,<3.15"` range')
    if re.search(r'build-backend\s*=\s*["\']hatchling\.build["\']', text.lower()) is None:
        missing_bits.append('`build-backend = "hatchling.build"`')
    if missing_bits:
        add_failure(
            report,
            subject="pyproject.toml",
            problem=f"missing required repo-contract markers: {', '.join(missing_bits)}",
            why="once the root package metadata file exists, the validator must prove it materially carries the documented package and build contract.",
            rule="Root `pyproject.toml` must expose the project metadata, supported Python range, and documented build backend.",
            inspect_next="pyproject.toml",
        )
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        add_failure(
            report,
            subject="pyproject.toml",
            problem=f"invalid TOML: {error}",
            why="the root package metadata file must be parseable before build, typecheck, and tool configuration contracts can be trusted.",
            rule="Root `pyproject.toml` must be valid TOML.",
            inspect_next="pyproject.toml",
        )
        return

    wheel_packages = (
        data.get("tool", {})
        .get("hatch", {})
        .get("build", {})
        .get("targets", {})
        .get("wheel", {})
        .get("packages")
    )
    if wheel_packages != ["src/retl"]:
        add_failure(
            report,
            subject="pyproject.toml",
            problem="wheel package discovery is not limited to the active `src/retl` package",
            why="the root wheel must package only the active src-layout core and must not discover archived legacy code, root `retl/`, or absent destination connector packages.",
            rule='`[tool.hatch.build.targets.wheel] packages` must be exactly `["src/retl"]`.',
            inspect_next="pyproject.toml",
        )

    mypy_path = data.get("tool", {}).get("mypy", {}).get("mypy_path", [])
    if mypy_path != ["src", "tools/checks"]:
        add_failure(
            report,
            subject="pyproject.toml",
            problem=f"mypy path does not name the active src package root exactly: {mypy_path!r}",
            why="type checking must resolve the active `src/retl` package and repo-local checks without falling back to root `retl/` or archived reference code.",
            rule='`[tool.mypy] mypy_path` must be exactly `["src", "tools/checks"]`.',
            inspect_next="pyproject.toml",
        )

    ruff_config = data.get("tool", {}).get("ruff", {})
    ruff_src = ruff_config.get("src", [])
    if ruff_src != ["src", "tests", "tools"]:
        add_failure(
            report,
            subject="pyproject.toml",
            problem=f"Ruff source roots do not match the active src-layout surfaces: {ruff_src!r}",
            why="lint import classification must cover active source, tests, and repo-local tools without treating root `retl/` as a source root.",
            rule='`[tool.ruff] src` must be exactly `["src", "tests", "tools"]`.',
            inspect_next="pyproject.toml",
        )

    pytest_options = data.get("tool", {}).get("pytest", {}).get("ini_options", {})
    pytest_pythonpath = pytest_options.get("pythonpath", [])
    if pytest_pythonpath != ["src"]:
        add_failure(
            report,
            subject="pyproject.toml",
            problem=f"pytest pythonpath does not point at the active src package root: {pytest_pythonpath!r}",
            why="test-time imports must resolve `retl` from `src/retl` instead of a root package directory or installed legacy reference code.",
            rule='pytest `pythonpath` must be exactly `["src"]`.',
            inspect_next="pyproject.toml",
        )


def validate_lockfile_surface(report: Report, uv_lock: Path) -> None:
    text = read_text(uv_lock)
    if not text.strip():
        add_failure(
            report,
            subject="uv.lock",
            problem="lockfile exists but is empty",
            why="the control-plane contract treats `uv.lock` as a committed dependency-resolution artifact, not a placeholder file.",
            rule="`uv.lock` must be a real committed lockfile when present.",
            inspect_next="uv.lock",
        )
    elif "version =" not in text:
        add_failure(
            report,
            subject="uv.lock",
            problem="lockfile does not look like a committed `uv` lockfile",
            why="a placeholder text file is not enough to prove the single-lockfile contract for the repo.",
            rule="`uv.lock` must be a recognizable committed `uv` lockfile when present.",
            inspect_next="uv.lock",
        )


def validate_makefile_surface(report: Report, makefile: Path) -> None:
    text = read_text(makefile)
    targets = make_target_recipes(text)
    missing_targets = [target for target in REQUIRED_MAKE_TARGETS if target not in targets]
    if missing_targets:
        add_failure(
            report,
            subject="Makefile",
            problem=f"missing documented command targets: {', '.join(missing_targets)}",
            why="once the command surface exists, the validator must prove the stable contributor and CI entrypoints are actually present.",
            rule="`Makefile` must expose the documented bootstrap, lint, test, build, and publish command families.",
            inspect_next="Makefile",
        )
        return

    incoherent_targets = [
        target
        for target in REQUIRED_MAKE_TARGETS
        if not recipe_satisfies_target(
            target, targets[target], archive_mode=reference_archive_mode(makefile.parent)
        )
    ]
    if incoherent_targets:
        add_failure(
            report,
            subject="Makefile",
            problem=f"documented targets do not run the documented command surface: {', '.join(incoherent_targets)}",
            why="target-name presence alone does not prove the canonical contributor and CI entrypoints actually execute the documented `uv` workflows.",
            rule="Each required `Makefile` target must run the documented `uv` command family, while connector build or publish targets must either act on active connector packages or fail explicitly during the archive stage.",
            inspect_next="Makefile",
        )


def validate_workflow_surface(report: Report, workflows_dir: Path) -> None:
    if not workflows_dir.is_dir():
        add_failure(
            report,
            subject=".github/workflows",
            problem="workflow surface exists but is not a directory",
            why="CI policy is defined as a directory of executable workflow files, not as a single placeholder path.",
            rule="`.github/workflows/` must be a directory when present.",
            inspect_next=".github/workflows",
        )
        return

    workflow_files = sorted(
        path
        for path in workflows_dir.iterdir()
        if path.is_file() and path.suffix in {".yml", ".yaml"}
    )
    if not workflow_files:
        add_failure(
            report,
            subject=".github/workflows",
            problem="missing workflow definitions under `.github/workflows/`",
            why="a workflow directory without executable workflow files does not satisfy the CI contract.",
            rule="`.github/workflows/` must contain executable workflow definitions when present.",
            inspect_next=".github/workflows",
        )
        return

    missing_files = [
        path
        for path in REQUIRED_WORKFLOW_FILES
        if not (workflows_dir.parent.parent / path).exists()
    ]
    if missing_files:
        add_failure(
            report,
            subject=".github/workflows",
            problem=f"missing required workflow entrypoints: {', '.join(missing_files)}",
            why="the repository layout contract names specific workflow entrypoints, not just a generic workflows directory.",
            rule="`.github/workflows/` must include the documented `main.yml`, `lint.yml`, and `test_common.yml` entrypoints when present.",
            inspect_next=".github/workflows",
        )

    for rel_path in REQUIRED_WORKFLOW_FILES:
        workflow_path = workflows_dir.parent.parent / rel_path
        if workflow_path.exists():
            validate_required_workflow_trunk_triggers(
                report, workflow_path, workflows_dir.parent.parent
            )

    combined = "\n".join(read_text(path).lower() for path in workflow_files)
    missing_versions = [version for version in SUPPORTED_PYTHON_VERSIONS if version not in combined]
    if missing_versions:
        add_failure(
            report,
            subject=".github/workflows",
            problem=f"workflow matrix is missing documented Python versions: {', '.join(missing_versions)}",
            why="the release-policy docs make the supported Python matrix explicit; CI cannot imply it through one local lockfile resolution.",
            rule="Workflow definitions must make Python 3.12, 3.13, and 3.14 coverage explicit.",
            inspect_next=".github/workflows",
        )

    required_workflow_behaviors = {
        "format verification": (
            "make format-check",
            "ruff format --check .",
            "make check",
        ),
        "lint execution": ("make lint", "ruff check .", "make check"),
        "type checking": ("make typecheck", "mypy src tests", "make check"),
        "lockfile consistency": ("make lint-lock", "uv lock --check"),
        "docs/architecture validation": (
            "uv run python tools/checks/validate_repo_skeleton.py",
            "uv run python tools/checks/validate_architecture.py",
        ),
        "default code-change verification": ("make check",),
        "test execution": ("make test", "uv run pytest tests -q", "make check"),
    }
    missing_behaviors = [
        behavior
        for behavior, markers in required_workflow_behaviors.items()
        if not any(marker in combined for marker in markers)
    ]
    if missing_behaviors:
        add_failure(
            report,
            subject=".github/workflows",
            problem=f"workflow definitions are missing documented CI behaviors: {', '.join(missing_behaviors)}",
            why="CI policy is behavioral: it must prove lockfile, docs or architecture, and test surfaces instead of only existing as a directory of files.",
            rule="Workflow definitions must exercise the documented lockfile, docs or architecture, and test entrypoints.",
            inspect_next=".github/workflows",
        )

    publish_workflows = (
        workflows_dir / "publish.yml",
        workflows_dir / "publish-testpypi.yml",
    )
    reference_http_publish_markers = [
        relative_repo_path(path, workflows_dir.parent.parent)
        for path in publish_workflows
        if path.exists() and "reference_http" in read_text(path).lower()
    ]
    if reference_http_publish_markers:
        add_failure(
            report,
            subject=".github/workflows",
            problem=(
                "reference_http is still selectable in PyPI publish workflows: "
                + ", ".join(reference_http_publish_markers)
            ),
            why="the reference HTTP connector is a repo-local test and authoring surface, not a public PyPI deployment target.",
            rule="PyPI publish workflows must not build or publish the repo-local `reference_http` connector.",
            inspect_next=".github/workflows/publish.yml",
        )


def validate_required_workflow_trunk_triggers(
    report: Report, workflow_path: Path, root: Path
) -> None:
    text = read_text(workflow_path)
    missing_events = [
        event
        for event in ("push", "pull_request")
        if not workflow_event_targets_branch(text, event, TRUNK_BRANCH)
    ]
    if not missing_events:
        return

    rel_path = relative_repo_path(workflow_path, root)
    add_failure(
        report,
        subject=rel_path,
        problem=(
            f"required workflow does not target trunk branch `{TRUNK_BRANCH}` for: "
            + ", ".join(missing_events)
        ),
        why=(
            "CI is part of the trunk-based control plane; every required workflow must "
            "validate pushes to trunk and pull requests targeting trunk independently."
        ),
        rule="Each required workflow must declare `push.branches: [main]` and `pull_request.branches: [main]`.",
        inspect_next=rel_path,
    )


def workflow_event_targets_branch(text: str, event_name: str, branch_name: str) -> bool:
    on_body = workflow_on_body(text)
    if on_body is None:
        return False
    event_block = workflow_event_block(on_body, event_name)
    if event_block is None:
        return False
    return branch_list_contains(event_block, branch_name)


def workflow_on_body(text: str) -> str | None:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if re.match(r"^on:\s*$", line):
            body: list[str] = []
            for next_line in lines[index + 1 :]:
                if re.match(r"^[A-Za-z0-9_-]+:\s*", next_line):
                    break
                body.append(next_line)
            return "\n".join(body)
        if re.match(r"^on:\s*\[", line):
            return line
    return None


def workflow_event_block(on_body: str, event_name: str) -> str | None:
    lines = on_body.splitlines()
    event_re = re.compile(rf"^  {re.escape(event_name)}:\s*$")
    for index, line in enumerate(lines):
        if not event_re.match(line):
            continue
        block: list[str] = []
        for next_line in lines[index + 1 :]:
            if re.match(r"^  [A-Za-z0-9_-]+:\s*$", next_line):
                break
            block.append(next_line)
        return "\n".join(block)
    return None


def branch_list_contains(event_block: str, branch_name: str) -> bool:
    escaped = re.escape(branch_name)
    inline_pattern = re.compile(
        rf"branches\s*:\s*\[[^\]]*(?:['\"]?{escaped}['\"]?)[^\]]*\]",
        re.I,
    )
    multiline_pattern = re.compile(rf"branches\s*:\s*\n(?:\s*-\s*['\"]?{escaped}['\"]?)", re.I)
    return bool(inline_pattern.search(event_block) or multiline_pattern.search(event_block))


def imported_modules(py_file: Path) -> set[str]:
    modules: set[str] = set()
    tree = ast.parse(read_text(py_file), filename=str(py_file))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def module_all_exports(py_file: Path) -> set[str]:
    tree = ast.parse(read_text(py_file), filename=str(py_file))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets
        ):
            continue
        value = ast.literal_eval(node.value)
        if isinstance(value, (list, tuple, set)):
            return {str(item) for item in value}
    return set()


def extract_root_api_exports_from_product_doc(doc_path: Path) -> set[str]:
    body = section_body(read_text(doc_path), "Root API Surface") or ""
    return {match.group(1) for match in re.finditer(r"@?retl\.([A-Za-z_]+)", body)}


def extract_root_api_exports_from_example_doc(doc_path: Path) -> set[str]:
    body = section_body(read_text(doc_path), "Root module exports") or ""
    exports: set[str] = set()
    for line in body.splitlines():
        match = re.match(r"^-\s+`?([A-Za-z_]+)`?\s*$", line.strip())
        if match:
            exports.add(match.group(1))
    return exports


def top_level_packages(package_root: Path) -> set[str]:
    return {
        child.name
        for child in package_root.iterdir()
        if child.is_dir() and (child / "__init__.py").exists()
    }


def destination_connector_package_roots(destination_connectors_dir: Path) -> dict[Path, set[str]]:
    return {
        destination_connector_root: top_level_packages(destination_connector_root)
        for destination_connector_root in sorted(
            child
            for child in destination_connectors_dir.iterdir()
            if child.is_dir() and not child.name.startswith(".")
        )
    }


def modules_with_prefix(modules: Iterable[str], forbidden_roots: set[str]) -> list[str]:
    return sorted({module for module in modules if module.split(".", 1)[0] in forbidden_roots})


def validate_python_import_boundaries(
    report: Report,
    *,
    subject: str,
    py_files: Iterable[Path],
    forbidden_roots: set[str],
    rule: str,
    inspect_next: str,
    root: Path,
) -> None:
    if not forbidden_roots:
        return

    for py_file in sorted(py_files):
        forbidden_imports = modules_with_prefix(imported_modules(py_file), forbidden_roots)
        if not forbidden_imports:
            continue
        add_failure(
            report,
            subject=subject,
            problem=(
                f"`{relative_repo_path(py_file, root)}` imports forbidden package roots: "
                + ", ".join(f"`{module}`" for module in forbidden_imports)
            ),
            why="package-layering checks must prove dependency direction mechanically once runtime and destination connector code surfaces exist.",
            rule=rule,
            inspect_next=inspect_next,
        )


def validate_root_retl_inactive(report: Report, retl_dir: Path, root: Path) -> None:
    active_files = sorted(
        path
        for path in retl_dir.rglob("*.py")
        if "__pycache__" not in path.parts and not path.name.startswith(".")
    )
    if active_files:
        rendered = ", ".join(relative_repo_path(path, root) for path in active_files[:8])
        if len(active_files) > 8:
            rendered += ", ..."
        add_failure(
            report,
            subject="retl",
            problem=f"root `retl/` contains active Python package files: {rendered}",
            why="the active core package root has moved to `src/retl`; keeping importable Python files under root `retl/` creates a second active runtime surface.",
            rule="Root `retl/` must not contain active Python package files after the src-layout migration.",
            inspect_next="src/retl",
        )


def validate_src_retl_surface(report: Report, retl_dir: Path, root: Path) -> None:
    if not retl_dir.is_dir():
        add_failure(
            report,
            subject="src/retl",
            problem="runtime package surface exists but is not a directory",
            why="the core runtime package must remain a distinct directory boundary when it is present.",
            rule="`src/retl/` must be a package directory when present.",
            inspect_next="src/retl",
        )
        return
    init_file = retl_dir / "__init__.py"
    if not init_file.exists():
        add_failure(
            report,
            subject="src/retl",
            problem="missing `src/retl/__init__.py`",
            why="once the runtime package surface exists, the root package entrypoint must be present and explicit.",
            rule="The core `src/retl/` package surface must include `src/retl/__init__.py`.",
            inspect_next="src/retl",
        )
        return

    forbidden_root_shims = (
        "collect.py",
        "progress.py",
        "reconcile.py",
        "results.py",
        "runner.py",
        "specs.py",
        "stage.py",
    )
    reintroduced_shims = [name for name in forbidden_root_shims if (retl_dir / name).exists()]
    if reintroduced_shims:
        add_failure(
            report,
            subject="src/retl",
            problem=(
                "pre-release root compatibility or facade files are present: "
                + ", ".join(f"`src/retl/{name}`" for name in reintroduced_shims)
            ),
            why=(
                "the stable public API is the `import retl` root export surface; direct "
                "root facade modules must not remain as a second methodology."
            ),
            rule=(
                "`src/retl/` must not contain the removed root shim or facade modules: "
                "`collect.py`, `progress.py`, `reconcile.py`, "
                "`results.py`, `runner.py`, `specs.py`, or `stage.py`."
            ),
            inspect_next="docs/product.md",
        )

    destination_connectors_dir = root / "destination_connectors"
    connector_packages = (
        set().union(*destination_connector_package_roots(destination_connectors_dir).values())
        if destination_connectors_dir.is_dir()
        else set()
    )
    validate_python_import_boundaries(
        report,
        subject="src/retl",
        py_files=retl_dir.rglob("*.py"),
        forbidden_roots={"destination_connectors", *connector_packages},
        rule="Core runtime code under `src/retl/` must not import concrete destination connector packages or the `destination_connectors/` workspace directly.",
        inspect_next="src/retl",
        root=root,
    )


def validate_public_api_alignment(report: Report, root: Path) -> None:
    retl_init = root / "src" / "retl" / "__init__.py"
    if not retl_init.exists():
        return

    docs = docs_root(root)
    product_doc = docs / "product.md"
    example_doc = docs / "examples.md"
    if not product_doc.exists():
        return

    actual_exports = module_all_exports(retl_init)
    if "audience" in actual_exports:
        add_failure(
            report,
            subject="src/retl/__init__.py",
            problem="removed root audience alias export is present: `audience`",
            why=(
                "`audience_model` is the retained root audience authoring surface; "
                "exporting `audience` keeps the removed alias pair alive."
            ),
            rule="The root package must export `audience_model`, not the removed `audience` alias.",
            inspect_next="docs/product.md",
        )

    product_exports = extract_root_api_exports_from_product_doc(product_doc)

    if actual_exports != product_exports:
        add_failure(
            report,
            subject="src/retl",
            problem=(
                "root package exports do not match `docs/product.md`: "
                f"actual={sorted(actual_exports)}, doc={sorted(product_exports)}"
            ),
            why="the implemented root package must stay aligned with the frozen product public-API contract.",
            rule="`src/retl/__init__.py` and `docs/product.md` must describe the same stable root exports.",
            inspect_next="src/retl/__init__.py",
        )

    if not example_doc.exists():
        return
    example_exports = extract_root_api_exports_from_example_doc(example_doc)
    if not example_exports:
        return
    if product_exports != example_exports:
        add_failure(
            report,
            subject=relative_docs_path(example_doc, root),
            problem=(
                "top-level example exports do not match the stable product doc: "
                f"example={sorted(example_exports)}, product={sorted(product_exports)}"
            ),
            why="the examples tree must illustrate the stable root API rather than becoming a second competing contract surface.",
            rule="`docs/examples.md` must stay aligned with the stable product public-API doc.",
            inspect_next=relative_docs_path(example_doc, root),
        )


def validate_destination_versioning_surfaces(report: Report, root: Path) -> None:
    surfaces = DESTINATION_VERSIONING_DOC_SURFACES + DESTINATION_VERSIONING_FIXTURE_SURFACES
    combined_text = ""
    for rel_path in surfaces:
        path = root / rel_path
        if not path.exists():
            continue
        text = read_text(path)
        combined_text += "\n" + text

    missing = [
        field for field in DESTINATION_COMPATIBILITY_REQUIRED_FIELDS if field not in combined_text
    ]
    if missing:
        add_failure(
            report,
            subject="destination compatibility docs and fixtures",
            problem=(
                "destination compatibility surfaces are missing required narrowed field(s): "
                + ", ".join(f"`{field}`" for field in missing)
            ),
            why=(
                "operators and replay checks need the package compatibility range and the "
                "definition fingerprint to distinguish load-time compatibility from replay safety."
            ),
            rule=(
                "Destination compatibility docs and CLI fixtures must name both "
                "`supported_retl_versions` and `destination_definition_fingerprint`."
            ),
            inspect_next="docs/destinations.md",
        )

    for rel_path in DESTINATION_VERSIONING_FIXTURE_SURFACES:
        path = root / rel_path
        if not path.exists():
            continue
        text = read_text(path)
        missing = [
            field for field in DESTINATION_COMPATIBILITY_REQUIRED_FIELDS if field not in text
        ]
        if missing:
            add_failure(
                report,
                subject=rel_path,
                problem=(
                    "destination compatibility surface is missing required narrowed field(s): "
                    + ", ".join(f"`{field}`" for field in missing)
                ),
                why=(
                    "operators and replay checks need the package compatibility range and the "
                    "definition fingerprint to distinguish load-time compatibility from replay safety."
                ),
                rule=(
                    "Destination compatibility docs and CLI fixtures must name both "
                    "`supported_retl_versions` and `destination_definition_fingerprint`."
                ),
                inspect_next=rel_path,
            )


def validate_supported_surface_language(report: Report, root: Path) -> None:
    for path in supported_surface_language_files(root):
        text = read_text(path)
        match = GENERIC_ROADMAP_VERSION_PATTERN.search(text)
        if match is None:
            continue
        rel_path = relative_repo_path(path, root)
        line_number = text[: match.start()].count("\n") + 1
        add_failure(
            report,
            subject=f"{rel_path}:{line_number}",
            problem="current maintained surface uses generic roadmap-version shorthand",
            why=(
                "the repository should describe supported behavior, supported schemas, "
                "and compatibility ranges directly instead of implying a roadmap generation."
            ),
            rule=(
                "Use concrete supported-surface language or a named persisted schema identifier; "
                "the maintained repository should not carry generic roadmap shorthand."
            ),
            inspect_next=rel_path,
        )


def validate_trunk_branch_policy(report: Report, root: Path) -> None:
    lifecycle_doc = root / "docs" / "control-plane.md"
    if lifecycle_doc.exists():
        text = read_text(lifecycle_doc).lower()
        required = (
            "canonical trunk branch is `main`",
            "release candidates",
            "hotfixes",
            "rollbacks",
            "recommended branch prefixes are `feat/`, `fix/`, `chore/`, and `docs/`",
        )
        missing = [phrase for phrase in required if phrase not in text]
        if missing:
            add_failure(
                report,
                subject=relative_repo_path(lifecycle_doc, root),
                problem=f"trunk policy is missing required lifecycle language: {', '.join(missing)}",
                why=(
                    "the lifecycle policy must make one protected trunk branch authoritative for "
                    "normal work, releases, hotfixes, and rollback decisions."
                ),
                rule="Lifecycle policy must define `main` as trunk.",
                inspect_next=relative_repo_path(lifecycle_doc, root),
            )


def containing_paragraph(text: str, start: int, end: int) -> str:
    paragraph_start = text.rfind("\n\n", 0, start)
    paragraph_end = text.find("\n\n", end)
    if paragraph_start == -1:
        paragraph_start = 0
    else:
        paragraph_start += 2
    if paragraph_end == -1:
        paragraph_end = len(text)
    return text[paragraph_start:paragraph_end]


def validate_cleanup_proof_surfaces(report: Report, root: Path) -> None:
    for path in cleanup_scan_files(root):
        text = read_text(path)
        for match in DEFERRED_OBLIGATION_PATTERN.finditer(text):
            paragraph = containing_paragraph(text, match.start(), match.end()).lower()
            if "docs/plans/deferred-work.md" in paragraph or "deferred-work.md" in paragraph:
                continue
            rel_path = relative_repo_path(path, root)
            line_number = text[: match.start()].count("\n") + 1
            add_failure(
                report,
                subject=f"{rel_path}:{line_number}",
                problem="deferred cleanup marker is not tied to the deferred-work ledger",
                why=(
                    "TODO-style obligations drift unless they are cleared in the implementing "
                    "change or recorded in the repository's durable deferred-work ledger."
                ),
                rule=(
                    "Deferred repository obligations must not use bare TODO, FIXME, or XXX "
                    "markers; record intentional deferrals in `docs/plans/deferred-work.md`."
                ),
                inspect_next="docs/plans/deferred-work.md",
            )

    for path in feature_toggle_scan_files(root):
        text = read_text(path)
        lowered = text.lower()
        for marker in FEATURE_TOGGLE_MARKERS:
            start = lowered.find(marker)
            if start == -1:
                continue
            paragraph = containing_paragraph(text, start, start + len(marker)).lower()
            missing = []
            if "public behavior" not in paragraph and "behavior:" not in paragraph:
                missing.append("public behavior")
            if "validation" not in paragraph and "proof" not in paragraph:
                missing.append("validation surface")
            if not any(
                term in paragraph for term in ("remove", "removal", "expires", "clears when")
            ):
                missing.append("removal condition")
            if not missing:
                continue
            rel_path = relative_repo_path(path, root)
            line_number = text[:start].count("\n") + 1
            add_failure(
                report,
                subject=f"{rel_path}:{line_number}",
                problem=(
                    "feature toggle or compatibility bridge is missing cleanup metadata: "
                    + ", ".join(missing)
                ),
                why=(
                    "incremental trunk work can hide incomplete behavior only when reviewers "
                    "can see what is hidden or bridged, how it is proved, and when it is removed."
                ),
                rule=(
                    "Feature toggles and compatibility bridges in runtime or destination connector code must "
                    "name the public behavior, validation surface, and removal condition."
                ),
                inspect_next="docs/control-plane.md",
            )
            break


def cleanup_scan_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for rel_root in CLEANUP_SCAN_ROOTS:
        candidate = root / rel_root
        if candidate.is_file() and is_cleanup_scan_file(candidate, root):
            files.append(candidate)
            continue
        if not candidate.is_dir():
            continue
        for path in candidate.rglob("*"):
            if is_cleanup_scan_file(path, root):
                files.append(path)
    return sorted(set(files))


def feature_toggle_scan_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for rel_root in FEATURE_TOGGLE_SCAN_ROOTS:
        candidate = root / rel_root
        if not candidate.is_dir():
            continue
        for path in candidate.rglob("*"):
            if is_cleanup_scan_file(path, root):
                files.append(path)
    return sorted(set(files))


def is_cleanup_scan_file(path: Path, root: Path) -> bool:
    if not path.is_file() or path.suffix not in CLEANUP_SCAN_SUFFIXES:
        return False
    try:
        rel_path = path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return not any(part in CLEANUP_EXCLUDED_PARTS for part in rel_path.parts)


def supported_surface_language_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for rel_root in SUPPORTED_SURFACE_LANGUAGE_ROOTS:
        candidate = root / rel_root
        if candidate.is_file():
            if is_supported_surface_language_file(candidate, root):
                files.append(candidate)
            continue
        if not candidate.is_dir():
            continue
        for path in candidate.rglob("*"):
            if is_supported_surface_language_file(path, root):
                files.append(path)
    return sorted(set(files))


def is_supported_surface_language_file(path: Path, root: Path) -> bool:
    if not path.is_file() or path.suffix not in SUPPORTED_SURFACE_LANGUAGE_SUFFIXES:
        return False
    try:
        rel_path = path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    if any(part in SUPPORTED_SURFACE_LANGUAGE_EXCLUDED_PARTS for part in rel_path.parts):
        return False
    return not any(
        rel_path.is_relative_to(prefix) for prefix in SUPPORTED_SURFACE_LANGUAGE_ALLOWED_PREFIXES
    )


def validate_destination_connectors_surface(
    report: Report, destination_connectors_dir: Path, root: Path
) -> None:
    if not destination_connectors_dir.is_dir():
        add_failure(
            report,
            subject="destination_connectors",
            problem="destination connector package surface exists but is not a directory",
            why="first-party destination packages must remain isolated under a dedicated directory when present.",
            rule="`destination_connectors/` must be a directory when present.",
            inspect_next="destination_connectors",
        )
        return

    destination_connector_roots = destination_connector_package_roots(destination_connectors_dir)
    all_connector_packages = set().union(*destination_connector_roots.values())
    for destination_connector_root, package_roots in destination_connector_roots.items():
        connector_pyproject = destination_connector_root / "pyproject.toml"
        connector_text = read_text(connector_pyproject)
        connector_missing = []
        if "[project]" not in connector_text:
            connector_missing.append("`[project]` table")
        if re.search(r'(?m)^name\s*=\s*["\'][^"\']+["\']', connector_text) is None:
            connector_missing.append("package `name`")
        if not has_bounded_retl_dependency(connector_text):
            connector_missing.append("bounded `retl` dependency range")
        if connector_missing:
            add_failure(
                report,
                subject=relative_repo_path(connector_pyproject, root),
                problem=f"connector metadata is missing documented compatibility markers: {', '.join(connector_missing)}",
                why="destination connector packages are authoritative metadata surfaces and must declare their own bounded compatibility with the core distribution.",
                rule="Each `destination_connectors/*/pyproject.toml` must expose project metadata and a bounded `retl` dependency range.",
                inspect_next=relative_repo_path(connector_pyproject, root),
            )
        connector_license = destination_connector_root / "LICENSE-Apache-2.0.txt"
        if not connector_license.exists():
            add_failure(
                report,
                subject=relative_repo_path(connector_license, root),
                problem="connector Apache license notice is missing",
                why="first-party publishable connector packages must carry the outbound Apache-2.0 license notice with the repository's legal identity.",
                rule=(
                    "Each `destination_connectors/*/LICENSE-Apache-2.0.txt` must exist "
                    "and match the canonical destination Apache license notice checksum."
                ),
                inspect_next=relative_repo_path(destination_connector_root, root),
            )
        else:
            connector_license_digest = hashlib.sha256(connector_license.read_bytes()).hexdigest()
            if connector_license_digest != DESTINATION_CONNECTOR_APACHE_LICENSE_SHA256:
                add_failure(
                    report,
                    subject=relative_repo_path(connector_license, root),
                    problem=(
                        "connector Apache license notice does not match the canonical "
                        "destination license checksum"
                    ),
                    why=(
                        "first-party publishable connector packages must carry identical "
                        "outbound Apache-2.0 license notices so copyright holder and "
                        "license terms cannot drift by package."
                    ),
                    rule=(
                        "Each `destination_connectors/*/LICENSE-Apache-2.0.txt` must "
                        "match SHA-256 "
                        f"`{DESTINATION_CONNECTOR_APACHE_LICENSE_SHA256}`."
                    ),
                    inspect_next=relative_repo_path(connector_license, root),
                )
        if not package_roots:
            continue
        py_files = [
            path
            for package_root in package_roots
            for path in (destination_connector_root / package_root).rglob("*.py")
        ]
        validate_python_import_boundaries(
            report,
            subject="destination_connectors",
            py_files=py_files,
            forbidden_roots={"destination_connectors", *(all_connector_packages - package_roots)},
            rule="Destination connector package code must stay isolated to its own package root and must not import sibling destination connector packages through `destination_connectors/` or other destination connector package roots.",
            inspect_next=relative_repo_path(destination_connector_root, root),
            root=root,
        )


def format_report(report: Report) -> str:
    lines = [report.title]
    if report.failures:
        lines.append(f"FAILURES: {len(report.failures)}")
        for finding in report.failures:
            lines.extend(
                [
                    f"- {finding.subject}: {finding.problem}",
                    f"  Why it matters: {finding.why}",
                    f"  Rule violated: {finding.rule}",
                    f"  Inspect next: {finding.inspect_next}",
                ]
            )
    else:
        lines.append("FAILURES: none")

    if report.unavailable:
        lines.append(f"UNAVAILABLE SURFACES: {len(report.unavailable)}")
        for finding in report.unavailable:
            lines.extend(
                [
                    f"- {finding.subject}: {finding.problem}",
                    f"  Why it matters: {finding.why}",
                    f"  Rule: {finding.rule}",
                    f"  Inspect next: {finding.inspect_next}",
                ]
            )
    else:
        lines.append("UNAVAILABLE SURFACES: none")

    lines.append("RESULT: " + ("PASS" if report.ok else "FAIL"))
    return "\n".join(lines)
