from __future__ import annotations

import importlib
import inspect
from types import ModuleType

import pytest

import retl
from retl.backends.duckdb import duckdb as duckdb_source
from retl.declarations.provenance import canonical_declaration


def _legacy_runner_mode_option() -> str:
    return "_".join(("stale", "state", "policy"))


def _legacy_runner_mode_value() -> str:
    return "_".join(("".join(("super", "sede")), "failed"))


def snapshot_source() -> retl.Source:
    return retl.source(name="customers", query="select * from customers")


def checkpointed_source() -> retl.Source:
    return retl.source(
        name="purchase_events",
        mode="checkpointed",
        query="select * from purchase_events",
        checkpoint={
            "cursor": "purchased_at",
            "primary_key": "purchase_id",
            "cursor_type": "string",
            "primary_key_type": "string",
        },
    )


class FakeSource:
    mode = "snapshot"


def test_root_constructor_exports_do_not_collide_with_runtime_packages() -> None:
    assert inspect.isfunction(retl.event)
    assert inspect.isfunction(retl.source)
    assert inspect.isfunction(retl.state)
    assert inspect.isfunction(retl.sync)
    assert inspect.isfunction(retl.target)

    state_runtime = importlib.import_module("retl.state_runtime")
    reconcile_module = importlib.import_module("retl.state_runtime.reconcile")
    sync_runtime = importlib.import_module("retl.sync_runtime")
    submission_module = importlib.import_module("retl.sync_runtime.submission")
    runner_module = importlib.import_module("retl.runtime.runner")
    auth_module = importlib.import_module("retl.auth")

    assert isinstance(state_runtime, ModuleType)
    assert isinstance(reconcile_module, ModuleType)
    assert isinstance(sync_runtime, ModuleType)
    assert isinstance(submission_module, ModuleType)
    assert isinstance(runner_module, ModuleType)
    assert isinstance(auth_module, ModuleType)
    assert inspect.isfunction(retl.state)
    assert inspect.isfunction(retl.sync)
    assert inspect.isfunction(retl.runner)
    assert retl.auth is auth_module
    assert retl.auth.none().kind == "none"
    assert retl.Runner is runner_module.Runner


def test_destination_auth_compatibility_module_is_not_available() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("retl.destinations.auth")


def test_declaration_submodule_imports_preserve_root_constructors() -> None:
    source_module = importlib.import_module("retl.declarations.source")
    state_module = importlib.import_module("retl.declarations.state")
    event_module = importlib.import_module("retl.declarations.event")
    sync_module = importlib.import_module("retl.declarations.sync")

    assert inspect.isfunction(retl.source)
    assert inspect.isfunction(retl.state)
    assert inspect.isfunction(retl.event)
    assert inspect.isfunction(retl.sync)
    assert retl.source is source_module.source
    assert retl.state is state_module.state
    assert retl.event is event_module.event
    assert retl.sync is sync_module.sync


def test_from_retl_import_constructors_match_root_public_api() -> None:
    from retl import event, source, state, sync, target

    assert source is retl.source
    assert state is retl.state
    assert event is retl.event
    assert sync is retl.sync
    assert target is retl.target

    customers = source(name="customers", query="select * from customers")
    customer_state = state(
        name="customer_state",
        source=customers,
        key={"customer": "customer_id"},
    )
    customer_sync = sync(
        name="customer_profiles",
        declaration=customer_state,
        destination=object(),
        surface="user_profile",
    )

    assert isinstance(customers, retl.Source)
    assert isinstance(customer_state, retl.State)
    assert isinstance(customer_sync, retl.Sync)


def test_root_public_api_excludes_advanced_implementation_surfaces() -> None:
    removed_root_exports = {
        "CollectRequest",
        "ConfigRegistry",
        "ConfigResolver",
        "DeclarationStageResult",
        "PhaseEvidence",
        "PhaseName",
        "PhaseStatus",
        "PhaseStatusValue",
        "RunIndex",
        "SecretRegistry",
        "SourceAdapter",
        "SourceBackend",
        "SourceCapabilities",
        "SourceGroupResult",
        "SyncReport",
        "SyncResult",
        "operations",
        "stores",
    }

    assert removed_root_exports.isdisjoint(retl.__all__)
    submodule_names = {"operations", "stores"}
    for name in removed_root_exports - submodule_names:
        assert not hasattr(retl, name)


def test_sources_public_api_excludes_removed_adapter_collection_surfaces() -> None:
    removed_source_exports = {
        "CollectEvidence",
        "CollectRequest",
        "SourceAdapter",
        "SourceBackend",
        "collect_source",
    }

    assert removed_source_exports.isdisjoint(retl.sources.__all__)
    for name in removed_source_exports:
        assert not hasattr(retl.sources, name)


def test_columnar_runtime_wrapper_import_surfaces_are_public() -> None:
    from retl.events import (
        EventCollectEvidence,
        EventImportPage,
        EventReconcileEvidence,
        EventReconcilePageEvidence,
        produce_event_collect,
        stage_event_declaration,
    )
    from retl.events import (
        StageWorkPage as EventStageWorkPage,
    )
    from retl.events import (
        reconcile_event_imports as reconcile_event_imports_from_package,
    )
    from retl.events.reconcile import (
        EventImportPage as EventImportPageFromReconcile,
    )
    from retl.events.reconcile import (
        EventReconcileEvidence as EventReconcileEvidenceFromReconcile,
    )
    from retl.events.reconcile import (
        reconcile_event_imports,
    )
    from retl.runtime import (
        EventImportPage as RuntimeEventImportPage,
    )
    from retl.runtime import (
        StateOperationPage as RuntimeStateOperationPage,
    )
    from retl.runtime import (
        reconcile_event_imports as runtime_reconcile_event_imports,
    )
    from retl.runtime import (
        reconcile_state_operations,
    )
    from retl.state_runtime import (
        StageWorkPage as StateStageWorkPage,
    )
    from retl.state_runtime import (
        StateCollectEvidence,
        StateOperationPage,
        StateReconcileEvidence,
        produce_state_collect,
        reconcile_sync,
        stage_declaration,
        stage_resend_all,
    )

    assert StateOperationPage is RuntimeStateOperationPage
    assert EventImportPage is RuntimeEventImportPage
    assert EventImportPageFromReconcile is RuntimeEventImportPage
    assert EventReconcileEvidence is EventReconcileEvidenceFromReconcile
    assert issubclass(EventReconcileEvidence, EventReconcilePageEvidence)
    assert StateStageWorkPage is EventStageWorkPage

    assert callable(produce_state_collect)
    assert callable(reconcile_sync)
    assert callable(reconcile_state_operations)
    assert callable(produce_event_collect)
    assert callable(stage_declaration)
    assert callable(stage_resend_all)
    assert callable(stage_event_declaration)
    assert reconcile_event_imports_from_package is reconcile_event_imports
    assert runtime_reconcile_event_imports.__name__ == "reconcile_event_imports"

    assert inspect.isclass(StateCollectEvidence)
    assert inspect.isclass(EventCollectEvidence)
    assert inspect.isclass(StateReconcileEvidence)


def test_runtime_entrypoints_do_not_accept_source_checkpoint_store() -> None:
    runtime = importlib.import_module("retl.runtime")
    runner_module = importlib.import_module("retl.runtime.runner")

    assert "checkpoint_store" not in inspect.signature(runtime.run_syncs).parameters
    assert "source_checkpoint_store" not in inspect.signature(runner_module.runner).parameters
    assert "source_checkpoint_store" not in inspect.signature(runner_module.Runner).parameters


def state_declaration() -> retl.State:
    return retl.state(
        name="customer_state",
        source=snapshot_source(),
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )


def event_declaration() -> retl.Event:
    return retl.event(
        name="purchase",
        source=checkpointed_source(),
        key={"purchase": "purchase_id"},
        occurred_at="purchased_at",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"order_total": "order_total"},
    )


def declaration_with_identifiers(kind: str, identifiers: object) -> retl.State | retl.Event:
    if kind == "state":
        return retl.state(
            name="customer_state",
            source=snapshot_source(),
            key={"customer": "customer_id"},
            identifiers=identifiers,  # type: ignore[arg-type]
        )
    return retl.event(
        name="purchase",
        source=checkpointed_source(),
        key={"purchase": "purchase_id"},
        occurred_at="purchased_at",
        identifiers=identifiers,  # type: ignore[arg-type]
    )


def test_source_defaults_to_snapshot_mode() -> None:
    source = retl.source(name="customers", query="select * from customers")

    assert isinstance(source, retl.Source)
    assert source.name == "customers"
    assert source.mode == "snapshot"
    assert source.query == "select * from customers"
    assert source.checkpoint is None


def test_source_accepts_explicit_snapshot_mode() -> None:
    source = retl.source(
        name="customers",
        mode="snapshot",
        query="select * from customers",
    )

    assert isinstance(source, retl.Source)
    assert source.mode == "snapshot"


def test_source_accepts_checkpointed_mode() -> None:
    source = checkpointed_source()

    assert isinstance(source, retl.Source)
    assert source.mode == "checkpointed"
    assert source.checkpoint == {
        "cursor": "purchased_at",
        "primary_key": "purchase_id",
        "cursor_type": "string",
        "primary_key_type": "string",
    }


def test_source_accepts_duckdb_backend_for_snapshot_state() -> None:
    backend = duckdb_source(
        database="warehouse.duckdb",
        read_only=True,
        default_schema="mart",
    )
    source = retl.source(
        name="customers",
        backend=backend,
        query="select customer_id, email, plan from customers",
    )
    state = retl.state(
        name="customer_state",
        source=source,
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )

    assert source.backend is backend
    assert backend.capabilities.supports_snapshot is True
    assert state.source is source


def test_source_accepts_duckdb_backend_for_checkpointed_event() -> None:
    backend = duckdb_source(database="events.duckdb")
    source = retl.source(
        name="purchase_events",
        backend=backend,
        mode="checkpointed",
        query="select purchase_id, email, purchased_at from purchase_events",
        checkpoint={
            "cursor": "purchased_at",
            "primary_key": "purchase_id",
            "cursor_type": "string",
            "primary_key_type": "string",
        },
    )
    event = retl.event(
        name="purchase",
        source=source,
        key={"purchase": "purchase_id"},
        occurred_at="purchased_at",
        identifiers=[{"type": "email", "value": "email"}],
        payload={},
    )

    assert source.backend is backend
    assert backend.capabilities.supports_checkpointed is True
    assert event.source is source


def test_source_identity_includes_sanitized_duckdb_backend_identity() -> None:
    backend = duckdb_source(
        database="warehouse.duckdb",
        config={"token": retl.secrets["warehouse_token"]},
    )
    source_without_backend = retl.source(name="customers", query="select * from customers")
    source_with_backend = retl.source(
        name="customers",
        backend=backend,
        query="select * from customers",
    )
    source_with_other_backend = retl.source(
        name="customers",
        backend=duckdb_source(database="other.duckdb"),
        query="select * from customers",
    )

    assert retl.sources.source_identity(source_with_backend) != retl.sources.source_identity(
        source_without_backend
    )
    assert retl.sources.source_identity(source_with_backend) != retl.sources.source_identity(
        source_with_other_backend
    )
    assert source_with_backend.backend is not None
    assert backend.sanitized_identity["config_keys"] == "token"
    assert "warehouse_token" not in str(backend.sanitized_identity)


def test_destination_binding_loads_builtin_connector() -> None:
    destination = retl.destinations.load(
        "retl/mock",
        binding_name="mock_primary",
    )

    assert isinstance(destination, retl.DestinationBinding)
    assert destination.destination_ref == "retl/mock"
    assert destination.binding_name == "mock_primary"
    assert "profile_properties" in destination.surfaces
    assert destination.auth_mode == "none"
    assert destination.config == {}
    assert destination.credentials == {}


def test_state_accepts_snapshot_source_only() -> None:
    state = state_declaration()

    assert isinstance(state, retl.State)
    assert state.name == "customer_state"
    assert state.source.mode == "snapshot"
    assert state.key == {"customer": "customer_id"}
    assert state.identifiers == [{"type": "email", "value": "email"}]
    assert state.payload == {"plan": "plan"}


def test_state_accepts_targeted_snapshot_example() -> None:
    state = retl.state(
        name="customer_audience_state",
        source=snapshot_source(),
        key={"customer": "customer_id"},
        target="audience_key",
        identifiers=[{"type": "email", "value": "email"}],
        payload={},
    )

    assert state.target == "audience_key"
    assert state.payload == {}


def test_state_accepts_static_target_helper() -> None:
    static_target = retl.target("newsletter_customers")
    state = retl.state(
        name="customer_audience_state",
        source=snapshot_source(),
        key={"customer": "customer_id"},
        target=static_target,
        identifiers=[{"type": "email", "value": "email"}],
    )

    assert isinstance(static_target, retl.StaticTarget)
    assert state.target == static_target
    assert state.target.value == "newsletter_customers"


@pytest.mark.parametrize("value", ("", "   ", 12))
def test_static_target_helper_rejects_invalid_values(value: object) -> None:
    with pytest.raises(retl.DeclarationValidationError, match="Static target"):
        retl.target(value)  # type: ignore[arg-type]


def test_state_rejects_invalid_target_type() -> None:
    with pytest.raises(retl.DeclarationValidationError, match="target"):
        retl.state(
            name="customer_audience_state",
            source=snapshot_source(),
            key={"customer": "customer_id"},
            target=object(),  # type: ignore[arg-type]
            identifiers=[{"type": "email", "value": "email"}],
        )


def test_static_and_column_targets_have_distinct_declaration_identity() -> None:
    source = snapshot_source()
    column_target = retl.state(
        name="customer_audience_state",
        source=source,
        key={"customer": "customer_id"},
        target="newsletter_customers",
        identifiers=[{"type": "email", "value": "email"}],
    )
    static_target = retl.state(
        name="customer_audience_state",
        source=source,
        key={"customer": "customer_id"},
        target=retl.target("newsletter_customers"),
        identifiers=[{"type": "email", "value": "email"}],
    )
    other_static_target = retl.state(
        name="customer_audience_state",
        source=source,
        key={"customer": "customer_id"},
        target=retl.target("vip_customers"),
        identifiers=[{"type": "email", "value": "email"}],
    )

    assert canonical_declaration(column_target)["target"] == "newsletter_customers"
    assert canonical_declaration(static_target)["target"] == {
        "kind": "static",
        "value": "newsletter_customers",
    }
    assert canonical_declaration(static_target) != canonical_declaration(other_static_target)


def test_state_rejects_checkpointed_source() -> None:
    with pytest.raises(ValueError, match=r"(?=.*State)(?=.*snapshot Source)"):
        retl.state(
            name="customer_state",
            source=checkpointed_source(),
            key={"customer": "customer_id"},
            identifiers=[{"type": "email", "value": "email"}],
            payload={"plan": "plan"},
        )


def test_state_rejects_non_source_object() -> None:
    with pytest.raises(ValueError, match="source.*Source"):
        retl.state(
            name="customer_state",
            source=FakeSource(),  # type: ignore[arg-type]
            key={"customer": "customer_id"},
        )


def test_event_accepts_checkpointed_source_only() -> None:
    event = event_declaration()

    assert isinstance(event, retl.Event)
    assert event.name == "purchase"
    assert event.source.mode == "checkpointed"
    assert event.key == {"purchase": "purchase_id"}
    assert event.occurred_at == "purchased_at"
    assert event.identifiers == [{"type": "email", "value": "email"}]
    assert event.payload == {"order_total": "order_total"}


def test_event_rejects_snapshot_source() -> None:
    with pytest.raises(ValueError, match=r"(?=.*Event)(?=.*checkpointed Source)"):
        retl.event(
            name="purchase",
            source=snapshot_source(),
            key={"purchase": "purchase_id"},
            occurred_at="purchased_at",
            identifiers=[{"type": "email", "value": "email"}],
            payload={"order_total": "order_total"},
        )


def test_event_rejects_non_source_object() -> None:
    fake_source = FakeSource()
    fake_source.mode = "checkpointed"

    with pytest.raises(ValueError, match="source.*Source"):
        retl.event(
            name="purchase",
            source=fake_source,  # type: ignore[arg-type]
            key={"purchase": "purchase_id"},
            occurred_at="purchased_at",
        )


@pytest.mark.parametrize("kind", ("state", "event"))
def test_declaration_identifiers_accept_scalar_and_list_source_mappings(kind: str) -> None:
    declaration = declaration_with_identifiers(
        kind,
        [
            {"type": "email", "value": "email"},
            {"type": "email", "values": "emails"},
        ],
    )

    assert declaration.identifiers == [
        {"type": "email", "value": "email"},
        {"type": "email", "values": "emails"},
    ]
    with pytest.raises(TypeError):
        declaration.identifiers[0]["type"] = "phone"  # type: ignore[index]


@pytest.mark.parametrize("kind", ("state", "event"))
@pytest.mark.parametrize(
    ("identifier", "match"),
    (
        ({}, "type"),
        ({"type": "", "value": "email"}, "type"),
        ({"type": "   ", "value": "email"}, "type"),
        ({"type": 1, "value": "email"}, "type"),
        ({"type": "email"}, "exactly one"),
        ({"type": "email", "value": "email", "values": "emails"}, "exactly one"),
        ({"type": "email", "value": ""}, "value.*non-empty string"),
        ({"type": "email", "value": "   "}, "value.*non-empty string"),
        ({"type": "email", "value": 1}, "value.*non-empty string"),
        ({"type": "email", "values": ""}, "values.*non-empty string"),
        ({"type": "email", "values": "   "}, "values.*non-empty string"),
        ({"type": "email", "values": ["emails"]}, "values.*non-empty string"),
        ({"type": "email", "value": "email", "format": "sha256"}, "unsupported key"),
        ("email", "entries must be mappings"),
    ),
)
def test_declaration_identifiers_reject_invalid_mappings(
    kind: str,
    identifier: object,
    match: str,
) -> None:
    with pytest.raises(retl.DeclarationValidationError, match=match):
        declaration_with_identifiers(kind, [identifier])


def test_event_requires_occurred_at() -> None:
    with pytest.raises(ValueError, match="occurred_at"):
        retl.event(
            name="purchase",
            source=checkpointed_source(),
            key={"purchase": "purchase_id"},
            occurred_at="",
            identifiers=[{"type": "email", "value": "email"}],
            payload={"order_total": "order_total"},
        )


def test_sync_requires_surface() -> None:
    with pytest.raises(ValueError, match="surface"):
        retl.sync(
            name="customer_profiles",
            declaration=state_declaration(),
            destination=object(),
            surface="",
        )


def test_sync_requires_destination() -> None:
    with pytest.raises(ValueError, match="destination"):
        retl.sync(
            name="customer_profiles",
            declaration=state_declaration(),
            destination=None,
            surface="user_profile",
        )


def test_sync_requires_state_or_event_declaration() -> None:
    with pytest.raises(ValueError, match="declaration"):
        retl.sync(
            name="customer_profiles",
            declaration=object(),  # type: ignore[arg-type]
            destination=object(),
            surface="user_profile",
        )


def test_sync_binds_declaration_destination_and_surface() -> None:
    destination = object()
    declaration = state_declaration()

    sync = retl.sync(
        name="customer_profiles",
        declaration=declaration,
        destination=destination,
        surface="user_profile",
    )

    assert isinstance(sync, retl.Sync)
    assert sync.name == "customer_profiles"
    assert sync.declaration is declaration
    assert sync.destination is destination
    assert sync.surface == "user_profile"
    assert sync.operations == ("upsert", "remove")
    assert not hasattr(sync, "acknowledgement_policy")
    assert not hasattr(sync, "delivery_outcome")


def test_state_sync_accepts_explicit_operations() -> None:
    sync = retl.sync(
        name="customer_profiles",
        declaration=state_declaration(),
        destination=object(),
        surface="user_profile",
        operations=("upsert",),
    )

    assert sync.operations == ("upsert",)


def test_state_sync_deduplicates_explicit_operations() -> None:
    sync = retl.sync(
        name="customer_profiles",
        declaration=state_declaration(),
        destination=object(),
        surface="user_profile",
        operations=("upsert", "remove", "upsert"),
    )

    assert sync.operations == ("upsert", "remove")


def test_state_sync_rejects_empty_operations() -> None:
    with pytest.raises(ValueError, match="operations"):
        retl.sync(
            name="customer_profiles",
            declaration=state_declaration(),
            destination=object(),
            surface="user_profile",
            operations=(),
        )


def test_state_sync_rejects_invalid_operations() -> None:
    with pytest.raises(ValueError, match="operations"):
        retl.sync(
            name="customer_profiles",
            declaration=state_declaration(),
            destination=object(),
            surface="user_profile",
            operations=("delete",),  # type: ignore[arg-type]
        )


def test_event_sync_rejects_operations() -> None:
    with pytest.raises(ValueError, match="Event Sync"):
        retl.sync(
            name="purchase_imports",
            declaration=event_declaration(),
            destination=object(),
            surface="purchase_event",
            operations=("upsert",),
        )


def test_state_sync_has_no_old_policy_knobs() -> None:
    sync = retl.sync(
        name="customer_profiles",
        declaration=state_declaration(),
        destination=object(),
        surface="user_profile",
    )

    assert not hasattr(sync, "state_" + "strategy")
    assert not hasattr(sync, "delete_" + "policy")
    assert not hasattr(retl, "AcknowledgementPolicy")
    assert not hasattr(retl, "DeliveryOutcome")


def test_sync_accepts_explicit_on_failure_mode() -> None:
    sync = retl.sync(
        name="customer_profiles",
        declaration=state_declaration(),
        destination=object(),
        surface="user_profile",
        on_failure="stop_on_terminal",
    )

    assert sync.on_failure == "stop_on_terminal"


def test_sync_defaults_to_continue_on_any_failure_mode() -> None:
    sync = retl.sync(
        name="customer_profiles",
        declaration=state_declaration(),
        destination=object(),
        surface="user_profile",
    )

    assert sync.on_failure == "continue_on_any"


def test_sync_rejects_invalid_on_failure_mode() -> None:
    with pytest.raises(ValueError, match="on_failure"):
        retl.sync(
            name="customer_profiles",
            declaration=state_declaration(),
            destination=object(),
            surface="user_profile",
            on_failure="keep_going",  # type: ignore[arg-type]
        )


def test_sync_rejects_removed_failure_policy_keyword() -> None:
    removed_keyword = "terminal" + "_failure_policy"
    with pytest.raises(TypeError, match="unexpected keyword"):
        retl.sync(
            name="customer_profiles",
            declaration=state_declaration(),
            destination=object(),
            surface="user_profile",
            **{removed_keyword: object()},  # type: ignore[arg-type]
        )


def test_removed_failure_policy_is_not_publicly_importable() -> None:
    assert not hasattr(retl, "Terminal" + "FailurePolicy")
    declarations = importlib.import_module("retl.declarations")
    policies = importlib.import_module("retl.declarations.policies")
    assert not hasattr(declarations, "Terminal" + "FailurePolicy")
    assert not hasattr(policies, "Terminal" + "FailurePolicy")


def test_sync_rejects_old_state_policy_keyword_arguments() -> None:
    kwargs = {
        "name": "customer_snapshot",
        "declaration": state_declaration(),
        "destination": object(),
        "surface": "user_profile",
        "state_" + "strategy": "diff",
    }

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        retl.sync(**kwargs)  # type: ignore[arg-type]


def test_sync_rejects_removed_delivery_policy_keyword_arguments() -> None:
    for keyword in ("acknowledgement_policy", "delivery_outcome"):
        kwargs = {
            "name": "customer_snapshot",
            "declaration": state_declaration(),
            "destination": object(),
            "surface": "user_profile",
            keyword: "accepted",
        }

        with pytest.raises(TypeError, match="unexpected keyword argument"):
            retl.sync(**kwargs)  # type: ignore[arg-type]


def test_runner_run_forwards_to_ordered_work_runtime() -> None:
    from retl.runtime import executor

    sync = retl.sync(
        name="customer_profiles",
        declaration=state_declaration(),
        destination=object(),
        surface="user_profile",
    )
    calls: list[dict[str, object]] = []

    def fake_run_syncs(**kwargs: object) -> retl.RunResult:
        calls.append(kwargs)
        return retl.RunResult(
            runner_name=str(kwargs["runner_name"]),
            status="succeeded",
            dry_run=bool(kwargs["dry_run"]),
            source_groups=(),
            declaration_stages=(),
            syncs=(),
        )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(executor, "run_syncs", fake_run_syncs)
    try:
        result = retl.runner(name="crm_to_lifecycle").run(sync, dry_run=True)
    finally:
        monkeypatch.undo()

    assert result.status == "succeeded"
    assert calls
    assert calls[0]["syncs"] == [sync]
    assert calls[0]["resend_all"] is False
    assert _legacy_runner_mode_option() not in calls[0]


def test_state_runner_accepts_resend_all_public_option() -> None:
    from retl.runtime import executor

    sync = retl.sync(
        name="customer_profiles",
        declaration=state_declaration(),
        destination=object(),
        surface="user_profile",
    )
    calls: list[dict[str, object]] = []

    def fake_run_syncs(**kwargs: object) -> retl.RunResult:
        calls.append(kwargs)
        return retl.RunResult(
            runner_name=str(kwargs["runner_name"]),
            status="succeeded",
            dry_run=bool(kwargs["dry_run"]),
            source_groups=(),
            declaration_stages=(),
            syncs=(),
        )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(executor, "run_syncs", fake_run_syncs)
    try:
        retl.runner(name="crm_to_lifecycle").run(sync, resend_all=True)
    finally:
        monkeypatch.undo()

    assert calls[0]["resend_all"] is True
    assert _legacy_runner_mode_option() not in calls[0]


def test_runner_run_omits_legacy_mode_public_option() -> None:
    sync = retl.sync(
        name="customer_profiles",
        declaration=state_declaration(),
        destination=object(),
        surface="user_profile",
    )
    legacy_option = _legacy_runner_mode_option()

    assert legacy_option not in inspect.signature(retl.Runner.run).parameters

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        retl.runner(name="crm_to_lifecycle").run(
            sync,
            **{legacy_option: _legacy_runner_mode_value()},  # type: ignore[arg-type]
        )


def test_runner_run_many_omits_legacy_mode_public_option() -> None:
    sync = retl.sync(
        name="customer_profiles",
        declaration=state_declaration(),
        destination=object(),
        surface="user_profile",
    )
    legacy_option = _legacy_runner_mode_option()

    assert legacy_option not in inspect.signature(retl.Runner.run_many).parameters

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        retl.runner(name="crm_to_lifecycle").run_many(
            [sync],
            **{legacy_option: _legacy_runner_mode_value()},  # type: ignore[arg-type]
        )


def test_event_runner_rejects_resend_all_clearly() -> None:
    sync = retl.sync(
        name="purchase_imports",
        declaration=event_declaration(),
        destination=object(),
        surface="purchase_event",
    )

    with pytest.raises(
        retl.DeclarationValidationError,
        match=r"resend_all=True.*State.*Event Syncs.*purchase_imports",
    ):
        retl.runner(name="crm_to_lifecycle").run(sync, resend_all=True)


def test_mixed_state_event_run_many_resend_all_rejects_before_runtime_mutation() -> None:
    from retl.runtime import executor

    state_sync = retl.sync(
        name="customer_profiles",
        declaration=state_declaration(),
        destination=object(),
        surface="user_profile",
    )
    event_sync = retl.sync(
        name="purchase_imports",
        declaration=event_declaration(),
        destination=object(),
        surface="purchase_event",
    )

    def fail_if_called(**_: object) -> retl.RunResult:
        raise AssertionError("resend_all Event preflight must reject before executor mutation")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(executor, "run_syncs", fail_if_called)
    try:
        with pytest.raises(
            retl.DeclarationValidationError,
            match=r"resend_all=True.*State.*Event Syncs.*purchase_imports",
        ):
            retl.runner(name="crm_to_lifecycle").run_many(
                [state_sync, event_sync],
                resend_all=True,
            )
    finally:
        monkeypatch.undo()


def test_run_many_rejects_duplicate_sync_names() -> None:
    declaration = state_declaration()
    sync_a = retl.sync(
        name="customer_profiles",
        declaration=declaration,
        destination=object(),
        surface="user_profile",
    )
    sync_b = retl.sync(
        name="customer_profiles",
        declaration=declaration,
        destination=object(),
        surface="profile_properties",
    )

    with pytest.raises(retl.DeclarationValidationError, match="unique Sync names"):
        retl.runner(name="crm_to_lifecycle").run_many([sync_a, sync_b])
