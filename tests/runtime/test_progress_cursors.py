from __future__ import annotations

import retl
from retl.runtime.progress import destination_progress_scope


def _destination(name: str = "primary_destination") -> retl.DestinationBinding:
    return retl.DestinationBinding(binding_name=name, destination_ref="retl/mock")


def _state() -> retl.State:
    return retl.state(
        name="customer_state",
        source=retl.source(name="customers", query="select * from customers"),
        key={"customer": "customer_id"},
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"},
    )


def _event() -> retl.Event:
    return retl.event(
        name="purchase",
        source=retl.source(
            name="purchases",
            query="select * from purchases",
            mode="checkpointed",
            checkpoint={
                "cursor": "purchased_at",
                "primary_key": "purchase_id",
                "cursor_type": "string",
                "primary_key_type": "string",
            },
        ),
        key={"purchase": "purchase_id"},
        occurred_at="purchased_at",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"order_total": "order_total"},
    )


def _sync(
    declaration: retl.Declaration,
    *,
    name: str = "customer_sync",
    destination: retl.DestinationBinding | None = None,
    surface: str = "profile_properties",
) -> retl.Sync:
    return retl.sync(
        name=name,
        declaration=declaration,
        destination=destination or _destination(),
        surface=surface,
    )


def test_progress_scope_is_built_from_state_sync() -> None:
    sync = _sync(_state(), destination=_destination("crm_primary"))

    scope = destination_progress_scope(sync)

    assert scope.sync_name == "customer_sync"
    assert scope.destination_name == "crm_primary"
    assert scope.surface == "profile_properties"
    assert scope.family == "state"
    assert scope.declaration_name == "customer_state"


def test_progress_scope_is_built_from_event_sync() -> None:
    sync = _sync(
        _event(),
        name="purchase_sync",
        destination=_destination("events_primary"),
        surface="purchase_event",
    )

    scope = destination_progress_scope(sync)

    assert scope.sync_name == "purchase_sync"
    assert scope.destination_name == "events_primary"
    assert scope.surface == "purchase_event"
    assert scope.family == "event"
    assert scope.declaration_name == "purchase"
