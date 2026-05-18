from __future__ import annotations

import pytest

import retl
from retl.destinations.compatibility import (
    DestinationCompatibilityError,
    check_surface_compatibility,
    validate_surface_compatibility,
)
from retl.destinations.surfaces import DestinationSurface, IdentifierRequirement


def _snapshot_source() -> retl.Source:
    return retl.source(name="customers", query="select * from customers")


def _checkpointed_source() -> retl.Source:
    return retl.source(
        name="purchases",
        query="select * from purchases",
        mode="checkpointed",
        checkpoint={
            "cursor": "purchased_at",
            "primary_key": "purchase_id",
            "cursor_type": "string",
            "primary_key_type": "string",
        },
    )


def _state(*, target: bool = False, payload: dict[str, str] | None = None) -> retl.State:
    return retl.state(
        name="customer_state",
        source=_snapshot_source(),
        key={"customer": "customer_id"},
        target="audience_key" if target else None,
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"} if payload is None else payload,
    )


def _state_static_target(*, payload: dict[str, str] | None = None) -> retl.State:
    return retl.state(
        name="customer_state",
        source=_snapshot_source(),
        key={"customer": "customer_id"},
        target=retl.target("newsletter_customers"),
        identifiers=[{"type": "email", "value": "email"}],
        payload={"plan": "plan"} if payload is None else payload,
    )


def _state_with_identifiers(identifiers: list[dict[str, str]]) -> retl.State:
    return retl.state(
        name="customer_state",
        source=_snapshot_source(),
        key={"customer": "customer_id"},
        identifiers=identifiers,
        payload={"plan": "plan"},
    )


def _event(*, payload: dict[str, str] | None = None) -> retl.Event:
    return retl.event(
        name="purchase",
        source=_checkpointed_source(),
        key={"purchase": "purchase_id"},
        occurred_at="purchased_at",
        identifiers=[{"type": "email", "value": "email"}],
        payload={"order_total": "order_total"} if payload is None else payload,
    )


def _event_with_identifiers(identifiers: list[dict[str, str]]) -> retl.Event:
    return retl.event(
        name="purchase",
        source=_checkpointed_source(),
        key={"purchase": "purchase_id"},
        occurred_at="purchased_at",
        identifiers=identifiers,
        payload={"order_total": "order_total"},
    )


def _requires_email() -> tuple[IdentifierRequirement, ...]:
    return (IdentifierRequirement(match="all_of", identifier_types=("email",)),)


def _sync(
    declaration: retl.Declaration,
    *,
    surface: str = "profile_properties",
    operations: tuple[retl.StateOperation, ...] | None = None,
) -> retl.Sync:
    return retl.sync(
        name="customer_sync",
        declaration=declaration,
        destination=object(),
        surface=surface,
        operations=operations,
    )


def test_state_surface_accepts_matching_state_contract() -> None:
    surface = DestinationSurface(
        name="profile_properties",
        declaration_family="state",
        supported_operations=("upsert", "remove"),
        accepted_identifier_types=("email",),
        identifier_requirements=_requires_email(),
        required_payload_fields=("plan",),
    )

    compatibility = validate_surface_compatibility(sync=_sync(_state()), surface=surface)

    assert compatibility.valid is True
    assert compatibility.operation_kinds == ("upsert", "remove")


def test_state_to_event_surface_fails_before_mutation() -> None:
    surface = DestinationSurface(
        name="purchase_event",
        declaration_family="event",
        supported_operations=("import",),
        accepted_identifier_types=("email",),
        identifier_requirements=_requires_email(),
    )

    with pytest.raises(DestinationCompatibilityError, match="State Sync"):
        validate_surface_compatibility(sync=_sync(_state()), surface=surface)


def test_event_to_state_surface_fails_before_mutation() -> None:
    surface = DestinationSurface(
        name="profile_properties",
        declaration_family="state",
        supported_operations=("upsert",),
        accepted_identifier_types=("email",),
        identifier_requirements=_requires_email(),
    )

    with pytest.raises(DestinationCompatibilityError, match="Event Sync"):
        validate_surface_compatibility(
            sync=_sync(_event(), surface="profile_properties"),
            surface=surface,
        )


def test_required_and_unsupported_target_modes_are_validated() -> None:
    required = DestinationSurface(
        name="list_membership",
        declaration_family="state",
        supported_operations=("upsert", "remove"),
        target_mode="required",
        accepted_identifier_types=("email",),
        identifier_requirements=_requires_email(),
    )
    unsupported = DestinationSurface(
        name="profile_properties",
        declaration_family="state",
        supported_operations=("upsert", "remove"),
        target_mode="unsupported",
        accepted_identifier_types=("email",),
        identifier_requirements=_requires_email(),
    )

    assert check_surface_compatibility(sync=_sync(_state()), surface=required).issues[0].code == (
        "target_required"
    )
    assert (
        check_surface_compatibility(
            sync=_sync(_state(target=True)),
            surface=unsupported,
        )
        .issues[0]
        .code
        == "target_unsupported"
    )
    assert (
        check_surface_compatibility(
            sync=_sync(_state_static_target()),
            surface=required,
        ).valid
        is True
    )
    assert (
        check_surface_compatibility(
            sync=_sync(_state_static_target()),
            surface=unsupported,
        )
        .issues[0]
        .code
        == "target_unsupported"
    )


def test_upsert_only_state_surface_rejects_possible_remove_work() -> None:
    surface = DestinationSurface(
        name="upsert_only_profile",
        declaration_family="state",
        supported_operations=("upsert",),
        accepted_identifier_types=("email",),
        identifier_requirements=_requires_email(),
    )

    with pytest.raises(DestinationCompatibilityError, match="remove"):
        validate_surface_compatibility(sync=_sync(_state()), surface=surface)


def test_upsert_only_state_surface_accepts_upsert_only_sync() -> None:
    surface = DestinationSurface(
        name="upsert_only_profile",
        declaration_family="state",
        supported_operations=("upsert",),
        accepted_identifier_types=("email",),
        identifier_requirements=_requires_email(),
    )

    compatibility = validate_surface_compatibility(
        sync=_sync(_state(), operations=("upsert",)),
        surface=surface,
    )

    assert compatibility.valid is True
    assert compatibility.operation_kinds == ("upsert",)


def test_missing_identifier_and_payload_requirements_fail() -> None:
    surface = DestinationSurface(
        name="profile_properties",
        declaration_family="state",
        supported_operations=("upsert", "remove"),
        accepted_identifier_types=("phone",),
        identifier_requirements=(
            IdentifierRequirement(match="all_of", identifier_types=("phone",)),
        ),
        required_payload_fields=("plan",),
    )

    compatibility = check_surface_compatibility(
        sync=_sync(_state(payload={})),
        surface=surface,
    )

    assert {issue.code for issue in compatibility.issues} == {
        "unsupported_identifier_type",
        "missing_all_identifier_types",
        "missing_payload_fields",
    }


def test_sync_no_longer_accepts_delivery_policy_arguments() -> None:
    for keyword in ("acknowledgement_policy", "delivery_outcome"):
        kwargs = {
            "name": "customer_sync",
            "declaration": _state(),
            "destination": object(),
            "surface": "profile_properties",
            keyword: "accepted",
        }

        with pytest.raises(TypeError):
            retl.sync(**kwargs)  # type: ignore[arg-type]


def test_surface_delivery_outcome_is_singular_and_validated() -> None:
    accepted = DestinationSurface(
        name="event_import",
        declaration_family="event",
        supported_operations=("import",),
        delivery_outcome="accepted",
    )

    assert accepted.delivery_outcome == "accepted"

    with pytest.raises(retl.DeclarationValidationError, match="delivery_outcome"):
        DestinationSurface(
            name="bad_event_import",
            declaration_family="event",
            supported_operations=("import",),
            delivery_outcome="confirmed",  # type: ignore[arg-type]
        )

    with pytest.raises(retl.DeclarationValidationError, match="delivery_outcome"):
        DestinationSurface(
            name="legacy_event_import",
            declaration_family="event",
            supported_operations=("import",),
            delivery_outcome=("accepted",),  # type: ignore[arg-type]
        )


def test_event_surface_validates_event_payload_requirements() -> None:
    surface = DestinationSurface(
        name="purchase_event",
        declaration_family="event",
        supported_operations=("import",),
        accepted_identifier_types=("email",),
        identifier_requirements=_requires_email(),
        required_payload_fields=("order_total",),
    )

    assert validate_surface_compatibility(
        sync=_sync(_event(), surface="purchase_event"),
        surface=surface,
    ).valid
    assert (
        check_surface_compatibility(
            sync=_sync(_event(payload={}), surface="purchase_event"),
            surface=surface,
        )
        .issues[0]
        .code
        == "missing_payload_fields"
    )


def test_surface_rejects_duplicate_accepted_identifier_types() -> None:
    with pytest.raises(retl.DeclarationValidationError, match="accepted_identifier_types"):
        DestinationSurface(
            name="profile_properties",
            declaration_family="state",
            supported_operations=("upsert",),
            accepted_identifier_types=("email", "email"),
        )


def test_surface_rejects_identifier_requirement_outside_accepted_set() -> None:
    with pytest.raises(retl.DeclarationValidationError, match="outside"):
        DestinationSurface(
            name="profile_properties",
            declaration_family="state",
            supported_operations=("upsert",),
            accepted_identifier_types=("email",),
            identifier_requirements=(
                IdentifierRequirement(match="any_of", identifier_types=("phone",)),
            ),
        )


def test_surface_rejects_unknown_identifier_requirement_match() -> None:
    with pytest.raises(retl.DeclarationValidationError, match="match"):
        IdentifierRequirement(match="one_of", identifier_types=("email",))  # type: ignore[arg-type]


def test_unsupported_declared_identifier_type_fails_compatibility() -> None:
    surface = DestinationSurface(
        name="profile_properties",
        declaration_family="state",
        supported_operations=("upsert", "remove"),
        accepted_identifier_types=("email",),
        identifier_requirements=_requires_email(),
    )

    compatibility = check_surface_compatibility(
        sync=_sync(
            _state_with_identifiers(
                [
                    {"type": "email", "value": "email"},
                    {"type": "phone", "value": "phone"},
                ]
            ),
        ),
        surface=surface,
    )

    assert [issue.code for issue in compatibility.issues] == ["unsupported_identifier_type"]


def test_any_of_identifier_requirement_accepts_any_named_identifier() -> None:
    surface = DestinationSurface(
        name="custom_audiences",
        declaration_family="state",
        supported_operations=("upsert", "remove"),
        accepted_identifier_types=("email", "phone_e164", "mobile_advertising_id"),
        identifier_requirements=(
            IdentifierRequirement(
                match="any_of",
                identifier_types=("email", "phone_e164", "mobile_advertising_id"),
            ),
        ),
    )

    compatibility = validate_surface_compatibility(
        sync=_sync(
            _state_with_identifiers(
                [{"type": "mobile_advertising_id", "value": "mobile_advertising_id"}]
            ),
        ),
        surface=surface,
    )

    assert compatibility.valid


def test_any_of_identifier_requirement_rejects_missing_named_identifiers() -> None:
    surface = DestinationSurface(
        name="custom_audiences",
        declaration_family="state",
        supported_operations=("upsert", "remove"),
        accepted_identifier_types=("email", "phone_e164", "mobile_advertising_id"),
        identifier_requirements=(
            IdentifierRequirement(
                match="any_of",
                identifier_types=("email", "phone_e164", "mobile_advertising_id"),
            ),
        ),
    )

    compatibility = check_surface_compatibility(
        sync=_sync(_state_with_identifiers([])),
        surface=surface,
    )

    assert [issue.code for issue in compatibility.issues] == ["missing_any_identifier_type"]


def test_all_of_identifier_requirement_requires_every_named_identifier() -> None:
    surface = DestinationSurface(
        name="profile_merge",
        declaration_family="state",
        supported_operations=("upsert", "remove"),
        accepted_identifier_types=("email", "external_id"),
        identifier_requirements=(
            IdentifierRequirement(match="all_of", identifier_types=("email", "external_id")),
        ),
    )

    compatibility = check_surface_compatibility(
        sync=_sync(
            _state_with_identifiers([{"type": "email", "value": "email"}]),
        ),
        surface=surface,
    )

    assert [issue.code for issue in compatibility.issues] == ["missing_all_identifier_types"]

    valid = validate_surface_compatibility(
        sync=_sync(
            _state_with_identifiers(
                [
                    {"type": "email", "value": "email"},
                    {"type": "external_id", "value": "external_id"},
                ]
            ),
        ),
        surface=surface,
    )
    assert valid.valid


def test_multiple_identifier_requirements_must_all_pass() -> None:
    surface = DestinationSurface(
        name="profile_enrichment",
        declaration_family="state",
        supported_operations=("upsert", "remove"),
        accepted_identifier_types=("email", "phone_e164", "external_id"),
        identifier_requirements=(
            IdentifierRequirement(match="all_of", identifier_types=("email",)),
            IdentifierRequirement(match="any_of", identifier_types=("phone_e164", "external_id")),
        ),
    )

    compatibility = check_surface_compatibility(
        sync=_sync(
            _state_with_identifiers([{"type": "email", "value": "email"}]),
        ),
        surface=surface,
    )

    assert [issue.code for issue in compatibility.issues] == ["missing_any_identifier_type"]

    valid = validate_surface_compatibility(
        sync=_sync(
            _state_with_identifiers(
                [
                    {"type": "email", "value": "email"},
                    {"type": "external_id", "value": "external_id"},
                ]
            ),
        ),
        surface=surface,
    )
    assert valid.valid


def test_meta_event_identifier_policy_accepts_external_id_without_email() -> None:
    surface = DestinationSurface(
        name="events",
        declaration_family="event",
        supported_operations=("import",),
        accepted_identifier_types=("email", "phone_e164", "external_id"),
        identifier_requirements=(
            IdentifierRequirement(
                match="any_of",
                identifier_types=("email", "phone_e164", "external_id"),
            ),
        ),
        required_payload_fields=("order_total",),
    )

    compatibility = validate_surface_compatibility(
        sync=_sync(
            _event_with_identifiers([{"type": "external_id", "value": "external_id"}]),
            surface="events",
        ),
        surface=surface,
    )

    assert compatibility.valid
