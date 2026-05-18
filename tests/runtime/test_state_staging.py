from __future__ import annotations

import retl
from retl.state_runtime.staging import declaration_identity


def _source() -> retl.Source:
    return retl.source(name="customers", query="select customer_id, email from customers")


def test_declaration_identity_distinguishes_static_and_column_targets() -> None:
    source = _source()
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

    assert declaration_identity(column_target) != declaration_identity(static_target)
    assert declaration_identity(static_target) != declaration_identity(other_static_target)
