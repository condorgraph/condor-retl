# Reference Mapping

This page maps older RETL resource language to the active State/Event
primitives. It is a reference page only. Core docs use **State** and **Event**
as the public model and do not define profile, membership, consent, or
subscription as first-class resources.

It also preserves the runtime vocabulary mapping for the Runner cutover:
older `deployment` language maps to active `runner` language.

## Mapping Summary

| Older concept | State/Event pattern |
| --- | --- |
| profile | State without Target, usually sent to a profile-shaped Destination Surface |
| membership | State with Target, usually sent to a list, audience, segment, or group surface |
| consent | State with Payload fields required by a consent-shaped Destination Surface |
| subscription | State with Target or Payload fields required by a subscription-shaped surface |
| event | Event |
| deployment | runner |

The functional split is not old resource category. The functional split is:

- **State** for desired current facts that can produce `upsert` and sometimes
  `remove`
- **Event** for occurred facts imported from checkpointed windows

## Profile-Style State

Older profile syncs map to untargeted State. The State Key identifies the
subject, Identifiers let the destination find that subject, and Payload carries
the current fields the selected surface expects.

```python
customer_state = retl.state(
    name="customer_state",
    source=customers,
    key={"customer": "customer_id"},
    identifiers=[{"type": "email", "value": "email"}],
    payload={"plan": "plan", "lifetime_value": "lifetime_value"},
)
```

The destination-specific meaning comes from the selected surface:

```python
retl.sync(
    name="braze_customer_profiles",
    declaration=customer_state,
    destination=braze,
    surface="user_profile",
)
```

## Membership-Style State

Older membership syncs map to targeted State. Target is destination-facing
routing and is part of State identity by default.

```python
audience_state = retl.state(
    name="customer_audience_state",
    source=audience_rows,
    key={"customer": "customer_id"},
    target="audience_key",
    identifiers=[{"type": "email", "value": "email"}],
    payload={},
)
```

If a row disappears for one customer and target, State collect emits `remove`
work for that targeted State identity only. The Sync removal policy decides
whether that remove work is sent or skipped for a destination.

## Consent-Style State

Older consent syncs map to State where the selected Destination Surface defines
the required Payload shape and operation support.

```python
marketing_permission = retl.state(
    name="marketing_permission",
    source=permissions,
    key={"customer": "customer_id", "channel": "channel"},
    identifiers=[{"type": "email", "value": "email"}],
    payload={"channel": "channel", "status": "permission_status"},
)
```

Core RETL does not assign special meaning to fields like `status` or `channel`.
The Destination Surface contract defines whether those fields are required and
how they become partner API calls.

## Subscription-Style State

Older subscription syncs can be modeled either as targeted State or as
untargeted State with surface-required Payload fields.

Use targeted State when the destination object is the routing key:

```python
subscription_state = retl.state(
    name="subscription_group_state",
    source=subscription_rows,
    key={"customer": "customer_id"},
    target="subscription_group_key",
    identifiers=[{"type": "email", "value": "email"}],
    payload={"status": "subscription_status"},
)
```

Use untargeted State when the destination surface expects fields in Payload
instead of a separate Target.

The connector surface owns that choice. Core RETL does not need a separate
subscription primitive to express it.

## Event

Older event syncs remain Event. Event is separate from State because it records
occurrence rather than desired current state.

```python
purchase_events = retl.event(
    name="purchase",
    source=purchases,
    key={"purchase": "purchase_id"},
    occurred_at="purchased_at",
    identifiers=[{"type": "email", "value": "email"}],
    payload={"order_total": "order_total"},
)
```

Events require checkpointed Sources and do not use State removal policy,
resend-all staging, or core Target routing.

## Helpers Later

Product-level helpers may later make common patterns shorter. For example, a
helper could produce a targeted State declaration for a common list-membership
workflow.

Helpers should compile down to State or Event declarations. They should not add
new runtime primitives unless they introduce behavior that State or Event
cannot express.
