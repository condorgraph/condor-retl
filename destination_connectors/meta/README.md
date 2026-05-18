# RETL Meta Destination

`condor-retl-meta` is the active first-party Meta Ads destination connector
package for the State/Event architecture. It exposes `retl/meta` through the
`retl.destinations` entry point group.

## Surfaces

- `custom_audiences` accepts targeted State Operations and sends Meta Custom
  Audiences add/remove users requests. It accepts `email`, `phone_e164`, and
  `mobile_advertising_id` Identifiers and requires at least one of those
  accepted types. The surface supports managed targets: when a State row emits a
  logical target that is not mapped or already present in the runtime Target
  Registry, the connector looks for an existing Customer File Custom Audience
  with that exact name and creates one when missing.
- `events` accepts Event imports and sends Meta Conversions API event requests.
  It accepts `email`, `phone_e164`, and `external_id` Identifiers and requires
  at least one of those accepted types. Event bindings require
  `action_source` and either a single `pixel_id` or an `event_routes` mapping
  from Event name to pixel id. Payload fields `fbc`, `fbp`, `client_ip_address`,
  `client_user_agent`, `event_source_url`, and `opt_out` are rendered into the
  Meta Conversions API request shape; remaining payload fields become
  `custom_data`. Optional `test_event_code` is sent at the request body level.

Both surfaces use bearer-token auth with the `access_token` credential field.
Both surfaces declare `delivery_outcome="succeeded"` because successful Meta
responses provide definitive per-request success evidence. Dry runs plan
request batches and do not submit membership or event mutations. For managed
Custom Audiences, dry runs may issue read-only lookup requests to determine
whether a target would be reused or planned for creation.

The access token may be loaded with:

```python
credential_namespace="destinations.meta"
```

which creates `retl.secrets["destinations.meta.access_token"]` without
resolving the token during binding construction.

## Identifier Rendering

Meta hashed matching identifiers are rendered with RETL's shared destination
SHA-256 helpers. For Custom Audiences, `email` and `phone_e164` values are
lowercased and hashed unless the input is already a 64-character hexadecimal
SHA-256 value, in which case it is trimmed, preserved, and lowercased.
`mobile_advertising_id` is trimmed and sent unhashed.

For Conversions API Events, `email` and `phone_e164` use the same lowercase
hash-or-preserve behavior. `external_id` is also rendered as SHA-256, but it is
not lowercased before hashing; 64-character hexadecimal input is preserved as a
pre-hashed value. The connector does not add email, phone, or external-id format
validation beyond existing non-empty request-rendering checks.

## Binding Config

Shared config:

- `ad_account_id` is required and may be provided with or without the `act_`
  prefix.
- `api_version` defaults to `v25.0` and is normalized without leading or
  trailing slashes.
- `transport` may be injected for offline tests. When omitted, non-dry-run
  submissions use the connector's `requests` transport.
- `custom_audience_customer_file_source` defaults to `USER_PROVIDED_ONLY` and
  may be set to `PARTNER_PROVIDED_ONLY` or `BOTH_USER_AND_PARTNER_PROVIDED`
  when creating managed Customer File Custom Audiences.

The connector owns the production API origin `https://graph.facebook.com` in
package code. Tests use injected transports to capture requests without
changing that origin. `base_url` is not a Meta config field and should not
appear in Meta TOML examples, even as a commented placeholder.

`ad_account_id`, `api_version`, and `custom_audience_customer_file_source` are
namespace-loadable public config:

```python
destination = retl.destinations.load(
    "retl/meta",
    binding_name="meta_primary",
    credential_namespace="destinations.meta",
    config_namespace="destinations.meta",
)
```

The namespace can provide `DESTINATIONS__META__AD_ACCOUNT_ID`,
`DESTINATIONS__META__API_VERSION`, and
`DESTINATIONS__META__CUSTOM_AUDIENCE_CUSTOMER_FILE_SOURCE`. Event routing values
such as `pixel_id` or `event_routes` remain explicit because route mappings are
not inferred from namespaces.

Managed Custom Audience lifecycle uses Meta's documented Customer File Custom
Audience flow: read from `/customaudiences` under the ad account, create an
empty audience with `subtype=CUSTOM` and `customer_file_source`, then submit
membership rows to `/{audience_id}/users`. Dry runs may perform the read needed
to identify existing audiences, but do not create audiences or write Target
Registry rows.

`events` config:

- `action_source` is required.
- Provide either `pixel_id` for a single route or `event_routes` to map Event
  names to pixel ids.
- Optional `test_event_code` is included in the request body for Meta test
  events.

Config parsing, URL joining, transport lookup, and diagnostic sanitization live
in `retl_meta.common`. Partner request translation, identifier rendering, and
target/event routing live in `retl_meta.hooks`.

Final submission applies runtime-resolved bearer auth headers when building each
`HttpRequest`. Request planning remains auth-free and secret-free, so dry-run
request plans stay deterministic and redacted.

## Proof Level

Default tests provide:

1. Contract proof for package loading, connector metadata, surfaces, auth, and
   evidence shape.
2. Translation proof for Custom Audiences State Operations and Conversions API
   Event Imports without network access.
3. Mocked transport proof for request rendering, target mapping, route
   selection, managed Custom Audience find/create, runtime-store-backed target
   registry lookup, resolved auth placement, response classification, redacted
   diagnostics, transport failure handling, selected request-plan reuse, and
   destination batch ledger behavior.

## Sandbox

Live sandbox tests are opt-in:

```bash
source local/env/.env.meta-sandbox
make test-sandbox-meta
```

Default repository checks exclude `live_sandbox` tests and do not contact Meta.
