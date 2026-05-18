# RETL Klaviyo Destination

`condor-retl-klaviyo` is a first-party Klaviyo destination connector package. It
exposes `retl/klaviyo` through the `retl.destinations` entry point group.

## Surfaces

- `profiles` accepts State upserts and submits Klaviyo Bulk Profile Import jobs.
  It accepts `email`, `phone_e164`, and `external_id` Identifiers and requires
  at least one of those accepted types. The surface is targetless and does not
  support remove operations.
- `list_memberships` accepts targeted State upserts and submits Klaviyo Bulk
  Profile Import jobs with a `lists` relationship. It accepts `email`,
  `phone_e164`, `external_id`, and `klaviyo_profile_id` Identifiers and
  requires at least one of those accepted types. This surface creates or updates
  profiles and adds them to the target list; it does not support remove
  operations. Logical Targets can be resolved through explicit mappings, the
  runtime Target Registry, or managed list lookup/create by exact list name.
- `list_memberships_by_profile_id` accepts targeted State upserts and removes
  through Klaviyo's List relationships API. It accepts only
  `klaviyo_profile_id`, supports `upsert` and `remove`, and is the surface to
  use for authoritative list membership reconciliation. Logical Targets can be
  resolved through explicit mappings, the runtime Target Registry, or managed
  list lookup/create by exact list name.

These surfaces use Klaviyo private API key auth with the `private_api_key`
credential field. Successful submission returns `delivery_outcome="accepted"`
for Bulk Profile Import surfaces because Klaviyo returns a 202 job acceptance
before asynchronous profile import processing is complete.
`list_memberships_by_profile_id` returns `delivery_outcome="succeeded"` because
Klaviyo returns 204 after the list relationship mutation is accepted by the
synchronous endpoint.

The private API key may be loaded with:

```python
credential_namespace="destinations.klaviyo"
```

which creates `retl.secrets["destinations.klaviyo.private_api_key"]` without
resolving the secret during binding construction.

## Profile Rendering

Each RETL upsert record becomes one JSON:API `profile` object inside a
`profile-bulk-import-job` request. RETL identifiers render to Klaviyo profile
attributes as follows:

- `email` -> `email`
- `phone_e164` -> `phone_number`
- `external_id` -> `external_id`
- `klaviyo_profile_id` -> profile resource `id`

Payload fields named `first_name`, `last_name`, `organization`, `locale`,
`title`, `image`, `location`, `anonymous_id`, and `properties` render as
Klaviyo profile attributes. Other payload fields render into the Klaviyo
`properties` object. `null` payload values are omitted because Klaviyo's Bulk
Profile Import API does not clear profile fields with `null`.

For `list_memberships`, the target resolves to the Klaviyo List ID and is sent
as the Bulk Profile Import job's `relationships.lists.data[0].id`. For
`list_memberships_by_profile_id`, the target resolves to the Klaviyo List ID in
`/api/lists/{id}/relationships/profiles`, and each row renders as
`{"type": "profile", "id": "<klaviyo_profile_id>"}`.

Both list surfaces support managed Targets. During normal runner execution,
when a logical Target is not already mapped or present in the runtime Target
Registry, the connector looks for an existing Klaviyo List with the exact target
name and creates one when missing. Dry runs can plan managed list creation
without creating remote lists or writing Target Registry rows.

Consent is intentionally out of scope for these surfaces. Klaviyo documents
that Bulk Profile Import and List relationship mutations do not update general
consent or subscription status; use Klaviyo's Subscribe or Unsubscribe Profiles
APIs through separate future surfaces when consent updates are needed.

## Binding Config

Shared config:

- `api_revision` defaults to `2026-04-15`.
- `transport` may be injected for offline tests. When omitted, non-dry-run
  submissions use the connector's `requests` transport.

The connector owns the production API origin `https://a.klaviyo.com` in package
code. Tests use injected transports to capture requests without changing that
origin.

`api_revision` is namespace-loadable public config:

```python
destination = retl.destinations.load(
    "retl/klaviyo",
    binding_name="klaviyo_primary",
    credential_namespace="destinations.klaviyo",
    config_namespace="destinations.klaviyo",
)
```

The namespace can provide `DESTINATIONS__KLAVIYO__API_REVISION`.

## API Source

This connector is based on Klaviyo's official Bulk Profile Import API docs,
accessed 2026-05-11:

- `https://developers.klaviyo.com/en/reference/bulk_import_profiles`
- `https://developers.klaviyo.com/en/docs/use_klaviyos_bulk_profile_import_api`
- `https://developers.klaviyo.com/en/reference/add_profiles_to_list`
- `https://developers.klaviyo.com/en/reference/remove_profiles_from_list`

The connector default API revision is `2026-04-15`, matching the latest stable
reference page on the access date. Request planning includes the required
`revision` header, batches at up to 10,000 profiles, and enforces the documented
5 MB request payload ceiling through RETL request batching. List relationship
requests batch at Klaviyo's documented 1,000 profile relationship objects per
request.

## Proof Level

Default tests provide:

1. Contract proof for package loading, connector metadata, surface shape, auth,
   and namespace-loadable config.
2. Translation proof for profile State upserts, list membership imports, and
   profile-ID list membership adds/removes without network access.
3. Mocked transport proof for request rendering, auth placement, response
   classification, redacted diagnostics, transport failure handling, selected
   request-plan reuse, and dry-run behavior.

## Sandbox

Live sandbox tests are opt-in:

```bash
source local/env/.env.klaviyo.sandbox
make test-sandbox-klaviyo
```

The sandbox env file must provide `DESTINATIONS__KLAVIYO__PRIVATE_API_KEY`.
The profile and `list_memberships` sandbox tests submit synthetic Bulk Profile
Import jobs for `lanacooper735@outlook.com`. The list sandbox test creates a
temporary managed list, resolves the generated Klaviyo profile ID by email, adds
and removes that profile through `list_memberships_by_profile_id`, and deletes
the temporary list. Default repository checks exclude `live_sandbox` tests and
do not contact Klaviyo.
