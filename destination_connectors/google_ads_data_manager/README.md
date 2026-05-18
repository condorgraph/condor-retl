# RETL Google Ads Data Manager Destination

`condor-retl-google-ads-data-manager` is the first-party RETL destination
connector package for Google Ads Data Manager.

## Surfaces

- `customer_match`: State surface for Google Ads Customer Match audience
  membership. It supports `upsert` and `remove`, requires Target, and accepts
  `email`, `phone_e164`, `address`, `mobile_advertising_id`, and `external_id`
  Identifiers. It declares `delivery_outcome="accepted"` because Data Manager
  returns an asynchronous `requestId` for later diagnostics rather than final
  success evidence.
- `customer_match_contact_id`: State surface for Google Ads Customer Match
  contact-list membership. It supports `upsert` and `remove`, requires Target,
  accepts only `email`, `phone_e164`, and `address` Identifiers, and supports
  managed Target creation. Missing logical Targets are created as Data Manager
  `CONTACT_ID` UserLists with `displayName` set to the logical Target,
  `membershipStatus="OPEN"`, `membershipDuration="46656000s"` (540 days), and
  first-party contact data source metadata.
- `events`: Event surface for Google Data Manager event ingestion. It supports
  `import`, does not accept Target, and accepts `email`, `phone_e164`, and
  `external_id` Identifiers. It declares `delivery_outcome="accepted"` because
  `events:ingest` returns an asynchronous `requestId`.

This connector maps RETL `upsert` to the Data Manager API version 1 audience
member ingest endpoint and RETL `remove` to the version 1 audience member remove
endpoint. It maps RETL Event imports on the `events` surface to
Data Manager `events:ingest`.

## Identifier Rendering

`email` and `phone_e164` values are rendered as SHA-256 hexadecimal values for
Customer Match `UserData`. A 64-character hexadecimal value is treated as
already hashed, trimmed, preserved, and lowercased. Other email values are
lowercased, stripped of whitespace, and then hashed; Gmail and Googlemail local
parts also have dots removed before hashing. Other phone values are trimmed and
hashed. The connector does not add email or phone format validation beyond
existing non-empty request-rendering checks.

Official docs used for this implementation, accessed 2026-05-07:

- Customer Match overview:
  https://developers.google.com/data-manager/api/devguides/audiences/google-ads/customer-match
- Upload members:
  https://developers.google.com/data-manager/api/devguides/audiences/google-ads/customer-match/upload-data
- Ingest REST reference:
  https://developers.google.com/data-manager/api/reference/rest/v%31/audienceMembers/ingest
- Remove REST reference:
  https://developers.google.com/data-manager/api/reference/rest/v%31/audienceMembers/remove
- Destination fields:
  https://developers.google.com/data-manager/api/reference/rest/v%31/Destination
- User data formatting:
  https://developers.google.com/data-manager/api/devguides/concepts/formatting
- Limits:
  https://developers.google.com/data-manager/api/devguides/limits
- Event ingest REST reference, accessed 2026-05-11:
  https://developers.google.com/data-manager/api/reference/rest/v%31/events/ingest
- Request status REST reference, accessed 2026-05-11:
  https://developers.google.com/data-manager/api/reference/rest/v%31/requestStatus/retrieve
- UserLists REST reference, accessed 2026-05-12:
  https://developers.google.com/data-manager/api/reference/rest/v%31/accountTypes.accounts.userLists
- Create UserList REST reference, accessed 2026-05-12:
  https://developers.google.com/data-manager/api/reference/rest/v%31/accountTypes.accounts.userLists/create

## Auth

The connector declares two auth modes. Both produce a short-lived bearer token
authorized for the Data Manager API scope:

```text
https://www.googleapis.com/auth/datamanager
```

`access_token` accepts an already resolved OAuth access token:

```text
DESTINATIONS__GOOGLE_ADS__ACCESS_TOKEN
```

`service_account` accepts service-account JSON key fields and exchanges them
with Google Auth at runtime:

```text
DESTINATIONS__GOOGLE_ADS__SERVICE_ACCOUNT__PROJECT_ID
DESTINATIONS__GOOGLE_ADS__SERVICE_ACCOUNT__CLIENT_EMAIL
DESTINATIONS__GOOGLE_ADS__SERVICE_ACCOUNT__PRIVATE_KEY
```

Optional service-account fields are `private_key_id` and `token_uri`.

## Binding Config

Required shared config:

- `operating_account_id`: Google Ads customer ID that receives the audience data.
- `customer_match_terms_accepted`: must be `True` for `UserData` and
  `MobileData` uploads. The connector then sends
  `termsOfService.customerMatchTermsOfServiceStatus=ACCEPTED`. Namespace-loaded
  public config may provide this as `true`, `false`, `"true"`, or `"false"`.

Optional shared config:

- `operating_account_type`: defaults to `GOOGLE_ADS`.
- `login_account_id` and `login_account_type`: for manager or data partner
  access paths. `login_account_type` defaults to `GOOGLE_ADS`.
- `linked_account_id` and `linked_account_type`: for data partner access paths.
- `event_destination_id`: required for the `events` surface. It maps to Data
  Manager `productDestinationId` in the request `destinations[]` entry.
- `encoding`: `HEX` by default. Used for `UserData` uploads.
- `ad_user_data_consent` and `ad_personalization_consent`: optional
  operator-supplied request-level consent values. The connector does not default
  either field because RETL cannot infer consent. When supplied, values must be
  `CONSENT_GRANTED`, `CONSENT_DENIED`, or `CONSENT_STATUS_UNSPECIFIED`.
- `request_status_poll_interval_seconds`: event ingest status visibility initial
  backoff. Defaults to `1`. Namespace-loaded public config may provide this as
  a TOML number or numeric string.
- `request_status_poll_timeout_seconds`: event ingest status visibility polling
  timeout. Defaults to `16`. Namespace-loaded public config may provide this as
  a TOML number or numeric string.

The connector owns the production API origin
`https://datamanager.googleapis.com` in package code. Tests use injected
transports to capture requests without changing that origin.

Target maps to the Data Manager `productDestinationId`, which is the Google Ads
audience ID. Configure explicit Target mappings when logical target names differ
from audience IDs.

For managed contact lists, use the `customer_match_contact_id` surface. Runtime
Target resolution first checks explicit Target mappings and the Target Registry,
then looks up a Data Manager UserList by logical Target display name. If none
exists in a non-dry-run sync, the connector creates a `CONTACT_ID` UserList
before membership submission and stores the returned list ID in the Target
Registry. This managed surface intentionally does not create `MOBILE_ID` or
`USER_ID` lists; use the broad `customer_match` surface with explicit Target
mappings for mobile advertising IDs or external IDs.

```python
import retl
from retl.destinations.targets import RemoteTarget, TargetMapping

destination = retl.destinations.load(
    "retl/google-ads-data-manager",
    binding_name="google_ads_customer_match",
    auth_mode="access_token",
    credential_namespace="destinations.google_ads",
    config_namespace="destinations.google_ads",
    config={"customer_match_terms_accepted": True},
    target_mappings=(
        TargetMapping(logical_target="vip_customers", remote=RemoteTarget("1234567890")),
    ),
)
```

Managed contact-list bindings can omit `target_mappings` when source Targets
should create or reuse Data Manager contact UserLists:

```python
destination = retl.destinations.load(
    "retl/google-ads-data-manager",
    binding_name="google_ads_customer_match",
    auth_mode="access_token",
    credential_namespace="destinations.google_ads",
    config_namespace="destinations.google_ads",
    config={"customer_match_terms_accepted": True},
)

sync = retl.sync(
    name="google_ads_contact_customer_match",
    declaration=contact_state,
    destination=destination,
    surface="customer_match_contact_id",
)
```

Service-account bindings use `auth_mode="service_account"`:

```python
destination = retl.destinations.load(
    "retl/google-ads-data-manager",
    binding_name="google_ads_customer_match",
    auth_mode="service_account",
    credential_namespace="destinations.google_ads.service_account",
    config_namespace="destinations.google_ads",
    config={"customer_match_terms_accepted": True},
)
```

The connector declares namespace-loadable config for shared account, consent,
base URL, and encoding fields. Explicit config entries override namespace-loaded
values field by field. Target mappings remain explicit and are not inferred from
the config namespace.

Consent fields may be set at the destination config level when every submitted
record shares the same consent status:

```python
destination = retl.destinations.load(
    "retl/google-ads-data-manager",
    binding_name="google_ads_customer_match",
    auth_mode="access_token",
    credential_namespace="destinations.google_ads",
    config_namespace="destinations.google_ads",
    config={
        "customer_match_terms_accepted": True,
        "ad_user_data_consent": "CONSENT_GRANTED",
        "ad_personalization_consent": "CONSENT_GRANTED",
    },
)
```

For per-record consent, include `ad_user_data_consent` or
`ad_personalization_consent` in the State Payload instead. Those fields are
optional, but when present they must use the same Google consent values.

## Event Import Payloads

The `events` surface renders one Data Manager `Event` per RETL Event import
record. `occurred_at` becomes `eventTimestamp`, `event_key["event_id"]` or the
record identity becomes `transactionId`, and `event_name` becomes `eventName`.
The connector also passes common Data Manager event fields from Payload using
their Data Manager camelCase names or RETL-friendly snake_case aliases, including
`currency`, `conversion_value`, `event_source`, `gclid`, `gbraid`, `wbraid`,
`user_agent`, `ip_address`, `client_id`, `user_id`, `app_instance_id`,
`cart_data`, `custom_variables`, and `additional_event_parameters`.

`email` and `phone_e164` identifiers become `userData.userIdentifiers` after the
same SHA-256 rendering used by Customer Match. `external_id` becomes top-level
`userId` when Payload does not already provide `user_id` or `userId`.

After `events:ingest` returns a `requestId`, the connector polls
`requestStatus:retrieve` at elapsed times of 1, 2, 4, 8, and 16 seconds by
default. A
temporary `404 NOT_FOUND` during this visibility window is treated as a status
read race, not as a failed ingest. If status is still 404 after the window, RETL
returns accepted evidence with the remote request handle and bounded diagnostic
detail; it does not resubmit the event request.

## Identifier Formatting

- `email` values are normalized and SHA-256 hex hashed unless already supplied
  as a 64-character hex SHA-256 value. Gmail and Googlemail dots before `@` are
  removed before hashing.
- `phone_e164` values are SHA-256 hex hashed unless already supplied as a
  64-character hex SHA-256 value.
- `address` values must be objects containing `given_name`, `family_name`,
  `region_code`, and `postal_code`. Names are lowercased and SHA-256 hex hashed.
  Region and postal code are not hashed.
- `mobile_advertising_id` values are sent unhashed in `mobileData.mobileIds`.
- `external_id` values are sent as `userIdData.userId`.

Each RETL record must render to exactly one Data Manager `AudienceMember` union:
`userData`, `mobileData`, or `userIdData`. A single record can combine
`email`, `phone_e164`, and `address` identifiers as `UserData`, but it cannot
mix those with mobile IDs or external IDs.

## Proof Level

Default tests are offline and deterministic. They prove connector metadata,
config validation, request planning, hashing/normalization, Target mapping,
dry-run behavior, selected-plan submission, mocked transport success/failure,
and bounded partner diagnostics.

Live sandbox tests are opt-in and read credentials from
`local/env/.env.google_ads-sandbox` when present:

```bash
make test-sandbox-google-ads
```

The sandbox tests exchange the configured service account for a Data Manager API
access token. They add a synthetic hashed email to the configured Customer Match
audience, submit a matching removal request, and submit one synthetic Event
import to the configured event destination. The tests require:

- `DESTINATIONS__GOOGLE_ADS__SERVICE_ACCOUNT__PROJECT_ID`
- `DESTINATIONS__GOOGLE_ADS__SERVICE_ACCOUNT__CLIENT_EMAIL`
- `DESTINATIONS__GOOGLE_ADS__SERVICE_ACCOUNT__PRIVATE_KEY`
- `DESTINATIONS__GOOGLE_ADS__OPERATING_ACCOUNT_ID`
- `DESTINATIONS__GOOGLE_ADS__CUSTOMER_MATCH__CONTAINERS__SAMPLE_CUSTOMERS`
- `DESTINATIONS__GOOGLE_ADS__EVENTS__DESTINATION_ID`

The live event sandbox proves live API acceptance only. It disables status
polling for the live mutation so the test does not depend on final conversion
attribution, click validity, or the request-status visibility race. Live managed
audience creation, read-back list-size validation, and final event attribution
validation are intentionally deferred.
