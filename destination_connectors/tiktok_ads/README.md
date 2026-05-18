# TikTok Ads Destination Connector

This package exposes the first-party `retl/tiktok-ads` destination connector.

## Surfaces

- `custom_audiences`: State surface for TikTok Ads Custom Audience membership.
  Supports `upsert` and `remove`, requires a Target audience, and supports
  managed target creation.

RETL accepts `email`, `phone_e164`, and `mobile_advertising_id` identifiers.
The connector hashes values with SHA-256 unless they are already SHA-256 hex.
Email and phone values are trimmed and lowercased before hashing; mobile
advertising IDs are trimmed and lowercased before hashing.

## Auth

The connector uses TikTok's `Access-Token` header:

```python
destination = retl.destinations.load(
    "retl/tiktok-ads",
    binding_name="tiktok_ads_primary",
    credential_namespace="destinations.tiktok_ads",
    config_namespace="destinations.tiktok_ads",
)
```

Environment-backed names:

```text
DESTINATIONS__TIKTOK_ADS__ACCESS_TOKEN
DESTINATIONS__TIKTOK_ADS__ADVERTISER_ID
DESTINATIONS__TIKTOK_ADS__API_VERSION
DESTINATIONS__TIKTOK_ADS__MOBILE_ADVERTISING_ID_TYPE
```

## Config

Required public config:

- `advertiser_id`: TikTok Ads advertiser ID.

Optional public config:

- `api_version`: defaults to TikTok Business API version 1.3, including the
  TikTok path prefix used by the Business API.
- `mobile_advertising_id_type`: defaults to `MAID_SHA256`; may be
  `IDFA_SHA256` or `GAID_SHA256` when the source column is platform-specific.

The connector owns the production API origin `https://business-api.tiktok.com`
in package code. Tests use injected transports to capture requests without
changing that origin.

## API Notes

This connector uses TikTok Business API version 1.3 DMP Custom Audience
endpoints. Membership data is rendered as an in-memory text file, uploaded to
TikTok, and then referenced by the create/update request:

- `POST /open_api/{api_version}/dmp/custom_audience/file/upload/`
- `POST /open_api/{api_version}/dmp/custom_audience/create/`
- `POST /open_api/{api_version}/dmp/custom_audience/update/`

Managed target lookup uses:

- `GET /open_api/{api_version}/dmp/custom_audience/list/`

TikTok audience processing can be asynchronous; submission evidence is therefore
reported as `accepted`.

## Proof

Default package tests are deterministic and offline. They use injected
transports to verify request paths, headers, bodies, target mapping, dry-run
behavior, response classification, and managed target lookup/create behavior.

The opt-in live sandbox test reads `local/env/.env.tiktok_ads-sandbox` when
present and runs with:

```bash
make test-sandbox-tiktok-ads
```

It creates a disposable Custom Audience through file upload, submits one
synthetic `upsert` and one synthetic `remove`, asserts TikTok acceptance
evidence, and best-effort deletes the audience through the DMP Custom Audience
delete endpoint.
