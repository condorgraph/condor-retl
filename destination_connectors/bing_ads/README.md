# RETL Microsoft Advertising Destination

`condor-retl-bing-ads` is the first-party Microsoft Advertising destination
connector package for RETL State contracts. It exposes `retl/bing-ads` through the
`retl.destinations` entry point group.

## Surface

- `customer_lists` accepts targeted State Operations and sends Microsoft
  Advertising Campaign Management API v13 `ApplyCustomerListUserData` requests
  to add or remove Customer Match users from Customer List audiences. It accepts
  `email`, `phone_e164`, and `mobile_advertising_id` Identifiers and requires at
  least one of those accepted types.

The surface supports managed targets. When a State row emits a logical target
that is not mapped or already present in the runtime Target Registry, the
connector calls `GetAudiencesByIds` with `Type=CustomerList` and empty
`AudienceIds` to inspect account and customer scoped Customer List audiences.
For REST JSON the connector sends `AudienceIds` as `null`, because the live REST
endpoint accepts `null` or an omitted field but rejects an empty JSON array,
then creates a missing target through `AddAudiences` with `Type=CustomerList`.
Dry runs may perform the read-only lookup, but do not create audiences or write
Target Registry rows.

The surface uses custom `microsoft_advertising` auth with `access_token` and
`developer_token` credential fields. Request planning remains auth-free and
secret-free; runtime submission adds `Authorization`, `DeveloperToken`,
`CustomerAccountId`, and `CustomerId` headers after secret resolution.

## Identifier Rendering

`email` and `phone_e164` values are lowercased and SHA-256 hashed unless the
input is already a 64-character hexadecimal SHA-256 value, in which case it is
trimmed, preserved, and lowercased. `mobile_advertising_id` is trimmed and sent
unhashed. Microsoft accepts one `CustomerListItemSubType` per request, so RETL
partitions batches by target, operation, and identifier subtype.

## Binding Config

Required config:

- `customer_account_id`: the ad account id used in Microsoft Advertising request
  headers and as the default parent for account-scoped managed targets.
- `customer_id`: the manager customer id used in request headers and as the
  parent for customer-scoped managed targets.

Optional config:

- `api_version` defaults to `v13` and is pinned to that value.
- `target_scope` defaults to `Account` and may be set to `Customer` for managed
  Customer List creation.
- `membership_duration` defaults to `-1`; Microsoft allows `1` through `390` or
  `-1` for no expiration. Namespace-loaded public config may provide this as
  `-1` or `"-1"`.
- `accept_customer_match_terms` defaults to `True` and is sent as
  `AcceptCustomerMatchTerm` on each membership upload. Namespace-loaded public
  config may provide this as `true`, `false`, `"true"`, or `"false"`.
- `transport` may be injected for offline tests. When omitted, non-dry-run
  submissions use the connector's `requests` transport.

The connector owns the production API origin
`https://campaign.api.bingads.microsoft.com` in package code. Tests use injected
transports to capture requests without changing that origin.

The namespace-loadable public config fields are `customer_account_id`,
`customer_id`, `api_version`, `target_scope`, `membership_duration`,
and `accept_customer_match_terms`.

```python
destination = retl.destinations.load(
    "retl/bing-ads",
    binding_name="bing_ads_primary",
    credential_namespace="destinations.bing_ads",
    config_namespace="destinations.bing_ads",
)
```

The credential namespace creates
`retl.secrets["destinations.bing_ads.access_token"]` and
`retl.secrets["destinations.bing_ads.developer_token"]` without resolving those
secrets during binding construction.

## Microsoft API References

Implementation decisions were checked against Microsoft Learn on 2026-05-13:

- Campaign Management API v13 `ApplyCustomerListUserData` REST JSON:
  <https://learn.microsoft.com/en-us/advertising/campaign-management-service/applycustomerlistuserdata?view=bingads-13>
- Campaign Management API v13 `CustomerList` object:
  <https://learn.microsoft.com/en-us/advertising/campaign-management-service/customerlist?view=bingads-13>
- Campaign Management API v13 `AddAudiences` REST JSON:
  <https://learn.microsoft.com/en-us/advertising/campaign-management-service/addaudiences?view=bingads-13>
- Campaign Management API v13 `GetAudiencesByIds` REST JSON:
  <https://learn.microsoft.com/en-us/advertising/campaign-management-service/getaudiencesbyids?view=bingads-13>

## Proof Level

Default tests provide contract proof for connector metadata, namespace loading,
config validation, Customer List request rendering, hashed identifier handling,
managed target lookup/create, runtime Target Registry reuse, response
classification, selected request-plan submission, and transport failure handling.

## Sandbox

Live sandbox tests are opt-in and read credentials from
`local/env/.env.bing_ads-sandbox` when present:

```bash
make test-sandbox-bing-ads
```

The sandbox test creates a disposable managed Customer List audience through
`AddAudiences`, submits one synthetic email add and remove through
`ApplyCustomerListUserData`, and then deletes the disposable list through
`DeleteAudiences`. It does not require a pre-existing Customer List. The test
requires:

- `DESTINATIONS__BING_ADS__ACCESS_TOKEN`, or both
  `DESTINATIONS__BING_ADS__CLIENT_ID` and
  `DESTINATIONS__BING_ADS__REFRESH_TOKEN`
- `DESTINATIONS__BING_ADS__DEVELOPER_TOKEN`
- `DESTINATIONS__BING_ADS__CUSTOMER_ACCOUNT_ID`
- `DESTINATIONS__BING_ADS__CUSTOMER_ID`

Optional sandbox config:

- `DESTINATIONS__BING_ADS__API_VERSION`
- `DESTINATIONS__BING_ADS__ACCEPT_CUSTOMER_MATCH_TERMS`
