# Destination Connectors Root

This directory is the active first-party destination connector root for the
State/Event rewrite.

Active packages follow the template in `docs/control-plane.md`: package-local
metadata, an importable `retl_<connector>` module, a `retl.destinations` entry
point, and package-local tests.

- `reference_http/` proves the active State/Event destination package path with
  `state_records` and `event_imports` surfaces. It is a local proof connector,
  not a production partner integration.
- `meta/` is the first active production partner package. It exposes
  `custom_audiences` and `events` surfaces and keeps live sandbox validation
  behind the opt-in `live_sandbox` marker.
- `bing_ads/` exposes Microsoft Advertising Customer Match Customer List
  membership through Campaign Management API v13 and supports managed Customer
  List targets.
- `google_ads_data_manager/` exposes Google Ads Data Manager Customer Match
  audience membership through Data Manager API version 1 audience member
  ingestion and removal endpoints.
- `klaviyo/` exposes Klaviyo profile create/upsert through the Bulk Profile
  Import API.
- `tiktok_ads/` exposes TikTok Ads Custom Audience membership through the
  Business API Segment endpoints and supports managed Custom Audience targets.

First-party packages declare namespace-loadable public config where it is safe
and bounded. Credentials use the selected Auth Mode's required fields;
connector docs list the supported `DESTINATIONS__...` environment names.
