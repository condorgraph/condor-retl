# RETL Reference HTTP Destination

`condor-retl-reference-http` is the repo-local reference package scaffold for
proving RETL destination package loading and HTTP request planning with
State/Event Destination Surface contracts. It is used by tests and connector
authors, and is not published to PyPI.

The package exposes the `retl/reference-http` connector through the
`retl.destinations` entry point group.

## Surfaces

- `state_records` accepts State Operations, supports `upsert` and `remove`, and
  accepts `email` Identifiers, requires `email`, `customer_id` State Key field,
  and `status` Payload field.
- `event_imports` accepts Event imports, declares
  `delivery_outcome="succeeded"`, accepts `email` Identifiers, and requires
  `email`, `event_id` Event Key field, and `event_name` Payload field.

Both surfaces produce definitive succeeded delivery evidence when the mocked
HTTP transport returns successful response classification. Accepted-only
fixtures remain available for negative tests that prove accepted evidence does
not satisfy a succeeded surface.

Both surfaces declare `retl.auth.none()` for local package proof. Dry runs build
deterministic, redacted HTTP request plans and do not execute transport.

## Binding Config

The reference connector is local-test infrastructure, so it does not read
secrets and does not require destination credentials.

- `base_url` sets the absolute HTTP(S) origin used for non-dry-run submissions.
  It defaults to `https://reference-http.example.test` and must not contain
  user info, query parameters, or fragments.
- `request_batch_max_rows` controls the maximum records per planned request
  batch. It defaults to `1000`.
- `transport` may be an injected object with a `send(HttpRequest) ->
  HttpResponse` method for offline submission tests. It is test-only config and
  is excluded from public request-plan config.

`base_url` and `request_batch_max_rows` are namespace-loadable public config:

```python
destination = retl.destinations.load(
    "retl/reference-http",
    binding_name="reference_http",
    config_namespace="destinations.reference_http",
    config={"transport": transport},
)
```

The namespace can provide `DESTINATIONS__REFERENCE_HTTP__BASE_URL` and
`DESTINATIONS__REFERENCE_HTTP__REQUEST_BATCH_MAX_ROWS`. Injected `transport`
stays explicit and is never loaded from a namespace.

Config parsing, URL validation, URL joining, transport lookup, and public config
filtering live in `retl_reference_http.common`. Keep partner-specific request
translation in hooks, but put reusable connector-local support code in a small
support module so production destinations can follow the same package shape.

Final submission applies runtime-resolved auth headers when building each
`HttpRequest`. Request planning remains auth-free and secret-free, so dry-run
request plans stay deterministic and redacted.

## Proof Level

This package is the canonical minimal generic/private HTTP destination shape.
It deliberately keeps configurable `base_url` because it represents caller-owned
HTTP endpoints. First-party partner connectors should not copy that part of the
surface; partner packages bake their production origins in code and use injected
transports in tests. Its default tests provide:

1. Contract proof for package loading, connector metadata, surfaces, auth, and
   evidence shape.
2. Translation proof for deterministic State Operation and Event Import request
   plans without network access.
3. Mocked transport proof for request rendering, dry-run behavior, selected
   request-plan reuse, resolved auth placement, response classification,
   redaction, transport failure handling, and submission evidence aggregation.

## Sandbox Template

There is no live partner sandbox workflow for this reference connector. The
`tests/sandbox/` suite is an offline template for production destinations to
copy when adding an opt-in `live_sandbox` proof path:

- mark the tests with `pytest.mark.live_sandbox`;
- guard execution with `RETL_RUN_LIVE_SANDBOX=1`;
- read binding values from `DESTINATIONS__<PROVIDER>__...` environment names;
- keep default checks offline by relying on `-m "not live_sandbox"`;
- use synthetic records and bounded, redacted evidence.

Production connectors should replace the template's injected transport with a
partner sandbox or disposable-resource transport, and should document cleanup or
read-only constraints in the connector README.
