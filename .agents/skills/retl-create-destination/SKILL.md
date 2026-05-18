---
name: retl-create-destination
description: Create and update RETL destination connector packages and Destination Surface contracts using the repository-owned connector rules.
---

# RETL Destination Connector Work

Use this skill when adding or changing a RETL destination connector, destination
surface, target lifecycle, delivery outcome behavior, receipt classification, or
connector-local proof path.

Destination connectors translate RETL State Operations and Event imports into a
partner API. They must be destination-specific at the boundary and repository-
native everywhere else: use RETL's existing Surface, auth, request planning,
HTTP, target, receipt, diagnostics, registry, and testing primitives instead of
copying ETL/runtime/helper code into a destination package.

## Start From Repo Contracts

Before writing code, read the durable contracts that apply to the change:

- `docs/destinations.md` for Destination Surface, Target, delivery outcomes,
  auth, connector boundary, and compatibility rules.
- `docs/runtime.md` for validation-before-mutation, dry-run, Progress, and
  receipt placement.
- `docs/control-plane.md` for package boundaries, proof obligations, and
  architecture checks.
- `destination_connectors/reference_http/` as the canonical minimal connector
  shape for definitions, hooks, entry points, dry-run planning, submission
  evidence, and tests.
- `destination_connectors/meta/` as the current production partner example for
  auth, batching, target mapping, partner request rendering, redacted
  diagnostics, and opt-in sandbox tests.
- The active plan under `docs/plans/active/` for slice-specific decisions.

If these sources conflict, the durable docs and existing core contracts win over
plan notes and package-local precedent.

## Research The Partner API

For a new or materially changed production partner connector, search online for
the latest official API documentation before choosing endpoints or request
shapes. Prefer the partner's official docs, OpenAPI specs, SDK reference pages,
changelogs, migration guides, and deprecation notices. Capture the docs you used
in the active plan, connector-local README, tests, or implementation notes:
exact URLs, API family names, version identifiers, publish/update dates when
available, and the access date. Use code comments only when a non-obvious API
decision needs a durable pointer.

If official docs cannot be reached from the current environment, stop before
inventing partner behavior. Ask for the relevant official docs or approval to
access them, and record blocked discovery as an explicit deferral.

If the user names only a broad platform with multiple materially different
destination surfaces, clarify the intended API family, canonical resource
families, operations, and target lifecycle before implementation. Do not guess
between surfaces such as audience membership, conversion events, and profile
imports.

When multiple partner APIs or versions are available, pick the latest stable,
generally available API that supports the requested behavior. Do not ask the
user to choose between API versions unless the repo task explicitly depends on a
business policy that cannot be inferred. Avoid beta, preview, experimental,
legacy, or soon-to-be-deprecated APIs unless there is no stable path; if using
one is unavoidable, document the constraint and keep the connector surface narrow.
When API versioning appears in request paths, query parameters, headers, or
payload schemas, prefer a pinned connector default in binding config rather than
runtime network discovery. Mocked transport tests must assert the generated
request includes the chosen version.

Prefer raw HTTP integration through RETL's destination HTTP primitives when a
documented HTTP API is available. Use partner SDKs only when explicitly requested
or when the partner has no practical non-SDK integration path.

Translate partner docs into RETL concepts:

- Partner object or endpoint -> `DestinationSurface`.
- Partner object subtype plus lifecycle -> one coherent `DestinationSurface`.
  Split broad resource families into separate surfaces when target subtype,
  lookup, creation, membership mutation, read-only behavior, or immutability
  differs.
- Partner accepted identifiers -> `accepted_identifier_types` and
  `IdentifierRequirement`.
- Partner required event/state fields -> `required_key_fields`,
  `required_payload_fields`, binding `config`, or credentials, depending on who
  should provide the value.
- Partner route such as list id, audience id, pixel id, property id, or account
  id -> Target, binding config, or a connector-owned route mapping. Do not infer
  routing from arbitrary Payload fields unless the surface contract explicitly
  requires those fields as route inputs.
- Partner success, async acceptance, retryable failure, and record failure
  semantics -> surface `delivery_outcome`, `ResponseClassificationPolicy`, and
  `DestinationSubmissionEvidence`.

## Package Shape

First-party packages live under `destination_connectors/<name>/` and should
match the existing packages:

- `pyproject.toml` with package-local metadata, a `condor-retl-<name>`
  distribution name, Apache-2.0 license metadata, a bounded `condor-retl`
  dependency range, and a `retl.destinations` entry point.
- An importable module named `retl_<name>/`.
- `definitions.py` for connector refs, surface declarations, auth modes, and
  the `declarative_connector(...)` call.
- `hooks.py` for request planning, partner request rendering, submission, and
  receipt classification.
- Optional small support modules such as `common.py` for partner constants,
  config parsing, URL joining, transport adapters, hashing/normalization, and
  bounded diagnostic extraction.
- `tests/` with package-local unit and integration-style proof using injected
  transports rather than live network calls by default.
- `README.md` that explains the surfaces, auth, binding config, and any
  non-default sandbox workflow.

Use connector refs like `retl/<partner>` for first-party packages and expose a
module-level `connector`.

Choose package scope before scaffolding. Default to a partner-family package
when likely surfaces share auth, base URL, API versioning, error handling,
rate-limit semantics, account identifiers, or test helpers. Keep each
Destination Surface scoped to one coherent remote target subtype and lifecycle.
Do not combine managed membership containers, lookup-only references,
rule-derived resources, read-only resources, or immutable resources behind one
surface just because the partner groups them under one broad resource family.
Use a surface-specific package only when the partner surface is clearly isolated
or the user explicitly asks for it.

Do not rename persisted connector refs or surface refs casually. If moving an
implementation into a broader package, preserve compatibility bridges unless the
user explicitly accepts a breaking migration.

## Surface Definitions

Prefer declarative `DestinationSurface` definitions. Add hooks only for partner
request rendering, target lookup/create, transport, receipt classification,
async handle parsing, or auth flows that cannot be expressed declaratively.

Define each surface from the partner behavior:

- `declaration_family`: `state` for reconciled State Operations, `event` for
  checkpointed Event imports.
- `supported_operations`: `("upsert",)`, `("upsert", "remove")`, or
  `("import",)`.
- `target_mode`: `required`, `optional`, or `unsupported`.
- `supports_managed_targets`: `True` only when every Target on that surface has
  the same managed find/create lifecycle. If some Targets in the partner family
  are lookup-only, read-only, immutable, or rule-derived, split them into
  separate surfaces instead of relying on naming, Payload fields, config flags,
  or connector-local branching to change Target semantics.
- `accepted_identifier_types` and `identifier_requirements`: exactly what the
  partner endpoint can use.
- `required_key_fields` and `required_payload_fields`: fields RETL must validate
  before irreversible writes.
- `delivery_outcome`: use `succeeded` when successful submission proves final
  destination completion, `accepted` when it only proves queued or accepted
  delivery.
- `request_template`: relative method/path/header/query templates only. Do not
  include absolute URLs, query strings in paths, auth headers, or secrets.

Keep connector-specific behavior in connector packages or built-in connector
definitions, not in State/Event declarations. Core RETL must not infer partner
behavior from arbitrary Payload fields.

For mutable container or audience/list membership surfaces, default to both
`upsert` and `remove` when the partner API supports them. Treat add-only or
remove-only support as a scoped limitation that must be justified by partner
docs or explicit user scope, then reflected in capabilities, tests, README, and
plan notes.

Be precise about identifiers. Do not treat a partner profile attribute named
`external_id` as the same thing as a partner-native resource id unless official
docs say so. Model materially different identifier systems with distinct
accepted identifier types or namespaces, and test that at least one wrong
identifier is rejected before transport.

## Use Existing Runtime Primitives

Do not replicate RETL runtime or helper code inside a destination package. Before
writing new infrastructure, look for and reuse the repo-owned modules under
`src/retl/destinations/`:

- `auth.py`: `none`, `bearer_token`, `api_key`, `basic`,
  `oauth2_client_credentials`, `oauth_jwt`, and `custom` auth modes plus
  resolved auth objects.
- `request_batch.py`: `plan_request_batches`, `RequestBatchingPolicy`,
  `RequestBatchContext`, `DestinationWorkRecord`, and `RequestBatchPlan`.
- `http.py`: `HttpRequest`, `HttpResponse`, transport protocol, redaction, and
  sensitive-name helpers.
- `identifiers.py`: SHA-256 hex detection, SHA-256 hashing, and
  hash-or-preserve rendering for partner-required hashed identifiers. Keep
  connector-specific normalization explicit in the caller and do not use hashing
  helpers as email, phone, or external-id format validation.
- `receipts.py`: `ResponseClassificationPolicy`, `RemoteHandlePolicy`,
  `classify_response`, and diagnostic sanitization.
- submission evidence types: `DestinationSubmissionEvidence` and
  `DestinationReceipt`.
- `targets.py`: target mapping and registry helpers such as `registry_key`.
- `registry.py` and `surfaces.py`: connector and surface contracts.

Destination packages may define thin partner-specific adapters: config parsing,
URL construction, identifier normalization required by the partner, body hooks,
partition hooks, record hooks, response message extraction, and a transport
wrapper around `requests` or another existing dependency. For partner-required
SHA-256 identifier rendering, use `retl.destinations.identifiers` instead of
copying local regex or hashlib helpers. They should not implement their own
reconciliation, batching ledger, secret resolver, receipt model, dry-run model,
registry, or generic HTTP abstraction.

## Auth, Config, And Secrets

Declare explicit auth modes in `definitions.py`. Public, mock, local-only, or
reference connectors use `retl.destinations.auth.none()` rather than omitting
auth. Production connectors should model the partner's stable auth scheme:
bearer token, API key, basic auth, OAuth2 client credentials, OAuth JWT, or a
small custom hook when necessary.

Credentials belong in `DestinationBinding.credentials` as `SecretRef` or
`SecretLiteral`; bare strings are invalid for secrets. Public non-secret values
such as account ids, API version, base URL override, property id, list id, test
mode flag, or route mapping belong in `DestinationBinding.config`.

Prefer namespace binding for common operator-facing values. A connector's
required auth fields can be sourced with `credential_namespace`, which creates
`SecretRef` values from the selected Auth Mode without resolving secret
material. Connector-declared public config can be sourced with
`config_namespace`, but only for fields listed in the connector's
`config_namespace_fields` metadata. Keep target mappings, injected transports,
and other test-only objects explicit. Explicit `credentials` and `config`
entries must continue to override namespace-derived values field by field.

Never persist or expose resolved headers, cookies, query parameters, private
keys, tokens, client secrets, auth-bearing URLs, or raw secret material. Use
RETL redaction and diagnostic helpers for any partner error detail.

When a connector has multiple auth modes, tests must cover one complete selected
mode, missing credentials across all modes, partial multi-field credentials,
omitted optional fields, ambiguous credentials when more than one mode is
complete, and error messages that name modes and fields without leaking values.

## Environment Variables

Connector docs, local examples, live sandbox tests, and README snippets must use
the same environment naming model resolved by `retl.config[...]` and
`retl.secrets[...]`:

```text
DESTINATIONS__<PROVIDER>__<SHARED_FIELD>
DESTINATIONS__<PROVIDER>__<SURFACE>__<SURFACE_FIELD>
RETL_* for test harness controls only
```

Spell the same binding contract with dotted Python keys:

```python
retl.secrets["destinations.<provider>.<shared_secret>"]
retl.config["destinations.<provider>.<shared_config>"]
retl.config["destinations.<provider>.<surface>.<surface_config>"]
```

Use provider namespaces for shared auth, account, API version, and
cross-surface settings. Use surface namespaces for partner-product-specific
targets, routes, and options. Do not introduce separate sandbox-test names such
as `META_ACCESS_TOKEN` or `GOOGLE_ADS_CUSTOMER_MATCH_AUDIENCE_ID` when a
`DESTINATIONS__...` key represents the same destination binding value.

Live sandbox tests read destination credentials, account ids, target ids,
pixels, event routes, and similar binding values from `DESTINATIONS__...`
variables. Keep only test behavior switches under `RETL_*`, such as
`RETL_RUN_LIVE_SANDBOX`, evidence directory settings, or connector-specific
live-mutation opt-ins. Actual secret values belong in ignored local env files
such as `local/env/.env.<connector>-sandbox`; committed docs and tests contain
names only.

## Planning And Submission

Follow the reference pattern:

1. The batch planning hook accepts `binding`, `surface`, and reconciled State or
   Event evidence.
2. It obtains `operation_pages` or `import_pages`; if work is not available yet,
   it returns an empty `DryRunSubmissionPlan` with a bounded note.
3. It calls `plan_request_batches(...)` with the surface name, work family,
   request template, batching policy, public config, and connector-owned hooks
   for body rendering, partitioning, or record mapping.
4. Dry-run submission returns `DestinationSubmissionEvidence.planned(...)` and
   sends no network request.
5. Non-dry-run submission uses selected request plans when provided. Do not
   replan after the runtime has selected ledger-backed request plans.
6. Submission builds final `HttpRequest` values by joining a validated base URL,
   the planned relative path, planned query/headers, planned body, and resolved
   auth placement.
7. Each response is classified with a connector-owned
   `ResponseClassificationPolicy`; aggregate counts, receipts, handles, HTTP
   status, partner codes, retry information, and bounded summaries into
   `DestinationSubmissionEvidence`.

When the partner requires record grouping, use `partition_key`, `record_hook`,
`body_hook`, and `RequestBatchingPolicy` rather than custom batching. When a
partner route is Target-backed, resolve logical targets through explicit target
mappings or the binding's Target Registry before rendering the request; normal
runner execution injects the runtime-store-backed registry into the binding.
When a connector supports managed Targets and lookup or creation requires
destination auth, expose a connector-owned `managed_target_client_hook` so the
runtime can build the client after auth resolution and before target
resolution. Do not resolve secrets inside destination binding construction.

## Tests And Proof

Default tests must be deterministic and offline. Use injected recording/static
transports to prove request paths, headers, bodies, batching, auth placement,
response classification, evidence counts, redaction, target mapping, and dry-run
behavior.

Use a layered proof model and make each connector's highest supported proof
level explicit in the active plan, connector-local README, tests, or evidence:

1. Contract proof: package imports, connector loads, metadata, surfaces, auth,
   config, capabilities, and receipt/evidence shape are valid.
2. Translation proof: State Operations or Event imports become deterministic
   partner-shaped request plans without network access.
3. Mocked transport proof: HTTP method, path, headers, auth placement, query,
   body, success, retryable failure, terminal failure, throttling, and polling
   behavior are exercised through injected transport.
4. Simulator or dummy proof: repo-owned fake infrastructure proves lifecycle
   behavior independent of a partner API.
5. Gated live proof: real partner writes are optional, explicitly enabled, and
   excluded from default checks.

Default CI should normally require levels 1 through 3 for HTTP destinations.
Do not imply production-live execution in docs, capabilities, or examples when a
connector only reaches contract or translation proof.

For each production connector or meaningful surface change, add focused tests
for:

- Connector metadata: ref, surfaces, operations, identifiers, target mode, auth.
- Request planning: row limits, partitioning, path templates, target or route
  resolution, body rendering, and payload normalization such as hashing.
- Dry-run: request batches are planned, no transport is called, and evidence is
  bounded.
- Submission: selected request plans are reused, auth is applied from
  `resolved_auth`, transport failures become pre-acceptance evidence, and
  success/failure responses aggregate correctly.
- Diagnostics: partner messages and details are useful, redacted, and bounded.
- Event-specific behavior: occurred-at handling, event identity, import
  operation, route selection, and required payload fields.
- State-specific behavior: upsert/remove semantics, target requirements, and
  managed target behavior when implemented.

Live sandbox tests are optional and must be opt-in only, marked outside default
checks, use sandbox or disposable partner resources, synthetic data, read-back
assertions where available, best-effort cleanup, and redacted evidence. If
cleanup cannot be safe and repeatable, keep the live test read-only or record it
as a deferral. Add a Makefile target only when the repo has explicit sandbox
credentials and cleanup rules for that partner.

If managed target creation, validation, live audience lifecycle work, or another
documented partner surface is not implemented, say so explicitly in plan scope,
connector-local tests or evidence, README/examples, and `docs/plans/tech-debt.md`.

## Finish Checklist

Before finishing, run the focused package tests and applicable repo checks.
For code changes, the normal baseline is:

```bash
make check
```

At minimum for destination docs/skill/check-only changes, run:

```bash
uv run python tools/checks/validate_repo_skeleton.py
uv run python tools/checks/validate_architecture.py
```

Also run `make lint-lock` when dependency, packaging, or lockfile surfaces
change, plus any narrower Makefile or pytest targets needed for the changed
connector.

Record intentional deferrals in `docs/plans/tech-debt.md`; do not leave
unsupported behavior implied only by comments, prompts, or plan prose.
