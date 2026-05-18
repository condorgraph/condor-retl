# Contributing

Follow the documented control-plane, architecture, and test contracts before
widening the public API.

## Contributor License Agreement

Code and documentation contributions require a signed Contributor License
Agreement before merge.

The agreement text is published as the
[Condor Contributor License Agreement Gist](https://gist.github.com/chriscmorin/bae5d671e45fc152c18f38961669a85e).
Pull requests are checked by CLA Assistant using that Gist. Changes to the
agreement may require contributors to sign the updated version. If you have not
signed the current agreement, CLA Assistant will comment on your pull request
with a signing link. Sign in with GitHub, accept the agreement, and wait for the
CLA check to pass before merge.

The CLA does not transfer copyright to Dataration LLC. It grants Dataration LLC,
doing business as Condor, the rights needed to use, distribute, sublicense, and
relicense contributions, including under future project licenses.

The outbound license for accepted contributions depends on the files changed:
the core `condor-retl` package is licensed under `Elastic-2.0`, and
first-party destination connector packages are licensed under `Apache-2.0`.

## Branch Flow

`main` is the trunk branch and the source of truth for routine development,
release candidates, hotfixes, tags, and rollbacks.

Use small, short-lived branches that target `main`. Direct commits to `main`
are acceptable only when branch protection, review rules, and required checks
allow them. Incomplete user-facing behavior must be hidden behind a documented
feature toggle, compatibility bridge, or non-exposure boundary instead of being
parked on a long-lived branch.

## Proof Before Merge

For meaningful changes:

- start from repo-owned context in `docs/`
- add or update a failing test, characterization fixture, architecture check,
  dry-run proof, or simulator evidence when the behavior can be captured
  mechanically
- run `make check`
- run `make lint-lock` when dependency, packaging, or lockfile surfaces change
- update docs, tests, checks, and deferred-work records in the same unit of work
