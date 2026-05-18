# RETL Docs Site

This directory owns the Docusaurus source for public, user-facing RETL
documentation.

The repository root `docs/` tree remains the durable AI, contributor,
control-plane, architecture, runtime-contract, and planning surface. Do not copy
the compact root docs into this site. When the same subject matters to both
audiences, keep the durable rule in `docs/` and write a narrower user-facing
task page here.

## Local Commands

Install dependencies from this directory:

```sh
npm install
```

Run the local development server:

```sh
npm run dev
```

Build the static site locally:

```sh
npm run build
```

Serve the local build:

```sh
npm run serve
```

Run the docs-site validation target:

```sh
npm run check
```

## Scope

The committed source includes local install, development, build, serve, and
validation commands. Deployment credentials and host-specific publish scripts
stay outside the repository under ignored `local/` files.

This slice intentionally has no committed hosting integration, GitHub Pages,
Cloudflare workflow, generated API docs, Docusaurus versioning, or `llms.txt`
workflow.

For repository policy and implementation contracts, start with
[`../docs/index.md`](../docs/index.md).
