---
title: Install RETL
description: Install the core package, AI skills, source backend extras, and destination connector packages.
---

Most users should install RETL, install the packaged AI skills into their
project, and then let an agent use those skills to create the first dry-run
sync.

## Core package

```sh
pip install condor-retl
```

The distribution package is `condor-retl`; the Python import package is `retl`.

```python
import retl
```

## AI skills

Install the user-facing RETL skills into the project where your sync code will
live:

```sh
retl install-skills .
```

The default destinations are `.agents/skills/` and `.claude/skills/`. Use
`--destination` only when your agent expects project-local skills somewhere
else:

```sh
retl install-skills . --destination .retl/user-skills
```

After installation, ask your agent to use `retl-start-project` for a new setup
or `retl-create-sync` when the project already has RETL structure.

## Source backend extras

Install the backend extras your project uses.

```sh
pip install "condor-retl[duckdb]"
pip install "condor-retl[snowflake]"
pip install "condor-retl[bigquery]"
pip install "condor-retl[databricks]"
pip install "condor-retl[postgresql]"
```

## Destination packages

Destination connectors are separate installable packages. Install only the
connectors your project needs.

```sh
pip install condor-retl-meta
pip install condor-retl-klaviyo
pip install condor-retl-google-ads-data-manager
pip install condor-retl-bing-ads
pip install condor-retl-tiktok-ads
```

See [connector packages](../reference/connector-packages.md) for refs and
surface names.
