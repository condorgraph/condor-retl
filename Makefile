dev:
	uv sync --all-extras --group dev

format:
	uv run ruff format .

format-check:
	uv run ruff format --check .

lint:
	uv run ruff check .

typecheck:
	MYPYPATH=destination_connectors/reference_http:destination_connectors/bing_ads:destination_connectors/meta:destination_connectors/google_ads_data_manager:destination_connectors/klaviyo:destination_connectors/tiktok_ads uv run mypy src tests destination_connectors/reference_http destination_connectors/bing_ads destination_connectors/meta destination_connectors/google_ads_data_manager destination_connectors/klaviyo destination_connectors/tiktok_ads

lint-lock:
	uv lock --check

test:
	uv run pytest tests destination_connectors/reference_http/tests destination_connectors/bing_ads/tests destination_connectors/meta/tests destination_connectors/google_ads_data_manager/tests destination_connectors/klaviyo/tests destination_connectors/tiktok_ads/tests -q -n auto -m "not live_sandbox"

test-serial:
	uv run pytest tests destination_connectors/reference_http/tests destination_connectors/bing_ads/tests destination_connectors/meta/tests destination_connectors/google_ads_data_manager/tests destination_connectors/klaviyo/tests destination_connectors/tiktok_ads/tests -q -m "not live_sandbox"

test-common:
	uv run pytest tests/architecture -q -n auto

check:
	$(MAKE) format-check
	$(MAKE) lint
	$(MAKE) typecheck
	$(MAKE) test
	uv run python tools/checks/validate_repo_skeleton.py
	uv run python tools/checks/validate_architecture.py

test-sandbox-meta:
	@if [ -f local/env/.env.meta-sandbox ]; then \
		. local/env/.env.meta-sandbox; \
		RETL_RUN_LIVE_SANDBOX=1 uv run pytest destination_connectors/meta/tests/sandbox -q -m live_sandbox; \
	else \
		RETL_RUN_LIVE_SANDBOX=1 uv run pytest destination_connectors/meta/tests/sandbox -q -m live_sandbox; \
	fi

test-sandbox-google-ads:
	@if [ -f local/env/.env.google_ads-sandbox ]; then \
		. local/env/.env.google_ads-sandbox; \
		RETL_RUN_LIVE_SANDBOX=1 uv run pytest destination_connectors/google_ads_data_manager/tests/sandbox -q -m live_sandbox; \
	else \
		RETL_RUN_LIVE_SANDBOX=1 uv run pytest destination_connectors/google_ads_data_manager/tests/sandbox -q -m live_sandbox; \
	fi

test-sandbox-bing-ads:
	@if [ -f local/env/.env.bing_ads-sandbox ]; then \
		. local/env/.env.bing_ads-sandbox; \
		RETL_RUN_LIVE_SANDBOX=1 uv run pytest destination_connectors/bing_ads/tests/sandbox -q -m live_sandbox; \
	else \
		RETL_RUN_LIVE_SANDBOX=1 uv run pytest destination_connectors/bing_ads/tests/sandbox -q -m live_sandbox; \
	fi

test-sandbox-klaviyo:
	@if [ -f local/env/.env.klaviyo.sandbox ]; then \
		. local/env/.env.klaviyo.sandbox; \
		RETL_RUN_LIVE_SANDBOX=1 uv run pytest destination_connectors/klaviyo/tests/sandbox -q -m live_sandbox; \
	else \
		RETL_RUN_LIVE_SANDBOX=1 uv run pytest destination_connectors/klaviyo/tests/sandbox -q -m live_sandbox; \
	fi

test-sandbox-tiktok-ads:
	@if [ -f local/env/.env.tiktok_ads-sandbox ]; then \
		. local/env/.env.tiktok_ads-sandbox; \
		RETL_RUN_LIVE_SANDBOX=1 uv run pytest destination_connectors/tiktok_ads/tests/sandbox -q -m live_sandbox; \
	else \
		RETL_RUN_LIVE_SANDBOX=1 uv run pytest destination_connectors/tiktok_ads/tests/sandbox -q -m live_sandbox; \
	fi

test-sandbox-snowflake:
	@if [ -f local/env/.env.snowflake-sandbox ]; then \
		. local/env/.env.snowflake-sandbox; \
		RETL_RUN_LIVE_SANDBOX=1 uv run --extra snowflake pytest tests/backends/sandbox/test_snowflake_live_sandbox.py -q -m live_sandbox; \
	else \
		RETL_RUN_LIVE_SANDBOX=1 uv run --extra snowflake pytest tests/backends/sandbox/test_snowflake_live_sandbox.py -q -m live_sandbox; \
	fi

test-sandbox-bigquery:
	@if [ -f local/env/.env.bigquery-sandbox ]; then \
		set -a; . local/env/.env.bigquery-sandbox; set +a; \
	fi; \
	if [ -z "$${RETL_BIGQUERY_PROJECT:-}" ] && [ -z "$${BACKENDS__BIGQUERY__PROJECT:-}" ]; then \
		RETL_BIGQUERY_PROJECT=$$(gcloud config get-value project 2>/dev/null); \
		export RETL_BIGQUERY_PROJECT; \
	fi; \
	RETL_RUN_LIVE_SANDBOX=1 uv run --extra bigquery pytest tests/backends/sandbox/test_bigquery_live_sandbox.py -q -m live_sandbox

test-sandbox-databricks:
	@if [ -f local/env/.env.databricks-sandbox ]; then \
		. local/env/.env.databricks-sandbox; \
		RETL_RUN_LIVE_SANDBOX=1 uv run --extra databricks pytest tests/backends/sandbox/test_databricks_live_sandbox.py -q -m live_sandbox; \
	else \
		RETL_RUN_LIVE_SANDBOX=1 uv run --extra databricks pytest tests/backends/sandbox/test_databricks_live_sandbox.py -q -m live_sandbox; \
	fi

test-sandbox-postgresql:
	@if [ -f local/env/.env.postgresql-sandbox ]; then \
		. local/env/.env.postgresql-sandbox; \
		uv run --extra postgresql pytest tests/backends/sandbox/test_postgresql_live_sandbox.py -q -m live_sandbox; \
	else \
		uv run --extra postgresql pytest tests/backends/sandbox/test_postgresql_live_sandbox.py -q -m live_sandbox; \
	fi

build-library:
	uv build

build-destination-connector:
	@test -n "$(PACKAGE)" || (printf '%s\n' 'Usage: make build-destination-connector PACKAGE=<connector-directory>'; exit 2)
	uv build --out-dir destination_connectors/$(PACKAGE)/dist destination_connectors/$(PACKAGE)

publish-library:
	uv publish

publish-destination-connector:
	@test -n "$(PACKAGE)" || (printf '%s\n' 'Usage: make publish-destination-connector PACKAGE=<connector-directory>'; exit 2)
	@test "$(PACKAGE)" != "reference_http" || (printf '%s\n' 'reference_http is repo-local and is not published to PyPI'; exit 2)
	uv publish destination_connectors/$(PACKAGE)/dist/*
