from __future__ import annotations

import inspect
from dataclasses import fields

import retl
import retl.runtime as runtime
from retl.runtime import executor
from retl.runtime.defaults import (
    DEFAULT_RECONCILE_BATCH_MAX_BYTES,
    DEFAULT_RECONCILE_BATCH_MAX_ROWS,
    DEFAULT_STAGE_BATCH_MAX_ROWS,
)


def test_runtime_batch_default_constants_match_runner_defaults() -> None:
    runner_fields = {field.name: field.default for field in fields(retl.Runner)}
    runner = retl.runner(name="default-batches")

    assert runner_fields["stage_batch_max_rows"] == DEFAULT_STAGE_BATCH_MAX_ROWS
    assert runner_fields["reconcile_batch_max_rows"] == DEFAULT_RECONCILE_BATCH_MAX_ROWS
    assert runner_fields["reconcile_batch_max_bytes"] == DEFAULT_RECONCILE_BATCH_MAX_BYTES
    assert runner.stage_batch_max_rows == DEFAULT_STAGE_BATCH_MAX_ROWS
    assert runner.reconcile_batch_max_rows == DEFAULT_RECONCILE_BATCH_MAX_ROWS
    assert runner.reconcile_batch_max_bytes == DEFAULT_RECONCILE_BATCH_MAX_BYTES


def test_runtime_batch_default_constants_match_callable_defaults() -> None:
    for callable_ in (runtime.run_syncs, executor.run_syncs):
        signature = inspect.signature(callable_)
        assert signature.parameters["stage_batch_max_rows"].default == DEFAULT_STAGE_BATCH_MAX_ROWS
        assert (
            signature.parameters["reconcile_batch_max_rows"].default
            == DEFAULT_RECONCILE_BATCH_MAX_ROWS
        )
        assert (
            signature.parameters["reconcile_batch_max_bytes"].default
            == DEFAULT_RECONCILE_BATCH_MAX_BYTES
        )
