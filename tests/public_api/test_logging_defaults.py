from __future__ import annotations

import json
import logging
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from retl.logging import get_logger, install_null_handler


@contextmanager
def preserve_logger_state(*names: str) -> Iterator[None]:
    root = logging.getLogger()
    root_state = (list(root.handlers), root.level, root.propagate, root.disabled)
    logger_states = {
        name: (
            list(logging.getLogger(name).handlers),
            logging.getLogger(name).level,
            logging.getLogger(name).propagate,
            logging.getLogger(name).disabled,
        )
        for name in names
    }
    try:
        yield
    finally:
        root.handlers[:] = root_state[0]
        root.setLevel(root_state[1])
        root.propagate = root_state[2]
        root.disabled = root_state[3]
        for name, state in logger_states.items():
            logger = logging.getLogger(name)
            logger.handlers[:] = state[0]
            logger.setLevel(state[1])
            logger.propagate = state[2]
            logger.disabled = state[3]


def test_import_retl_installs_only_package_null_handler() -> None:
    script = """
import json
import logging

root = logging.getLogger()
before = {
    "root_handlers": len(root.handlers),
    "root_level": root.level,
    "root_propagate": root.propagate,
}

import retl

retl_logger = logging.getLogger("retl")
retl_logger.warning("default retl warning")
after = {
    "root_handlers": len(root.handlers),
    "root_level": root.level,
    "root_propagate": root.propagate,
    "retl_level": retl_logger.level,
    "retl_propagate": retl_logger.propagate,
    "retl_handlers": [type(handler).__name__ for handler in retl_logger.handlers],
    "has_configure_logging": hasattr(retl, "configure_logging"),
}
print(json.dumps({"before": before, "after": after}, sort_keys=True))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).parents[2],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert result.stderr == ""
    assert payload["after"]["root_handlers"] == payload["before"]["root_handlers"]
    assert payload["after"]["root_level"] == payload["before"]["root_level"]
    assert payload["after"]["root_propagate"] == payload["before"]["root_propagate"]
    assert payload["after"]["retl_level"] == logging.NOTSET
    assert payload["after"]["retl_propagate"] is True
    assert payload["after"]["retl_handlers"] == ["NullHandler"]
    assert payload["after"]["has_configure_logging"] is True


def test_install_null_handler_is_idempotent() -> None:
    with preserve_logger_state("retl"):
        logger = logging.getLogger("retl")
        logger.handlers.clear()

        install_null_handler()
        install_null_handler()

        assert [type(handler) for handler in logger.handlers] == [logging.NullHandler]


def test_get_logger_returns_retl_namespace_loggers() -> None:
    with preserve_logger_state("retl", "retl.runtime", "retl.runtime.executor"):
        assert get_logger() is logging.getLogger("retl")
        assert get_logger("runtime.executor") is logging.getLogger("retl.runtime.executor")
        assert get_logger("retl.runtime") is logging.getLogger("retl.runtime")
