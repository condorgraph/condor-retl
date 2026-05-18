from __future__ import annotations

import io
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

import retl
from retl.logging import configure_logging


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


def test_configure_logging_emits_readable_text_output() -> None:
    stream = io.StringIO()
    with preserve_logger_state("retl", "retl.runtime"):
        configure_logging(level="INFO", format="text", stream=stream)

        logging.getLogger("retl.runtime").info("runtime started")

    output = stream.getvalue()
    assert "INFO" in output
    assert "retl.runtime" in output
    assert "runtime started" in output


def test_root_configure_logging_emits_readable_text_output() -> None:
    stream = io.StringIO()
    with preserve_logger_state("retl", "retl.runtime"):
        retl.configure_logging(level="INFO", format="text", stream=stream)

        logging.getLogger("retl.runtime").info("runtime started")

    output = stream.getvalue()
    assert "INFO" in output
    assert "retl.runtime" in output
    assert "runtime started" in output


def test_configure_logging_emits_parseable_json_output_with_safe_extra() -> None:
    stream = io.StringIO()
    with preserve_logger_state("retl", "retl.runtime"):
        configure_logging(level="INFO", format="json", stream=stream)

        logging.getLogger("retl.runtime").info(
            "sync completed",
            extra={
                "event": "sync_completed",
                "run_id": "run-1",
                "operation_count": 3,
            },
        )

    payload = json.loads(stream.getvalue())
    assert payload == {
        "event": "sync_completed",
        "level": "INFO",
        "logger": "retl.runtime",
        "message": "sync completed",
        "operation_count": 3,
        "run_id": "run-1",
    }


def test_root_configure_logging_emits_parseable_json_output() -> None:
    stream = io.StringIO()
    with preserve_logger_state("retl", "retl.runtime"):
        retl.configure_logging(level="INFO", format="json", stream=stream)

        logging.getLogger("retl.runtime").info(
            "sync completed",
            extra={"event": "sync_completed", "run_id": "run-1"},
        )

    payload = json.loads(stream.getvalue())
    assert payload["event"] == "sync_completed"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "retl.runtime"
    assert payload["message"] == "sync completed"
    assert payload["run_id"] == "run-1"


def test_root_configure_logging_is_public_export() -> None:
    from retl import configure_logging as root_configure_logging

    assert root_configure_logging is retl.configure_logging
    assert root_configure_logging is configure_logging
    assert "configure_logging" in retl.__all__


def test_json_logging_extra_cannot_override_stable_fields() -> None:
    stream = io.StringIO()
    with preserve_logger_state("retl", "retl.runtime"):
        logger = configure_logging(level="INFO", format="json", stream=stream)
        handler = next(
            handler for handler in logger.handlers if not isinstance(handler, logging.NullHandler)
        )
        assert handler.formatter is not None

        record = logging.makeLogRecord(
            {
                "name": "retl.runtime",
                "levelno": logging.INFO,
                "levelname": "INFO",
                "msg": "sync completed",
                "message": "overridden",
                "level": "DEBUG",
                "logger": "external",
                "event": "sync_completed",
            }
        )

        payload = json.loads(handler.formatter.format(record))

    assert payload == {
        "event": "sync_completed",
        "level": "INFO",
        "logger": "retl.runtime",
        "message": "sync completed",
    }


def test_configure_logging_accepts_level_names_and_ints() -> None:
    stream = io.StringIO()
    with preserve_logger_state("retl", "retl.runtime"):
        logger = configure_logging(level="WARNING", format="text", stream=stream)
        assert logger.level == logging.WARNING

        logging.getLogger("retl.runtime").info("hidden")
        logging.getLogger("retl.runtime").warning("visible")

        logger = configure_logging(level=logging.INFO, format="text", stream=stream)
        assert logger.level == logging.INFO
        logging.getLogger("retl.runtime").info("now visible")

    output = stream.getvalue()
    assert "hidden" not in output
    assert "visible" in output
    assert "now visible" in output


def test_configure_logging_rejects_invalid_levels_and_formats() -> None:
    with preserve_logger_state("retl"):
        with pytest.raises(ValueError, match="standard level name"):
            configure_logging(level="VERBOSE", format="text")

        with pytest.raises(ValueError, match="one of"):
            configure_logging(level="INFO", format="xml")  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="non-negative"):
            configure_logging(level=-1, format="text")


def test_configure_logging_does_not_mutate_root_logger() -> None:
    root = logging.getLogger()
    root_state = (list(root.handlers), root.level, root.propagate, root.disabled)
    with preserve_logger_state("retl"):
        configure_logging(level="INFO", format="json", stream=io.StringIO())

        assert list(root.handlers) == root_state[0]
        assert root.level == root_state[1]
        assert root.propagate == root_state[2]
        assert root.disabled == root_state[3]


def test_configure_logging_is_idempotent_for_default_handler() -> None:
    first_stream = io.StringIO()
    second_stream = io.StringIO()
    with preserve_logger_state("retl", "retl.runtime"):
        logger = configure_logging(level="INFO", format="text", stream=first_stream)
        first_handlers = list(logger.handlers)

        configure_logging(level="DEBUG", format="json", stream=second_stream)
        logging.getLogger("retl.runtime").debug("debug visible")

        assert list(logger.handlers) == first_handlers

    assert first_stream.getvalue() == ""
    assert json.loads(second_stream.getvalue())["message"] == "debug visible"


def test_configure_logging_keeps_package_null_handler() -> None:
    stream = io.StringIO()
    with preserve_logger_state("retl", "retl.runtime"):
        logger = logging.getLogger("retl")
        retl.configure_logging(level="INFO", format="json", stream=stream)

        handler_types = [type(handler) for handler in logger.handlers]

    assert logging.NullHandler in handler_types
    assert logging.StreamHandler in handler_types


def test_json_logging_redacts_sensitive_message_and_extra_values() -> None:
    stream = io.StringIO()
    with preserve_logger_state("retl", "retl.runtime"):
        configure_logging(level="INFO", format="json", stream=stream)

        logging.getLogger("retl.runtime").info(
            "Partner rejected Authorization: Bearer secret-token",
            extra={
                "access_token": "secret-token",
                "diagnostic": "client_secret=json-secret",
                "payload": {"email": "person@example.test"},
            },
        )

    payload = json.loads(stream.getvalue())
    serialized = json.dumps(payload, sort_keys=True)
    assert payload["message"] == "Partner rejected Authorization=[redacted]"
    assert payload["access_token"] == "[redacted]"
    assert payload["diagnostic"] == "client_secret=[redacted]"
    assert payload["payload"] == "[redacted]"
    assert "secret-token" not in serialized
    assert "json-secret" not in serialized
    assert "person@example.test" not in serialized


def test_json_logging_redacts_sensitive_extra_value_shapes() -> None:
    stream = io.StringIO()
    with preserve_logger_state("retl", "retl.runtime"):
        configure_logging(level="INFO", format="json", stream=stream)

        logging.getLogger("retl.runtime").info(
            "sync diagnostic",
            extra={
                "client_secret": "raw-client-secret",
                "authorization": "Bearer raw-bearer-token",
                "cookie": "session=raw-cookie",
                "auth_url": "https://user:raw-url-secret@example.test/private",
                "payload": {"trait": "raw-payload-value"},
                "state_identity": {"customer": "raw-customer-id"},
                "identifier": "raw-identifier",
                "request_fingerprint": "fingerprint-ok",
            },
        )

    payload = json.loads(stream.getvalue())
    serialized = json.dumps(payload, sort_keys=True)
    assert payload["client_secret"] == "[redacted]"
    assert payload["authorization"] == "[redacted]"
    assert payload["cookie"] == "[redacted]"
    assert payload["auth_url"] == "[redacted]"
    assert payload["payload"] == "[redacted]"
    assert payload["state_identity"] == "[redacted]"
    assert payload["identifier"] == "[redacted]"
    assert payload["request_fingerprint"] == "fingerprint-ok"
    assert "raw-client-secret" not in serialized
    assert "raw-bearer-token" not in serialized
    assert "raw-cookie" not in serialized
    assert "raw-url-secret" not in serialized
    assert "raw-payload-value" not in serialized
    assert "raw-customer-id" not in serialized
    assert "raw-identifier" not in serialized


def test_json_logging_redacts_auth_material_in_neutral_text() -> None:
    stream = io.StringIO()
    with preserve_logger_state("retl", "retl.runtime"):
        configure_logging(level="INFO", format="json", stream=stream)

        logging.getLogger("retl.runtime").info(
            "request https://user:raw-url-secret@example.test/private Cookie: session=raw-cookie",
            extra={
                "diagnostic": (
                    "retry https://user:raw-extra-secret@example.test/private "
                    "via https://example.test/public Cookie: session=raw-extra-cookie"
                ),
                "request_fingerprint": "fingerprint-ok",
            },
        )

    payload = json.loads(stream.getvalue())
    serialized = json.dumps(payload, sort_keys=True)
    assert payload["request_fingerprint"] == "fingerprint-ok"
    assert "https://[redacted]@example.test/private" in payload["message"]
    assert "https://example.test/public" in payload["diagnostic"]
    assert "Cookie=[redacted]" in payload["message"]
    assert "Cookie=[redacted]" in payload["diagnostic"]
    assert "raw-url-secret" not in serialized
    assert "raw-extra-secret" not in serialized
    assert "raw-cookie" not in serialized
    assert "raw-extra-cookie" not in serialized


def test_text_logging_redacts_sensitive_message_values() -> None:
    stream = io.StringIO()
    with preserve_logger_state("retl", "retl.runtime"):
        configure_logging(level="INFO", format="text", stream=stream)

        logging.getLogger("retl.runtime").info("failed with token=secret")

    output = stream.getvalue()
    assert "token=[redacted]" in output
    assert "token=secret" not in output
