"""``WorkerNotOpenError`` names the failure to open without carrying its secrets.

The error travels to the result backend and the caller; the full cause stays
on ``__cause__`` (never pickled) and in the worker's own log.
"""

from __future__ import annotations

import pickle

from loom.celery.errors import WorkerNotOpenError

_DSN = "postgresql://u:secret@host/db"


class _ConnectFailed(Exception):
    pass


def _error() -> WorkerNotOpenError:
    cause = _ConnectFailed(f"could not connect to {_DSN}?sslmode=require")
    try:
        raise WorkerNotOpenError(cause) from cause
    except WorkerNotOpenError as error:
        return error


def test_the_message_names_the_cause_type_and_masks_its_password() -> None:
    error = _error()

    assert "secret" not in str(error)
    assert "_ConnectFailed" in str(error)
    assert "postgresql://u:***@host/db" in str(error)


def test_the_full_cause_stays_on_the_chain_only() -> None:
    error = _error()

    assert isinstance(error.__cause__, _ConnectFailed)
    assert "secret" not in repr(vars(error))
    assert b"secret" not in pickle.dumps(error)


def test_a_dsn_after_a_query_or_fragment_is_masked_too() -> None:
    cause = _ConnectFailed(f"retry?attempt=2 then {_DSN} #3 redis://:pw@cache:6379/0")

    message = str(WorkerNotOpenError(cause))

    assert ("secret" in message, "pw@" in message) == (False, False)
