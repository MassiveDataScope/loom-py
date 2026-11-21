"""``WorkerNotOpenError`` names the failure to open by its public form, never its text.

The error travels to the result backend and the caller; the cause's details
stay in the worker's own log.
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


class TestWorkerNotOpenError:
    def test_the_message_names_the_cause_type_and_none_of_its_text(self) -> None:
        message = str(_error())

        assert "_ConnectFailed" in message
        assert ("secret" in message, "host/db" in message) == (False, False)

    def test_nothing_of_the_cause_text_is_kept_or_pickled(self) -> None:
        error = _error()

        assert "secret" not in repr(vars(error))
        assert b"secret" not in pickle.dumps(error)
