from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from typing import Any, Final

from sqlalchemy import TextClause, event, text
from sqlalchemy.orm import Session

SessionSettings = Callable[[], Mapping[str, str] | None]
"""Returns the settings to apply to the transaction about to start, or ``None`` to apply none.

loom does not interpret the keys or values. A product returns, for example, the
boundary value and the subject of the current request, and writes its Postgres
policies against ``current_setting(key, true)``.
"""

_KEY: Final = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+")
SET_SETTINGS: Final = (
    "SELECT set_config(s.key, s.value, true) FROM jsonb_each_text(CAST(:settings AS jsonb)) AS s"
)
_SET_SETTINGS: Final = text(SET_SETTINGS)


def settings_statement(
    values: Mapping[str, str] | None,
) -> tuple[TextClause, dict[str, str]] | None:
    """Build the one statement that sets *values* for the current transaction.

    Returns:
        :data:`SET_SETTINGS` as a clause with the parameters of
        :func:`settings_parameters`, or ``None`` when there is nothing to set.
    """
    parameters = settings_parameters(values)
    return None if parameters is None else (_SET_SETTINGS, parameters)


def settings_parameters(values: Mapping[str, str] | None) -> dict[str, str] | None:
    """The bound parameters of :data:`SET_SETTINGS` that set *values*.

    The statement is a constant; the keys and values travel as one bound JSON
    document. Keys are two or more identifiers joined by dots, such as
    ``prefix.name``, which is what Postgres requires of a custom setting.

    Args:
        values: The settings to apply, or ``None``.

    Returns:
        The one bound JSON document, or ``None`` when there is nothing to set.

    Raises:
        ValueError: If a key is not a dotted string of two or more identifiers.
        TypeError: If a value is not a ``str``.
    """
    if not values:
        return None
    for key, value in values.items():
        if not isinstance(key, str) or _KEY.fullmatch(key) is None:
            raise ValueError(
                f"session setting key must be a dotted str such as 'prefix.name', got {key!r}"
            )
        if not isinstance(value, str):
            raise TypeError(
                f"session setting value for {key!r} must be str, got {type(value).__name__}"
            )
    return {"settings": json.dumps(dict(values))}


def install_session_settings(session_class: type[Session], provider: SessionSettings) -> None:
    """Apply *provider*'s settings at the start of every outer transaction of *session_class*.

    The listener runs once per outer transaction and not for savepoints. A provider
    that raises, an invalid key, or a non-``str`` value fails the transaction before
    its first product statement reaches the database, and invalidates its connection,
    so the session refuses further statements until it is rolled back.

    Args:
        session_class: The synchronous ``Session`` subclass the listener is bound to.
        provider: The settings provider called at the start of each transaction.
    """

    @event.listens_for(session_class, "after_begin")
    def _apply_session_settings(_session: Session, transaction: Any, connection: Any) -> None:
        if transaction.nested:
            return
        try:
            parameters = settings_parameters(provider())
            if parameters is not None:
                connection.execute(_SET_SETTINGS, parameters)
        except BaseException:
            connection.invalidate()
            raise
