from __future__ import annotations

from enum import StrEnum


class OnDelete(StrEnum):
    CASCADE = "CASCADE"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"
    RESTRICT = "RESTRICT"
    NO_ACTION = "NO ACTION"


class OnUpdate(StrEnum):
    CASCADE = "CASCADE"
    SET_NULL = "SET NULL"
    RESTRICT = "RESTRICT"
    NO_ACTION = "NO ACTION"


class Cardinality(StrEnum):
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class ServerDefault(StrEnum):
    NOW = "now"
    UUID4 = "uuid4"


class ServerOnUpdate(StrEnum):
    NOW = "now"

    @classmethod
    def is_now(cls, value: ServerOnUpdate | str | None) -> bool:
        """Return whether ``value`` requests the server clock, as enum or string.

        Args:
            value: A ``server_onupdate`` declaration; the string form is
                matched case-insensitively.

        Returns:
            ``True`` for ``ServerOnUpdate.NOW`` or ``"now"`` in any case.
        """
        return isinstance(value, str) and value.strip().lower() == cls.NOW.value


class KeyFill(StrEnum):
    """How a store fills a primary-key column the frame does not supply."""

    AUTOINCREMENT = "autoincrement"
    DERIVED = "derived"
    SERVER_DEFAULT = "server_default"

    @property
    def description(self) -> str:
        """The fill as a message reads it: ``autoincrement``, ``derived by the store``..."""
        return _KEY_FILL_DESCRIPTIONS[self]


_KEY_FILL_DESCRIPTIONS = {
    KeyFill.AUTOINCREMENT: "autoincrement",
    KeyFill.DERIVED: "derived by the store",
    KeyFill.SERVER_DEFAULT: "filled by the server",
}
