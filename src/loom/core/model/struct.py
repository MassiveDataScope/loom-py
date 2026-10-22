"""Framework-wide base struct types."""

from __future__ import annotations

from typing import Any

import msgspec

from loom.core.model.redaction import ensure_redacted


class _RedactingStructMeta(msgspec.StructMeta):
    """Run the redaction check before a struct's own ``__post_init__``."""

    def __new__(
        mcls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> _RedactingStructMeta:
        user_post_init = namespace.get("__post_init__")
        if user_post_init is not None:

            def __post_init__(self: msgspec.Struct, _user: Any = user_post_init) -> None:
                ensure_redacted(self)
                _user(self)

            namespace["__post_init__"] = __post_init__
        return super().__new__(mcls, name, bases, namespace, **kwargs)


class LoomStruct(msgspec.Struct):
    """Base struct for Loom logical data types.

    ``LoomStruct`` is intentionally neutral. Specialised framework types such
    as responses or persistence models define their own serialisation
    behaviour.
    """

    @classmethod
    def loom_message_type(cls) -> str:
        """Return the stable logical message type for this model class."""
        override = getattr(cls, "__loom_message_type__", None)
        if override is not None:
            return str(override)
        return f"{cls.__module__}.{cls.__qualname__}"


class LoomFrozenStruct(msgspec.Struct, frozen=True, metaclass=_RedactingStructMeta):
    """Base immutable struct for Loom value contracts.

    Use this for configuration, DSL declarations, wire envelopes, and other
    value objects that should not expose mutable assignment after creation.
    A secret, address or redacted-mapping field must already hold that type:
    a frozen struct cannot wrap a plain value after it is built.
    """

    def __post_init__(self) -> None:
        ensure_redacted(self)

    @classmethod
    def loom_message_type(cls) -> str:
        """Return the stable logical message type for this model class."""
        override = getattr(cls, "__loom_message_type__", None)
        if override is not None:
            return str(override)
        return f"{cls.__module__}.{cls.__qualname__}"
