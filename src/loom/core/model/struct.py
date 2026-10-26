"""Framework-wide base struct types."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import msgspec

from loom.core.model.redaction import ensure_redacted


class _PostInit:
    """Run the redaction check, then the struct's own ``__post_init__``."""

    __slots__ = ("_post_init",)

    def __init__(self, post_init: Callable[[msgspec.Struct], None]) -> None:
        self._post_init = post_init

    def _run(self, struct: msgspec.Struct) -> None:
        ensure_redacted(struct)
        self._post_init(struct)


class _CheckedPostInit(_PostInit):
    """What msgspec calls, with the instance. ``super()`` binds it first."""

    __slots__ = ()

    def __get__(
        self,
        instance: msgspec.Struct | None,
        owner: type | None = None,
    ) -> _BoundPostInit | _CheckedPostInit:
        if instance is None:
            return self
        return _BoundPostInit(self._post_init, instance)

    def __call__(self, struct: msgspec.Struct) -> None:
        self._run(struct)


class _BoundPostInit(_PostInit):
    """The same check, bound to one instance for ``super()``."""

    __slots__ = ("_instance",)

    def __init__(
        self,
        post_init: Callable[[msgspec.Struct], None],
        instance: msgspec.Struct,
    ) -> None:
        super().__init__(post_init)
        self._instance = instance

    def __call__(self) -> None:
        self._run(self._instance)


class _RedactingStructMeta(msgspec.StructMeta):
    """Install the redaction check on every frozen struct."""

    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> _RedactingStructMeta:
        user_post_init = namespace.get("__post_init__")
        if user_post_init is None:
            namespace["__post_init__"] = ensure_redacted
        else:
            namespace["__post_init__"] = _CheckedPostInit(user_post_init)
        return super().__new__(cls, name, bases, namespace, **kwargs)


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
    a frozen struct cannot wrap a plain value after it is built. The check
    runs before the struct's own ``__post_init__``.
    """

    @classmethod
    def loom_message_type(cls) -> str:
        """Return the stable logical message type for this model class."""
        override = getattr(cls, "__loom_message_type__", None)
        if override is not None:
            return str(override)
        return f"{cls.__module__}.{cls.__qualname__}"
