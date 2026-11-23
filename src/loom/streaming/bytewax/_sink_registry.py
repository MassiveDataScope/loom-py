"""Sink registry for StreamingRunner.

Defines the RegisteredSink protocol, RuntimeSinkBinding struct, and SinkRegistry
that the runner uses to resolve sinks from YAML config at startup.
"""

from __future__ import annotations

from typing import Any, ClassVar, Literal, Protocol, runtime_checkable

from loom.core.config import ConfigContext
from loom.core.errors import InvalidType
from loom.core.model import LoomFrozenStruct
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._exceptions import DuplicateErrorSinkError


@runtime_checkable
class RegisteredSink(Protocol):
    """Protocol a registrable sink class must satisfy.

    Attributes:
        sink_type: Unique type identifier. Must match the ``type`` field in the YAML config.
        config_type: msgspec Struct type used to deserialize the YAML config section.

    Example::

        class ClickHouseErrorTableSink:
            sink_type = "clickhouse_error_table"
            config_type = ClickHouseErrorTableSinkConfig

            @classmethod
            def build_binding(cls, cfg, ctx):
                ...
                return RuntimeSinkBinding(purpose="errors", sink=..., kinds=(ErrorKind.TASK,))
    """

    sink_type: ClassVar[str]
    config_type: ClassVar[type]

    @classmethod
    def build_binding(cls, cfg: Any, ctx: ConfigContext) -> RuntimeSinkBinding: ...


class RuntimeSinkBinding(LoomFrozenStruct, frozen=True):
    """Resolved sink binding returned by a RegisteredSink.

    Args:
        purpose: Role of this sink within the runner.
        sink: Instantiated sink object ready for use.
        kinds: For ``purpose="errors"``, the ErrorKind values this sink handles.
    """

    purpose: Literal["errors", "terminal", "audit"]
    sink: object
    kinds: tuple[ErrorKind, ...] = ()


class SinkRegistry:
    """Holds registered sink classes and resolves them against a ConfigContext.

    Each StreamingRunner owns one SinkRegistry. Not thread-safe; intended for
    startup-time registration only.
    """

    def __init__(self) -> None:
        self._sinks: dict[str, Any] = {}

    def __contains__(self, item: object) -> bool:
        """Return True if the class is registered in this registry."""
        return item in self._sinks.values()

    def register(self, cls: type) -> None:
        """Register a sink class.

        Args:
            cls: Class that satisfies the RegisteredSink protocol.

        Raises:
            InvalidType: If cls does not implement RegisteredSink.
        """
        if not isinstance(cls, RegisteredSink):
            raise InvalidType(
                f"{cls!r} does not satisfy the RegisteredSink protocol. "
                "Required class attributes: sink_type (str), config_type (type), "
                "and classmethod build_binding(cfg, ctx)."
            )
        self._sinks[cls.sink_type] = cls

    def resolve(self, ctx: ConfigContext) -> list[RuntimeSinkBinding]:
        """Resolve all registered sinks against the given config.

        Iterates streaming.sinks.* YAML entries, matches by type field,
        deserializes the section, calls build_binding, and validates for
        duplicate ErrorKind assignments.

        Args:
            ctx: Config context to read streaming.sinks from.

        Returns:
            List of RuntimeSinkBinding, one per matched YAML entry.

        Raises:
            DuplicateErrorSinkError: When two sinks claim the same ErrorKind.
        """
        if not ctx.has("streaming.sinks"):
            return []

        raw_sinks: dict[str, Any] = ctx.section_or_default("streaming.sinks", dict, {})
        bindings: list[RuntimeSinkBinding] = []

        for yaml_key, entry in raw_sinks.items():
            sink_type = entry.get("type") if isinstance(entry, dict) else None
            if sink_type is None or sink_type not in self._sinks:
                continue
            cls = self._sinks[sink_type]
            cfg = ctx.section(f"streaming.sinks.{yaml_key}", cls.config_type)
            binding = cls.build_binding(cfg, ctx)
            bindings.append(binding)

        _validate_no_kind_duplicates(bindings)
        return bindings


def _validate_no_kind_duplicates(bindings: list[RuntimeSinkBinding]) -> None:
    seen: dict[ErrorKind, str] = {}
    for binding in bindings:
        if binding.purpose != "errors":
            continue
        for kind in binding.kinds:
            if kind in seen:
                raise DuplicateErrorSinkError(
                    f"ErrorKind.{kind} is claimed by more than one registered sink."
                )
            seen[kind] = repr(binding.sink)


__all__ = ["RegisteredSink", "RuntimeSinkBinding", "SinkRegistry"]
