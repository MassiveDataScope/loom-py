"""Streaming step declarations and resource contracts."""

from __future__ import annotations

from abc import ABC
from typing import Any, ClassVar, Generic, Protocol, TypeVar, runtime_checkable

from loom.core.config import Configurable
from loom.core.logger import LoggerPort, get_logger
from loom.streaming.core._message import StreamPayload
from loom.streaming.nodes._shape import StreamShape

InT = TypeVar("InT", bound=StreamPayload, contravariant=True)
OutT = TypeVar("OutT", bound=StreamPayload, covariant=True)
ResourceT = TypeVar("ResourceT")
ResourceCoT = TypeVar("ResourceCoT", covariant=True)


@runtime_checkable
class ResourceFactory(Protocol[ResourceT]):
    """Create and close step resources under runtime control."""

    def create(self) -> ResourceT:
        """Create one worker-local resource."""
        ...

    def close(self, resource: ResourceT) -> None:
        """Close one resource created by this factory."""
        ...


@runtime_checkable
class StepContext(Protocol[ResourceCoT]):
    """Execution context with explicit resource access."""

    @property
    def resource(self) -> ResourceCoT:
        """Worker-local resource available to the step."""
        ...


class Step(Configurable, ABC, Generic[InT, OutT]):
    """Base class for declarative streaming steps.

    Pattern:
        Declarative step.
    """

    resource: ClassVar[type[ResourceFactory[Any]] | None] = None
    name: ClassVar[str] = ""
    input_shape: ClassVar[StreamShape]
    output_shape: ClassVar[StreamShape]
    _log: LoggerPort | None = None

    @property
    def log(self) -> LoggerPort:
        """Structured logger bound to this step class and module."""
        logger = self._log
        if logger is None:
            cls = type(self)
            logger = get_logger(cls.__qualname__).bind(
                component="step",
                class_name=cls.__qualname__,
                module=cls.__module__,
                step_name=cls.step_name(),
            )
            self._log = logger
        return logger

    @classmethod
    def step_name(cls) -> str:
        """Resolved step name for observability and validation errors."""
        return cls.name or cls.__qualname__


class RecordStep(Step[InT, OutT], ABC):
    """Streaming step that consumes and produces one record at a time.

    Declare the subclass itself in a flow, not an instance. The compiler
    materializes the class during binding resolution.

    Pattern:
        Record-shaped step.

    Subclasses define ``execute(self, message, **kwargs) -> OutT`` with the
    explicit payload and dependency signature they need. The runtime uses duck
    typing so user code can express explicit dependencies without mypy forcing
    a single override shape.
    """

    router_branch_safe: ClassVar[bool] = True
    input_shape: ClassVar[StreamShape] = StreamShape.RECORD
    output_shape: ClassVar[StreamShape] = StreamShape.RECORD


class BatchStep(Step[InT, OutT], ABC):
    """Streaming step that consumes and produces one batch at a time.

    Declare the subclass itself in a flow, not an instance. The compiler
    materializes the class during binding resolution.

    Pattern:
        Batch-shaped step.

    Subclasses define ``execute(self, messages, **kwargs) -> OutT`` with the
    explicit batch and dependency signature they need.
    """

    router_branch_safe: ClassVar[bool] = True
    input_shape: ClassVar[StreamShape] = StreamShape.BATCH
    output_shape: ClassVar[StreamShape] = StreamShape.BATCH


class ExpandStep(Step[InT, OutT], ABC):
    """Streaming step that expands one record into many output messages.

    Declare the subclass itself in a flow, not an instance. The compiler
    materializes the class during binding resolution.

    Pattern:
        Record fan-out step.

    Subclasses define ``execute(self, message, **kwargs) -> Iterable[OutT]``
    with the explicit payload and dependency signature they need.
    """

    router_branch_safe: ClassVar[bool] = True
    input_shape: ClassVar[StreamShape] = StreamShape.RECORD
    output_shape: ClassVar[StreamShape] = StreamShape.RECORD


class BatchExpandStep(Step[InT, OutT], ABC):
    """Streaming step that expands each message of a batch into its own outputs.

    Declare the subclass itself in a flow, not an instance. The compiler
    materializes the class during binding resolution.

    Pattern:
        Batch fan-out step.

    Subclasses define ``execute(self, messages, **kwargs) -> list[list[OutT]]``
    with the explicit batch and dependency signature they need. The result
    holds one list per message, in batch order: list ``i`` holds the payloads
    ``messages[i]`` expands into, and each takes that message's metadata. An
    empty list filters the message out. The result and each of its lists are
    iterated once, so generators work.

    These results raise ``TypeError`` and every message of the batch becomes
    an error envelope: a result whose length differs from the batch, a flat
    list of payloads, a ``Message`` among the outputs, and a ``str``,
    ``bytes`` or mapping in place of the result or of one of its lists.
    """

    router_branch_safe: ClassVar[bool] = True
    input_shape: ClassVar[StreamShape] = StreamShape.BATCH
    output_shape: ClassVar[StreamShape] = StreamShape.RECORD


__all__ = [
    "BatchExpandStep",
    "BatchStep",
    "ExpandStep",
    "RecordStep",
    "ResourceFactory",
    "Step",
    "StepContext",
]
