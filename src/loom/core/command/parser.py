from __future__ import annotations

from typing import Any, Protocol, TypeVar

RawT = TypeVar("RawT", contravariant=True)


class CommandAdapter(Protocol[RawT]):
    """Contract for compiling a Command into a transport-specific schema
    and parsing raw input into a Command instance.

    Implementations translate the Command (single source of truth) into
    validation schemas for specific transports (Pydantic, Avro, msgspec, etc.).

    Args:
        RawT: The raw input type the adapter accepts (e.g. dict, bytes).
    """

    def compile_schema(self, command_cls: type) -> type:
        """Compile a Command class into a transport-specific validation schema.

        Args:
            command_cls: The Command subclass to compile.

        Returns:
            A transport-specific schema type derived from the Command.
        """
        ...

    def parse(self, command_cls: type, raw: RawT) -> tuple[Any, frozenset[str]]:
        """Parse raw input into a Command instance with field tracking.

        Args:
            command_cls: The Command subclass to instantiate.
            raw: The raw input data.

        Returns:
            A tuple of (command_instance, fields_set) where fields_set
            contains the names of fields present in the raw input.
        """
        ...
