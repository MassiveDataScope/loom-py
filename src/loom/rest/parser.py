"""Pydantic-backed command adapter for FastAPI request parsing."""

from __future__ import annotations

from types import UnionType
from typing import Any, TypeVar, Union, cast, get_args, get_origin

import msgspec
import pydantic

from loom.core.command.base import Command
from loom.core.command.introspection import (
    get_calculated_fields,
    get_internal_fields,
    get_patch_fields,
)

CommandT = TypeVar("CommandT", bound=Command)


class PydanticAdapter:
    """Implements ``CommandAdapter[dict]`` using Pydantic for validation.

    Builds a Pydantic ``BaseModel`` from a Command's input fields (excluding
    ``internal`` and ``calculated`` fields) and uses it to validate raw dicts.

    Example::

        adapter = PydanticAdapter()
        adapter.register(CreateUser, UpdateUser)
        cmd, fields_set = adapter.parse(CreateUser, {"email": "a@b.com", "name": "Jo"})
    """

    def __init__(self) -> None:
        self._cache: dict[type, type[pydantic.BaseModel]] = {}

    def register(self, *command_classes: type) -> None:
        """Eagerly compile schemas for the given command classes.

        Call at application startup to avoid lazy compilation on first request.

        Args:
            command_classes: Command subclasses to pre-compile.
        """
        for cls in command_classes:
            self.compile_schema(cls)

    def compile_schema(self, command_cls: type) -> type[pydantic.BaseModel]:
        """Build a Pydantic BaseModel from a Command's input fields.

        Args:
            command_cls: A Command subclass.

        Returns:
            A Pydantic BaseModel subclass with fields matching the
            command's input fields.
        """
        cached = self._cache.get(command_cls)
        if cached is not None:
            return cached

        struct_fields = {f.name: f for f in msgspec.structs.fields(command_cls)}
        excluded = set(get_calculated_fields(command_cls)) | set(get_internal_fields(command_cls))
        patch_fields = set(get_patch_fields(command_cls))

        field_definitions: dict[str, Any] = {}
        for name, sf in struct_fields.items():
            if name in excluded:
                continue

            annotation = _without_unset_type(sf.type)
            if name in patch_fields:
                field_definitions[name] = (annotation, None)
            elif sf.default is msgspec.NODEFAULT:
                field_definitions[name] = (annotation, ...)
            else:
                field_definitions[name] = (annotation, sf.default)

        model = cast(
            type[pydantic.BaseModel],
            pydantic.create_model(
                f"{command_cls.__name__}Schema",
                **field_definitions,
            ),
        )
        self._cache[command_cls] = model
        return model

    def parse(
        self, command_cls: type[CommandT], raw: dict[str, Any]
    ) -> tuple[CommandT, frozenset[str]]:
        """Validate raw dict via Pydantic and convert to a Command instance.

        Args:
            command_cls: The Command subclass to instantiate.
            raw: The raw input dictionary.

        Returns:
            A tuple of (command_instance, fields_set) where fields_set
            contains the names of fields present in the raw input.

        Raises:
            pydantic.ValidationError: If ``raw`` fails validation against
                the compiled pydantic schema.
            BoundaryValidationError: If ``from_payload`` rejects the
                validated payload while converting it to *command_cls*.
        """
        schema = self.compile_schema(command_cls)
        validated = schema.model_validate(raw)

        payload = validated.model_dump(mode="python", exclude_unset=True)
        return command_cls.from_payload(payload)


def _without_unset_type(annotation: Any) -> Any:
    if annotation is msgspec.UnsetType:
        return Any

    origin = get_origin(annotation)
    if origin is None:
        return annotation

    if origin in (UnionType, Union):
        args = tuple(arg for arg in get_args(annotation) if arg is not msgspec.UnsetType)
        if not args:
            return Any
        result = args[0]
        for arg in args[1:]:
            result = result | arg
        return result

    return annotation
