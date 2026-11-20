"""Valid test instances of any loom type, built by polyfactory."""

from __future__ import annotations

from dataclasses import is_dataclass
from typing import Any, Generic, TypeGuard, TypeVar

from polyfactory.factories.base import BaseFactory
from polyfactory.factories.dataclass_factory import DataclassFactory
from polyfactory.factories.msgspec_factory import MsgspecFactory
from polyfactory.field_meta import FieldMeta

T = TypeVar("T")

_SEED = 20261120


class LoomFactory(DataclassFactory[T], MsgspecFactory[T], Generic[T]):  # type: ignore[type-var]
    """Base factory for dataclasses and msgspec Structs, deterministic by seed.

    It is registered as a polyfactory base factory, so ``make(Model)`` and every
    nested field of a loom type build with no declaration. Field rules that fit
    one field belong in the type, as ``Annotated[int, msgspec.Meta(ge=0)]``,
    which polyfactory reads. A rule that does not fit one field (a cross-field
    invariant, a reference to another type) gets a subclass in the area that
    owns the type::

        class BackoffFactory(LoomFactory[Backoff]):
            __set_as_default_factory_for_type__ = True

            first_s = Use(lambda: 1.0)
            max_s = Use(lambda: 30.0)

    Once registered this way, ``make(Backoff)`` and any type nesting a
    ``Backoff`` use it.
    """

    __is_base_factory__ = True
    __random_seed__ = _SEED

    @classmethod
    def is_supported_type(cls, value: Any) -> TypeGuard[type[T]]:
        """Report whether *value* is a dataclass or a msgspec Struct."""
        return is_dataclass(value) or MsgspecFactory.is_supported_type(value)

    @classmethod
    def get_model_fields(cls) -> list[FieldMeta]:
        """Read the fields of the model with the reader that matches its kind."""
        if is_dataclass(cls.__model__):
            return super().get_model_fields()
        return super(DataclassFactory, cls).get_model_fields()

    @classmethod
    def should_use_default_value(cls, field_meta: FieldMeta) -> bool:
        """Use the model's default only for a field no factory in the chain declares.

        A registered factory declares a field to replace a default that is not
        valid on its own, so ``__use_defaults__`` never overrides a declared field.
        """
        if cls._declares(field_meta.name):
            return False
        return super().should_use_default_value(field_meta)

    @classmethod
    def _declares(cls, name: str) -> bool:
        """Report whether a factory between this class and ``LoomFactory`` declares *name*."""
        for owner in cls.__mro__:
            if owner is LoomFactory:
                return False
            if name in vars(owner):
                return True
        return False

    @classmethod
    def restart_sequence(cls) -> None:
        """Reseed this factory and every subclass, so the next build starts over."""
        cls.seed_random(_SEED)
        for subclass in cls.__subclasses__():
            subclass.restart_sequence()


def make(model: type[T], /, **fields: Any) -> T:
    """Build a valid instance of *model*.

    Every value in *fields* is used as given. Every other field is generated
    deterministically (the same call builds the same values), honouring the
    ``Annotated`` constraints of the type, or comes from a registered
    ``LoomFactory`` subclass. A test must pass explicitly every value it asserts
    on, because generated values may change.

    Args:
        model: A dataclass or msgspec Struct.
        **fields: Values for the fields the caller cares about.

    Returns:
        A real instance of *model*.
    """
    LoomFactory.restart_sequence()
    builder: type[BaseFactory[T]] = LoomFactory._get_or_create_factory(model)  # noqa: SLF001
    return builder.build(**fields)
