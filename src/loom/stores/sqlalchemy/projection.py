from __future__ import annotations

from typing import Any, Generic, TypeVar

T = TypeVar("T")


class Projection(Generic[T]):
    """Descriptor for derived fields with cache dependency metadata."""

    __slots__ = ("loader", "profiles", "depends_on", "default", "_attr_name")

    def __init__(
        self,
        *,
        loader: Any,
        profiles: tuple[str, ...] = ("default",),
        depends_on: tuple[str, ...] = (),
        default: T | None = None,
    ) -> None:
        """Initialise a projection descriptor.

        Args:
            loader: A ``ProjectionLoader`` used to batch-fetch values.
            profiles: Loading profile names for which this projection is active.
            depends_on: Cache dependency specs (``"entity:fk_field"`` format).
            default: Fallback value when the loader returns no result for a parent.
        """
        self.loader = loader
        self.profiles = profiles
        self.depends_on = depends_on
        self.default = default
        self._attr_name = ""

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._attr_name = f"_projection_{name}"

    def __get__(self, obj: Any, objtype: type[Any] | None = None) -> Any:
        if obj is None:
            return self
        return getattr(obj, self._attr_name, None)

    def __set__(self, obj: Any, value: Any) -> None:
        setattr(obj, self._attr_name, value)

    def has_value(self, obj: Any) -> bool:
        """Check whether the projection value has been populated on the given object.

        Args:
            obj: Model instance to inspect.

        Returns:
            ``True`` if the projection attribute has been set.
        """
        return hasattr(obj, self._attr_name)
