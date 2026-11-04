"""``Joined[...]``: a repository a use case writes through to a store besides its primary.

A use case writes to its primary store, the ``__store__`` of its main model,
and reads from any store. A constructor parameter annotated ``Joined[...]``
declares one more store it writes to: the store of the repository's model,
which joins the invocation and commits with it (ADR 0008).
"""

from __future__ import annotations

from typing import Annotated, Final, TypeAlias, TypeVar, get_args, get_origin

T = TypeVar("T")


class _JoinedMarker:
    """The metadata ``Joined[...]`` adds to its repository annotation."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "Joined"


_JOINED: Final = _JoinedMarker()

Joined: TypeAlias = Annotated[T, _JOINED]
"""Declares that a use case writes through this repository to the store of its model.

Wrap the repository's annotation, which names the model, in a use case or
job constructor. Dependency injection resolves the repository as usual; a
write through a repository of a store the use case neither has as primary
nor declares ``Joined`` raises ``UndeclaredStoreWrite`` before the driver is
called. Composition refuses a ``Joined`` store that cannot roll back.

Example::

    class PlaceOrder(UseCase[Order, OrderResponse]):
        def __init__(
            self, orders: RepoFor[Order], audit: Joined[RepoFor[AuditEntry]]
        ) -> None: ...
"""


def joined_repository(annotation: object) -> object | None:
    """Return the repository annotation ``Joined[...]`` wraps, or ``None`` when not ``Joined``.

    Args:
        annotation: A constructor parameter's annotation, ``Annotated`` metadata kept.
    """
    if get_origin(annotation) is not Annotated:
        return None
    repository, *metadata = get_args(annotation)
    return repository if any(item is _JOINED for item in metadata) else None


__all__ = ["Joined"]
