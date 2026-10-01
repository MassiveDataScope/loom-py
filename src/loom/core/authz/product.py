"""The product's authorization declaration, published through the ``loom.authz`` entry point."""

from __future__ import annotations

from collections.abc import Mapping
from importlib.metadata import entry_points
from typing import Protocol, runtime_checkable

from loom.core.authz._roles import Permission, RoleCatalog

ENTRY_POINT_GROUP = "loom.authz"


@runtime_checkable
class AuthzProduct(Protocol):
    """What loom needs from a product to decide, delegate and elevate."""

    @property
    def catalog(self) -> RoleCatalog: ...

    @property
    def delegate(self) -> Permission: ...

    @property
    def elevations(self) -> Mapping[str, Permission]: ...


_registered: AuthzProduct | None = None


def register_authz_product(product: AuthzProduct) -> None:
    """Install ``product`` for this process, ahead of any entry point."""
    global _registered
    _registered = product


def clear_authz_product() -> None:
    """Forget the registered product; for tests."""
    global _registered
    _registered = None


def load_authz_product() -> AuthzProduct | None:
    """Return the registered product, else the one the ``loom.authz`` entry point names."""
    if _registered is not None:
        return _registered
    entry = next(iter(entry_points(group=ENTRY_POINT_GROUP)), None)
    if entry is None:
        return None
    loaded: object = entry.load()
    product = loaded() if callable(loaded) else loaded
    if not isinstance(product, AuthzProduct):
        raise TypeError(f"entry point {entry.name!r} does not provide an AuthzProduct")
    return product
