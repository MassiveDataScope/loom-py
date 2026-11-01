"""The ``none`` persistence backend: no persistence at all."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar

from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel
from loom.core.persistence.abc import PersistenceWiring
from loom.core.persistence.profile import BackendRuntime
from loom.core.persistence.stores import NONE_BACKEND, StoreCapabilities, StoreSpec


class NoneBackend:
    """Backend for ``stores.default.backend: none``, and for a worker without ``stores``.

    No unit-of-work factory, no repositories, no startup resource; the rest
    of the store section, if any, is ignored.
    """

    name: ClassVar[str] = NONE_BACKEND
    repository_type: ClassVar[type[Any] | None] = None

    def capabilities(self, spec: StoreSpec) -> StoreCapabilities:
        """Describe a backend that serves no models or cursors."""
        return StoreCapabilities(serves_models=False, transactional=False, pages_with_cursors=False)

    def build(
        self,
        ctx: ConfigContext,
        models: Sequence[type[BaseModel]],
        runtime: BackendRuntime,
    ) -> PersistenceWiring:
        """Build a wiring in which every hook is a no-op.

        Args:
            ctx: Ignored; this backend reads no configuration.
            models: Ignored; this backend serves no repositories.
            runtime: Ignored; this backend opens no connection.

        Returns:
            The no-persistence wiring.
        """
        del ctx, models, runtime
        return PersistenceWiring(
            uow_factory=None,
            repo_registration_module=_register_no_repositories,
        )


def _register_no_repositories(container: LoomContainer) -> None:
    """No-op DI module: nothing to register."""


__all__ = ["NoneBackend"]
