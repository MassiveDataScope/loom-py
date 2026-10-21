"""The ``none`` persistence backend: no persistence at all."""

from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar

from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel
from loom.core.persistence.abc import PersistenceWiring
from loom.core.persistence.profile import BackendRuntime


class NoneBackend:
    """Backend for ``persistence.backend: none``.

    No unit-of-work factory, no repositories, no startup resource, no default
    repository type; a ``database`` section, if present, is ignored.
    """

    name: ClassVar[str] = "none"

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
            default_repository_type=None,
        )


def _register_no_repositories(container: LoomContainer) -> None:
    """No-op DI module: nothing to register."""


__all__ = ["NoneBackend"]
