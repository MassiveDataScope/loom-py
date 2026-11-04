"""Discovery engine that scans explicitly declared modules."""

from __future__ import annotations

from loom.core.discovery._utils import (
    _append_unique,
    collect_from_modules,
    collect_use_cases_from_interfaces,
    import_modules,
)
from loom.core.discovery.base import AGENTS_ONLY_HINT, DiscoveryResult
from loom.core.use_case.use_case import UseCase


class ModulesDiscoveryEngine:
    """Discover models, use cases and interfaces from module paths."""

    def __init__(self, module_paths: list[str]) -> None:
        self._module_paths = module_paths

    def discover(self) -> DiscoveryResult:
        """Scan modules and supplement use cases found in auto interfaces.

        Jobs the modules declare are collected too.

        Returns:
            :class:`~loom.core.discovery.base.DiscoveryResult` with all
            discovered models, use cases, and interfaces.

        Raises:
            ValueError: When no module paths are provided. The message points
                at ``app.discovery.mode: manifest`` with ``AGENTS`` for an
                application whose only content is agents.
        """
        if not self._module_paths:
            raise ValueError(
                f"modules discovery requires at least one module path. {AGENTS_ONLY_HINT}"
            )

        modules = import_modules(self._module_paths)
        models, use_cases, interfaces, jobs = collect_from_modules(modules)

        seen_ucs: set[type[UseCase[object, object]]] = set(use_cases)
        for uc in collect_use_cases_from_interfaces(interfaces):
            _append_unique(use_cases, seen_ucs, uc)

        return DiscoveryResult(
            models=tuple(models),
            use_cases=tuple(use_cases),
            interfaces=tuple(interfaces),
            jobs=tuple(jobs),
        )
