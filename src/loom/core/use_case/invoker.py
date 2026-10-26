"""Application-level invocation facade for use cases."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Protocol, runtime_checkable

from loom.core.engine.compilable import Compilable
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.registry import UseCaseRegistry, model_entity_key


@runtime_checkable
class ApplicationInvoker(Protocol):
    """Protocol for non-HTTP invocation entrypoints."""

    async def invoke(
        self,
        use_case: type[Compilable],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any: ...

    async def invoke_name(
        self,
        key: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any: ...

    def entity(self, model: type[Any]) -> EntityInvoker: ...


@dataclass(frozen=True)
class EntityInvoker:
    """Convenience facade for entity-scoped AutoCRUD invocation."""

    _entity_key: str
    _app: ApplicationInvoker

    def _key(self, action: str) -> str:
        return f"{self._entity_key}:{action}"

    @staticmethod
    def _with_default_profile(params: dict[str, Any]) -> dict[str, Any]:
        if "profile" in params:
            return params
        return {**params, "profile": "default"}

    async def create(self, *, payload: dict[str, Any]) -> Any:
        return await self._app.invoke_name(self._key("create"), payload=payload)

    async def get(self, *, params: dict[str, Any]) -> Any:
        return await self._app.invoke_name(
            self._key("get"),
            params=self._with_default_profile(params),
        )

    async def list(
        self,
        *,
        params: dict[str, Any],
        payload: dict[str, Any] | None = None,
    ) -> Any:
        return await self._app.invoke_name(
            self._key("list"),
            params=self._with_default_profile(params),
            payload=payload,
        )

    async def update(
        self,
        *,
        params: dict[str, Any],
        payload: dict[str, Any],
    ) -> Any:
        return await self._app.invoke_name(
            self._key("update"),
            params=params,
            payload=payload,
        )

    async def delete(self, *, params: dict[str, Any]) -> Any:
        return await self._app.invoke_name(self._key("delete"), params=params)


@dataclass(frozen=True)
class AppInvoker:
    """Default implementation for app-level invocation.

    Args:
        factory: Builds the use-case instance to execute.
        executor: Runs the built instance against its execution plan.
        registry: Resolves a use case from its registered key.
        identity: Verified caller every invocation runs as. ``None`` — the
            default — passes no caller to the executor, so a use case declaring
            ``Caller()`` raises ``Unauthenticated`` exactly as it does today; a
            transport that has a caller binds one with :meth:`for_identity`.

    The identity is a field rather than something read from ambient state
    inside :meth:`invoke`: a facade that discovers who it runs as would make
    the caller of one invocation depend on what the surrounding task happened
    to set, which is the ambient-configuration rule the REST layer already
    keeps to a single documented site.
    """

    factory: UseCaseFactory
    executor: RuntimeExecutor
    registry: UseCaseRegistry
    identity: Identity | None = None

    def for_identity(self, identity: Identity) -> AppInvoker:
        """Return a copy of this invoker bound to *identity*.

        Args:
            identity: Verified caller the copy invokes every use case as.

        Returns:
            A new invoker sharing this one's factory, executor and registry.

        Example::

            bound = invoker.for_identity(caller)
            await bound.invoke(ReadInvoice, params={"id": 7})
        """
        # ``replace`` copies fields added later.
        return replace(self, identity=identity)

    async def invoke(
        self,
        use_case: type[Compilable],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        instance: Compilable = self.factory.build(use_case)
        return await self.executor.execute(
            instance, params=params, payload=payload, identity=self.identity
        )

    async def invoke_name(
        self,
        key: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        use_case = self.registry.resolve(key)
        return await self.invoke(use_case, params=params, payload=payload)

    def entity(self, model: type[Any]) -> EntityInvoker:
        """Return a CRUD-focused entity facade bound to ``model``."""
        return EntityInvoker(model_entity_key(model), self)
