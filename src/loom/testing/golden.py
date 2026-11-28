"""Golden testing utilities for Loom use cases.

Provides deterministic :func:`serialize_plan` for snapshotting
:class:`~loom.core.engine.plan.ExecutionPlan` objects, and
:class:`GoldenHarness` for executing use cases in isolation with fake
repositories, controlled error injection, and performance baselines.

Usage::

    harness = GoldenHarness()
    harness.inject_repo(ProductRepo, FakeProductRepo(), model=Product)
    result = await harness.run(CreateProductUseCase, payload={"name": "X", "price": 1.0})
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import msgspec

from loom.core.bootstrap.kernel import KernelRecipe
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.engine.plan import ExecutionPlan
from loom.core.identity import Identity
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase
from loom.testing.runner import AgentHandleDouble, McpHandleDouble, _AgentDoubles, _McpDoubles


def _qname(obj: Any) -> str:
    return f"{obj.__module__}.{obj.__qualname__}"


def serialize_plan(plan: ExecutionPlan) -> dict[str, Any]:
    """Produce a deterministic, JSON-serialisable snapshot of an ``ExecutionPlan``.

    All keys are sorted alphabetically so the output is stable across runs
    regardless of insertion order.  Types and callables are encoded as their
    fully qualified ``module.qualname`` string.

    Args:
        plan: Compiled execution plan to serialise.

    Returns:
        Dictionary containing only JSON-primitive values (str, int, list,
        dict, None).  Suitable for ``json.dumps`` comparison.

    Example::

        snapshot = serialize_plan(compiler.get_plan(MyUseCase))
        assert snapshot["use_case"] == "my_app.use_cases.MyUseCase"
    """
    param_bindings = [
        {"annotation": _qname(pb.annotation), "name": pb.name} for pb in plan.param_bindings
    ]
    input_binding = (
        {
            "command_type": _qname(plan.input_binding.command_type),
            "name": plan.input_binding.name,
        }
        if plan.input_binding is not None
        else None
    )
    load_steps = [
        {
            "against": ls.against,
            "entity_type": _qname(ls.entity_type),
            "lookup_kind": ls.lookup_kind.value,
            "name": ls.name,
            "on_missing": ls.on_missing.value,
            "profile": ls.profile,
            "source_kind": ls.source_kind.value,
            "source_name": ls.source_name,
        }
        for ls in plan.load_steps
    ]
    exists_steps = [
        {
            "against": es.against,
            "entity_type": _qname(es.entity_type),
            "name": es.name,
            "on_missing": es.on_missing.value,
            "source_kind": es.source_kind.value,
            "source_name": es.source_name,
        }
        for es in plan.exists_steps
    ]
    compute_steps = [{"fn": _qname(cs.fn)} for cs in plan.compute_steps]
    rule_steps = [{"fn": _qname(rs.fn)} for rs in plan.rule_steps]
    caller_binding = {"name": plan.caller_binding.name} if plan.caller_binding is not None else None

    return {
        "caller_binding": caller_binding,
        "compute_steps": compute_steps,
        "exists_steps": exists_steps,
        "input_binding": input_binding,
        "load_steps": load_steps,
        "param_bindings": param_bindings,
        "rule_steps": rule_steps,
        "use_case": _qname(plan.use_case_type),
    }


class _ErrorProxy:
    """Wraps a fake repository to raise configured errors on specific methods.

    Args:
        target: The underlying fake repository instance.
        error_map: Mapping of method name to exception to raise.
    """

    def __init__(self, target: Any, error_map: dict[str, Exception]) -> None:
        object.__setattr__(self, "_target", target)
        object.__setattr__(self, "_error_map", error_map)

    def __getattr__(self, name: str) -> Any:
        error_map: dict[str, Exception] = object.__getattribute__(self, "_error_map")
        if name in error_map:
            err = error_map[name]

            async def _raise(*args: Any, **kwargs: Any) -> Any:
                raise err

            return _raise
        target = object.__getattribute__(self, "_target")
        return getattr(target, name)


class GoldenHarness:
    """Executes use cases in isolation with fake repositories, fake ports and doubles.

    Allows injecting fake repo instances, binding adapters or fake ports,
    standing doubles in for ``Agent()`` and ``Mcp()`` markers, forcing
    errors on specific methods, and asserting performance baselines — all
    without a real database, model or MCP server.

    Example::

        harness = GoldenHarness()
        harness.inject_repo(IProductRepo, FakeProductRepo(), model=Product)
        harness.force_error(IProductRepo, "create", Conflict("duplicate"))
        result = await harness.run(CreateProductUseCase, payload={"name": "X"})
    """

    def __init__(self) -> None:
        self._providers: dict[type, tuple[Callable[..., Any], Scope]] = {}
        self._instances: dict[type, Any] = {}
        self._model_map: dict[type, type] = {}
        self._error_overrides: dict[tuple[type, str], Exception] = {}
        self._agent_doubles: dict[str, AgentHandleDouble] = {}
        self._mcp_doubles: dict[str, McpHandleDouble] = {}
        self._compiler = UseCaseCompiler()

    def inject_repo(
        self,
        interface: type,
        fake_instance: Any,
        *,
        model: type | None = None,
    ) -> None:
        """Register a fake repository instance for an interface type.

        Args:
            interface: Repository interface used as the DI resolution key.
            fake_instance: Fake repository instance to inject.
            model: Optional domain model to register a ``RepoFor`` mapping,
                enabling auto-injection for use cases that inherit the default
                ``UseCase.__init__``.
        """
        self.register_instance(interface, fake_instance)
        if model is not None:
            self._model_map[model] = interface

    def register(
        self, interface: type, provider: Callable[..., Any], *, scope: Scope = Scope.REQUEST
    ) -> None:
        """Bind *provider*, typically an adapter class, to *interface*.

        Its constructor's ``Agent()`` and ``Mcp()`` markers resolve through
        the doubles of :meth:`with_agent` and :meth:`with_mcp`.

        Args:
            interface: The port used as the DI resolution key.
            provider: As in :meth:`~loom.core.di.container.LoomContainer.register`.
            scope: Lifetime scope. Defaults to :attr:`~Scope.REQUEST`.
        """
        self._providers[interface] = (provider, scope)

    def register_instance(self, interface: type, instance: Any) -> None:
        """Bind an already-built fake to *interface*, replacing any provider bound to it.

        No provider of *interface* is built and no marker of one is resolved.
        :meth:`force_error` applies to it.

        Args:
            interface: The port used as the DI resolution key.
            instance: The fake every resolution of *interface* returns.
        """
        self._instances[interface] = instance

    def with_agent(self, name: str, double: AgentHandleDouble) -> GoldenHarness:
        """Stand *double* in for every ``Agent(name)`` marker, in ``execute()`` or a constructor.

        Without it such a marker fails closed with a :class:`RuntimeError`.

        Returns:
            ``self`` for chaining.
        """
        self._agent_doubles[name] = double
        return self

    def with_mcp(self, server: str, double: McpHandleDouble) -> GoldenHarness:
        """Stand *double* in for every ``Mcp(server, ...)`` marker, narrowed to its ``include``.

        Without it such a marker fails closed with a :class:`RuntimeError`.

        Returns:
            ``self`` for chaining.
        """
        self._mcp_doubles[server] = double
        return self

    def force_error(self, interface: type, method: str, error: Exception) -> None:
        """Force a specific repository method to raise an error.

        Args:
            interface: Repository interface type.
            method: Method name to intercept.
            error: Exception instance to raise on call.
        """
        self._error_overrides[(interface, method)] = error

    def simulate_system_error(self, interface: type, method: str) -> None:
        """Simulate a ``SystemError`` on a specific repository method.

        Args:
            interface: Repository interface type.
            method: Method name to intercept.
        """
        self._error_overrides[(interface, method)] = SystemError(
            f"simulated system error in {interface.__name__}.{method}"
        )

    def _wrap(self, interface: type, instance: Any) -> Any:
        overrides = {
            method: err
            for (iface, method), err in self._error_overrides.items()
            if iface is interface
        }
        return _ErrorProxy(instance, overrides) if overrides else instance

    def _build_container(self) -> LoomContainer:
        container = KernelRecipe.container()
        for interface, (provider, scope) in self._providers.items():
            container.register(interface, provider, scope=scope)
        for interface, instance in self._instances.items():
            container.register_instance(interface, self._wrap(interface, instance))
        for model, interface in self._model_map.items():
            container.register_repo(model, interface)
        return container

    async def run(
        self,
        use_case_type: type[UseCase[Any, Any]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        identity: Identity | None = None,
    ) -> Any:
        """Build and execute a use case with its fakes, adapters and doubles.

        The use case is built inside its execution, so a constructor
        ``Caller()`` receives *identity*, as on every other entry point.

        Args:
            use_case_type: UseCase subclass to execute.
            params: Primitive parameter values keyed by name.
            payload: Raw dict for ``Input()`` command construction.
            identity: Caller the execution runs as.  Required by use cases
                declaring ``Caller()``; pass
                :data:`~loom.core.identity.identity.ANONYMOUS` to pin the
                unauthenticated path.

        Returns:
            Result produced by the use case.

        Raises:
            loom.core.errors.Unauthenticated: If the use case declares
                ``Caller()``, in ``execute()`` or in its constructor, or
                resolves an ``Agent()`` or ``Mcp()`` marker inside its
                execution, and no *identity* is given.
            RuntimeError: If an ``Agent(name)`` or ``Mcp(server, ...)``
                marker has no double from :meth:`with_agent` or :meth:`with_mcp`.
        """
        self._compiler.compile(use_case_type)
        container = self._build_container()
        declarer = f"{use_case_type.__qualname__} or a dependency it builds"
        harness = type(self).__name__
        executor = UseCaseExecutor(
            self._compiler,
            resolvers=container.kinds,
            agent_resolver=_AgentDoubles(declarer, harness, self._agent_doubles),
            mcp_resolver=_McpDoubles(declarer, harness, self._mcp_doubles),
        )
        return await executor.run(
            use_case_type,
            factory=UseCaseFactory(container),
            params=params,
            payload=payload,
            identity=identity,
        )

    async def run_with_baseline(
        self,
        use_case_type: type[UseCase[Any, Any]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        identity: Identity | None = None,
        name: str,
        max_ms: float,
        baseline_dir: Path,
    ) -> Any:
        """Execute a use case and assert it completes within a time baseline.

        Writes a ``<name>.json`` file to ``baseline_dir`` recording the
        measured duration.  Raises ``AssertionError`` if execution exceeds
        ``max_ms``.

        Args:
            use_case_type: UseCase subclass to execute.
            params: Primitive parameter values keyed by name.
            payload: Raw dict for ``Input()`` command construction.
            identity: Caller the execution runs as.
            name: Baseline identifier used as the filename.
            max_ms: Maximum allowed execution duration in milliseconds.
            baseline_dir: Directory where baseline JSON files are written.

        Returns:
            Result produced by the use case.

        Raises:
            AssertionError: If elapsed time exceeds ``max_ms``.
        """
        start = time.perf_counter()
        result = await self.run(use_case_type, params=params, payload=payload, identity=identity)
        elapsed_ms = (time.perf_counter() - start) * 1000

        baseline_dir.mkdir(parents=True, exist_ok=True)
        record = {"elapsed_ms": round(elapsed_ms, 3), "max_ms": max_ms, "name": name}
        (baseline_dir / f"{name}.json").write_text(json.dumps(record, indent=2, sort_keys=True))

        if elapsed_ms > max_ms:
            raise AssertionError(
                f"Performance baseline exceeded for '{name}': {elapsed_ms:.2f}ms > {max_ms:.2f}ms"
            )
        return result


def _serialize_result(result: Any) -> Any:
    """Convert a use-case result to a JSON-serialisable value."""
    if isinstance(result, msgspec.Struct):
        return msgspec.to_builtins(result)
    return result
