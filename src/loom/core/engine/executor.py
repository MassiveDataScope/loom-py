from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar, overload

from loom.core.authz.elevation import ElevationSink
from loom.core.engine.boundaries import Boundaries, detached
from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.kinds import AGENT, MCP
from loom.core.engine.metrics import MetricsAdapter
from loom.core.engine.observation import Observer
from loom.core.engine.plan import ExecutionPlan
from loom.core.engine.preparation import ExecutionInputs, ParameterBindingError, Preparation
from loom.core.execution import (
    ExecutionKinds,
    ExecutionResolver,
    enter,
    execution_identity,
    leave,
)
from loom.core.identity import Identity, current_caller
from loom.core.logger import LoggerPort, get_logger
from loom.core.uow.coordinator import Coordinator
from loom.core.uow.scope import current_scope, refuse_write_under_read_only

if TYPE_CHECKING:
    from loom.core.job.job import Job
    from loom.core.use_case.factory import UseCaseFactory
    from loom.core.use_case.use_case import UseCase

__all__ = ["ParameterBindingError", "RuntimeExecutor"]

ResultT = TypeVar("ResultT")
_TargetT = TypeVar("_TargetT")


class RuntimeExecutor:
    """Executes UseCases from their compiled ExecutionPlan without reflection.

    The public facade of one execution: it computes who the execution acts
    as (given, else the identity acting, else no one), opens the execution
    context, which fixes that identity, builds the instance inside it, and
    delegates the rest to its three pieces:

    - :mod:`~loom.core.engine.boundaries`: the unit of work (a top-level
      writer opens an invocation scope over ``coordinator`` and commits it;
      a nested one joins this task's scope; a read-only one enlists no
      store) and the post-commit actions it settles.
    - :mod:`~loom.core.engine.preparation`: the plan's steps that build the
      arguments of ``execute()`` (parameters, caller, ``Agent()``/``Mcp()``,
      command, loads, computes, rules) and the call itself.
    - :mod:`~loom.core.engine.observation`: ``EXEC_START`` before the unit of
      work opens and exactly one terminal outcome, ``EXEC_DONE`` or
      ``EXEC_ERROR`` with the ``error_kind`` of the step that failed, with
      structured logs (``usecase``, ``duration_ms``, ``status``).

    One acting identity governs everything a request causes (spec 011,
    T09a): row-level security, the jobs it dispatches, its nested executions
    and the tasks it starts, even those that outlive it, all act as the
    identity ``Caller()`` receives. Only an explicit switch changes it: given
    an identity other than the one acting, an execution logs
    ``identity_switched`` and acts as the given one, and so does everything
    it causes. A caller acts within its delegation
    (``identity.max_delegation_seconds`` from the request's start): past it,
    a task that outlived the request can no longer act as that caller.

    Args:
        compiler: Compiler used to retrieve cached plans.
        coordinator: Optional coordinator of the application's stores.
            When provided, each top-level :meth:`execute` call opens one
            invocation scope over it, which enlists the stores the use case
            declares. Nested executions in the same task join that scope.
        debug_execution: When ``True``, emits ``[STEP]`` logs for every
            pipeline stage. Defaults to ``False`` (summary logs only).
        logger: Optional logger. Defaults to the framework logger.
        metrics: Optional metrics adapter. When provided, receives
            ``EXEC_START``, ``EXEC_DONE``, and ``EXEC_ERROR`` events.
        repo_resolver: Optional callable that resolves a repository instance
            from an entity model type. Used by ``Load``/``Exists`` when
            ``dependencies`` override is not passed to :meth:`execute`.
        resolvers: The execution kinds whose resolvers build each
            ``Agent()`` / ``Mcp()`` parameter; the kernel passes its
            container's, so both share one registry. A new one by default.
        builder: Builds the instance :meth:`run` executes from its class,
            inside that execution; the kernel passes its factory's
            ``build``. ``None`` leaves :meth:`run` to the ``factory`` it is given.
        agent_resolver: Bound as by :meth:`bind_agent_resolver`, which
            explains why the kernel binds it after construction instead.
        mcp_resolver: Bound as by :meth:`bind_mcp_resolver`.

    Example::

        executor = RuntimeExecutor(
            compiler,
            coordinator=OrderedCommits({DEFAULT_STORE: uow_factory}),
            metrics=prometheus_adapter,
        )
        result = await executor.execute(
            use_case,
            params={"user_id": 1},
            payload={"email": "new@corp.com"},
        )
    """

    def __init__(
        self,
        compiler: UseCaseCompiler,
        *,
        coordinator: Coordinator | None = None,
        debug_execution: bool = False,
        logger: LoggerPort | None = None,
        metrics: MetricsAdapter | None = None,
        repo_resolver: Callable[[type[Any]], Any] | None = None,
        resolvers: ExecutionKinds | None = None,
        builder: Callable[[type[Any]], Any] | None = None,
        agent_resolver: Callable[[str, Identity], Any] | None = None,
        mcp_resolver: Callable[[str, tuple[str, ...], Identity], Any] | None = None,
        elevation_sink: ElevationSink | None = None,
    ) -> None:
        self._compiler = compiler
        self._builder = builder
        # Kept as given, for introspection by the composition root's tests.
        self._coordinator = coordinator
        self._elevation_sink = elevation_sink
        self._metrics = metrics
        self._kinds = ExecutionKinds() if resolvers is None else resolvers
        self._observer = Observer(logger or get_logger(__name__), metrics, debug_execution)
        self._boundaries = Boundaries(coordinator, elevation_sink)
        self._preparation = Preparation(self._kinds, repo_resolver, self._observer)
        self._agent_resolver: Callable[[str, Identity], Any] | None = None
        self._mcp_resolver: Callable[[str, tuple[str, ...], Identity], Any] | None = None
        if agent_resolver is not None:
            self.bind_agent_resolver(agent_resolver)
        if mcp_resolver is not None:
            self.bind_mcp_resolver(mcp_resolver)

    def bind_agent_resolver(self, resolver: Callable[[str, Identity], Any]) -> None:
        """Bind the resolver used by every ``Agent()`` marker parameter.

        A separate step from construction because the resolver depends on the
        AI runtime, and the AI pillar is optional and built later: the
        composition root creates this executor first — so every other use
        case can be compiled and served regardless of whether an ``ai:``
        section exists — and wires this resolver in second, once that
        runtime exists. A use case declaring ``Agent()`` before this is
        called fails at its first execution with a clear error, not with a
        missing-attribute crash.

        The resolver is bound to :data:`~loom.core.engine.kinds.AGENT` on
        this executor's registry: as is when it implements
        :class:`~loom.core.execution.ExecutionResolver`, else wrapped to be
        called with the execution identity at resolve time.

        Args:
            resolver: Builds one agent handle from the agent's deployment
                name and the verified caller of one execution.

        Raises:
            RuntimeError: If a resolver is already bound.
        """
        if self._agent_resolver is not None:
            raise RuntimeError("RuntimeExecutor.bind_agent_resolver() called more than once")
        given = resolver  # its declared type; pyright narrows `resolver` below
        shim = resolver if isinstance(resolver, ExecutionResolver) else _LegacyAgent(resolver)
        self._kinds.bind(AGENT, shim)
        self._agent_resolver = given

    def bind_mcp_resolver(self, resolver: Callable[[str, tuple[str, ...], Identity], Any]) -> None:
        """Bind the resolver used by every ``Mcp()`` marker parameter.

        A second resolver, not an overload of :meth:`bind_agent_resolver`:
        that one is typed to two arguments (the agent name and the verified
        caller), this one to three (the server name, the marker's own
        ``include`` and the verified caller). Serving both kinds through one
        callable would need a discriminant argument telling them apart,
        which is the multi-behaviour flag this project's engineering rules
        forbid — so this is a distinct method, bound separately, the same
        way :meth:`bind_agent_resolver` is bound separately from
        construction: the AI runtime it depends on exists only once the AI
        pillar has built it.

        Args:
            resolver: Builds one MCP handle from the marker's server name,
                its own declared ``include`` and the verified caller of one
                execution.

        Raises:
            RuntimeError: If a resolver is already bound.
        """
        if self._mcp_resolver is not None:
            raise RuntimeError("RuntimeExecutor.bind_mcp_resolver() called more than once")
        given = resolver  # its declared type; pyright narrows `resolver` below
        shim = resolver if isinstance(resolver, ExecutionResolver) else _LegacyMcp(resolver)
        self._kinds.bind(MCP, shim)
        self._mcp_resolver = given

    async def run(
        self,
        use_case_type: type[Compilable],
        *,
        factory: UseCaseFactory | None = None,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        dependencies: dict[type[Any], Any] | None = None,
        load_overrides: dict[type[Any], Any] | None = None,
        read_only: bool = False,
        identity: Identity | None = None,
    ) -> Any:
        """Build ``use_case_type`` inside a new execution and execute it there.

        The instance is built once the execution has its identity and its
        context, so a constructor's ``Caller()``, ``Agent()`` or ``Mcp()`` and
        a per-execution adapter see the same execution as ``execute()``. A
        constructor failure is logged as the execution failing and raised
        as is, before ``EXEC_START``, with no runtime event.

        The plan is the one compiled for ``use_case_type``, not for the class
        of what the builder returns: a builder must return an instance of
        exactly the requested class.

        Args:
            use_case_type: Compiled use case or job class to build and run.
            factory: Factory that constructs the instance with its
                dependencies; overrides the executor's ``builder``.
            params: Primitive parameter values keyed by name.
            payload: Raw dict for command construction via ``Input()``.
            dependencies: Mapping of entity type to repository for the load steps.
            load_overrides: Pre-loaded entities by type, bypassing repo calls.
            read_only: When ``True``, no unit of work is opened.
            identity: Verified caller for this execution, as for :meth:`execute`.

        Returns:
            The result produced by ``execute()``.

        Raises:
            TypeError: When neither ``factory`` nor the executor's ``builder``
                is given.
        """
        build = self._builder if factory is None else factory.build
        if build is None:
            raise TypeError(
                "RuntimeExecutor.run() needs factory=... or an executor constructed "
                "with builder=..."
            )
        plan = self._plan_for(use_case_type)
        return await self._execute(
            plan,
            build,
            use_case_type,
            ExecutionInputs(params, payload, dependencies, load_overrides),
            identity,
            read_only or plan.read_only,
        )

    @overload
    async def execute(
        self,
        compilable: UseCase[Any, ResultT],
        *,
        params: dict[str, Any] | None = ...,
        payload: dict[str, Any] | None = ...,
        dependencies: dict[type[Any], Any] | None = ...,
        load_overrides: dict[type[Any], Any] | None = ...,
        read_only: bool = ...,
        identity: Identity | None = ...,
    ) -> ResultT: ...

    @overload
    async def execute(
        self,
        compilable: Job[ResultT],
        *,
        params: dict[str, Any] | None = ...,
        payload: dict[str, Any] | None = ...,
        dependencies: dict[type[Any], Any] | None = ...,
        load_overrides: dict[type[Any], Any] | None = ...,
        read_only: bool = ...,
        identity: Identity | None = ...,
    ) -> ResultT: ...

    @overload
    async def execute(
        self,
        compilable: Compilable,
        *,
        params: dict[str, Any] | None = ...,
        payload: dict[str, Any] | None = ...,
        dependencies: dict[type[Any], Any] | None = ...,
        load_overrides: dict[type[Any], Any] | None = ...,
        read_only: bool = ...,
        identity: Identity | None = ...,
    ) -> Any: ...

    async def execute(
        self,
        compilable: Compilable,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        dependencies: dict[type[Any], Any] | None = None,
        load_overrides: dict[type[Any], Any] | None = None,
        read_only: bool = False,
        identity: Identity | None = None,
    ) -> Any:
        """Execute a compiled instance via its ExecutionPlan.

        Accepts any object satisfying the :class:`~loom.core.engine.compilable.Compilable`
        protocol — both :class:`~loom.core.use_case.use_case.UseCase` and
        :class:`~loom.core.job.job.Job` instances are valid inputs.

        When a ``uow_factory`` was provided at construction and no
        invocation scope is published in the current async context, opens
        one, runs the pipeline in it and ends it: commit on success,
        rollback on any exception, cancellation included.  A nested call in
        the task that owns the scope joins it and never commits.
        Post-commit actions queued during the execution run after the scope
        has closed, those of each store as its outcome allows; when they
        fail the result is a
        :class:`~loom.core.errors.PostCommitError` and the
        transaction stays committed.

        Args:
            compilable: Constructed instance to execute.
            params: Primitive parameter values keyed by name.
            payload: Raw dict for command construction via ``Input()``.
            dependencies: Mapping of entity type to repository, used for
                ``LoadById()`` / ``Load()`` / ``Exists()`` steps.
            load_overrides: Pre-loaded entities by type, bypassing repo
                calls. Used by test harnesses.
            read_only: When ``True``, the execution may write to no store
                and enlists none.  In the task that owns the enclosing
                invocation its reads join the stores that invocation
                enlisted; anywhere else it opens a read-only invocation of
                its own and its reads run with a session per call.  A use
                case or job it runs that writes is refused.  Automatically
                set to ``True`` by the HTTP layer for GET routes.  Also
                honoured when ``plan.read_only`` is ``True``.
            identity: Verified caller for this execution, supplied by the
                transport.  ``None`` acts as the identity acting
                (:func:`~loom.core.identity.current_caller`: the enclosing
                execution's, the transport's, or the one a task inherited),
                else with no caller (``current_identity()`` then reads
                ``ANONYMOUS``).  Another identity than the one acting is an
                explicit switch: logged as ``identity_switched``.
                Required, given or acting, when the plan declares a
                ``Caller()``, ``Agent()`` or ``Mcp()`` parameter; pass
                :data:`~loom.core.identity.identity.ANONYMOUS` explicitly to
                run a declared-identity use case without a caller.

        Returns:
            The result produced by ``execute()``.

        Raises:
            loom.core.errors.RuleViolations: If one or more rule steps fail.
            NotFound: If a Load step finds no entity in the repository.
            Unauthenticated: If the plan declares ``Caller()`` and no identity
                was supplied.
            loom.core.errors.PostCommitError: If a post-commit
                action failed after the unit of work committed.
            loom.core.errors.ScopeOwnershipError: If a scope is published
                in this context but this task does not own it, or it is
                already closed.
            loom.core.errors.StoresNotCommitted: If two or more stores
                of the invocation did not end the same way.
            loom.core.errors.NestedIdentityMismatch: If this execution
                joins the invocation scope of this task as an identity other
                than the one the scope was opened with (a switched identity
                never joins its parent's unit of work).
            loom.core.errors.NestedElevation: If the execution running
                in this context has elevated a scope.
            loom.core.errors.UndeclaredStoreWrite: If this execution
                writes and a read-only execution encloses it.
        """
        plan = self._plan_for(type(compilable))
        return await self._execute(
            plan,
            _given,
            compilable,
            ExecutionInputs(params, payload, dependencies, load_overrides),
            identity,
            read_only or plan.read_only,
        )

    async def _execute(
        self,
        plan: ExecutionPlan,
        build: Callable[[_TargetT], Compilable],
        target: _TargetT,
        inputs: ExecutionInputs,
        identity: Identity | None,
        read_only: bool,
    ) -> Any:
        """Run one execution of ``build(target)``, behind both :meth:`run` and :meth:`execute`.

        *read_only* is already the caller's flag or the plan's. The effective
        identity is computed here, once: the one given, else the one acting
        (``None`` for no caller). :func:`enter` fixes it before the instance
        is built, reporting a switch and keeping or starting its delegation,
        so the arguments, the acting identity and everything the execution
        causes see the same value.

        An execution that ended is no execution: a task that outlived it,
        seen here by its closed invocation scope, runs as at top level, out
        of that execution's scope and channel
        (:func:`~loom.core.engine.boundaries.detached`), still acting as the
        same caller within its delegation window.
        """
        scope = current_scope()
        if scope is not None and scope.closed:
            with detached():
                return await self._execute(plan, build, target, inputs, identity, read_only)
        if identity is None:
            identity = current_caller()
        use_case = plan.use_case_type.__qualname__
        execution_token = enter(identity, use_case)
        try:
            if not read_only:
                refuse_write_under_read_only(f"{use_case}, which writes,")
            try:
                compilable = build(target)
            except Exception as exc:
                self._observer.unbuilt(use_case, exc)
                raise
            attempt = self._observer.start(use_case)
            call = self._preparation.call
            return await self._boundaries.run(
                self._boundaries.of(scope, read_only),
                plan,
                attempt,
                lambda: attempt.business(call(plan, compilable, inputs, identity)),
            )
        finally:
            leave(execution_token)

    def _plan_for(self, uc_type: type[Compilable]) -> ExecutionPlan:
        """Return the plan compiled for exactly ``uc_type``, compiling on demand.

        ``__execution_plan__`` is read from the class's own namespace, not
        through the MRO: a subclass must never run under its parent's plan.
        """
        plan = uc_type.__dict__.get("__execution_plan__")
        if isinstance(plan, ExecutionPlan):
            return plan
        return self._compiler.compile(uc_type)


def _given(compilable: Compilable) -> Compilable:
    """What :meth:`RuntimeExecutor.execute` builds: the instance it was handed."""
    return compilable


class _LegacyAgent:
    """Calls a ``(name, identity)`` agent resolver with the execution identity."""

    __slots__ = ("_resolver",)

    def __init__(self, resolver: Callable[[str, Identity], Any]) -> None:
        self._resolver = resolver

    def resolve(self, declaration: Any, annotation: object) -> object:
        return self._resolver(declaration.name, execution_identity())


class _LegacyMcp:
    """Calls a ``(server, include, identity)`` MCP resolver with the execution identity."""

    __slots__ = ("_resolver",)

    def __init__(self, resolver: Callable[[str, tuple[str, ...], Identity], Any]) -> None:
        self._resolver = resolver

    def resolve(self, declaration: Any, annotation: object) -> object:
        return self._resolver(declaration.server, declaration.include, execution_identity())
