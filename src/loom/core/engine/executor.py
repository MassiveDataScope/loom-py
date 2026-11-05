from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from contextvars import Token
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

from loom.core.authz.elevation import ElevationSink, elevated_scopes, elevation_scope
from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.metrics import MetricsAdapter
from loom.core.engine.plan import ExecutionPlan, ExistsStep, LoadStep
from loom.core.engine.post_commit import (
    PostCommitChannel,
    PostCommitError,
    active_channel,
    bind_channel,
    reset_channel,
)
from loom.core.errors import NotFound, RuleViolation, RuleViolations, Unauthenticated
from loom.core.identity import Identity, current_identity, reset_identity, set_identity
from loom.core.logger import LoggerPort, get_logger
from loom.core.tracing import get_trace_id
from loom.core.uow.coordinator import Coordinator, InvocationOutcome
from loom.core.uow.scope import (
    Frame,
    InvocationScope,
    NestedElevation,
    NestedIdentityMismatch,
    current_scope,
    invocation,
    invocation_error,
    unscoped,
)
from loom.core.use_case.errors import JoinedAutocommitStore, UseCaseErrorCode
from loom.core.use_case.markers import LookupKind, OnMissing, SourceKind

if TYPE_CHECKING:
    from loom.core.job.job import Job
    from loom.core.use_case.factory import UseCaseFactory
    from loom.core.use_case.use_case import UseCase

ResultT = TypeVar("ResultT")

_LOG_EXEC = "[EXEC]"
_LOG_DONE = "[DONE]"
_LOG_FAIL = "[FAIL]"
_STATUS_SUCCESS = "success"
_STATUS_FAILURE = "failure"
_READ_ONLY = Frame(None)
_STATUS_RULE_FAILURE = "rule_failure"
_ERROR_KIND_BEGIN = "begin"
_ERROR_KIND_BUSINESS = "business"
_ERROR_KIND_COMMIT = "commit"
_ERROR_KIND_CANCELLED = "cancelled"
_ERROR_KIND_POST_COMMIT = "post_commit"


class ParameterBindingError(ValueError):
    """Raised when a primitive execute parameter cannot be coerced to its declared type."""


@dataclass(frozen=True, slots=True)
class _ExecutionInputs:
    """Everything one execution receives from the caller, as a single value.

    Groups the per-call inputs so the internal pipeline stages take one
    argument instead of one positional per input kind.
    """

    params: dict[str, Any] | None = None
    payload: dict[str, Any] | None = None
    dependencies: dict[type[Any], Any] | None = None
    load_overrides: dict[type[Any], Any] | None = None
    identity: Identity | None = None


@dataclass(slots=True)
class _ExecutionState:
    """Identity, timings and current phase of one execution.

    ``phase`` names the lifecycle step in flight so a failure can be
    reported with the ``error_kind`` of the step that raised; it is
    ``commit`` from the flush on. ``post_commit`` is set once the
    coordinator has ended the invocation scope this execution owns: the
    actions its stores' outcomes let run, which
    :meth:`RuntimeExecutor._run_lifecycle` drains even on failure — a write
    already committed must never lose its queued invalidations to another
    store's failure or an unrelated teardown one.
    """

    use_case_name: str
    trace_id: str | None
    logger: LoggerPort
    start: float = field(default_factory=time.perf_counter)
    phase: str = _ERROR_KIND_BEGIN
    pipeline_ms: float | None = None
    commit_ms: float | None = None
    post_commit: PostCommitChannel | None = None

    def elapsed_ms(self) -> float:
        return (time.perf_counter() - self.start) * 1000

    def error_kind(self, error: BaseException) -> str:
        if isinstance(error, asyncio.CancelledError):
            return _ERROR_KIND_CANCELLED
        if isinstance(error, PostCommitError):
            # Raised by the drain of an inner execution that owned its own
            # unit of work: the failure is not this execution's business logic.
            return _ERROR_KIND_POST_COMMIT
        return self.phase


class RuntimeExecutor:
    """Executes UseCases from their compiled ExecutionPlan without reflection.

    Receives a fully constructed UseCase instance and drives execution
    through the fixed pipeline: bind params → build command → load entities
    → apply computes → check rules → call execute.

    Owns the execution lifecycle around that pipeline.  When a
    ``coordinator`` is provided, each top-level execution that is not
    read-only opens an :class:`~loom.core.uow.scope.InvocationScope` and
    ends it through that coordinator: commit on success, rollback on any exception, cancellation
    included.  A nested execution in the task that owns the scope pushes its
    frame and never commits; one from another task, or into a closed scope,
    raises :class:`~loom.core.uow.scope.ScopeOwnershipError`.  Post-commit
    actions (job dispatches, cache invalidations) are held by the store that
    owns them and, once the scope has closed, run on the
    :class:`~loom.core.engine.post_commit.PostCommitChannel` of the execution
    that owns the scope as that store's outcome allows (or on the outermost
    execution's channel when there is no scope).

    An execution given an ``identity`` installs it as the ambient
    :func:`~loom.core.identity.current_identity` until it ends, drain
    included, so row-level security and the jobs it dispatches see the
    identity ``Caller()`` receives.  A nested execution that joins the
    invocation scope must run as the identity the scope was opened with, and
    none may start while its parent holds an elevation.

    No signature inspection occurs at runtime. All structural information
    comes from the cached ExecutionPlan produced by UseCaseCompiler.

    Emits ``RuntimeEvent`` objects to the optional ``MetricsAdapter``:
    ``EXEC_START`` before the unit of work opens and exactly one terminal
    event, ``EXEC_DONE`` once the unit of work has committed and closed or
    ``EXEC_ERROR`` with the ``error_kind`` of the step that failed.
    Enriches log calls with structured fields (``usecase``,
    ``duration_ms``, ``status``) for structured log consumers.

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
        agent_resolver: Optional callable resolving an ``Agent()`` marker
            parameter to a handle, given the agent's deployment name and the
            verified caller of this execution. ``None`` until
            :meth:`bind_agent_resolver` is called — see that method for why
            it is bound after construction rather than passed in here.
        mcp_resolver: Optional callable resolving an ``Mcp()`` marker
            parameter to a handle, given the marker's server name, its own
            declared ``include`` and the verified caller of this execution.
            ``None`` until :meth:`bind_mcp_resolver` is called, for the same
            reason ``agent_resolver`` is bound after construction.

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
        agent_resolver: Callable[[str, Identity], Any] | None = None,
        mcp_resolver: Callable[[str, tuple[str, ...], Identity], Any] | None = None,
        elevation_sink: ElevationSink | None = None,
    ) -> None:
        self._compiler = compiler
        self._coordinator = coordinator
        self._elevation_sink = elevation_sink
        self._debug = debug_execution
        self._logger = logger or get_logger(__name__)
        self._metrics = metrics
        self._repo_resolver = repo_resolver
        self._agent_resolver = agent_resolver
        self._mcp_resolver = mcp_resolver

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

        Args:
            resolver: Builds one agent handle from the agent's deployment
                name and the verified caller of one execution.

        Raises:
            RuntimeError: If a resolver is already bound.
        """
        if self._agent_resolver is not None:
            raise RuntimeError("RuntimeExecutor.bind_agent_resolver() called more than once")
        self._agent_resolver = resolver

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
        self._mcp_resolver = resolver

    async def run(
        self,
        use_case_type: type[Compilable],
        *,
        factory: UseCaseFactory,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        dependencies: dict[type[Any], Any] | None = None,
        load_overrides: dict[type[Any], Any] | None = None,
        read_only: bool = False,
        identity: Identity | None = None,
    ) -> Any:
        """Build ``use_case_type`` through ``factory`` and execute it.

        Args:
            use_case_type: Compiled use case or job class to build and run.
            factory: Factory that constructs the instance with its dependencies.
            params: Primitive parameter values keyed by name.
            payload: Raw dict for command construction via ``Input()``.
            dependencies: Mapping of entity type to repository for the load steps.
            load_overrides: Pre-loaded entities by type, bypassing repo calls.
            read_only: When ``True``, no unit of work is opened.
            identity: Verified caller for this execution.

        Returns:
            The result produced by ``execute()``.
        """
        instance = factory.build(use_case_type)
        return await self.execute(
            instance,
            params=params,
            payload=payload,
            dependencies=dependencies,
            load_overrides=load_overrides,
            read_only=read_only,
            identity=identity,
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
        :class:`~loom.core.engine.post_commit.PostCommitError` and the
        transaction stays committed.

        Args:
            compilable: Constructed instance to execute.
            params: Primitive parameter values keyed by name.
            payload: Raw dict for command construction via ``Input()``.
            dependencies: Mapping of entity type to repository, used for
                ``LoadById()`` / ``Load()`` / ``Exists()`` steps.
            load_overrides: Pre-loaded entities by type, bypassing repo
                calls. Used by test harnesses.
            read_only: When ``True``, opens no invocation scope and joins
                none, even if a ``uow_factory`` was provided.  Its reads
                join the stores the enclosing invocation enlisted only in
                the task that owns it; anywhere else (a ``gather`` child)
                they run with a session per call.
                Automatically set to ``True`` by the HTTP layer for GET
                routes.  Also honoured when ``plan.read_only`` is ``True``.
            identity: Verified caller for this execution, supplied by the
                transport.  Required when the plan declares a ``Caller()``
                parameter; pass
                :data:`~loom.core.identity.identity.ANONYMOUS` explicitly to
                run a declared-identity use case without a caller.

        Returns:
            The result produced by ``execute()``.

        Raises:
            loom.core.errors.RuleViolations: If one or more rule steps fail.
            NotFound: If a Load step finds no entity in the repository.
            Unauthenticated: If the plan declares ``Caller()`` and no identity
                was supplied.
            loom.core.engine.post_commit.PostCommitError: If a post-commit
                action failed after the unit of work committed.
            loom.core.uow.scope.ScopeOwnershipError: If a scope is published
                in this context but this task does not own it, or it is
                already closed.
            loom.core.uow.scope.StoresNotCommitted: If two or more stores
                of the invocation did not end the same way.
            loom.core.uow.scope.NestedIdentityMismatch: If this execution
                joins the invocation scope of this task as an identity other
                than the one the scope was opened with.
            loom.core.uow.scope.NestedElevation: If the execution running
                in this context has elevated a scope.
        """
        plan = self._plan_for(type(compilable))
        inputs = _ExecutionInputs(
            params=params,
            payload=payload,
            dependencies=dependencies,
            load_overrides=load_overrides,
            identity=identity,
        )
        scope = current_scope()
        token = None if identity is None else set_identity(identity)
        try:
            if read_only or plan.read_only:
                if scope is not None and scope.owned_here():
                    async with scope.frame(_READ_ONLY):
                        return await self._run_lifecycle(
                            plan, compilable, inputs, None, None, read_only=True
                        )
                with nullcontext() if scope is None else unscoped():
                    return await self._run_lifecycle(
                        plan, compilable, inputs, None, None, read_only=True
                    )
            owned = self._coordinator if scope is None else None
            return await self._run_lifecycle(plan, compilable, inputs, owned, scope)
        finally:
            if token is not None:
                reset_identity(token)

    def _plan_for(self, uc_type: type[Compilable]) -> ExecutionPlan:
        """Return the plan compiled for exactly ``uc_type``, compiling on demand.

        ``__execution_plan__`` is read from the class's own namespace, not
        through the MRO: a subclass must never run under its parent's plan.
        """
        plan = uc_type.__dict__.get("__execution_plan__")
        if isinstance(plan, ExecutionPlan):
            return plan
        return self._compiler.compile(uc_type)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _run_lifecycle(
        self,
        plan: ExecutionPlan,
        compilable: Compilable,
        inputs: _ExecutionInputs,
        owned: Coordinator | None,
        enclosing: InvocationScope | None,
        *,
        read_only: bool = False,
    ) -> Any:
        """Run one execution: start event, invocation scope, one terminal event, drain.

        Args:
            plan: The compiled plan of ``compilable``.
            compilable: The instance to execute.
            inputs: What the caller passed.
            owned: The coordinator of the scope this execution opens, if it
                opens one.
            enclosing: The scope already published in this context, which this
                execution joins, if any.
            read_only: Whether the execution writes nothing; it may not elevate.
        """
        state = self._begin_execution(plan.use_case_type.__qualname__)
        channel = PostCommitChannel() if owned is not None or active_channel() is None else None
        channel_token = bind_channel(channel) if channel is not None else None
        try:
            _refuse_nesting(state.use_case_name)
            # The elevation frame wraps the coordinator's flush and commit too:
            # the session-settings provider reads ``elevated_scopes()`` when a
            # transaction begins, which may be at that flush.
            async with elevation_scope(
                owns_transaction=owned is not None,
                sink=self._elevation_sink,
                read_only=read_only,
            ):
                if owned is None:
                    result = await self._run_in_frame(enclosing, state, plan, compilable, inputs)
                else:
                    result = await self._run_in_invocation(owned, state, plan, compilable, inputs)
        except BaseException as exc:
            # ``BaseException``: a cancellation is a terminal outcome too,
            # accounted for and re-raised.  ``state.post_commit`` holds only
            # the actions of stores whose writes may be durable, so they run
            # even though the invocation failed (another store did not commit,
            # or resetting the scope's token raised).  Without it, nothing
            # durable happened: discard.
            #
            # Unbound here, before either branch, not left to the trailing
            # ``finally``: a drain must never run with its own channel still
            # bound (module docstring) — a nested execution started from an
            # action would otherwise see this channel as ``active_channel()``,
            # get no channel of its own, and enqueue onto one already
            # mid-drain, silently lost.  ``channel_token`` is nulled so the
            # ``finally`` below does not unbind a second time.
            self._unbind(channel_token)
            channel_token = None
            if state.post_commit is not None:
                await self._drain_committed_channel_logging_failure(state.post_commit)
            else:
                self._discard(channel)
            self._handle_failure(state, exc)
            raise
        finally:
            self._unbind(channel_token)
        try:
            self._handle_success(state)
        finally:
            # In a ``finally``: a failing metrics adapter must not skip the
            # actions of a committed invocation.
            settled = state.post_commit if state.post_commit is not None else channel
            if settled is not None:
                await settled.drain(committed=owned is not None)
        return result

    async def _run_in_invocation(
        self,
        coordinator: Coordinator,
        state: _ExecutionState,
        plan: ExecutionPlan,
        compilable: Compilable,
        inputs: _ExecutionInputs,
    ) -> Any:
        """Open an invocation scope, run the execution in it, end it, raise what it ended with.

        The scope is unpublished before this returns, so the drain that
        follows (an inline job dispatch among its actions) opens its own.
        """
        result: Any = None
        error: BaseException | None = None
        async with invocation(coordinator) as scope:
            scope.post_commit = active_channel()
            try:
                result = await self._run_in_frame(scope, state, plan, compilable, inputs)
            except BaseException as exc:  # NOSONAR S5754 - raised below, once the scope ended
                error = exc
            try:
                outcome = await self._finish(scope, error, state)
            finally:
                # ``finish`` records the outcome before it raises an
                # interruption caught while ending a store (a cancelled close).
                state.post_commit = PostCommitChannel()
                scope.settle_post_commit(state.post_commit, error)
        failure = invocation_error(outcome, error or scope.poisoned)
        if failure is not None:
            raise failure
        return result

    async def _run_in_frame(
        self,
        scope: InvocationScope | None,
        state: _ExecutionState,
        plan: ExecutionPlan,
        compilable: Compilable,
        inputs: _ExecutionInputs,
    ) -> Any:
        """Push this execution's frame on ``scope``, if any, and run the pipeline under it.

        Raises:
            JoinedAutocommitStore: When a ``Joined`` store cannot roll back.
        """
        frame = nullcontext() if scope is None else scope.frame(Frame(plan.primary, plan.joined))
        async with frame:
            if scope is not None:
                _refuse_autocommit_joined(scope, plan)
            return await self._run_pipeline(state, plan, compilable, inputs)

    @staticmethod
    async def _finish(
        scope: InvocationScope, error: BaseException | None, state: _ExecutionState
    ) -> InvocationOutcome:
        """End every participant of ``scope``; from the flush on, the phase is the commit."""
        if error is not None:
            return await scope.coordinator.finish(scope, error)
        state.phase = _ERROR_KIND_COMMIT
        started = time.perf_counter()
        try:
            return await scope.coordinator.finish(scope, None)
        finally:
            state.commit_ms = (time.perf_counter() - started) * 1000

    @staticmethod
    def _discard(channel: PostCommitChannel | None) -> None:
        if channel is not None:
            channel.discard()

    async def _drain_committed_channel_logging_failure(self, channel: PostCommitChannel) -> None:
        """Drain the actions of the stores that may have written, despite the exception.

        Called from an exception handler that is already about to re-raise
        the failure that brought it here: another store that did not commit
        (``StoresNotCommitted``), or a teardown error.  Letting some other
        exception from the drain
        replace that exception would hide the reason this path was reached
        at all, so any ordinary failure — a ``PostCommitError`` from a
        failed action, or any other ``Exception`` a misbehaving action
        raises directly — is logged instead.  A cancellation is not caught
        here: it is a terminal outcome in its own right, not a failure to
        log past.
        """
        try:
            await channel.drain(committed=True)
        except Exception:
            self._logger.exception("PostCommitDrainFailedDuringTeardown")

    @staticmethod
    def _unbind(token: Token[PostCommitChannel | None] | None) -> None:
        if token is not None:
            reset_channel(token)

    # ------------------------------------------------------------------
    # Pipeline
    # ------------------------------------------------------------------

    async def _run_pipeline(
        self,
        state: _ExecutionState,
        plan: ExecutionPlan,
        compilable: Compilable,
        inputs: _ExecutionInputs,
    ) -> Any:
        """Run the compiled pipeline and record its duration; emits no event."""
        state.phase = _ERROR_KIND_BUSINESS
        started = time.perf_counter()
        try:
            return await self._run_core_pipeline(plan, compilable, inputs)
        finally:
            state.pipeline_ms = (time.perf_counter() - started) * 1000

    def _begin_execution(self, use_case_name: str) -> _ExecutionState:
        trace_id = get_trace_id()
        logger = self._logger.bind(trace_id=trace_id) if trace_id else self._logger
        state = _ExecutionState(use_case_name=use_case_name, trace_id=trace_id, logger=logger)
        logger.info(f"{_LOG_EXEC} {use_case_name}", usecase=use_case_name)
        self._emit(
            RuntimeEvent(
                kind=EventKind.EXEC_START,
                use_case_name=use_case_name,
                trace_id=trace_id,
            )
        )
        return state

    async def _run_core_pipeline(
        self,
        plan: ExecutionPlan,
        compilable: Compilable,
        inputs: _ExecutionInputs,
    ) -> Any:
        bound: dict[str, Any] = {}
        self._check_query(plan, inputs.params or {})
        self._bind_params(plan, inputs.params or {}, bound)
        self._bind_caller(plan, inputs.identity, bound)
        self._bind_agents(plan, inputs.identity, bound)
        self._bind_mcp(plan, inputs.identity, bound)
        fields_set = self._build_command(plan, inputs.payload, bound)
        await self._execute_loads(
            plan, compilable, bound, inputs.dependencies, inputs.load_overrides
        )
        await self._execute_exists(plan, compilable, bound, inputs.dependencies)
        self._apply_computes(plan, bound, fields_set)
        self._check_rules(plan, bound, fields_set)
        return await self._invoke_execute(compilable, bound)

    async def _invoke_execute(
        self,
        compilable: Compilable,
        bound: dict[str, Any],
    ) -> Any:
        self._log_step("Execute core logic")
        execute_fn = compilable.execute
        if inspect.iscoroutinefunction(execute_fn):
            return await execute_fn(**bound)
        return execute_fn(**bound)

    def _handle_failure(self, state: _ExecutionState, error: BaseException) -> None:
        elapsed_ms = state.elapsed_ms()
        error_kind = state.error_kind(error)
        status = _STATUS_RULE_FAILURE if isinstance(error, RuleViolations) else _STATUS_FAILURE
        self._log_failure(state, error, status, elapsed_ms, error_kind)
        self._emit(
            RuntimeEvent(
                kind=EventKind.EXEC_ERROR,
                use_case_name=state.use_case_name,
                duration_ms=elapsed_ms,
                status=status,
                error=error,
                trace_id=state.trace_id,
                error_kind=error_kind,
                pipeline_ms=state.pipeline_ms,
                commit_ms=state.commit_ms,
            )
        )

    @staticmethod
    def _log_failure(
        state: _ExecutionState,
        error: BaseException,
        status: str,
        elapsed_ms: float,
        error_kind: str,
    ) -> None:
        message = f"{_LOG_FAIL} {state.use_case_name}"
        if status == _STATUS_RULE_FAILURE:
            state.logger.warning(
                message,
                usecase=state.use_case_name,
                duration_ms=elapsed_ms,
                status=status,
                error_kind=error_kind,
            )
            return
        state.logger.error(
            message,
            usecase=state.use_case_name,
            duration_ms=elapsed_ms,
            status=status,
            error_kind=error_kind,
            error=str(error),
        )

    def _handle_success(self, state: _ExecutionState) -> None:
        elapsed_ms = state.elapsed_ms()
        state.logger.info(
            f"{_LOG_DONE} {elapsed_ms:.1f}ms",
            usecase=state.use_case_name,
            duration_ms=elapsed_ms,
            status=_STATUS_SUCCESS,
        )
        self._emit(
            RuntimeEvent(
                kind=EventKind.EXEC_DONE,
                use_case_name=state.use_case_name,
                duration_ms=elapsed_ms,
                status=_STATUS_SUCCESS,
                trace_id=state.trace_id,
                pipeline_ms=state.pipeline_ms,
                commit_ms=state.commit_ms,
            )
        )

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    def _bind_params(
        self,
        plan: ExecutionPlan,
        params: dict[str, Any],
        bound: dict[str, Any],
    ) -> None:
        for pb in plan.param_bindings:
            if pb.name not in params:
                raise ValueError(
                    f"{plan.use_case_type.__qualname__}: missing required parameter '{pb.name}'"
                )
            raw = params[pb.name]
            bound[pb.name] = self._coerce_param(
                param_name=pb.name,
                annotation=pb.annotation,
                raw=raw,
                use_case_name=plan.use_case_type.__qualname__,
            )

    @staticmethod
    def _check_query(plan: ExecutionPlan, params: dict[str, Any]) -> None:
        """Refuse a ``Query()`` value that is not an instance of its type.

        A generic coercion would call the type with the raw value: a struct
        whose fields are positional would take a dict as its first field. The
        message names the type received, never the value.
        """
        binding = plan.query_binding
        if binding is None or binding.name not in params:
            return
        raw = params[binding.name]
        if not isinstance(raw, binding.query_type):
            raise ParameterBindingError(
                f"{plan.use_case_type.__qualname__}: parameter '{binding.name}' takes a "
                f"{binding.query_type.__name__} instance, got {type(raw).__name__}"
            )

    @staticmethod
    def _bind_caller(
        plan: ExecutionPlan,
        identity: Identity | None,
        bound: dict[str, Any],
    ) -> None:
        """Inject the declared caller identity, refusing to invent one.

        Substituting the anonymous identity for a missing one would turn a
        transport that forgot to propagate the caller into a silently
        unauthenticated execution.  The transport must say ``ANONYMOUS``
        explicitly for that to happen.
        """
        binding = plan.caller_binding
        if binding is None:
            return
        if identity is None:
            raise Unauthenticated(
                f"{plan.use_case_type.__qualname__}.execute declares "
                f"'{binding.name}: Identity = Caller()' but this execution carried no "
                "identity. The transport must pass identity=... to the executor "
                "(pass ANONYMOUS explicitly to run without a caller)."
            )
        bound[binding.name] = identity

    def _bind_agents(
        self,
        plan: ExecutionPlan,
        identity: Identity | None,
        bound: dict[str, Any],
    ) -> None:
        """Inject one handle per ``Agent()`` marker, closed over this caller.

        Requires an explicit identity for the same reason ``_bind_caller``
        does: substituting ``ANONYMOUS`` for a missing one would turn a
        transport bug into a silently unauthenticated agent call. An
        explicitly anonymous identity is still accepted here — the handle
        itself, not the executor, refuses it, and only once its ``run`` is
        actually invoked, so the refusal happens right before the model call
        rather than at bind time (design: an anonymous caller must be
        refused before touching the network, not before it is even known
        whether this execution will call ``run`` at all).
        """
        if not plan.agent_bindings:
            return
        if identity is None:
            raise Unauthenticated(
                f"{plan.use_case_type.__qualname__}.execute declares an Agent() "
                "parameter but this execution carried no identity. The transport "
                "must pass identity=... to the executor (pass ANONYMOUS explicitly "
                "to run without a caller)."
            )
        if self._agent_resolver is None:
            raise RuntimeError(
                f"{plan.use_case_type.__qualname__}.execute declares an Agent() "
                "parameter but no agent resolver is bound on this executor. Call "
                "RuntimeExecutor.bind_agent_resolver() once the AI runtime exists."
            )
        for binding in plan.agent_bindings:
            bound[binding.name] = self._agent_resolver(binding.agent, identity)

    def _bind_mcp(
        self,
        plan: ExecutionPlan,
        identity: Identity | None,
        bound: dict[str, Any],
    ) -> None:
        """Inject one handle per ``Mcp()`` marker, closed over this caller.

        Mirrors :meth:`_bind_agents`: an explicit identity is required for
        the same reason — substituting ``ANONYMOUS`` for a missing one would
        turn a transport bug into a silently unauthenticated tool call — and
        an unbound resolver fails loud, naming :meth:`bind_mcp_resolver`,
        instead of handing the raw marker object back to the use case, which
        would only fail later with an opaque ``AttributeError``.
        """
        if not plan.mcp_bindings:
            return
        if identity is None:
            raise Unauthenticated(
                f"{plan.use_case_type.__qualname__}.execute declares an Mcp() "
                "parameter but this execution carried no identity. The transport "
                "must pass identity=... to the executor (pass ANONYMOUS explicitly "
                "to run without a caller)."
            )
        if self._mcp_resolver is None:
            raise RuntimeError(
                f"{plan.use_case_type.__qualname__}.execute declares an Mcp() "
                "parameter but no MCP resolver is bound on this executor. Call "
                "RuntimeExecutor.bind_mcp_resolver() once the AI runtime exists."
            )
        for binding in plan.mcp_bindings:
            bound[binding.name] = self._mcp_resolver(binding.server, binding.include, identity)

    @staticmethod
    def _coerce_param(
        *,
        param_name: str,
        annotation: Any,
        raw: Any,
        use_case_name: str,
    ) -> Any:
        if annotation is Any:
            return raw
        if not isinstance(annotation, type):
            return raw
        if isinstance(raw, annotation):
            return raw
        try:
            return annotation(raw)
        except (TypeError, ValueError) as exc:
            raise ParameterBindingError(
                f"{use_case_name}: invalid value for parameter '{param_name}': "
                f"expected {annotation.__name__}, got {type(raw).__name__} ({raw!r})"
            ) from exc

    def _build_command(
        self,
        plan: ExecutionPlan,
        payload: dict[str, Any] | None,
        bound: dict[str, Any],
    ) -> frozenset[str]:
        if plan.input_binding is None:
            return frozenset()

        if payload is None:
            raise ValueError(
                f"{plan.use_case_type.__qualname__}: payload is required for a UseCase with Input()"
            )

        cmd_type = plan.input_binding.command_type
        if hasattr(cmd_type, "from_payload"):
            command, fields_set = cmd_type.from_payload(payload)
        else:
            raise TypeError(
                f"{plan.use_case_type.__qualname__}: "
                f"command type {cmd_type!r} must implement from_payload()"
            )
        bound[plan.input_binding.name] = command
        self._log_step("Bind Input")
        return cast(frozenset[str], fields_set)

    async def _execute_loads(
        self,
        plan: ExecutionPlan,
        compilable: Compilable,
        bound: dict[str, Any],
        dependencies: dict[type[Any], Any] | None,
        load_overrides: dict[type[Any], Any] | None,
    ) -> None:
        for ls in plan.load_steps:
            bound[ls.name] = await self._resolve_load(
                ls, plan, compilable, bound, dependencies, load_overrides
            )
            self._log_step(f"Load {ls.entity_type.__name__}")

    async def _execute_exists(
        self,
        plan: ExecutionPlan,
        compilable: Compilable,
        bound: dict[str, Any],
        dependencies: dict[type[Any], Any] | None,
    ) -> None:
        for es in plan.exists_steps:
            bound[es.name] = await self._resolve_exists(es, plan, compilable, bound, dependencies)
            self._log_step(f"Exists {es.entity_type.__name__}")

    def _apply_computes(
        self,
        plan: ExecutionPlan,
        bound: dict[str, Any],
        fields_set: frozenset[str],
    ) -> None:
        if plan.input_binding is None or not plan.compute_steps:
            return

        command = bound[plan.input_binding.name]
        for cs in plan.compute_steps:
            compute_fn = cast(Any, cs.fn)
            label = getattr(cs.fn, "__name__", type(cs.fn).__name__)
            if cs.accepts_context:
                command = compute_fn(command, fields_set, bound)
            else:
                command = compute_fn(command, fields_set)
            self._log_step(f"Compute {label}")

        bound[plan.input_binding.name] = command

    def _check_rules(
        self,
        plan: ExecutionPlan,
        bound: dict[str, Any],
        fields_set: frozenset[str],
    ) -> None:
        if plan.input_binding is None or not plan.rule_steps:
            return

        command = bound[plan.input_binding.name]
        violations: list[RuleViolation] = []

        for rs in plan.rule_steps:
            label = getattr(rs.fn, "__name__", type(rs.fn).__name__)
            try:
                if rs.accepts_context:
                    rs.fn(command, fields_set, bound)
                else:
                    rs.fn(command, fields_set)
                self._log_step(f"Rule {label}")
            except RuleViolation as exc:
                violations.append(exc)
                self._logger.warning(
                    f"[RULE] {label} failed: {exc.field}: {exc.message}",
                    usecase=plan.use_case_type.__qualname__,
                    field=exc.field,
                )

        if violations:
            raise RuleViolations(violations)

    # ------------------------------------------------------------------
    # Load resolution
    # ------------------------------------------------------------------

    async def _resolve_load(
        self,
        step: LoadStep,
        plan: ExecutionPlan,
        _compilable: Compilable,
        bound: dict[str, Any],
        dependencies: dict[type[Any], Any] | None,
        load_overrides: dict[type[Any], Any] | None,
    ) -> Any:
        if load_overrides and step.entity_type in load_overrides:
            return load_overrides[step.entity_type]

        if dependencies is None:
            repo = self._resolve_repo(step.entity_type)
            if repo is None and self._repo_resolver is None:
                raise RuntimeError(
                    f"No dependencies provided for LoadById({step.entity_type.__name__})"
                )
        else:
            repo = dependencies.get(step.entity_type)
            if repo is None:
                repo = self._resolve_repo(step.entity_type)
        if repo is None:
            raise RuntimeError(
                f"No repository registered for entity type '{step.entity_type.__name__}'"
            )

        value = self._resolve_lookup_value(step.source_kind, step.source_name, plan, bound)
        entity = await self._get_entity(repo, step, value)

        if entity is None:
            if step.on_missing is OnMissing.RETURN_NONE:
                return None
            if step.on_missing is OnMissing.RETURN_FALSE:
                return False
            raise NotFound(step.entity_type.__name__, id=value)

        return entity

    async def _resolve_exists(
        self,
        step: ExistsStep,
        plan: ExecutionPlan,
        _compilable: Compilable,
        bound: dict[str, Any],
        dependencies: dict[type[Any], Any] | None,
    ) -> bool:
        if dependencies is None:
            repo = self._resolve_repo(step.entity_type)
            if repo is None and self._repo_resolver is None:
                raise RuntimeError(
                    f"No dependencies provided for Exists({step.entity_type.__name__})"
                )
        else:
            repo = dependencies.get(step.entity_type)
            if repo is None:
                repo = self._resolve_repo(step.entity_type)
        if repo is None:
            raise RuntimeError(
                f"No repository registered for entity type '{step.entity_type.__name__}'"
            )

        value = self._resolve_lookup_value(step.source_kind, step.source_name, plan, bound)
        exists_by = getattr(repo, "exists_by", None)
        if not callable(exists_by):
            raise RuntimeError(
                "Repository for "
                f"'{step.entity_type.__name__}' must implement exists_by(field, value)"
            )

        exists_by_async = cast(Callable[[str, Any], Awaitable[bool]], exists_by)
        found = bool(await exists_by_async(step.against, value))
        if found:
            return True

        if step.on_missing is OnMissing.RAISE:
            raise NotFound(step.entity_type.__name__, id=value)
        return False

    async def _get_entity(self, repo: Any, step: LoadStep, value: Any) -> Any | None:
        if step.lookup_kind is LookupKind.BY_ID:
            return await repo.get_by_id(value, profile=step.profile)

        get_by = getattr(repo, "get_by", None)
        if not callable(get_by):
            raise RuntimeError(
                f"Repository for '{step.entity_type.__name__}' must implement get_by(field, value)"
            )
        get_by_async = cast(Callable[..., Awaitable[Any | None]], get_by)
        return await get_by_async(step.against, value, profile=step.profile)

    @staticmethod
    def _resolve_lookup_value(
        source_kind: SourceKind,
        source_name: str,
        plan: ExecutionPlan,
        bound: dict[str, Any],
    ) -> Any:
        if source_kind is SourceKind.PARAM:
            return bound[source_name]

        if plan.input_binding is None:
            raise RuntimeError("from_command source requires Input() binding")
        command = bound[plan.input_binding.name]
        if not hasattr(command, source_name):
            raise RuntimeError(f"Command '{type(command).__name__}' has no field '{source_name}'")
        return getattr(command, source_name)

    def _resolve_repo(self, entity_type: type[Any]) -> Any | None:
        if self._repo_resolver is None:
            return None
        return self._repo_resolver(entity_type)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _emit(self, event: RuntimeEvent) -> None:
        if self._metrics is not None:
            self._metrics.on_event(event)

    def _log_step(self, label: str) -> None:
        if self._debug:
            self._logger.info(f"[STEP] {label}")


def _refuse_autocommit_joined(scope: InvocationScope, plan: ExecutionPlan) -> None:
    """Refuse a ``Joined`` store of ``plan`` whose enlisted unit of work cannot roll back.

    What composition refuses at startup for the components it discovers,
    for any plan, compiled then or on its first execution.
    """
    for key in sorted(plan.joined):
        if not scope.participants[key].transactional:
            issue = UseCaseErrorCode.JOINED_AUTOCOMMIT_STORE.issue(
                component=plan.use_case_type, store=key
            )
            raise JoinedAutocommitStore(issue)


def _refuse_nesting(use_case: str) -> None:
    """Refuse ``use_case`` inside an elevated execution, or joining this task's scope as another.

    Its identity is compared with the scope's identity, never the ambient one a parent set.
    """
    scopes = elevated_scopes()
    if scopes:
        raise NestedElevation.opening(use_case, scopes)
    scope = current_scope()
    if scope is not None and scope.owned_here() and current_identity() != scope.identity:
        raise NestedIdentityMismatch.joining(use_case)
