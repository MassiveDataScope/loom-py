"""Neutral runtime contracts of the AI pillar.

Everything the compiler, the runtime and the HTTP layer share with an engine
lives here, and nothing here imports an engine: the bootstrap resolves the
provider through :mod:`loom.ai.registry` and hands the compiler plain values.

A run may continue a conversation the application loaded: the prior
history crosses this boundary as opaque, engine-native bytes inside
:class:`Conversation`, and the run's new messages come back the same way on
:attr:`AgentResult.messages` and :attr:`FinalEvent.messages`.  Loom defines no
message model and stores no history.

These contracts are experimental and may change within a major line; the
artifact format they serve is not.  See :mod:`loom.ai` for the distinction.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import (
    Any,
    ClassVar,
    Final,
    Generic,
    Protocol,
    TypeAlias,
    TypeVar,
    cast,
    overload,
    runtime_checkable,
)

import msgspec

from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.errors.model import BoundaryValidationError
from loom.core.identity import Identity
from loom.core.model import LoomFrozenStruct, LoomType

CONVERSATION_ID_MAX_LENGTH: Final[int] = 128
"""Longest ``conversation_id`` a run accepts; the value itself is opaque."""

AnswerT = TypeVar("AnswerT")
"""Output type of an :class:`AgentHandle`, carried by the annotation on the
use-case parameter it fills — never by the marker that fills it."""

ExpectedT = TypeVar("ExpectedT")
"""Per-run output type passed to :meth:`AgentHandle.run` through ``expect``,
independent of the handle's own :data:`AnswerT`."""

ToolResultT = TypeVar("ToolResultT")
"""Decoded type of a single :meth:`McpHandle.call`."""


class StateShape(LoomFrozenStruct, frozen=True, kw_only=True):
    """The one shape ``deps_type``/``deps_schema`` compile to.

    ``deps_schema``, ``deps_type: <symbol>`` and ``deps_type: dict`` are three
    authored spellings of one optional JSON Schema; the compiler resolves all
    three to this value, never to one of three interchangeable objects. Same
    idiom as its sibling :class:`~loom.ai.compiler._plan.CompiledOutput`,
    including a :class:`~loom.core.model.LoomType` field msgspec cannot
    itself encode, under the invariant :mod:`loom.ai.compiler._plan`
    documents.

    Attributes:
        schema: JSON Schema the artifact's state must satisfy, or ``None``
            under the ``deps_type: dict`` waiver, where no schema exists and
            template markers are not validated.
        loom_type: Boundary type produced at compile, decoding and
            normalising the state mapping, or ``None`` alongside
            ``schema is None``.
        defaults: The state a run supplying none renders: :attr:`loom_type`'s
            decode of an empty object, computed when the shape is built;
            ``None`` under the open form or when a field has no default.
    """

    schema: Mapping[str, Any] | None
    loom_type: LoomType | None
    defaults: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.loom_type is None:
            return
        try:
            defaults = self.decode(b"{}")
        except BoundaryValidationError:
            return
        msgspec.structs.force_setattr(self, "defaults", MappingProxyType(dict(defaults)))

    def resolve(self, agent: str, state: object | None) -> Mapping[str, Any] | None:
        """Return the state one run of *agent* renders: *state*, else :attr:`defaults`.

        Args:
            agent: The agent the run targets, named in a raised error.
            state: The caller's state, already decoded and normalised, or ``None``.

        Returns:
            *state* when given, else :attr:`defaults`.

        Raises:
            AgentRunError: ``STATE_REQUIRED`` when *state* is ``None`` and a
                field of :attr:`loom_type` has no default.
        """
        if state is not None:
            return cast(Mapping[str, Any], state)
        if self.defaults is None and self.loom_type is not None:
            raise AgentRunError(
                AgentRunErrorCode.STATE_REQUIRED,
                f"agent {agent!r} declares a state field with no default; "
                "pass 'state' explicitly for this run",
            )
        return self.defaults

    def decode(self, body: bytes) -> Mapping[str, Any]:
        """Decode JSON state bytes against this shape and return them as builtins.

        Args:
            body: The state as JSON bytes.

        Returns:
            The normalised mapping: :attr:`loom_type`'s decode turned to
            builtins, or the plain JSON value under the open form.

        Raises:
            BoundaryValidationError: When *body* does not fit :attr:`loom_type`.
            msgspec.DecodeError: When *body* is not JSON under the open form.
        """
        if self.loom_type is None:
            return cast(Mapping[str, Any], msgspec.json.decode(body))
        return cast(Mapping[str, Any], self.loom_type.to_builtins(self.loom_type.decode_json(body)))


class AgentUsage(LoomFrozenStruct, frozen=True, kw_only=True):
    """Resource accounting of one agent run.

    Nothing the engine reported is dropped. The counters any engine would
    plausibly report are named fields; every other field it returned — the
    audio counters, a provider's own extras, a field a future engine release
    adds — rides verbatim in ``details``, so a new counter reaches the caller
    without a change here. The engine's own usage type never crosses this
    boundary: a second engine fills this struct.

    Attributes:
        input_tokens: Tokens sent to the model across the run, cached ones
            included.
        output_tokens: Tokens produced by the model across the run.
        requests: Model requests issued during the run.
        duration_ms: Wall-clock duration of the run in milliseconds.
        cache_read_tokens: Input tokens served from the provider's prompt
            cache, already counted in ``input_tokens``. A cached token costs a
            fraction of a fresh one, so comparing models on ``input_tokens``
            alone can invert the ranking.
        cache_write_tokens: Input tokens written to the prompt cache, already
            counted in ``input_tokens``.
        tool_calls: Tool invocations the model completed during the run.
        cost: Run cost in the engine's currency, or ``None`` when the engine
            could not price the model. Absent rather than zero: a zero would
            silently win a cost comparison.
        details: Every field the engine reported that has no named field
            here, under the engine's own names. Not disjoint from the named
            counters: a provider that reports its own ``cached_tokens``
            alongside the normalised ``cache_read_tokens`` has both, so
            summing ``details`` double-counts.
    """

    input_tokens: int
    output_tokens: int
    requests: int
    duration_ms: int
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    tool_calls: int = 0
    cost: Decimal | None = None
    details: Mapping[str, int | float] = {}

    @property
    def total_tokens(self) -> int:
        """Return the input plus output tokens of the run."""
        return self.input_tokens + self.output_tokens

    @property
    def cache_hit_ratio(self) -> float:
        """Return the fraction of input tokens served from the prompt cache.

        Zero when the run reported no input tokens.
        """
        if self.input_tokens == 0:
            return 0.0
        return self.cache_read_tokens / self.input_tokens


class Attachment(LoomFrozenStruct, frozen=True, kw_only=True):
    """One binary part of a :data:`Prompt`, alongside its media type.

    Attributes:
        media_type: IANA media type of ``data``, e.g. ``"image/png"``.
        data: Raw bytes of the attachment.
    """

    media_type: str
    data: bytes


Prompt: TypeAlias = str | Sequence[str | Attachment]
"""A run's prompt: plain text, or an ordered mix of text and attachments."""


class Conversation(LoomFrozenStruct, frozen=True, kw_only=True):
    """The conversation a run continues.

    Attributes:
        conversation_id: The application's identifier of the conversation;
            opaque to loom.
        history: Prior turns in the engine's own serialised form, opaque to
            loom; ``None`` on the first turn.
    """

    conversation_id: str
    history: bytes | None = None


class AgentResult(LoomFrozenStruct, frozen=True, kw_only=True):
    """Outcome of a non-streaming agent run.

    Attributes:
        output: Answer already decoded and validated against the declared
            output shape.
        usage: Resource accounting of the run.
        interaction_id: Identifier the runtime minted for this run.
        hook_result: Return value of the ``on_output`` use case, when the plan
            declares one.
        messages: New messages of this run in the engine's own serialised
            form; ``None`` when they are longer than
            ``policies.max_history_bytes``.
        provider_details: What the provider reported about the final answer
            beyond the answer itself, verbatim and opaque to loom — a
            decision model's confidence and probabilities per field, another
            provider's finish reason. ``None`` when it reported nothing. In
            process only: the ``/run`` body and the ``final`` frame never
            carry it.
    """

    output: object
    usage: AgentUsage
    interaction_id: str | None = None
    hook_result: object | None = None
    messages: bytes | None = None
    provider_details: Mapping[str, Any] | None = None


class AgentAnswer(LoomFrozenStruct, Generic[AnswerT], frozen=True, kw_only=True):
    """Outcome of one run reached through an :class:`AgentHandle`.

    Carries this run's answer and this run's own accounting, and nothing
    else: ``usage`` is scoped to the single call that produced this answer
    and is never merged with the usage of another call the same handle made,
    or of the parent run that reached this agent in the first place. A use
    case that runs the same handle three times gets three independent
    ``AgentAnswer`` values with three independent ``usage`` fields; summing
    them, if a caller wants a total, is the caller's own arithmetic.

    Attributes:
        output: Decoded answer of this run — the artefact's declared output
            shape by default, or the type passed as ``expect`` when the run
            overrode it for this call only.
        usage: Resource accounting of this run only.
        interaction_id: Identifier the runtime minted for this run.
        provider_details: What the provider reported about this answer beyond
            the answer itself, verbatim and opaque to loom — a decision
            model's confidence and probabilities per field, another
            provider's finish reason — or ``None`` when it reported nothing.
    """

    output: AnswerT
    usage: AgentUsage
    interaction_id: str | None = None
    provider_details: Mapping[str, Any] | None = None


class McpHandle(Protocol):
    """A caller-bound, filtered view of one MCP server's tools.

    Two paths hand one out, and they differ in where the filter comes from.
    ``AgentHandle.mcp()`` returns the artefact's own grant: the very same
    composed view, over the same shared session and the same include/exclude
    predicate, that the model's own toolset runs over — nothing there is
    re-declared, so it cannot diverge. An ``Mcp()`` marker returns a view
    whose filter is the *signature's* own ``include``, which may be wider
    than any agent's. That is a second, independently declared filter, and
    the guarantee it carries is not that no wider view exists: it is that
    the widening is written down in the signature and verified against the
    server's real tool list at start-up.

    Both are views over one shared session, never a second connection, and
    both run under the same guard — authentication, timeout, span, expiry.
    Only the deadline's source differs: the agent path uses its plan's
    numbers, and the marker path, which belongs to no plan, uses the
    server's own ``timeout_ms``.
    """

    def tools(self) -> tuple[str, ...]:
        """Return the tool names visible through this grant's own filter.

        Returns:
            Tool names already narrowed by this view's own filter — the
            artefact's declared include/exclude when the handle came from
            an agent, the signature's own ``include`` when it came from an
            ``Mcp()`` marker.
        """
        ...

    async def call(
        self,
        tool: str,
        arguments: Mapping[str, Any],
        *,
        expect: type[ToolResultT],
    ) -> ToolResultT:
        """Call *tool* and decode its structured result into *expect*.

        Two methods exist here instead of one method with an optional
        ``expect`` — this one and :meth:`call_untyped` — because an optional
        argument would govern two different behaviours from one parameter,
        which this framework's rules on multi-behaviour flags forbid, and it
        would turn the unshaped path into an omission instead of a decision.
        Typed is what this method commits to; the unshaped path is the
        separately named, deliberate exception.

        Loom does not compare ``expect`` against the tool's published output
        schema — that comparison is JSON Schema subsumption, whose verdict
        would only be approximate. Decoding the structured result into
        ``expect`` is the check, and it is exact: a mismatch names the field
        and the type that did not fit.

        Args:
            tool: Tool name, as returned by :meth:`tools`.
            arguments: Arguments passed to the tool call.
            expect: Type the tool's structured result is decoded into. The
                tool must publish an output schema; one that does not is
                refused before any network call.

        Returns:
            The decoded result.

        Raises:
            AgentRunError: With a code naming why the call did not produce a
                decoded ``expect`` — the tool is outside this grant's filter,
                the tool publishes no output schema, the tool reported a
                failure, the server returned no structured content despite
                publishing a schema, the structured content did not decode
                into ``expect``, or — on the ``Mcp()`` marker path, under
                ``ai.remote_clients: optional`` — the server this grant names
                never connected at start-up (``TOOL_UNAVAILABLE``).
        """
        ...

    async def call_untyped(self, tool: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        """Call *tool* and return the server's own result, undecoded.

        The deliberate exception to :meth:`call`'s typed default — for a
        tool that never publishes an output schema, or for a caller who
        genuinely wants the server's own shape. ``Mapping[str, Any]`` is
        admissible here specifically because this signature has nowhere to
        put a type parameter; it is not a general-purpose escape hatch.

        Args:
            tool: Tool name, as returned by :meth:`tools`.
            arguments: Arguments passed to the tool call.

        Returns:
            The server's own structured result, unvalidated and undecoded.

        Raises:
            AgentRunError: With a code naming why the call failed — the tool
                is outside this grant's filter, the tool reported a failure,
                the server returned structured content that is not a
                mapping (a list, a scalar) — a contradiction of the protocol
                this method returns, distinct from returning no structured
                content at all, which comes back as ``{}`` — or, on the
                ``Mcp()`` marker path under ``ai.remote_clients: optional``,
                the server this grant names never connected at start-up
                (``TOOL_UNAVAILABLE``).
        """
        ...


class SqlGrantHandle(Protocol):
    """One artefact's own bounded view of one of its ``sql`` grants.

    Queries the same read-only connection under the same row and byte
    bounds, and the same plan timeout, that the artefact's own ``sql``
    capability enforces for the model — the granted view, not a second one a
    use case could widen.
    """

    async def query(
        self,
        statement: str,
        *,
        parameters: Mapping[str, Any] | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        """Run a read-only statement bounded by this grant's own limits.

        Args:
            statement: SQL statement to run against the granted connection.
            parameters: Server-side bound parameters, when the statement
                uses them.

        Returns:
            Result rows, each as a column-name-to-value mapping, truncated
            to this grant's row and byte bounds.

        Raises:
            AgentRunError: With a code naming why the query did not run —
                the connection is outside this artefact's grants, or the
                grant's own bounds rejected the result.
        """
        ...


class AgentHandle(Protocol[AnswerT]):
    """A named agent reached from another use case, bound to this run's caller.

    Filled by the executor when a use case declares one in its ``execute``
    signature through :func:`loom.core.use_case.markers.Agent`, and never
    constructed directly. The type argument this Protocol carries —
    ``AgentHandle[SeverityAssessment]`` on the parameter's annotation — is
    what the compiler checks against the named agent's own declared output
    at start-up; :func:`Agent` itself returns an untyped value, for the same
    reason every other marker in this vocabulary does.

    Exactly three arguments cross this boundary on a per-run basis: the
    prompt, the shape of the answer, and which conversation it continues.
    Nothing else does. What the agent may reach, what it may cost and which
    model serves it are decided once — by the artefact and by deployment
    configuration — and stay there; a per-run argument that changed any of
    them would be a second place the same policy could drift.
    """

    @overload
    async def run(
        self,
        prompt: Prompt,
        *,
        conversation_id: str | None = None,
        state: object | None = None,
    ) -> AgentAnswer[AnswerT]: ...

    @overload
    async def run(
        self,
        prompt: Prompt,
        *,
        expect: type[ExpectedT],
        conversation_id: str | None = None,
        state: object | None = None,
    ) -> AgentAnswer[ExpectedT]: ...

    async def run(
        self,
        prompt: Prompt,
        *,
        expect: type[ExpectedT] | None = None,
        conversation_id: str | None = None,
        state: object | None = None,
    ) -> AgentAnswer[AnswerT] | AgentAnswer[ExpectedT]:
        """Run the agent once and decode its answer.

        Without ``expect``, the answer is decoded into the artefact's own
        declared output shape — this handle's type argument. With
        ``expect``, that declared shape is a default rather than a ceiling:
        this run only is decoded into ``expect`` instead. The override
        applies to this call and nothing else; it is never merged with the
        artefact's declared shape and never carries over to the handle's
        next call.

        The artefact's own output check — the retry loop that asks the model
        to correct a violation of its declared schema — does not run when
        ``expect`` overrides the shape. Validating an overridden shape is
        the calling code's job.

        Permissions never travel through this call. What the agent may
        reach comes from the artefact's own grants and from the identity
        already bound to this handle; ``expect`` changes what comes back,
        never what the agent is allowed to do.

        Args:
            prompt: Prompt for this run: text, or text and attachments in
                order.
            expect: When given, decode this run's answer into this type
                instead of the artefact's declared output. Applies to this
                run only.
            conversation_id: Identifier of the conversation this run
                continues; ``None`` runs single-shot.
            state: This run's state, or ``None``. Reaches the artefact's
                dependency bundle normalised against its declared shape;
                given against an artefact declaring no ``deps_type`` or
                ``deps_schema``, the call is refused rather than dropped
                silently.

        Returns:
            The decoded answer, this run's own usage and its interaction id.

        Raises:
            AgentRunError: With ``AGENT_RUN_SHAPE_WITH_HOOK`` when ``expect``
                is given and the artefact's output hook command declares the
                output field — refused before the model is called, since the
                hook could not be handed an answer shaped by ``expect``; with
                ``STATE_UNDECLARED`` when ``state`` is given and the artefact
                declares no state shape.
        """
        ...

    async def run_text(
        self,
        prompt: Prompt,
        *,
        conversation_id: str | None = None,
        state: object | None = None,
    ) -> AgentAnswer[str]:
        """Run the agent for open prose, pinning this run's answer to ``str``.

        A named spelling of ``run(prompt, expect=str)`` rather than a third
        overload of it — the name states the form this run asks for, open
        prose, not the author's intent, because the form is the only part
        loom knows. Being a shape pin, this mode runs no output check
        (:meth:`run`'s own note on ``expect`` applies here too: the check is
        compiled against the artefact's declared schema and cannot validate
        another one) and is refused before the model is called under
        exactly the condition ``run``'s ``expect`` is: when the artefact's
        output hook command declares the ``output`` field, which was
        compiled against the declared shape and cannot be handed ``str``
        instead.

        Args:
            prompt: Prompt for this run: text, or text and attachments in
                order.
            conversation_id: Identifier of the conversation this run
                continues; ``None`` runs single-shot.
            state: This run's state, forwarded to :meth:`run` unchanged; see
                its own ``state`` for what it does and how it can fail.

        Returns:
            The model's own prose, this run's usage and its interaction id.

        Raises:
            AgentRunError: With ``AGENT_RUN_SHAPE_WITH_HOOK`` when the
                artefact's output hook command declares the ``output``
                field — refused before the model is called, for the same
                reason :meth:`run` raises it with ``expect`` given; with
                ``STATE_UNDECLARED`` for the same reason :meth:`run` raises
                it.
        """
        ...

    def mcp(self, server: str) -> McpHandle:
        """Return the artefact's own filtered view of one ``mcp`` grant.

        Args:
            server: Server name as the artefact's own ``mcp`` capability
                declares it. Not verified at start-up — only a compiled
                agent name and its output type are — so a typo here is
                caught on first call, not before.

        Returns:
            The grant's own view, filtered exactly as the model's is.

        Raises:
            AgentRunError: With ``MCP_GRANT_UNKNOWN`` when the artefact
                declares no ``mcp`` grant on that server name.
        """
        ...

    def sql(self, connection: str) -> SqlGrantHandle:
        """Return the artefact's own bounded view of one ``sql`` grant.

        Args:
            connection: Connection name as the artefact's own ``sql``
                capability declares it. Not verified at start-up, for the
                same reason :meth:`mcp`'s ``server`` is not.

        Returns:
            A view bounded by that grant's own row and byte limits, under
            the plan's timeout.

        Raises:
            AgentRunError: With ``SQL_GRANT_UNKNOWN`` when the artefact
                declares no ``sql`` grant on that connection name.
        """
        ...

    def native(self, *, state: object | None = None) -> object:
        """Return the engine's own objects for this agent, outside loom's supervision.

        The escape hatch: whatever the engine serving this agent was built
        from, handed over so calling code can drive the underlying library
        itself when loom's neutral surface does not cover what it needs.
        Typed as ``object`` because this module names no engine; the engine
        package publishes the accessor that narrows it — for the pydantic-ai
        engine, :func:`loom.ai.engines.pydantic_ai.native_agent`. This is the
        one place the full what-holds/what-is-lost list is written; every
        other mention of it points back here.

        **What still holds.** The artefact's grants: already built into what
        is returned, so a run driven this way still reaches exactly what the
        artefact granted, as the identity acting where the accessor's block
        is entered, refused outside any execution, past that caller's
        deadline or with no authenticated caller. ``policies.tool_timeout_ms`` still bounds every
        capability call, because it is compiled into the tools the returned
        agent already carries. The artefact's ``output_check`` validator
        still runs, for the same reason. So do loom's chain-cycle and
        agent-chain-depth bounds and its admission limit
        (``max_concurrent_runs``): the returned engine-native objects are
        reachable only inside the accessor's own ``async with`` block
        (:func:`loom.ai.engines.pydantic_ai.native_agent` for the
        pydantic-ai engine), which rejoins those bounds for the whole body.

        **What is lost**, always: loom's own retry policy (including the
        rule that an agent holding a capability never retries), ``run_timeout_ms``,
        ``max_iterations``, ``policies.on_unpriced_spend`` and the
        ``COST_NOT_MEASURABLE`` refusal it can raise, the ``on_output`` hook,
        the ``conversation`` loader, the event projection, loom's own output
        decoding (:func:`~loom.ai.engines.pydantic_ai._output.decode_output`
        for a ``msgspec.Struct`` artefact — a native run returns pydantic-ai's
        own answer, a mapping, not the artefact's declared type) and
        ``max_history_bytes``. An application using this hatch is also pinned
        to the engine it names: nothing here is portable across engines.

        Args:
            state: This run's state, resolved against the artefact's declared
                shape exactly as :meth:`run` resolves it.

        Returns:
            The engine-native objects, in the engine's own types.

        Raises:
            AgentRunError: With ``UNAUTHORIZED`` when this handle's identity
                is anonymous, so the hatch never yields what :meth:`run`
                would have refused; with ``STATE_UNDECLARED`` or
                ``STATE_REQUIRED`` when *state* does not match the
                artefact's declared shape.
            RuntimeError: When the runtime serving this handle was never
                entered.
            NotImplementedError: When the engine serving this agent publishes
                no native form.
        """
        ...

    def grants(self) -> tuple[str, ...]:
        """Return every grant name reachable through :meth:`mcp` and :meth:`sql`.

        Exists so application code — most usefully a test — can pin a grant
        name in one assertion instead of reading the artefact's YAML.

        Returns:
            Every ``mcp`` server name and ``sql`` connection name the
            artefact declares: every server first, then every connection,
            each group in declaration order.
        """
        ...


class TextDeltaEvent(
    LoomFrozenStruct, frozen=True, kw_only=True, tag="text_delta", tag_field="type"
):
    """Incremental model text.

    Attributes:
        text: Text fragment, passed through unmodified.
    """

    text: str


class ToolCallEvent(LoomFrozenStruct, frozen=True, kw_only=True, tag="tool_call", tag_field="type"):
    """The model invoked a tool.

    Attributes:
        tool: Tool name as the engine exposes it.
        call_id: Correlation id matching the eventual ``tool_result``.
        arguments: Arguments the model supplied.
    """

    tool: str
    call_id: str
    arguments: Mapping[str, Any]


class ToolResultEvent(
    LoomFrozenStruct, frozen=True, kw_only=True, tag="tool_result", tag_field="type"
):
    """A tool invocation completed.

    Attributes:
        call_id: Correlation id of the originating ``tool_call``.
        ok: Whether the tool succeeded.
        summary: Short human-readable outcome; never the full payload.
    """

    call_id: str
    ok: bool
    summary: str


class ErrorEvent(LoomFrozenStruct, frozen=True, kw_only=True, tag="error", tag_field="type"):
    """The run failed mid-stream.

    Attributes:
        code: Stable run-time failure code; the retry policy reads its class.
        message: Human-readable description.
        interaction_id: Identifier of the admitted run this failure belongs
            to; ``None`` before admission.
        usage: What the failed run had already spent, when the engine knew it.
            A run that made three model round trips and then failed its output
            schema still cost money, and a model that fails more must not rank
            better on cost for it. ``None`` when nothing was spent or nothing
            was measurable — a refusal before admission, a run a declared
            limit killed from outside the engine. Not on the wire: the stream
            contract puts ``usage`` on ``final`` only.
    """

    code: AgentRunErrorCode
    message: str
    interaction_id: str | None = None
    usage: AgentUsage | None = None


class FinalEvent(LoomFrozenStruct, frozen=True, kw_only=True, tag="final", tag_field="type"):
    """The run completed; the only variant carrying usage.

    Attributes:
        output: Answer already decoded and validated against the declared
            output shape.
        usage: Resource accounting of the whole run.
        interaction_id: Identifier the runtime minted for this run.
        hook_result: Return value of the ``on_output`` use case, when the plan
            declares one.
        messages: New messages of this run in the engine's own serialised
            form; ``None`` when they are longer than
            ``policies.max_history_bytes``.
        provider_details: What the provider reported about the final answer
            beyond the answer itself, as on :class:`AgentResult`; the
            runtime copies it there, and no wire surface publishes it.
    """

    output: object
    usage: AgentUsage
    interaction_id: str | None = None
    hook_result: object | None = None
    messages: bytes | None = None
    provider_details: Mapping[str, Any] | None = None


AgentEvent = TextDeltaEvent | ToolCallEvent | ToolResultEvent | ErrorEvent | FinalEvent
"""Closed five-member tagged union of streaming events.

Exactly one of ``final`` or ``error`` terminates every stream, and
``final`` is the only variant carrying usage.

Adding a variant requires two independent real consumers: a single
engine wanting a richer event is not grounds to widen a union every SSE
client, test fake and contract suite must understand.
"""


class AgentEngine(Protocol):
    """One compiled agent, ready to run.

    Engines take a single prompt and, optionally, the conversation the run
    continues: an opaque, engine-native history the application loaded.  Loom defines no
    message model and stores no history; an engine that receives ``conversation=None``
    runs single-shot.
    """

    async def run(
        self,
        prompt: Prompt,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> AgentResult:
        """Run the agent to completion.

        Args:
            prompt: Prompt for this run: text, or text and attachments in
                order.
            identity: Verified caller; every capability call runs as them.
            conversation: The conversation this run continues; ``None`` runs
                single-shot.
            state: This run's state, already decoded by whichever boundary
                received it and normalised against the artefact's declared
                shape — never parsed again here. ``None`` for an
                artefact declaring no state, or for a stateful one whose
                caller supplied none, in which case it carries the shape's
                own declared defaults instead of an empty value.

        Returns:
            The validated output, the run's usage and this run's messages,
            bounded by ``policies.max_history_bytes`` (``None`` above it).
        """
        ...

    def run_stream(
        self,
        prompt: Prompt,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
        """Run the agent, streaming events.

        Returns an async context manager rather than a bare iterator so that
        closing the stream — and the provider connection behind it — is
        deterministic on exit instead of being left to the garbage collector.

        Args:
            prompt: Prompt for this run: text, or text and attachments in
                order.
            identity: Verified caller; every capability call runs as them.
            conversation: The conversation this run continues; ``None`` runs
                single-shot.
            state: This run's state; see :meth:`run`'s own ``state`` for what
                it carries.

        Returns:
            An async context manager yielding the event stream.
        """
        ...

    async def health(self) -> HealthStatus:
        """Report the engine's current health without per-call network I/O.

        Returns:
            The engine's state, derived from outcomes it has already observed
            rather than from a probe issued on this call.
        """
        ...


@runtime_checkable
class NativeCapableEngine(Protocol):
    """An :class:`AgentEngine` that also publishes its own native form.

    ``native`` is optional on :class:`AgentEngine` itself; this Protocol
    exists only so :meth:`~loom.ai.execution.AgentRuntime.native` can narrow to
    it with ``isinstance`` instead of reflecting over ``getattr``.
    """

    def native(
        self,
        *,
        identity: Identity,
        state: object | None = None,
        guard: Callable[[], AbstractAsyncContextManager[None]],
    ) -> object:
        """Return this engine's own objects, with a dependency bundle built for *identity*.

        Args:
            identity: Verified caller the dependency bundle is built for;
                informational.
            state: This run's state, already resolved against the plan's
                declared shape.
            guard: Zero-argument factory of the async context manager a
                driven run enters to rejoin the runtime's chain and
                admission bounds.

        Returns:
            The engine's own objects, in the engine's own types.
        """
        ...


class HealthState(StrEnum):
    """The three health states, best first."""

    OK = "ok"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"

    @classmethod
    def worst(cls, states: Iterable[str]) -> HealthState:
        """Return the worst of *states*, ``OK`` when there are none.

        Args:
            states: Health states; one that is no :class:`HealthState`
                ranks as ``UNAVAILABLE``.

        Returns:
            The worst state.
        """
        unknown = _HEALTH_RANK[cls.UNAVAILABLE]
        return _BY_RANK[max((_HEALTH_RANK.get(state, unknown) for state in states), default=0)]


_BY_RANK: Final[tuple[HealthState, ...]] = tuple(HealthState)
_HEALTH_RANK: Final[Mapping[str, int]] = MappingProxyType(
    {state: rank for rank, state in enumerate(_BY_RANK)}
)


class HealthStatus(LoomFrozenStruct, frozen=True, kw_only=True):
    """Health of one agent engine, shared by every engine.

    Attributes:
        status: The engine's health state.
        detail: Optional human-readable explanation.
    """

    status: HealthState
    detail: str | None = None


class McpToolInfo(LoomFrozenStruct, frozen=True, kw_only=True):
    """One tool a session's server advertises.

    Attributes:
        name: Tool name as the server exposes it.
        has_output_schema: Whether the server published a schema for this
            tool's structured result. A :class:`McpHandle` refuses a typed
            call on a tool for which this is ``False``, before any network
            call — see :meth:`McpHandle.call`.
    """

    name: str
    has_output_schema: bool


class McpToolCallResult(LoomFrozenStruct, frozen=True, kw_only=True):
    """The server's own answer to one ``call_tool``, protocol-level and undecoded.

    Attributes:
        ok: ``False`` when the server flagged the call as failed.
        structured: The tool's structured result, or ``None`` when the server
            returned none — including every call the server flagged failed,
            whose content is never treated as an answer.
    """

    ok: bool
    structured: object | None = None


class McpSession(Protocol):
    """Minimal MCP session the runtime needs from any client library.

    ``list_tools`` returns :class:`McpToolInfo` so a caller can see which
    tools publish an output schema, and ``call_tool`` returns
    :class:`McpToolCallResult` so a caller can see the server's own failure
    flag.
    """

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        """Return the tools the server exposes.

        Returns:
            Every tool the server advertises, before any declared filter is
            applied, each carrying whether it publishes an output schema.
        """
        ...

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> McpToolCallResult:
        """Invoke one tool and return its protocol-level result.

        Args:
            name: Tool name as the server exposes it.
            arguments: Arguments to pass to the tool.

        Returns:
            The server's own error flag and structured content, neither
            interpreted nor decoded.
        """
        ...


class ConcurrentMcpSession:
    """Declares an :class:`McpSession` implementation already safe for concurrent calls.

    A JSON-RPC session is one framed stream: two callers writing into it at
    the same time can interleave their frames, and a caller cancelled
    mid-call can leave the stream desynchronised for whoever is waiting
    beside it. That is why a session gets serialised behind one lock by
    default. A session that already guards its own frames — one that
    multiplexes concurrent calls by matching each response back to its own
    request id, rather than writing straight through a single unmatched
    stream — declares that guarantee by also subclassing this, in addition
    to implementing :class:`McpSession`, and the runtime leaves it
    unwrapped.

    This class carries no members: subclassing it *is* the declaration. A
    session that does not subclass it is wrapped and serialised.

    A subclass keeping this promise must (a) tolerate overlapping calls by
    matching each response back to its own request id — the multiplexing
    that makes the guarantee true in the first place — and (b) must **not**
    shield a call from its own caller's cancellation the way the runtime's
    locked wrapper does: with no shared frame to desynchronise, there is
    nothing left to drain, and shielding would only stop the plan's own
    ``tool_timeout_ms`` from bounding the call.
    """

    __slots__ = ()


class ToolsetContext(Protocol):
    """What a ``kind: python`` factory may reach while building its toolset.

    A build-time object: the engine hands it to the factory once, at start-up,
    and nothing keeps it alive afterwards. A factory resolves the remotes it
    needs in its body and keeps the session on the toolset it returns; it must
    not call :meth:`remote` lazily from a tool at run time.

    :meth:`remote` is bounded to the ``mcp`` grants of the same agent: it
    returns the worker's shared session for a server the agent's own artifact
    declared, and nothing else. Calls made through that session go to the
    shared connection directly, so they bypass the ``include``/``exclude``
    filter of the ``mcp`` grant.
    """

    @property
    def agent(self) -> str:
        """Name of the plan being built."""
        ...

    @property
    def container(self) -> LoomContainer:
        """Application container the factory may resolve services from."""
        ...

    def remote(self, server: str) -> McpSession:
        """Return the agent's shared session for one of its ``mcp`` servers.

        Args:
            server: Server name as the agent's ``mcp`` grant declares it.

        Returns:
            The session the agent's own ``mcp`` toolset runs over.

        Raises:
            AgentCompilationError: When the agent has no ``mcp`` grant on that
                server.
        """
        ...


ToolsetFactory: TypeAlias = Callable[..., object]
"""Target of a ``kind: python`` capability, validated at compile time.

Called exactly once at build as ``factory(context, **params)``: the first
positional is a :class:`ToolsetContext`, and the artifact's ``params`` arrive
as keyword arguments. A factory declares its own named parameters, with
defaults, and returns the engine-facing toolset. The parameter names are
checked against the signature at compile time. It is a plain ``Callable``
alias rather than a Protocol because a Protocol fixing ``**params`` would
reject every factory that names them.
"""


class DepsFactory(Protocol):
    """Builds per-invocation dependencies for capability calls.

    Singleton services are captured once at build. A capability call reads
    ``container`` off the bundle and acts as the identity of the execution in
    progress. The bundle also carries the invocation's ``state``, beside the
    container.
    """

    def build(
        self,
        identity: Identity,
        container: LoomContainer,
        state: Mapping[str, Any] | None = None,
    ) -> object:
        """Build the dependency bundle for one invocation.

        Args:
            identity: The caller of this invocation; not read: a capability
                call acts as the identity of the execution in progress.
            container: Application container holding the singleton services.
            state: The invocation's state, a mapping normalised against the
                artefact's declared shape, or ``None`` for an artefact that
                declares no state or a call that supplies none.

        Returns:
            The engine-facing dependency bundle.
        """
        ...


NativeToolSupport = Callable[[InferenceTarget], frozenset[str]]
"""Answers which provider tools a model binding admits, by loom tool name.

Supplied by an engine as an optional ``native_tool_support`` attribute and read
with ``getattr``, so the compiler learns what a binding admits without importing
an engine. May raise :class:`~loom.ai.errors.AgentCompilationError` when the
provider SDK is missing.
"""

OutputCheck: TypeAlias = Callable[[Mapping[str, Any]], str | None]
"""Target of an ``output_check`` reference: a pure predicate over an answer.

Called once per attempt inside the engine's own output-retry loop. For a
``msgspec.Struct`` output the mapping is the one the engine parsed from the
model's answer — never loom's decoded object, so nothing is decoded twice.
For a pydantic output the check still receives a
``Mapping[str, Any]``: the engine calls it with
``loom_type.to_builtins(instance)`` of pydantic-ai's own validated instance,
so a check written once serves both libraries. Returns ``None`` to accept the
answer unchanged, or the text the model must read to correct itself, which
drives a real retry bounded by the artifact's ``policies.retries`` — a real
pydantic-ai output-tool retry for a pydantic output, exactly as for a
``msgspec.Struct`` one.

The return contract is the inverse of the usual predicate convention, which
is why this alias is published rather than left for an author to spell: it
is the one name in this module carrying ``Mapping[str, Any]``, because the
payload shape is the artifact's own declared schema, which loom cannot type
statically.

A returned mapping is never substituted for the answer: the engine never
uses this callable's return value to build the answer. A ``msgspec.Struct``
answer is decoded from the model's own bytes independently; a pydantic
answer is pydantic-ai's own validated instance, read off ``result.output``.
Either way, a check that builds and returns a different mapping has that
mapping discarded. Synchronous and dependency-free, because any side effect
inside the retry loop would run once per attempt.
"""


class AgentEngineProvider(Protocol):
    """Entry-point target in group ``loom.ai.engines``.

    Attributes:
        LOOM_AI_ENGINE_API: Handshake version, checked with ``getattr`` on
            load — never with ``isinstance``.
        native_tool_support: Optional :data:`NativeToolSupport`, read with
            ``getattr``; an engine that serves no ``native`` grant omits it.
    """

    LOOM_AI_ENGINE_API: ClassVar[int]

    def create_engine(
        self, plan: object, *, deps: DepsFactory, container: LoomContainer
    ) -> AgentEngine:
        """Build one engine for one compiled plan.

        Called exactly once per plan by the runtime, never per request.

        Args:
            plan: The compiled ``AgentPlan``, typed as ``object``.
            deps: Per-invocation dependency factory.
            container: Application container.

        Returns:
            The engine serving this plan.
        """
        ...

    def supported_capability_kinds(self) -> frozenset[str]:
        """Capability kinds this engine can serve.

        The compiler receives the result as a plain value resolved by the
        bootstrap — nothing in ``loom.ai`` imports an engine to obtain it.

        Returns:
            The supported ``kind`` identifiers.
        """
        ...
