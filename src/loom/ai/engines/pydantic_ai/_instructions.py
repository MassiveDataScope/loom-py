"""Instruction blocks reaching the model, projected through ``instructions=``.

An artifact's compiled instruction blocks
(:class:`~loom.ai.compiler.CompiledInstruction`) reach ``Agent.from_spec``
through the ``instructions=`` keyword
(:mod:`~loom.ai.engines.pydantic_ai._spec`), never through
``AgentSpec.instructions``. This module builds that keyword's value,
dispatching each block on its declared ``template`` — literal or Handlebars —
never on a boolean and never through a class hierarchy for blocks: the two
forms are the whole surface (:data:`_BLOCK_BUILDERS`).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from types import MappingProxyType
from typing import Any, Final, Protocol, TypeAlias, cast

from pydantic_ai import RunContext
from pydantic_ai.messages import InstructionPart

from loom.ai.abc import StateShape
from loom.ai.compiler import AgentPlan, CompiledInstruction
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.core.plugins.optional import MissingExtraError, import_optional

TEMPLATING_EXTRA: Final[str] = "ai-templates"
_HANDLEBARS_ISLAND: Final[str] = "loom.ai.engines.pydantic_ai.extras.handlebars"
"""The one module of the engine importing ``pydantic_handlebars``."""
"""``pyproject.toml`` optional-dependency extra installing ``pydantic_handlebars``."""

AgentInstructionItem: TypeAlias = InstructionPart | Callable[[RunContext[Any]], Awaitable[str]]
"""One element of the list :func:`build_instructions` returns."""

BlockBuilder: TypeAlias = Callable[
    [CompiledInstruction, "StateShape | None", str, str], AgentInstructionItem
]
"""Projects one compiled block onto an :data:`AgentInstructionItem`.

Every entry in :data:`_BLOCK_BUILDERS` takes the same four positions: the
block, the artifact's declared state shape (``None`` when it declares none),
the artifact name (for a compilation failure's message) and the block's own
label — its ``name`` when it has one, its position otherwise (FR-034).
"""


def ensure_templating_available(plan: AgentPlan) -> None:
    """Refuse to build an engine whose templated blocks cannot render.

    ``pydantic_handlebars`` is imported lazily, by
    ``pydantic_ai.template._import_pydantic_handlebars``; without this
    check, an artifact declaring ``template:`` while the extra is absent
    would only fail on its first request, after already taking the caller's
    prompt. Checking here, at the point
    :meth:`~loom.ai.engines.pydantic_ai.provider.PydanticAIEngineProvider.create_engine`
    builds the agent, moves that failure to start-up (FR-035).

    An artifact with no templated block never attempts the import, which is
    what keeps the extra optional: this function returns immediately for one.

    Args:
        plan: Compiled agent plan.

    Raises:
        AgentCompilationError: A block declares ``template:`` and
            ``pydantic_handlebars`` cannot be imported. One
            ``TEMPLATE_EXTRA_MISSING`` issue per such block, naming the
            artifact, the block and :data:`TEMPLATING_EXTRA`.
    """
    templated = [
        (index, block)
        for index, block in enumerate(plan.instructions)
        if block.template is not None
    ]
    if not templated or _pydantic_handlebars_importable():
        return
    raise AgentCompilationError(
        [
            AgentErrorCode.TEMPLATE_EXTRA_MISSING.issue(
                component=plan.name, block=_block_label(block, index), extra=TEMPLATING_EXTRA
            )
            for index, block in templated
        ]
    )


def _pydantic_handlebars_importable() -> bool:
    try:
        _handlebars()
    except MissingExtraError:
        return False
    return True


class _Handlebars(Protocol):
    """The two names the templating island publishes."""

    HandlebarsError: type[Exception]

    def compile_checked(
        self, text: str, schema: Mapping[str, Any] | None
    ) -> Callable[[Any], str]: ...


def _handlebars() -> _Handlebars:
    """Load the island; the cast records the contract every island keeps.

    Raises:
        MissingExtraError: When ``pydantic_handlebars`` is not installed.
    """
    return cast(_Handlebars, import_optional(_HANDLEBARS_ISLAND, extra=TEMPLATING_EXTRA))


def build_instructions(plan: AgentPlan) -> list[AgentInstructionItem]:
    """Project every compiled instruction block onto the engine's keyword form.

    Call :func:`ensure_templating_available` first; this function does not
    check the extra itself. Order is preserved end to end: the returned list
    is handed to ``Agent.from_spec(instructions=...)`` verbatim, and the two
    builders it dispatches to never reorder or drop a block (FR-023).

    Args:
        plan: Compiled agent plan.

    Returns:
        One item per block, in authored order.

    Raises:
        AgentCompilationError: A templated block fails to compile, or fails
            the compatibility check against its declared schema.
    """
    return [
        _BLOCK_BUILDERS[block.template](block, plan.state, plan.name, _block_label(block, index))
        for index, block in enumerate(plan.instructions)
    ]


def _block_label(block: CompiledInstruction, index: int) -> str:
    return block.name if block.name is not None else str(index)


def _literal_instruction(
    block: CompiledInstruction, state: StateShape | None, component: str, label: str
) -> InstructionPart:
    """Project a literal block onto a static, cacheable instruction part.

    ``dynamic`` stays at its default ``False``
    (``pydantic_ai.messages.InstructionPart.dynamic``), which is what puts
    this block's text in the provider's cacheable prefix (FR-025).
    The part carries ``block.name`` unchanged, ``None`` when the block
    declares none.

    Args:
        block: Compiled block; only ``text`` and ``name`` are read.
        state: Unused; a literal block never renders against state.
        component: Unused; a literal block never fails to compile.
        label: Unused; a literal block never fails to compile.

    Returns:
        The static instruction part the engine sends verbatim.
    """
    del state, component, label
    return InstructionPart(content=block.text, name=block.name)


def _handlebars_instruction(
    block: CompiledInstruction, state: StateShape | None, component: str, label: str
) -> Callable[[RunContext[Any]], Awaitable[str]]:
    """Compile a templated block once and return the closure that renders it.

    With a declared schema (``state.schema`` set), ``check_template_compatibility``
    runs first, straight against ``pydantic_handlebars`` — so a marker the
    schema does not declare fails here, at start-up, rather than silently
    rendering as the empty string (FR-032). Under the ``deps_type: dict``
    waiver, or when this artifact declares no state at all, there is no
    schema to check against and the template compiles bare (FR-033).
    Compilation happens once, in this function, never inside the returned
    closure — the closure only renders.

    ``pydantic_ai.TemplateStr`` is not used to reach ``pydantic_handlebars``
    here: its untyped render path, ``TemplateStr.render``, builds a fresh
    ``pydantic.TypeAdapter(type(deps))`` on every call — the trap that ruled
    out a typed compilation path for this pillar (spike, T001) — and would
    pay a ``dump_python`` round trip per block per request. This function
    calls ``pydantic_handlebars.compile``/``check_template_compatibility``
    directly and keeps the already-compiled template in the closure.

    The returned closure is ``async def``, not ``def``. On the route this
    module takes, a callable instruction is resolved by
    ``pydantic_ai._system_prompt.SystemPromptRunner.run``, whose synchronous
    branch awaits ``run_in_executor``: a plain ``def`` would pay a
    thread-pool hop per templated block per request for what is a
    synchronous string substitution. An ``async def`` closure skips that hop
    (NFR-002).

    The closure renders against ``ctx.deps.state``, never against ``ctx.deps``
    itself. ``ctx.deps`` is the bundle carrying the caller's verified
    ``Identity``, the application container and a caller-bound invoker
    (``loom.composition._ai``); rendering against the bundle would put all
    three within a marker's reach. Rendering against ``state`` — the
    normalised mapping the artifact declared a shape for — is the security
    boundary a template cannot cross (FR-036).

    The returned closure carries no name the engine can address: on this
    route, a name is read only off an
    :class:`~pydantic_ai.messages.InstructionPart`
    (``pydantic_ai._instructions.sourced_instruction``), and
    ``InstructionPart.content`` is a plain ``str`` that cannot carry a
    template. The engine's decorator route, ``pydantic_ai.Agent.instructions``,
    does read a callable's declared name, but it appends every block it
    registers, which would push every templated block behind every literal
    one and destroy the authored order :func:`build_instructions` exists to
    keep. The missing name is the price of keeping that order — it is not a
    limit of the engine, which can name a callable on its own route
    (FR-026).

    Args:
        block: Compiled block; ``text`` is the Handlebars source.
        state: Artifact's declared state shape, or ``None``. Its ``schema``,
            when present, is what a marker is checked against; absent, the
            template compiles unchecked.
        component: Artifact name, for a compilation failure's message.
        label: This block's own name, or its position (FR-034).

    Returns:
        An ``async`` closure rendering the compiled template against the
        run's state.

    Raises:
        AgentCompilationError: The template fails to parse, or fails the
            compatibility check against its declared schema.
    """
    handlebars = _handlebars()
    schema = None if state is None else state.schema
    try:
        rendered = handlebars.compile_checked(block.text, schema)
    except handlebars.HandlebarsError as exc:
        raise AgentCompilationError(
            [
                AgentErrorCode.TEMPLATE_COMPILATION_FAILED.issue(
                    component=component, block=label, reason=str(exc)
                )
            ]
        ) from exc

    async def render(ctx: RunContext[Any]) -> str:
        return rendered(ctx.deps.state)

    return render


_BLOCK_BUILDERS: Final[Mapping[str | None, BlockBuilder]] = MappingProxyType(
    {
        None: _literal_instruction,
        "handlebars": _handlebars_instruction,
    }
)
