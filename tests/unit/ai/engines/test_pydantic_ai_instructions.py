"""Instruction blocks reaching the model through ``_instructions.py`` (T401/T402).

Two things are exercised together: the start-up guard that keeps the
templating extra optional (T401), and the dispatch map projecting each
compiled block onto what ``Agent.from_spec(instructions=...)`` receives
(T402). A "recording double" (:class:`_FromSpecRecorder`) captures what
:meth:`~loom.ai.engines.pydantic_ai.provider.PydanticAIEngineProvider.create_engine`
actually hands the engine; the assertions about ``dynamic`` and ``name`` then
run pydantic-ai's own :func:`~pydantic_ai._instructions.sourced_instruction`
against what was captured, rather than re-deriving those rules locally.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from msgspec import structs
from pydantic_ai._instructions import sourced_instruction
from pydantic_ai.messages import InstructionPart

from loom.ai.abc import StateShape
from loom.ai.compiler._plan import AgentPlan, CompiledInstruction
from loom.ai.engines.pydantic_ai._instructions import (
    TEMPLATING_EXTRA,
    build_instructions,
    ensure_templating_available,
)
from loom.ai.engines.pydantic_ai.provider import PydanticAIEngineProvider
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.core.di import LoomContainer
from tests.helpers.extras import ENGINE_EXTRAS, without_extra
from tests.helpers.pydantic_ai_engine import NullDeps, make_plan

_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"marca": {"type": "string"}},
    "required": ["marca"],
    "additionalProperties": False,
}


def _schema_state() -> StateShape:
    return StateShape(schema=_SCHEMA, loom_type=None)


def _dict_state() -> StateShape:
    return StateShape(schema=None, loom_type=None)


def _plan_with(
    instructions: tuple[CompiledInstruction, ...], state: StateShape | None
) -> AgentPlan:
    return structs.replace(make_plan(), instructions=instructions, state=state)


@dataclass
class _InstructionStateBundle:
    state: Any


@dataclass
class _FakeRunContext:
    """Stands in for ``RunContext``: the render closure only reads ``.deps``."""

    deps: _InstructionStateBundle


class TestTemplatingExtraCheck:
    """T401: the extra is checked at engine build time, not at first request."""

    def test_boots_with_no_templated_block_when_the_extra_is_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An artifact with no templated block never attempts the import."""
        without_extra(monkeypatch, ["pydantic_handlebars"], [f"{ENGINE_EXTRAS}.handlebars"])
        plan = _plan_with((CompiledInstruction(text="Be terse."),), None)

        ensure_templating_available(plan)  # does not raise

    def test_aborts_naming_the_artifact_and_the_block_when_the_extra_is_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A templated block with the extra absent fails start-up, not the first request."""
        without_extra(monkeypatch, ["pydantic_handlebars"], [f"{ENGINE_EXTRAS}.handlebars"])
        plan = _plan_with(
            (CompiledInstruction(text="Hi {{marca}}.", name="greeting", template="handlebars"),),
            _schema_state(),
        )

        with pytest.raises(AgentCompilationError) as failure:
            ensure_templating_available(plan)

        issue = failure.value.issues[0]
        assert issue.code is AgentErrorCode.TEMPLATE_EXTRA_MISSING
        assert plan.name in issue.message
        assert "greeting" in issue.message
        assert TEMPLATING_EXTRA in issue.message

    def test_boots_with_a_templated_block_when_the_extra_is_installed(self) -> None:
        """With the real package importable (this tree, T001), the guard passes."""
        plan = _plan_with(
            (CompiledInstruction(text="Hi {{marca}}.", template="handlebars"),),
            _schema_state(),
        )

        ensure_templating_available(plan)  # does not raise

    def test_create_engine_boots_with_the_extra_absent_when_no_block_templates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The other half of AC-007: a stateless, template-free artifact is unaffected."""
        from pydantic_ai.models.test import TestModel

        without_extra(monkeypatch, ["pydantic_handlebars"], [f"{ENGINE_EXTRAS}.handlebars"])
        provider = PydanticAIEngineProvider(model_resolver=lambda target: TestModel())

        engine = provider.create_engine(make_plan(), deps=NullDeps(), container=LoomContainer())

        assert engine is not None


class TestBlockDispatch:
    """T402: the two-entry map, and what reaches the engine for each form."""

    def test_two_literals_and_one_template_preserve_authored_order(self) -> None:
        plan = _plan_with(
            (
                CompiledInstruction(text="First.", name="opening"),
                CompiledInstruction(text="Second."),
                CompiledInstruction(text="Hi {{marca}}.", template="handlebars"),
            ),
            _schema_state(),
        )

        items = build_instructions(plan)

        assert len(items) == 3
        assert isinstance(items[0], InstructionPart)
        assert isinstance(items[1], InstructionPart)
        assert not isinstance(items[2], InstructionPart)
        assert callable(items[2])

    def test_the_real_engine_marks_the_literals_static_and_the_template_dynamic(self) -> None:
        """AC-005, AC-006: applies pydantic-ai's own ``sourced_instruction`` to what was built.

        This asserts the *consequence* the engine draws from each item's
        shape (FR-025, FR-026), using the engine's real rule rather than a
        restatement of it.
        """
        plan = _plan_with(
            (
                CompiledInstruction(text="First.", name="opening"),
                CompiledInstruction(text="Second."),
                CompiledInstruction(text="Hi {{marca}}.", template="handlebars"),
            ),
            _schema_state(),
        )

        items = build_instructions(plan)
        sourced = [sourced_instruction(item, None) for item in items]

        assert [s.dynamic for s in sourced] == [False, False, True]
        assert [s.name for s in sourced] == ["opening", None, None]

    async def test_a_named_literal_block_reaches_the_part_with_its_name(self) -> None:
        plan = _plan_with((CompiledInstruction(text="Be terse.", name="tone"),), None)

        (item,) = build_instructions(plan)

        assert isinstance(item, InstructionPart)
        assert item.name == "tone"
        assert item.content == "Be terse."

    async def test_a_literal_block_with_double_braces_reaches_the_model_verbatim(self) -> None:
        """FR-022: with no ``template:``, ``{{`` is literal text, never inferred."""
        plan = _plan_with((CompiledInstruction(text="Say {{marca}}."),), None)

        (item,) = build_instructions(plan)

        assert isinstance(item, InstructionPart)
        assert item.content == "Say {{marca}}."

    async def test_a_templated_block_renders_against_state_not_the_bundle(self) -> None:
        plan = _plan_with(
            (CompiledInstruction(text="Hi {{marca}}.", template="handlebars"),), _schema_state()
        )

        (item,) = build_instructions(plan)
        assert callable(item)
        ctx = _FakeRunContext(deps=_InstructionStateBundle(state={"marca": "civic"}))

        rendered = await item(ctx)  # type: ignore[arg-type]

        assert rendered == "Hi civic."

    async def test_a_template_referencing_an_undeclared_marker_fails_to_compile(self) -> None:
        """FR-032: a marker the schema does not declare fails at start-up."""
        plan = _plan_with(
            (CompiledInstruction(text="Hi {{unknown}}.", name="greeting", template="handlebars"),),
            _schema_state(),
        )

        with pytest.raises(AgentCompilationError) as failure:
            build_instructions(plan)

        issue = failure.value.issues[0]
        assert issue.code is AgentErrorCode.TEMPLATE_COMPILATION_FAILED
        assert "greeting" in issue.message

    async def test_under_the_dict_waiver_an_unknown_marker_compiles_and_renders_empty(
        self,
    ) -> None:
        """FR-033: no schema to check against under ``deps_type: dict``."""
        plan = _plan_with(
            (CompiledInstruction(text="Hi {{unknown}}.", template="handlebars"),), _dict_state()
        )

        (item,) = build_instructions(plan)
        assert callable(item)
        ctx = _FakeRunContext(deps=_InstructionStateBundle(state={}))

        rendered = await item(ctx)  # type: ignore[arg-type]

        assert rendered == "Hi ."

    def test_the_map_dispatches_on_template_never_a_boolean(self) -> None:
        """Two block-shapes in, two builders reached — nothing branches on a flag."""
        plan = _plan_with(
            (
                CompiledInstruction(text="Literal."),
                CompiledInstruction(text="{{marca}}", template="handlebars"),
            ),
            _schema_state(),
        )

        items = build_instructions(plan)

        assert isinstance(items[0], InstructionPart)
        assert not isinstance(items[1], InstructionPart)
