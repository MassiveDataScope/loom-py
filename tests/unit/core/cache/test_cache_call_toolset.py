"""AC12: a bound toolset is something ``FunctionToolset`` can actually build.

This is the test the resolved-annotations step exists for. pydantic-ai reads a
tool's annotations with ``get_type_hints(original_func, include_extras=True)``
(``_function_schema.py:145``) and takes each parameter's type from the result,
so what the wrapper carries is what the tool schema says. Measured here rather
than assumed: ``get_type_hints`` reads the wrapper's **own**
``__annotations__`` but takes the globals from the end of the ``__wrapped__``
chain (``typing.py:2229-2237``), so ``functools.wraps``'s copied strings happen
to resolve for it too — while ``include_extras=False`` silently drops the
``Field(description=...)`` a tool argument's description lives in, which the
schema below does show.

The toolset classes live in :mod:`tests.unit.core.cache._toolset_module`, a
real module under ``from __future__ import annotations``, because that is the
only way the strings behave as they do in an artifact's own module.
"""

from __future__ import annotations

import logging
from typing import Any, get_args

import pytest

from loom.core.model.redaction import RedactedMap

pytest.importorskip("pydantic_ai", reason="requires the ai-pydantic extra")

from pydantic_ai.toolsets import FunctionToolset  # noqa: E402

from loom.core.cache import CacheConfig, CachedCalls, cached_calls  # noqa: E402
from loom.core.cache.wiring import cache_module  # noqa: E402
from loom.core.di.container import LoomContainer  # noqa: E402

from ._doubles import SERIALIZED_BACKEND  # noqa: E402
from ._toolset_module import CursorTools, Doc, SearchTools  # noqa: E402


@pytest.fixture
def calls() -> CachedCalls:
    """The binder a booted container hands to a ``kind: python`` factory."""
    container = LoomContainer()
    cache_module(CacheConfig(aiocache_config=RedactedMap({"default": dict(SERIALIZED_BACKEND)})))(
        container
    )
    return cached_calls(container)


def _parameters(toolset: FunctionToolset[Any], name: str) -> dict[str, Any]:
    """Return the JSON schema pydantic-ai generated for one tool's arguments."""
    schema: dict[str, Any] = toolset.tools[name].tool_def.parameters_json_schema
    return schema


class TestBindingIntoAToolset:
    def test_the_toolset_builds_and_keeps_the_declared_parameters(self, calls: CachedCalls) -> None:
        toolset = FunctionToolset(calls.bind(SearchTools()))

        assert set(toolset.tools) == {"search", "describe"}
        properties = _parameters(toolset, "search")["properties"]
        assert properties["query"]["type"] == "string"
        assert properties["limit"]["type"] == "integer"
        assert properties["limit"]["default"] == 10
        assert _parameters(toolset, "search")["required"] == ["query"]

    def test_an_annotated_parameter_keeps_its_description(self, calls: CachedCalls) -> None:
        """``include_extras=True`` is the difference between this and the docstring's text."""
        toolset = FunctionToolset(calls.bind(SearchTools()))

        limit = _parameters(toolset, "search")["properties"]["limit"]

        assert limit["description"] == "Never more than this many documents."

    def test_the_wrapper_carries_resolved_annotations_of_its_own(self, calls: CachedCalls) -> None:
        """The wrapper answers for itself, without a resolver walking ``__wrapped__``.

        FR-082's invariant, asserted where it is written rather than through
        pydantic-ai: both :func:`typing.get_type_hints` and
        :func:`inspect.get_annotations` take their globals from the end of the
        ``__wrapped__`` chain, so the strings ``functools.wraps`` copies happen
        to resolve for them too. Everything that reads ``__annotations__``
        straight sees only what is here.
        """
        search = next(method for method in calls.bind(SearchTools()) if method.__name__ == "search")

        hints = search.__annotations__

        assert hints["query"] is str
        assert hints["return"] == list[Doc]
        assert get_args(hints["limit"])[0] is int
        assert get_args(hints["limit"])[1].description == "Never more than this many documents."


class TestAnUnresolvableAnnotation:
    def test_the_method_comes_back_unwrapped_and_announced(
        self, calls: CachedCalls, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="loom.core.cache.calls"):
            bound = {method.__name__: method for method in calls.bind(CursorTools())}

        assert bound["resume"].__func__ is CursorTools.resume  # type: ignore[attr-defined]
        assert not getattr(bound["resume"], "__cache_call_wrapped__", False)
        assert getattr(bound["latest"], "__cache_call_wrapped__", False)
        warnings = [
            record for record in caplog.records if "CacheCallNotCacheable" in record.getMessage()
        ]
        assert len(warnings) == 1
        assert "CursorTools.resume" in warnings[0].getMessage()

    def test_the_cacheable_sibling_still_builds_a_toolset(self, calls: CachedCalls) -> None:
        latest = next(method for method in calls.bind(CursorTools()) if method.__name__ == "latest")

        toolset = FunctionToolset([latest])

        assert set(toolset.tools) == {"latest"}

    def test_the_unresolvable_method_fails_exactly_as_it_would_undecorated(
        self, calls: CachedCalls
    ) -> None:
        """Returning it unwrapped means loom neither fixed nor worsened it.

        pydantic-ai resolves a tool's hints itself, so a name only a type
        checker sees defeats it whether or not the cache is in the picture.
        Handing back the original object is what keeps the two identical: a
        wrapper cannot carry the declaring module's ``__globals__``.
        """
        tools = CursorTools()
        resume = next(method for method in calls.bind(tools) if method.__name__ == "resume")

        with pytest.raises(NameError, match="Cursor"):
            FunctionToolset([resume])
        with pytest.raises(NameError, match="Cursor"):
            FunctionToolset([tools.resume])
