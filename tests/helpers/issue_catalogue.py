"""The contract every compile-issue catalogue keeps (ADR 0003 part B).

A catalogue member builds its issue from keyword parameters, so the type
checker no longer sees a missing or misspelt one. Each pillar's catalogue test
runs three checks instead:

- every member renders from its declared parameters (:func:`check_renders`);
- every literal ``<Catalogue>.MEMBER.issue(...)`` call in ``src/loom`` passes
  exactly that member's parameters (:func:`issue_calls`);
- every other ``.issue(...)`` call, where the code is held in a variable, is
  listed in :data:`INDIRECT_CALLS` with the tests that build its issues on the
  real path (:func:`indirect_calls`). A static check cannot see which code
  such a call receives, so those tests are what pin its parameters.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import NamedTuple

from loom.core.errors.codes import IssueCode

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src" / "loom"
_ENCODING = "utf-8"

INDIRECT_CALLS: Mapping[str, tuple[str, ...]] = {
    "ai/compiler/phases/_output.py::_resolve_symbol": (
        "tests/unit/ai/phases/test_output.py::"
        "test_reports_type_ref_unresolvable_when_module_does_not_import",
        "tests/unit/ai/phases/test_output.py::"
        "test_reports_output_check_unresolvable_when_the_check_does_not_import",
        "tests/unit/ai/compiler/test_state_phase.py::test_a_missing_module_reports_unresolvable",
    ),
    "ai/compiler/phases/_output.py::_schema_to_annotation": (
        "tests/unit/ai/phases/test_output.py::"
        "test_reports_schema_invalid_when_output_schema_is_not_valid_json_schema",
        "tests/unit/ai/compiler/test_state_phase.py::"
        "test_an_invalid_deps_schema_reports_schema_invalid",
    ),
    "ai/config.py::_validate_remote_url": (
        "tests/unit/ai/test_config.py::test_fails_with_mcp_url_invalid_when_the_url_is_not_safe",
        "tests/unit/ai/test_config.py::test_fails_with_a2a_url_invalid_when_the_url_is_not_safe",
    ),
    "etl/compiler/_validators_agents.py::validate_step_agents": (
        "tests/unit/etl/test_with_agent.py::test_each_issue_kind_has_its_own_code",
    ),
}
"""Each ``.issue(...)`` call on a code held in a variable, keyed by
``<path under src/loom>::<enclosing function>``, with the tests that cover it."""


class IssueCall(NamedTuple):
    """One ``<Catalogue>.<MEMBER>.issue(...)`` call found in ``src/loom``.

    ``keywords`` is ``None`` when the call passes positional arguments or
    ``**kwargs``, which a static check cannot match against the template.
    """

    location: str
    member: str
    keywords: frozenset[str] | None


def check_renders(code: IssueCode) -> None:
    """Assert that *code* renders its issue from exactly its declared parameters.

    A positional (``{}``) placeholder fails the name check, and a dotted
    (``{a.b}``) one fails to render from the string it is given.
    """
    assert code.template.text.strip(), f"{code} has an empty message template"
    assert all(name.isidentifier() for name in code.params), f"{code}: invalid placeholder"
    issue = code.issue(**{name: f"<{name}>" for name in code.params})
    rendered = " ".join((issue.message, issue.component, issue.field or ""))
    assert issue.code is code
    assert all(f"<{name}>" in rendered for name in code.params)


def issue_calls(catalogue: type[IssueCode]) -> list[IssueCall]:
    """Return every literal ``<catalogue>.<MEMBER>.issue(...)`` call in ``src/loom``.

    Raises:
        AssertionError: If there is none, so a broken scan fails instead of
            leaving the parametrised test with no cases.
    """
    calls: list[IssueCall] = []
    for path, node, _ in _issue_calls():
        member = _literal_member(node, {catalogue.__name__})
        if member is None:
            continue
        named = [keyword.arg for keyword in node.keywords]
        positional = bool(node.args) or None in named
        keywords = None if positional else frozenset(name for name in named if name)
        calls.append(IssueCall(f"{path}:{node.lineno}", member, keywords))
    assert calls, f"no {catalogue.__name__}.MEMBER.issue(...) call found under {_SRC}"
    return calls


def indirect_calls(pillar: str) -> set[str]:
    """Return the ``path::function`` of every non-literal ``.issue(...)`` call in *pillar*."""
    catalogues = _catalogue_names()
    return {
        f"{path}::{function}"
        for path, node, function in _issue_calls()
        if path.startswith(f"{pillar}/") and _literal_member(node, catalogues) is None
    }


def missing_covering_tests(pillar: str) -> list[str]:
    """Return the tests that :data:`INDIRECT_CALLS` names for *pillar* but that do not exist."""
    missing: list[str] = []
    for site, tests in INDIRECT_CALLS.items():
        if not site.startswith(f"{pillar}/"):
            continue
        for test in tests:
            path, name = test.split("::")
            if f"def {name}(" not in (_ROOT / path).read_text(encoding=_ENCODING):
                missing.append(test)
    return missing


def _issue_calls() -> Iterator[tuple[str, ast.Call, str]]:
    """Yield every ``<anything>.issue(...)`` call in ``src/loom`` and its innermost function."""
    for path in sorted(_SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding=_ENCODING), str(path))
        owner: dict[int, str] = {}
        # ast.walk visits outer functions first, so an inner one overwrites.
        for function in ast.walk(tree):
            if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                owner.update({id(node): function.name for node in ast.walk(function)})
        relative = path.relative_to(_SRC).as_posix()
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "issue"
            ):
                yield relative, node, owner.get(id(node), "<module>")


def _literal_member(node: ast.Call, catalogues: set[str]) -> str | None:
    """Return ``MEMBER`` when *node* is ``<one of catalogues>.MEMBER.issue(...)``."""
    target = node.func.value if isinstance(node.func, ast.Attribute) else None
    if not isinstance(target, ast.Attribute) or not isinstance(target.value, ast.Name):
        return None
    return target.attr if target.value.id in catalogues else None


def _catalogue_names() -> set[str]:
    """Return the name of every class in ``src/loom`` that subclasses ``IssueCode``."""
    return {
        node.name
        for path in _SRC.rglob("*.py")
        for node in ast.walk(ast.parse(path.read_text(encoding=_ENCODING)))
        if isinstance(node, ast.ClassDef)
        and any(isinstance(base, ast.Name) and base.id == "IssueCode" for base in node.bases)
    }
