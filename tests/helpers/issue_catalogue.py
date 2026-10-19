"""The contract every compile-issue catalogue keeps (ADR 0003 part B).

A catalogue member builds its issue from keyword parameters, so the type
checker no longer sees a missing or misspelt one. Each pillar's catalogue test
uses these two checks instead: per member, its template renders from exactly
its declared parameters; per call site in ``src/loom``, the call passes exactly
those parameters.
"""

from __future__ import annotations

import ast
from pathlib import Path
from string import Formatter
from typing import NamedTuple

from loom.core.errors.codes import IssueCode

_SRC = Path(__file__).resolve().parents[2] / "src" / "loom"


class IssueCall(NamedTuple):
    """One ``<Catalogue>.<MEMBER>.issue(...)`` call found in ``src/loom``.

    ``keywords`` is ``None`` when the call passes positional arguments or
    ``**kwargs``, which a static check cannot match against the template.
    """

    location: str
    member: str
    keywords: frozenset[str] | None


def check_renders(code: IssueCode) -> None:
    """Assert that *code* renders its issue from exactly its declared parameters."""
    template = code.template
    parts = (template.text, template.component, template.field or "")
    names = {
        name for part in parts for _, name, _, _ in Formatter().parse(part) if name is not None
    }

    assert template.text.strip(), f"{code} has an empty message template"
    assert all(name.isidentifier() for name in names), f"{code}: positional or dotted placeholder"
    assert names == code.params
    issue = code.issue(**{name: f"<{name}>" for name in code.params})
    rendered = " ".join((issue.message, issue.component, issue.field or ""))
    assert issue.code is code
    assert all(f"<{name}>" in rendered for name in code.params)


def issue_calls(catalogue: type[IssueCode]) -> list[IssueCall]:
    """Return every ``<catalogue>.<MEMBER>.issue(...)`` call in ``src/loom``."""
    calls: list[IssueCall] = []
    for path in sorted(_SRC.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), str(path))):
            member = _issued_member(node, catalogue.__name__)
            if member is None:
                continue
            assert isinstance(node, ast.Call)
            named = [keyword.arg for keyword in node.keywords]
            positional = bool(node.args) or None in named
            keywords = None if positional else frozenset(name for name in named if name)
            calls.append(IssueCall(f"{path.relative_to(_SRC)}:{node.lineno}", member, keywords))
    return calls


def _issued_member(node: ast.AST, catalogue: str) -> str | None:
    """Return ``MEMBER`` when *node* is ``<catalogue>.MEMBER.issue(...)``."""
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return None
    target = node.func.value
    if node.func.attr != "issue" or not isinstance(target, ast.Attribute):
        return None
    if isinstance(target.value, ast.Name) and target.value.id == catalogue:
        return target.attr
    return None
