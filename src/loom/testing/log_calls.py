"""The static guard of logging call sites: loom runs it on itself, an application on its code.

A logging call follows the record definition *(event, level, fields)*:

- the message is an attribute of a ``LogEvent`` subclass, with no positional
  arguments after it;
- ``extra``, the fields of ``log_context(...)`` and a span's ``attributes=``,
  when written as a literal, are a dict whose keys are attributes of a
  ``LogKey`` subclass (``LogField`` or an ``<Area>LogField``);
- no value is text made from a value: ``str(...)``, ``repr(...)``, an
  f-string, ``.format()``, ``%`` or the name an ``except`` clause bound;
- a ``**`` spread inside ``extra`` or ``log_context`` comes only from a short
  named list (:data:`ALLOWED_SPREADS`); span attributes are typed
  ``Mapping[LogKey, Scalar]``, so a type checker covers what they spread.

A logger is a name ending in ``log`` or ``logger`` (``_log``, ``self._logger``).
The exception travels as ``exc_info=``, never as a value.

Example::

    from pathlib import Path

    from loom.testing.log_calls import log_call_offences

    def test_logging_calls_follow_the_standard() -> None:
        assert log_call_offences(Path("src/billing").rglob("*.py")) == []
"""

from __future__ import annotations

import ast
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Final, NamedTuple

LOGGING_METHODS: Final = frozenset({"debug", "info", "warning", "error", "exception", "critical"})
"""The logger methods whose first argument is the message; ``log`` takes the level first."""

ATTRIBUTE_CALLS: Final = frozenset({"span", "open_span", "start", "end", "exception", "annotate"})
"""Calls whose ``attributes=`` literal is checked: spans and lifecycle events."""

ALLOWED_SPREADS: Final = frozenset({"run_fields", "attributes"})
"""What ``extra`` or ``log_context`` may spread: each holds vocabulary fields only.

``run_fields()`` is a job run's fields; ``attributes`` a lifecycle event's, typed
``Mapping[LogKey, Scalar]``."""

_TEXT_BUILDERS: Final = frozenset({"str", "repr", "format"})

_Check = Callable[[ast.Call], None]


class LogCallOffence(NamedTuple):
    """One logging call that breaks the standard: where, and which rule."""

    path: str
    line: int
    rule: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.rule}"


def log_call_offences(
    paths: Iterable[Path], *, allowed_spreads: frozenset[str] = ALLOWED_SPREADS
) -> list[LogCallOffence]:
    """Every logging call in *paths* that breaks the standard, in file and line order.

    Args:
        paths: The Python files to read.
        allowed_spreads: Methods whose result ``extra`` may spread.

    Returns:
        The offences; empty when every call complies.
    """
    offences: list[LogCallOffence] = []
    for path in sorted(paths):
        checker = _CallSiteChecker(str(path), allowed_spreads)
        checker.visit(ast.parse(path.read_text(encoding="utf-8"), str(path)))
        offences.extend(checker.offences)
    return offences


def _name_of(node: ast.expr) -> str | None:
    """The last name of a dotted reference: ``logger`` for ``self.logger``."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _is_logger(node: ast.expr) -> bool:
    name = _name_of(node)
    return name is not None and name.lower().endswith(("log", "logger"))


def _is_member_of(node: ast.expr | None, suffix: str) -> bool:
    """Whether *node* is ``SomethingSuffix.MEMBER``: an attribute of a vocabulary class."""
    return (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id.endswith(suffix)
    )


class _CallSiteChecker(ast.NodeVisitor):
    def __init__(self, path: str, allowed_spreads: frozenset[str]) -> None:
        self.path = path
        self.allowed_spreads = allowed_spreads
        self.offences: list[LogCallOffence] = []
        self._bound: list[str] = []

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.name is None:
            self.generic_visit(node)
            return
        self._bound.append(node.name)
        self.generic_visit(node)
        self._bound.pop()

    def visit_Call(self, node: ast.Call) -> None:
        for check in self._checks_of(node):
            check(node)
        self.generic_visit(node)

    def _checks_of(self, node: ast.Call) -> Iterator[_Check]:
        func = node.func
        if isinstance(func, ast.Attribute) and _is_logger(func.value):
            if func.attr in LOGGING_METHODS:
                yield self._check_message_at(0)
                yield self._check_extra
            elif func.attr == "log":
                yield self._check_message_at(1)
                yield self._check_extra
        if _name_of(func) == "log_context":
            yield self._check_log_context
        if _name_of(func) in ATTRIBUTE_CALLS:
            yield self._check_attributes

    def _offend(self, node: ast.AST, rule: str) -> None:
        self.offences.append(LogCallOffence(self.path, getattr(node, "lineno", 0), rule))

    def _check_message_at(self, index: int) -> _Check:
        def check(node: ast.Call) -> None:
            if len(node.args) <= index or not _is_member_of(node.args[index], "LogEvent"):
                self._offend(node, "the message is not a LogEvent member")
            if len(node.args) > index + 1:
                self._offend(node, "positional arguments after the event")

        return check

    def _check_extra(self, node: ast.Call) -> None:
        for keyword in node.keywords:
            if keyword.arg == "extra":
                self._check_fields(keyword.value, "extra", typed=False)

    def _check_log_context(self, node: ast.Call) -> None:
        for argument in node.args:
            self._check_fields(argument, "log_context", typed=False)
        for keyword in node.keywords:
            self._offend(keyword.value, f"log_context keyword {keyword.arg!r} is not a LogKey")

    def _check_attributes(self, node: ast.Call) -> None:
        for keyword in node.keywords:
            if keyword.arg == "attributes" and isinstance(keyword.value, ast.Dict):
                self._check_fields(keyword.value, "attributes", typed=True)

    def _check_fields(self, value: ast.expr, where: str, *, typed: bool) -> None:
        """Check a fields literal; *typed* fields (span attributes) may spread any typed mapping."""
        if not isinstance(value, ast.Dict):
            if not self._is_allowed_spread(value):
                self._offend(value, f"{where} is not a dict literal")
            return
        for key, item in zip(value.keys, value.values, strict=True):
            if key is None:
                if not (typed or self._is_allowed_spread(item)):
                    self._offend(item, f"{where} spreads a mapping outside the allowed list")
                continue
            if not _is_member_of(key, "LogField"):
                self._offend(key, f"{where} key is not a LogKey member")
            self._check_value(item, where)

    def _is_allowed_spread(self, item: ast.expr) -> bool:
        source = item.func if isinstance(item, ast.Call) else item
        return _name_of(source) in self.allowed_spreads

    def _check_value(self, value: ast.expr, where: str) -> None:
        for node in ast.walk(value):
            rule = self._text_rule(node)
            if rule is not None:
                self._offend(node, f"{where} value is {rule}")
                return

    def _text_rule(self, node: ast.AST) -> str | None:
        if isinstance(node, ast.JoinedStr):
            return "an f-string"
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
            return "a % format"
        if isinstance(node, ast.Call) and _name_of(node.func) in _TEXT_BUILDERS:
            return f"{_name_of(node.func)}(...)"
        if isinstance(node, ast.Name) and node.id in self._bound:
            return f"the except-bound name {node.id!r}"
        return None
