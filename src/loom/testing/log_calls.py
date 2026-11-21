"""The static guards of logging: loom runs them on itself, an application on its code.

:func:`log_call_offences` checks logging call sites; :func:`exception_text_offences`
finds every place an exception's text is formatted, which no field, payload
or new error may carry.

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

    from loom.testing.log_calls import exception_text_offences, log_call_offences

    def test_logging_calls_follow_the_standard() -> None:
        assert log_call_offences(Path("src/billing").rglob("*.py")) == []

    def test_no_exception_text_leaves() -> None:
        assert exception_text_offences(Path("src/billing").rglob("*.py")) == []
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


_EXCEPTION_SUFFIXES: Final = ("Error", "Exception", "ExceptionGroup")
"""Annotation names read as an exception type: ``ValueError``, ``BaseException``, ``LoomError``."""

_TRACEBACK_TEXT: Final = frozenset({"format_exc", "format_exception", "format_exception_only"})
"""``traceback`` functions whose text carries every message of the chain."""


def exception_text_offences(paths: Iterable[Path]) -> list[LogCallOffence]:
    """Every place in *paths* that formats an exception's text, in file and line order.

    An exception's message is never a payload: a field, an answer, a broker
    message or a new error renders its public form instead
    (:func:`loom.core.logger.failure.public_form`). An exception is a name an
    ``except`` clause bound, or a name annotated with an exception type
    (a parameter or an annotated assignment); its text is ``str``, ``repr``,
    ``ascii`` or ``format`` of it or of its ``.args``, an f-string or a ``%``
    or ``.format()`` that holds them, or the text ``traceback`` formats.

    It cannot see aliasing through helpers or text built in user code; a
    behavioural canary covers those.

    Args:
        paths: The Python files to read.

    Returns:
        The offences; empty when nothing formats an exception.
    """
    offences: list[LogCallOffence] = []
    for path in sorted(paths):
        checker = _ExceptionTextChecker(str(path))
        checker.visit(ast.parse(path.read_text(encoding="utf-8"), str(path)))
        offences.extend(checker.offences)
    return offences


def _is_exception_annotation(annotation: ast.expr | None) -> bool:
    if annotation is None:
        return False
    return any(
        (name := _name_of(node)) is not None and name.endswith(_EXCEPTION_SUFFIXES)
        for node in ast.walk(annotation)
        if isinstance(node, ast.Name | ast.Attribute)
    )


class _ExceptionTextChecker(ast.NodeVisitor):
    def __init__(self, path: str) -> None:
        self.path = path
        self.offences: list[LogCallOffence] = []
        self._scopes: list[set[str]] = [set()]

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        arguments = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
        typed = {arg.arg for arg in arguments if _is_exception_annotation(arg.annotation)}
        self._scopes.append(typed)
        self.generic_visit(node)
        self._scopes.pop()

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if isinstance(node.target, ast.Name) and _is_exception_annotation(node.annotation):
            self._scopes[-1].add(node.target.id)
        self.generic_visit(node)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.name is None:
            self.generic_visit(node)
            return
        scope = self._scopes[-1]
        bound_before = node.name in scope
        scope.add(node.name)
        self.generic_visit(node)
        if not bound_before:
            scope.discard(node.name)

    def visit_Call(self, node: ast.Call) -> None:
        name = _name_of(node.func)
        if name in _TRACEBACK_TEXT:
            self._offend(node, f"traceback.{name}(...)")
        elif isinstance(node.func, ast.Name) and name in {"str", "repr", "ascii", "format"}:
            self._offend_on(node.args[:1], node, f"{name}(...)")
        elif name == "format" and isinstance(node.func, ast.Attribute):
            values = [*node.args, *(keyword.value for keyword in node.keywords)]
            self._offend_on(values, node, ".format(...)")
        self.generic_visit(node)

    def visit_FormattedValue(self, node: ast.FormattedValue) -> None:
        self._offend_on([node.value], node, "an f-string")
        self.generic_visit(node)

    def visit_BinOp(self, node: ast.BinOp) -> None:
        if isinstance(node.op, ast.Mod) and isinstance(node.left, ast.Constant | ast.JoinedStr):
            right = node.right
            values = list(right.elts) if isinstance(right, ast.Tuple) else [right]
            self._offend_on(values, node, "a % format")
        self.generic_visit(node)

    def _offend_on(self, values: Iterable[ast.expr], node: ast.AST, how: str) -> None:
        for value in values:
            exception = self._exception_in(value)
            if exception is not None:
                self._offend(node, f"{how} of the exception {exception!r}")
                return

    def _exception_in(self, value: ast.expr) -> str | None:
        """The exception name *value* is, or whose ``.args`` it reads, if any."""
        if isinstance(value, ast.Starred):
            value = value.value
        if isinstance(value, ast.Subscript):
            value = value.value
            if not (isinstance(value, ast.Attribute) and value.attr == "args"):
                return None
        if isinstance(value, ast.Attribute) and value.attr == "args":
            value = value.value
        if isinstance(value, ast.Name) and any(value.id in scope for scope in self._scopes):
            return value.id
        return None

    def _offend(self, node: ast.AST, rule: str) -> None:
        self.offences.append(LogCallOffence(self.path, getattr(node, "lineno", 0), rule))
