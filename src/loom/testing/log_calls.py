"""The static guards of logging: loom runs them on itself, an application on its code.

:func:`log_call_offences` checks logging call sites; :func:`exception_text_offences`
finds every place an exception's text is formatted, which no field, payload
or new error may carry; :func:`free_text_offences` finds every exception
raised with text built from values, where a ``LoomError`` takes declared attributes.

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

    def test_errors_are_built_from_fields() -> None:
        assert free_text_offences(Path("src/billing").rglob("*.py")) == []
"""

from __future__ import annotations

import ast
import builtins
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


class _Checker(ast.NodeVisitor):
    """A visitor of one file that collects the offences it finds."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.offences: list[LogCallOffence] = []

    def _offend(self, node: ast.AST, rule: str) -> None:
        self.offences.append(LogCallOffence(self.path, getattr(node, "lineno", 0), rule))

    @staticmethod
    def _name_of(node: ast.expr) -> str | None:
        """The last name of a dotted reference: ``logger`` for ``self.logger``."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        return None

    @staticmethod
    def _is_member_of(node: ast.expr | None, suffix: str) -> bool:
        """Whether *node* is ``SomethingSuffix.MEMBER``: an attribute of a vocabulary class."""
        return (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id.endswith(suffix)
        )


class _CallSiteChecker(_Checker):
    def __init__(self, path: str, allowed_spreads: frozenset[str]) -> None:
        super().__init__(path)
        self.allowed_spreads = allowed_spreads
        self._bound: list[str] = []

    def _is_logger(self, node: ast.expr) -> bool:
        name = self._name_of(node)
        return name is not None and name.lower().endswith(("log", "logger"))

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
        if isinstance(func, ast.Attribute) and self._is_logger(func.value):
            if func.attr in LOGGING_METHODS:
                yield self._check_message_at(0)
                yield self._check_extra
            elif func.attr == "log":
                yield self._check_message_at(1)
                yield self._check_extra
        if self._name_of(func) == "log_context":
            yield self._check_log_context
        if self._name_of(func) in ATTRIBUTE_CALLS:
            yield self._check_attributes

    def _check_message_at(self, index: int) -> _Check:
        def check(node: ast.Call) -> None:
            if len(node.args) <= index or not self._is_member_of(node.args[index], "LogEvent"):
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
            if not self._is_member_of(key, "LogField"):
                self._offend(key, f"{where} key is not a LogKey member")
            self._check_value(item, where)

    def _is_allowed_spread(self, item: ast.expr) -> bool:
        source = item.func if isinstance(item, ast.Call) else item
        return self._name_of(source) in self.allowed_spreads

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
        if isinstance(node, ast.Call) and self._name_of(node.func) in _TEXT_BUILDERS:
            return f"{self._name_of(node.func)}(...)"
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
    message or a new error carries its type or its declared attributes instead
    (:class:`loom.core.errors.failure.Failure`). An exception is a
    name an ``except`` clause bound, a name annotated with an exception type (a
    parameter or an annotated assignment, through a module type alias too), a
    name a ``for`` loop unpacks from a container of those, or a name assigned
    ``<future>.exception()``; its text is ``str``, ``repr``,
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


_CONTAINERS: Final = frozenset(
    {"list", "set", "frozenset", "Sequence", "Iterable", "Iterator", "Collection", "tuple"}
)
"""Generic types whose first argument is the type of what a ``for`` loop yields."""


class _ExceptionTextChecker(_Checker):
    """Find exception text, knowing which names hold an exception in each scope.

    A name holds one when an ``except`` clause binds it, when it is annotated
    with an exception type (a parameter or an annotated assignment, through a
    module type alias too), when a ``for`` loop unpacks it from a container of
    those, or when it is assigned ``<future>.exception()``.
    """

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self._scopes: list[set[str]] = [set()]
        self._annotations: list[dict[str, ast.expr]] = [{}]
        self._aliases: dict[str, ast.expr] = {}

    def visit_Module(self, node: ast.Module) -> None:
        for statement in node.body:
            if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
                target = statement.targets[0]
                if isinstance(target, ast.Name):
                    self._aliases[target.id] = statement.value
            elif (
                isinstance(statement, ast.AnnAssign)
                and isinstance(statement.target, ast.Name)
                and statement.value is not None
                and self._name_of(statement.annotation) == "TypeAlias"
            ):
                self._aliases[statement.target.id] = statement.value
        self.generic_visit(node)

    def _resolved(self, annotation: ast.expr) -> ast.expr:
        """*annotation*, or what the module type alias it names stands for."""
        if isinstance(annotation, ast.Name) and annotation.id in self._aliases:
            return self._aliases[annotation.id]
        return annotation

    def _is_exception_annotation(self, annotation: ast.expr | None) -> bool:
        if annotation is None:
            return False
        return any(
            (name := self._name_of(node)) is not None and name.endswith(_EXCEPTION_SUFFIXES)
            for node in ast.walk(self._resolved(annotation))
            if isinstance(node, ast.Name | ast.Attribute) and not isinstance(node.ctx, ast.Store)
        ) and not self._is_container(annotation)

    def _is_container(self, annotation: ast.expr) -> bool:
        resolved = self._resolved(annotation)
        return isinstance(resolved, ast.Subscript) and self._name_of(resolved.value) in _CONTAINERS

    def _element_of(self, annotation: ast.expr) -> ast.expr | None:
        """The type a ``for`` loop over a value of *annotation* yields, if it is a container."""
        resolved = self._resolved(annotation)
        if not self._is_container(resolved):
            return None
        assert isinstance(resolved, ast.Subscript)
        arguments = resolved.slice
        return arguments.elts[0] if isinstance(arguments, ast.Tuple) else arguments

    def _bind(self, name: str, annotation: ast.expr | None) -> None:
        if annotation is not None:
            self._annotations[-1][name] = annotation
        if self._is_exception_annotation(annotation):
            self._scopes[-1].add(name)

    def _annotation_of(self, name: str) -> ast.expr | None:
        for scope in reversed(self._annotations):
            if name in scope:
                return scope[name]
        return None

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self._scopes.append(set())
        self._annotations.append({})
        for arg in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]:
            self._bind(arg.arg, arg.annotation)
        self.generic_visit(node)
        self._annotations.pop()
        self._scopes.pop()

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if isinstance(node.target, ast.Name):
            self._bind(node.target.id, node.annotation)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        value = node.value
        is_future_exception = (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Attribute)
            and value.func.attr == "exception"
            and not value.args
        )
        if is_future_exception:
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self._scopes[-1].add(target.id)
        self.generic_visit(node)

    def visit_For(self, node: ast.For | ast.AsyncFor) -> None:
        iterated = node.iter
        annotation = self._annotation_of(iterated.id) if isinstance(iterated, ast.Name) else None
        element = None if annotation is None else self._element_of(annotation)
        if element is not None:
            self._bind_unpacked(node.target, element)
        self.generic_visit(node)

    visit_AsyncFor = visit_For

    def _bind_unpacked(self, target: ast.expr, element: ast.expr) -> None:
        if isinstance(target, ast.Name):
            self._bind(target.id, element)
            return
        resolved = self._resolved(element)
        if not (isinstance(target, ast.Tuple) and isinstance(resolved, ast.Subscript)):
            return
        if not isinstance(resolved.slice, ast.Tuple):
            return
        for item, kind in zip(target.elts, resolved.slice.elts, strict=False):
            self._bind_unpacked(item, kind)

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
        name = self._name_of(node.func)
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


def free_text_offences(paths: Iterable[Path]) -> list[LogCallOffence]:
    """Every exception in *paths* raised with free text, in file and line order.

    A ``LoomError`` is built from its declared attributes, ``Cls(**attributes)``, which
    it renders itself; text an f-string, a ``%`` or ``.format()`` builds is
    free text, whatever it holds. A fixed literal is not. An exception of a
    class outside the package (a builtin, a library's) takes a literal or the
    text of a failure loom describes, ``Failure(error).text``, and nothing else:
    any other value it carries out is free text.

    Args:
        paths: The Python files to read.

    Returns:
        The offences; empty when every raise builds its error from fields.
    """
    offences: list[LogCallOffence] = []
    for path in sorted(paths):
        tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
        checker = _FreeTextChecker(str(path), tree)
        checker.visit(tree)
        offences.extend(checker.offences)
    return offences


_FAILURE_TEXT: Final = frozenset({"text", "public_message"})
"""What ``Failure`` says about an exception that a foreign exception may carry."""


class _FreeTextChecker(_Checker):
    """Find each raise built from free text, knowing which classes come from outside the package.

    Args:
        path: The file, for the report.
        tree: Its AST, read for the classes it imports and defines.
    """

    def __init__(self, path: str, tree: ast.Module) -> None:
        super().__init__(path)
        self._package = path.replace("\\", "/").split("/src/", 1)[-1].split("/", 1)[0]
        self._foreign = self._foreign_names(tree)

    def visit_Raise(self, node: ast.Raise) -> None:
        raised = node.exc
        if isinstance(raised, ast.Call):
            arguments = [*raised.args, *(keyword.value for keyword in raised.keywords)]
            if any(map(self._is_built_text, raised.args)):
                self._offend(node, f"free text passed to {self._name_of(raised.func)}(...)")
            elif self._is_foreign(raised.func) and not all(map(self._is_safe, arguments)):
                callee = self._name_of(raised.func)
                self._offend(node, f"a value passed to the foreign {callee}(...)")
        self.generic_visit(node)

    def _is_foreign(self, func: ast.expr) -> bool:
        root = func
        while isinstance(root, ast.Attribute):
            root = root.value
        return isinstance(root, ast.Name) and root.id in self._foreign

    def _foreign_names(self, tree: ast.Module) -> set[str]:
        """The exception names this file takes from outside its package: imports and builtins."""
        names = {
            name
            for name, value in vars(builtins).items()
            if isinstance(value, type) and issubclass(value, BaseException)
        }
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module is not None:
                if node.module.split(".", 1)[0] != self._package:
                    names.update(alias.asname or alias.name for alias in node.names)
            elif isinstance(node, ast.Import):
                names.update(
                    alias.asname or alias.name.split(".", 1)[0]
                    for alias in node.names
                    if alias.name.split(".", 1)[0] != self._package
                )
            elif isinstance(node, ast.ClassDef):
                names.discard(node.name)
        return names

    @classmethod
    def _is_safe(cls, node: ast.expr) -> bool:
        """Whether *node* is a literal, or what ``Failure(...)`` says may describe a failure."""
        if isinstance(node, ast.Constant):
            return True
        target = node.func if isinstance(node, ast.Call) else node
        return (
            isinstance(target, ast.Attribute)
            and target.attr in _FAILURE_TEXT
            and isinstance(target.value, ast.Call)
            and cls._name_of(target.value.func) == "Failure"
        )

    @staticmethod
    def _is_built_text(node: ast.expr) -> bool:
        """Whether *node* is text built from values: a valued f-string, a ``%`` or ``.format()``."""
        if isinstance(node, ast.JoinedStr):
            return any(isinstance(part, ast.FormattedValue) for part in node.values)
        if isinstance(node, ast.BinOp):
            return isinstance(node.op, ast.Mod) and isinstance(
                node.left, ast.Constant | ast.JoinedStr
            )
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and (node.func.attr == "format" and isinstance(node.func.value, ast.Constant))
        )
