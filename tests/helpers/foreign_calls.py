"""Find each callable ``src/loom`` hands to code outside loom, and whether the boundary wraps it.

Rule 1 of spec 019, read from the source: any function, lambda, bound method
or closure passed as an argument, or applied as a decorator, to a callee
outside the ``loom`` package is wrapped by ``boundary(...)``. A closure a
factory returns is handed on too: at the factory's call sites, and wherever
the factory's signature names a foreign framework's type.

A callee is loom's when its name resolves to the ``loom`` package: an
import, a definition in the same module, ``self``, or a receiver whose type
an annotation, a constructor call or a class attribute declares. Any other
callee is foreign, so a receiver the scan cannot type counts as foreign.
:data:`EXEMPT_CALLEES` names the foreign callees that need no boundary: a
descriptor or decorator of loom's own call, a store of the callable, a call
inside loom's own call that raises the exception as it is, an executor that
hands it back to loom, and the test runner. Each entry carries its reason.
"""

from __future__ import annotations

import ast
import builtins
import sys
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Final

from tests.helpers.source_tree import SRC, parsed

LOOM: Final = "loom"
BOUNDARY: Final = "boundary"

_DESCRIPTOR: Final = "Python's descriptor protocol: loom's own attribute access calls it"
_OWN_CALL: Final = "wraps loom's own call; the exception reaches loom's caller as it is"
_NEVER_CALLED: Final = "reads or marks the callable and never calls it"
_STORED: Final = "stores the callable as a value; whoever calls it later is checked"
_PROPAGATES: Final = (
    "the stdlib or msgspec calls it inside loom's own call and raises its exception as it is"
)
_HANDED_BACK: Final = (
    "runs it on another thread or task and hands its exception back to loom's await or result"
)
_TEST_RUNNER: Final = "a test runner or test-data factory, whose report must show a failure whole"

EXEMPT_CALLEES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "builtins.property": _DESCRIPTOR,
        "builtins.classmethod": _DESCRIPTOR,
        "builtins.staticmethod": _DESCRIPTOR,
        "functools.cached_property": _DESCRIPTOR,
        "functools.cache": _OWN_CALL,
        "functools.lru_cache": _OWN_CALL,
        "functools.singledispatch": _OWN_CALL,
        "contextlib.contextmanager": _OWN_CALL,
        "contextlib.asynccontextmanager": _OWN_CALL,
        "abc.abstractmethod": _NEVER_CALLED,
        "typing.overload": _NEVER_CALLED,
        "functools.wraps": _NEVER_CALLED,
        "functools.update_wrapper": _NEVER_CALLED,
        "functools.partial": "binds arguments; whoever receives the partial is checked",
        "typing.cast": _NEVER_CALLED,
        "builtins.setattr": _STORED,
        "builtins.object.__setattr__": _STORED,
        "builtins.getattr": _STORED,
        "builtins.sorted": _PROPAGATES,
        "builtins.min": _PROPAGATES,
        "builtins.map": _PROPAGATES,
        "builtins.list.sort": _PROPAGATES,
        "collections.defaultdict": _PROPAGATES,
        "dataclasses.field": _PROPAGATES,
        "msgspec.field": _PROPAGATES,
        "msgspec.to_builtins": _PROPAGATES,
        "msgspec.json.encode": _PROPAGATES,
        "msgspec.json.Encoder": _PROPAGATES,
        "json.dumps": _PROPAGATES,
        "re.compile.sub": _PROPAGATES,
        "types.new_class": _PROPAGATES,
        "pkgutil.walk_packages": _PROPAGATES,
        "concurrent.futures.ThreadPoolExecutor.submit": _HANDED_BACK,
        "asyncio.to_thread": _HANDED_BACK,
        "anyio.from_thread.BlockingPortal.call": _HANDED_BACK,
        "anyio.create_task_group.start_soon": _HANDED_BACK,
        "sqlalchemy.ext.asyncio.AsyncConnection.run_sync": _HANDED_BACK,
        "pytest.fixture": _TEST_RUNNER,
        "polyfactory.fields.Use": _TEST_RUNNER,
    }
)
"""Foreign callees that need no boundary, each with the reason; every entry is used."""

_FOREIGN_FREE_ROOTS: Final = frozenset(sys.stdlib_module_names) | {"typing_extensions", "msgspec"}
"""Modules whose types in a signature are data, not a framework that calls back."""

_QUALIFIERS: Final = frozenset({"typing.Final", "typing.ClassVar", "typing.Optional"})
"""Annotation wrappers that declare no type of their own."""

_UNTYPED: Final = frozenset({"typing.Any", "typing.Final", "typing.ClassVar", "builtins.object"})
"""Annotations that leave a receiver untyped."""


@dataclass(frozen=True)
class Handoff:
    """One callable handed to a foreign callee.

    Attributes:
        path: The module, relative to ``src/loom``.
        line: Where it is handed.
        callee: The foreign callee, its name resolved.
        wrapped: Whether ``boundary(...)`` wraps it.
    """

    path: str
    line: int
    callee: str
    wrapped: bool

    @property
    def is_exempt(self) -> bool:
        """Whether the callee only computes over loom's data inside loom's call."""
        return self.callee in EXEMPT_CALLEES

    @property
    def offends(self) -> bool:
        """Whether this hand-off breaks the rule."""
        return not self.wrapped and not self.is_exempt

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.callee} takes a callable not wrapped by boundary"


@dataclass
class _Scope:
    """What one function body, or the module, binds."""

    bound: set[str] = field(default_factory=set)
    functions: set[str] = field(default_factory=set)
    wrapped: set[str] = field(default_factory=set)
    types: dict[str, str] = field(default_factory=dict)


@dataclass
class _ClassFacts:
    """What the scan knows about the class whose body it is in."""

    qualified: str
    methods: frozenset[str]
    callables: frozenset[str]
    attributes: dict[str, str]
    foreign_base: str | None


@dataclass(frozen=True)
class _Module:
    """The names one module binds at its top level, resolved."""

    dotted: str
    imports: Mapping[str, str]
    functions: frozenset[str]
    classes: frozenset[str]
    aliases: Mapping[str, ast.expr]
    factories: frozenset[str]

    @classmethod
    def of(cls, dotted: str, tree: ast.Module) -> _Module:
        body = tree.body
        functions = {n.name for n in body if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)}
        return cls(
            dotted=dotted,
            imports=MappingProxyType(_imports(dotted, tree)),
            functions=frozenset(functions),
            classes=frozenset(n.name for n in body if isinstance(n, ast.ClassDef)),
            aliases=MappingProxyType(_aliases(body)),
            factories=frozenset(
                n.name
                for n in body
                if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef) and _returns_closure(n)
            ),
        )

    def qualify(self, name: str) -> str | None:
        """The dotted name *name* stands for at the top level, or ``None``."""
        if name in self.imports:
            return self.imports[name]
        if name in self.functions or name in self.classes:
            return f"{self.dotted}.{name}"
        if hasattr(builtins, name):
            return f"builtins.{name}"
        return None

    def foreign_types_in(self, annotation: ast.expr | None, depth: int = 0) -> set[str]:
        """The framework types *annotation* names, aliases and string forms followed."""
        found: set[str] = set()
        if annotation is None or depth > 3:
            return found
        for node in ast.walk(annotation):
            match node:
                case ast.Constant(value=str(text)):
                    try:
                        found |= self.foreign_types_in(ast.parse(text, mode="eval").body, depth + 1)
                    except SyntaxError:
                        continue
                case ast.Name(id=name) if name in self.aliases:
                    found |= self.foreign_types_in(self.aliases[name], depth + 1)
                case ast.Name(id=name) if name in self.imports:
                    qualified = self.imports[name]
                    if _is_framework(qualified):
                        found.add(qualified)
        return found


class ForeignHandoffScan(ast.NodeVisitor):
    """Collect every callable one module hands to a foreign callee.

    Args:
        path: The module's path, for the report.
        dotted: The module's dotted name.
        tree: The module's AST.
    """

    def __init__(self, path: str, dotted: str, tree: ast.Module) -> None:
        self.path = path
        self.handoffs: list[Handoff] = []
        self.calls_checked = 0
        self._module = _Module.of(dotted, tree)
        self._scopes: list[_Scope] = []
        self._classes: list[_ClassFacts] = []

    def visit_Module(self, node: ast.Module) -> None:
        self._scopes.append(self._scope_of(node.body, ()))
        self.generic_visit(node)
        self._scopes.pop()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for decorator in node.decorator_list:
            self.visit(decorator)
        self._classes.append(self._class_facts(node))
        for statement in node.body:
            self.visit(statement)
        self._classes.pop()

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self._check_decorators(node)
        self._check_escape(node)
        for decorator in node.decorator_list:
            self.visit(decorator)
        self._scopes.append(self._scope_of(node.body, _parameters(node)))
        for statement in node.body:
            self.visit(statement)
        self._scopes.pop()

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function

    def visit_Call(self, node: ast.Call) -> None:
        handed = (*node.args, *(k.value for k in node.keywords))
        functions = [arg for arg in handed if self._is_function(arg) or self._is_boundary(arg)]
        if functions and not self._is_boundary(node.func):
            callee = self._owner(node.func)
            if not self._is_loom(callee):
                self.calls_checked += 1
                for arg in functions:
                    self._record(arg.lineno, callee, wrapped=self._is_boundary(arg))
        self.generic_visit(node)

    def _check_decorators(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        decorators = node.decorator_list
        for index, decorator in enumerate(decorators):
            if self._is_boundary(decorator):
                continue
            callee = self._owner(decorator.func if isinstance(decorator, ast.Call) else decorator)
            if self._is_loom(callee):
                continue
            below = decorators[index + 1 :]
            self.calls_checked += 1
            self._record(decorator.lineno, callee, wrapped=any(map(self._is_boundary, below)))

    def _check_escape(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        """A returned closure whose parameters take a framework's objects: it calls it."""
        nested = {
            n.name: n for n in node.body if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)
        }
        wrapped = self._scope_of(node.body, ()).wrapped
        for returned in _returned(node):
            closure = nested.get(returned.id) if isinstance(returned, ast.Name) else None
            if closure is None:
                continue
            frameworks: set[str] = set()
            for arg in _parameters(closure):
                frameworks |= self._module.foreign_types_in(arg.annotation)
            if frameworks:
                self.calls_checked += 1
                self._record(returned.lineno, min(frameworks), wrapped=returned.id in wrapped)

    def _record(self, line: int, callee: str | None, *, wrapped: bool) -> None:
        self.handoffs.append(Handoff(self.path, line, callee or "<untyped receiver>", wrapped))

    def _is_function(self, node: ast.expr) -> bool:
        match node:
            case ast.Lambda():
                return True
            case ast.Name(id=name):
                scope = self._binding(name)
                return name in self._module.functions if scope is None else name in scope.functions
            case ast.Attribute(value=ast.Name(id="self" | "cls"), attr=name):
                return bool(self._classes) and name in self._classes[-1].callables
            case ast.Call(func=ast.Name(id=name)) if name in self._module.factories:
                return True
            case ast.Call(func=func, args=[first, *_]) if self._owner(func) == "functools.partial":
                return self._is_function(first)
        return False

    def _is_boundary(self, node: ast.expr | None) -> bool:
        match node:
            case ast.Name(id=name) | ast.Call(func=ast.Name(id=name)):
                scope = self._binding(name)
                return name == BOUNDARY or (scope is not None and name in scope.wrapped)
        return False

    def _binding(self, name: str) -> _Scope | None:
        """The innermost scope that binds *name*, or ``None`` for a module-level import."""
        return next((scope for scope in reversed(self._scopes) if name in scope.bound), None)

    def _is_loom(self, owner: str | None) -> bool:
        return owner is not None and (owner == LOOM or owner.startswith(f"{LOOM}."))

    def _owner(self, node: ast.expr) -> str | None:
        """The dotted name of what *node* calls, through the module, the scope and the class."""
        match node:
            case ast.Name(id="self" | "cls"):
                return self._type_of(node)
            case ast.Name(id=name):
                return self._name_owner(name)
            case ast.Call(func=func):
                return self._owner(func)
            case ast.Attribute(value=value, attr=attr):
                receiver = self._type_of(value)
                return None if receiver is None else f"{receiver}.{attr}"
        return None

    def _name_owner(self, name: str) -> str | None:
        scope = self._binding(name)
        if scope is not None and scope is not self._scopes[0]:
            if name in scope.functions:
                return f"{self._module.dotted}.{name}"
            return scope.types.get(name)
        if self._classes and name in self._classes[-1].methods:
            return self._classes[-1].qualified
        if scope is not None and name in scope.types:
            return scope.types[name]
        return self._module.qualify(name)

    def _type_of(self, node: ast.expr) -> str | None:
        """The dotted type, or module, of an expression a method is called on."""
        match node:
            case ast.Name(id="self" | "cls") if self._classes:
                facts = self._classes[-1]
                return facts.foreign_base or facts.qualified
            case ast.Name(id=name):
                return self._name_owner(name)
            case ast.Attribute(value=ast.Name(id="self" | "cls"), attr=attr) if self._classes:
                facts = self._classes[-1]
                if attr in facts.attributes:
                    return facts.attributes[attr]
                if attr in facts.methods:
                    return f"{facts.qualified}.{attr}"
                return None
            case ast.Attribute(value=value, attr=attr):
                receiver = self._type_of(value)
                return None if receiver is None else f"{receiver}.{attr}"
            case ast.Call(func=func):
                return self._owner(func)
        return None

    def _annotation_type(self, annotation: ast.expr | None) -> str | None:
        """The dotted type an annotation declares; ``X | None`` and ``Optional[X]`` give ``X``."""
        match annotation:
            case ast.Constant(value=str(text)):
                try:
                    return self._annotation_type(ast.parse(text, mode="eval").body)
                except SyntaxError:
                    return None
            case ast.BinOp(left=left, op=ast.BitOr(), right=ast.Constant(value=None)):
                return self._annotation_type(left)
            case ast.Subscript(value=wrapper, slice=inner) if self._type_of(wrapper) in _QUALIFIERS:
                return self._annotation_type(inner)
            case ast.Subscript(value=value):
                return self._annotation_type(value)
            case ast.Name() | ast.Attribute():
                declared = self._type_of(annotation)
                return None if declared in _UNTYPED else declared
        return None

    def _value_type(self, value: ast.expr | None) -> str | None:
        """The dotted type a constructor call, an awaited one, or a typed name gives."""
        match value:
            case ast.Await(value=inner):
                return self._value_type(inner)
            case ast.Call(func=func, args=[declared, *_]) if self._owner(func) == "typing.cast":
                return self._annotation_type(declared)
            case ast.Call(func=func):
                return self._owner(func)
            case ast.Name() | ast.Attribute():
                return self._type_of(value)
            case ast.List() | ast.ListComp():
                return "builtins.list"
            case ast.Dict() | ast.DictComp():
                return "builtins.dict"
        return None

    def _scope_of(self, body: list[ast.stmt], parameters: tuple[ast.arg, ...]) -> _Scope:
        scope = _Scope()
        self._scopes.append(scope)
        for arg in parameters:
            scope.bound.add(arg.arg)
            declared = self._annotation_type(arg.annotation)
            if declared is not None:
                scope.types[arg.arg] = declared
        for node in _own_statements(body):
            self._bind(scope, node)
        self._scopes.pop()
        return scope

    def _bind(self, scope: _Scope, node: ast.AST) -> None:
        match node:
            case ast.FunctionDef(name=name) | ast.AsyncFunctionDef(name=name):
                scope.bound.add(name)
                scope.functions.add(name)
                if any(self._is_boundary(d) for d in node.decorator_list):
                    scope.wrapped.add(name)
            case ast.ClassDef(name=name):
                scope.bound.add(name)
                scope.types[name] = f"{self._module.dotted}.{name}"
            case ast.Assign(targets=[ast.Name(id=name)], value=value):
                self._bind_value(scope, name, value, None)
            case ast.AnnAssign(target=ast.Name(id=name), value=value, annotation=annotation):
                self._bind_value(scope, name, value, annotation)
            case ast.Assign(targets=targets):
                scope.bound.update(
                    n.id for t in targets for n in ast.walk(t) if isinstance(n, ast.Name)
                )
            case ast.For(target=target) | ast.AsyncFor(target=target):
                scope.bound.update(n.id for n in ast.walk(target) if isinstance(n, ast.Name))
            case ast.With(items=items) | ast.AsyncWith(items=items):
                for item in items:
                    if isinstance(item.optional_vars, ast.Name):
                        scope.bound.add(item.optional_vars.id)
                        bound = self._value_type(item.context_expr)
                        if bound is not None:
                            scope.types[item.optional_vars.id] = bound

    def _bind_value(
        self, scope: _Scope, name: str, value: ast.expr | None, annotation: ast.expr | None
    ) -> None:
        scope.bound.add(name)
        if self._is_boundary(value):
            scope.wrapped.add(name)
            scope.functions.add(name)
            return
        if value is not None and self._is_function(value):
            scope.functions.add(name)
            return
        declared = self._annotation_type(annotation) or self._value_type(value)
        if declared is not None:
            scope.types[name] = declared

    def _class_facts(self, node: ast.ClassDef) -> _ClassFacts:
        qualified = f"{self._module.dotted}.{node.name}"
        defined = [n for n in node.body if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)]
        methods = frozenset(n.name for n in defined)
        callables = frozenset(n.name for n in defined if not _is_attribute_access(n))
        bases = [self._type_of(base) for base in node.bases]
        foreign = next((b for b in bases if b is not None and not self._is_loom(b)), None)
        facts = _ClassFacts(qualified, methods, callables, {}, foreign_base=None)
        self._classes.append(facts)
        for statement in node.body:
            if isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
                declared = self._annotation_type(statement.annotation)
                if declared is not None:
                    facts.attributes[statement.target.id] = declared
        for method in node.body:
            if isinstance(method, ast.FunctionDef | ast.AsyncFunctionDef):
                facts.attributes.update(self._self_attributes(method))
        self._classes.pop()
        facts.foreign_base = None if _is_free(foreign) else foreign
        return facts

    def _self_attributes(self, method: ast.FunctionDef | ast.AsyncFunctionDef) -> dict[str, str]:
        scope = self._scope_of([], _parameters(method))
        self._scopes.append(scope)
        found: dict[str, str] = {}
        for node in ast.walk(method):
            match node:
                case ast.Assign(
                    targets=[ast.Attribute(value=ast.Name(id="self"), attr=attr)], value=value
                ):
                    declared = self._value_type(value)
                case ast.AnnAssign(
                    target=ast.Attribute(value=ast.Name(id="self"), attr=attr),
                    annotation=annotation,
                    value=value,
                ):
                    declared = self._annotation_type(annotation) or self._value_type(value)
                case _:
                    continue
            if declared is not None:
                found.setdefault(attr, declared)
        self._scopes.pop()
        return found


def _is_attribute_access(method: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Whether a method is read as an attribute: a property, its setter, a cached property."""
    for decorator in method.decorator_list:
        match decorator:
            case ast.Name(id="property" | "cached_property"):
                return True
            case ast.Attribute(attr="setter" | "deleter" | "cached_property"):
                return True
    return False


def _is_framework(qualified: str) -> bool:
    return not qualified.startswith(f"{LOOM}.") and not _is_free(qualified)


def _is_free(qualified: str | None) -> bool:
    return qualified is None or qualified.split(".", 1)[0] in _FOREIGN_FREE_ROOTS - {"builtins"}


def _parameters(node: ast.FunctionDef | ast.AsyncFunctionDef) -> tuple[ast.arg, ...]:
    arguments = node.args
    return (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs)


def _own_statements(body: list[ast.stmt]) -> Iterator[ast.AST]:
    """A body's statements and its blocks', in source order, not those of nested functions."""
    pending: list[ast.AST] = list(reversed(body))
    while pending:
        node = pending.pop()
        yield node
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | ast.Lambda):
            continue
        children = [child for child in ast.iter_child_nodes(node) if isinstance(child, ast.stmt)]
        pending.extend(reversed(children))


def _returned(node: ast.FunctionDef | ast.AsyncFunctionDef) -> Iterator[ast.expr]:
    for statement in _own_statements(node.body):
        if isinstance(statement, ast.Return) and statement.value is not None:
            yield statement.value


def _nested_functions(node: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    return {n.name for n in node.body if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)}


def _returns_closure(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    nested = _nested_functions(node)
    return any(
        isinstance(value, ast.Lambda) or (isinstance(value, ast.Name) and value.id in nested)
        for value in _returned(node)
    )


def _imports(dotted: str, tree: ast.Module) -> dict[str, str]:
    package = dotted.rsplit(".", 1)[0]
    imported: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                anchor = package.rsplit(".", node.level - 1)[0] if node.level > 1 else package
                base = f"{anchor}.{base}" if base else anchor
            for alias in node.names:
                imported[alias.asname or alias.name] = f"{base}.{alias.name}"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imported[alias.asname or alias.name.split(".")[0]] = (
                    alias.name if alias.asname else alias.name.split(".")[0]
                )
    return imported


def _aliases(body: list[ast.stmt]) -> dict[str, ast.expr]:
    aliases: dict[str, ast.expr] = {}
    for node in body:
        match node:
            case ast.AnnAssign(target=ast.Name(id=name), value=ast.expr() as value):
                aliases[name] = value
            case ast.Assign(targets=[ast.Name(id=name)], value=value):
                aliases[name] = value
            case ast.TypeAlias(name=ast.Name(id=name), value=value):
                aliases[name] = value
    return aliases


@dataclass(frozen=True)
class ScanResult:
    """What the scan found over a set of modules."""

    handoffs: tuple[Handoff, ...]
    calls_checked: int

    @property
    def offences(self) -> list[str]:
        """Each hand-off that breaks the rule, as text."""
        return [str(handoff) for handoff in self.handoffs if handoff.offends]


def scan(paths: list[Path], root: Path = SRC) -> ScanResult:
    """Every callable *paths* hand to a foreign callee."""
    handoffs: list[Handoff] = []
    calls = 0
    for path in sorted(paths):
        relative = path.resolve().relative_to(root.resolve()) if path.is_relative_to(root) else path
        dotted = ".".join((LOOM, *Path(relative).with_suffix("").parts)).removesuffix(".__init__")
        checker = ForeignHandoffScan(str(relative), dotted, parsed(path))
        checker.visit(parsed(path))
        handoffs.extend(checker.handoffs)
        calls += checker.calls_checked
    return ScanResult(tuple(handoffs), calls)
