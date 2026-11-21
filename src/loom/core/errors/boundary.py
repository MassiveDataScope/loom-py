"""The loom boundary: what loom hands to a foreign framework raises only clean ``LoomError``s.

Every function, lambda, bound method or closure loom passes as an argument,
or applies as a decorator, to a callee outside loom is wrapped by
:func:`boundary`, and an architecture test refuses one that is not. A loom
class whose methods a framework calls derives from :class:`BoundaryClass`;
the process hooks go through the boundary too (:class:`ProcessHooks`). At the boundary a
failure is translated once by :meth:`Failure.translate
<loom.core.errors.failure.Failure.translate>`: logged unless its stack is
already written, turned into a ``LoomError``, and cleaned of its chain and
notes. The exception is raised *after* the ``except`` block, so it carries
no ``__context__``.

A ``BaseException`` that is not an ``Exception`` passes untouched (a group
holding both has its ``Exception`` members translated), and so does
each host's control flow, declared by its adapter as a pass-through constant
beside the code that registers the callable.
"""

from __future__ import annotations

import functools
import inspect
import logging
import sys
import threading
from asyncio import AbstractEventLoop
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Mapping
from contextlib import aclosing
from types import TracebackType
from typing import Any, ClassVar, Final, Self, TypeVar, cast

from loom.core.errors.failure import TRANSLATED, Failure

F = TypeVar("F", bound=Callable[..., Any])

PassThrough = tuple[type[BaseException], ...]
"""Exceptions a host uses for control flow, which cross the boundary untouched."""

NO_PASS_THROUGH: Final[PassThrough] = ()

_WRAPPED: Final = "__loom_boundary__"


def boundary(fn: F, /, *, pass_through: PassThrough = NO_PASS_THROUGH) -> F:
    """Wrap *fn* so that what it raises leaves loom translated, with no foreign text.

    A decorator, as registration APIs expect a callable, so it stays a
    function. What the call returns decides the rest: an awaitable is awaited
    through the boundary, and a generator or async generator is driven through
    it step by step, values sent and exceptions thrown in forwarded; anything
    else is returned as it is. So a callable instance with an ``async def
    __call__``, or a function returning a coroutine, is covered like an
    ``async def``. A coroutine callable is wrapped in a coroutine function that
    awaits that same call, so frameworks that dispatch on the kind still see
    one. Wrapping twice wraps once.

    Args:
        fn: What loom hands to code outside loom.
        pass_through: The host's control-flow exceptions, raised as they are.

    Returns:
        A callable with the same signature.
    """
    if getattr(fn, _WRAPPED, False):
        return fn
    steps = _Steps(pass_through)

    def called(*args: Any, **kwargs: Any) -> Any:
        try:
            result = fn(*args, **kwargs)
        except pass_through:
            raise
        except TRANSLATED as exc:
            leaving = Failure(exc).translate()
        else:
            return steps.guard(result)
        raise leaving

    wrapper: Callable[..., Any] = called
    if inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(type(fn).__call__):

        async def awaited(*args: Any, **kwargs: Any) -> Any:
            return await called(*args, **kwargs)

        wrapper = awaited
    functools.update_wrapper(wrapper, fn)
    setattr(wrapper, _WRAPPED, True)
    return cast("F", wrapper)


class _Steps:
    """What a wrapped call returns, driven through the boundary: an awaitable or a generator.

    Args:
        pass_through: The host's control-flow exceptions, raised as they are.
    """

    __slots__ = ("_pass_through",)

    def __init__(self, pass_through: PassThrough) -> None:
        self._pass_through = pass_through

    def guard(self, result: Any) -> Any:
        """*result* itself, or a wrapper that drives it through the boundary."""
        if inspect.isasyncgen(result):
            return self._async_steps(result)
        if inspect.isgenerator(result):
            return self._steps(result)
        if inspect.isawaitable(result):
            return self._awaited(result)
        return result

    async def _awaited(self, awaitable: Awaitable[Any]) -> Any:
        try:
            return await awaitable
        except self._pass_through:
            raise
        except TRANSLATED as exc:
            leaving = Failure(exc).translate()
        raise leaving

    def _steps(self, steps: Generator[Any, Any, Any]) -> Generator[Any, Any, Any]:
        try:
            return (yield from steps)
        except self._pass_through:
            raise
        except TRANSLATED as exc:
            leaving = Failure(exc).translate()
        raise leaving

    async def _async_steps(self, steps: AsyncGenerator[Any, Any]) -> AsyncGenerator[Any, Any]:
        try:
            async with aclosing(steps):
                item = await steps.asend(None)
                while True:
                    try:
                        sent = yield item
                    except GeneratorExit:
                        raise
                    except BaseException as thrown:
                        item = await steps.athrow(thrown)
                    else:
                        item = await steps.asend(sent)
        except StopAsyncIteration:
            return
        except self._pass_through:
            raise
        except TRANSLATED as exc:
            leaving = Failure(exc).translate()
        raise leaving


class BoundaryClass:
    """Base of a loom class whose methods a foreign framework calls.

    An adapter declares, on its own base, the methods its framework calls
    (:attr:`BOUNDARY_METHODS`) and its control flow (:attr:`PASS_THROUGH`).
    When a class is created, each of those methods it resolves, its own,
    a mixin's or a base's, is wrapped by :func:`boundary` on the class.
    """

    BOUNDARY_METHODS: ClassVar[frozenset[str]] = frozenset()
    PASS_THROUGH: ClassVar[PassThrough] = NO_PASS_THROUGH

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for name in cls.BOUNDARY_METHODS:
            method = inspect.getattr_static(cls, name, None)
            if inspect.isfunction(method) and not getattr(method, _WRAPPED, False):
                setattr(cls, name, boundary(method, pass_through=cls.PASS_THROUGH))


class ProcessHooks:
    """The interpreter, thread and unraisable hooks, each going through the boundary.

    A runtime entry point installs them (Lambda Init, the ASGI lifespan, the
    Celery worker, the CLI), never ``configure_logging``, so a test runner keeps
    its own. Each hook translates the exception, which logs it once, then hands
    the translated exception to the hook it replaced, so what that hook prints
    holds no foreign text. A ``BaseException`` that is not an ``Exception``
    (Ctrl-C, ``SystemExit``) reaches the previous hook as it is. Warnings are
    captured as records of their category. :meth:`restore` puts the previous
    hooks back; as a context manager it restores on exit.
    """

    __slots__ = ("_previous_excepthook", "_previous_thread_hook", "_previous_unraisable")

    def __init__(self) -> None:
        self._previous_excepthook = sys.excepthook
        self._previous_thread_hook = threading.excepthook
        self._previous_unraisable = sys.unraisablehook

    def install(self) -> Self:
        """Put the hooks in place, chained to the ones there now."""
        sys.excepthook = self._excepthook
        threading.excepthook = self._thread_hook
        sys.unraisablehook = self._unraisable_hook
        logging.captureWarnings(True)
        return self

    def restore(self) -> None:
        """Put back the hooks that were there when this object was built."""
        sys.excepthook = self._previous_excepthook
        threading.excepthook = self._previous_thread_hook
        sys.unraisablehook = self._previous_unraisable
        logging.captureWarnings(False)

    def __enter__(self) -> Self:
        return self.install()

    def __exit__(self, *_: object) -> None:
        self.restore()

    @staticmethod
    def handle_asyncio_exception(loop: AbstractEventLoop, context: Mapping[str, Any]) -> None:
        """The exception handler of a loop loom owns: an exception goes through the boundary.

        A context with no exception is the loop's own report, which its
        default handler writes.
        """
        exc = context.get("exception")
        if isinstance(exc, TRANSLATED):
            Failure(exc).translate()
            return
        loop.default_exception_handler(dict(context))

    def _excepthook(
        self, kind: type[BaseException], exc: BaseException, tb: TracebackType | None
    ) -> None:
        if not isinstance(exc, TRANSLATED):
            self._previous_excepthook(kind, exc, tb)
            return
        leaving = Failure(exc).translate()
        self._previous_excepthook(type(leaving), leaving, leaving.__traceback__)

    def _thread_hook(self, args: threading.ExceptHookArgs) -> None:
        exc = args.exc_value
        if not isinstance(exc, TRANSLATED):
            self._previous_thread_hook(args)
            return
        leaving = Failure(exc).translate()
        self._previous_thread_hook(
            threading.ExceptHookArgs((type(leaving), leaving, leaving.__traceback__, args.thread))
        )

    def _unraisable_hook(self, args: Any) -> None:
        exc = args.exc_value
        if not isinstance(exc, TRANSLATED):
            self._previous_unraisable(args)
            return
        leaving = Failure(exc).translate()
        self._previous_unraisable(
            type(args)((type(leaving), leaving, leaving.__traceback__, None, None))
        )
