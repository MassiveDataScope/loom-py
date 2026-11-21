"""The loom boundary: what loom hands to a foreign framework raises only clean ``LoomError``s.

Every callable loom passes to a foreign registration API (a Celery task or
signal, a Prefect flow, task or hook, a Typer command, a tool, a SQLAlchemy
listener, a Starlette exception handler, a Lambda handler) is wrapped by
:func:`boundary`, and an architecture test refuses one that is not. A loom
class whose methods a framework calls derives from :class:`BoundaryClass`;
the process hooks go through the boundary too (:class:`ProcessHooks`). At the boundary a
failure is translated once by :meth:`Failure.translate
<loom.core.errors.failure.Failure.translate>`: logged unless its stack is
already written, turned into a ``LoomError``, and cleaned of its chain and
notes. The exception is raised *after* the ``except`` block, so it carries
no ``__context__``.

A ``BaseException`` that is not an ``Exception`` passes untouched, and so does
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
from collections.abc import AsyncIterator, Callable, Generator, Mapping
from contextlib import aclosing
from types import TracebackType
from typing import Any, ClassVar, Final, Self, TypeVar, cast

from loom.core.errors.failure import Failure

F = TypeVar("F", bound=Callable[..., Any])

PassThrough = tuple[type[BaseException], ...]
"""Exceptions a host uses for control flow, which cross the boundary untouched."""

NO_PASS_THROUGH: Final[PassThrough] = ()

_WRAPPED: Final = "__loom_boundary__"


def boundary(fn: F, /, *, pass_through: PassThrough = NO_PASS_THROUGH) -> F:
    """Wrap *fn* so that what it raises leaves loom translated, with no foreign text.

    A decorator, as registration APIs expect a callable, so it stays a
    function. A generator or async generator function is wrapped step by
    step, for streaming; ``StopIteration`` and ``StopAsyncIteration`` end it
    as usual. Wrapping twice wraps once.

    Args:
        fn: What loom hands to a foreign framework.
        pass_through: The host's control-flow exceptions, raised as they are.

    Returns:
        A callable of the same kind and signature.
    """
    if getattr(fn, _WRAPPED, False):
        return fn
    wrapped = _wrapper_of(fn, pass_through)
    functools.update_wrapper(wrapped, fn)
    setattr(wrapped, _WRAPPED, True)
    return cast("F", wrapped)


def _wrapper_of(fn: Callable[..., Any], pass_through: PassThrough) -> Callable[..., Any]:
    if inspect.isasyncgenfunction(fn):

        async def async_steps(*args: Any, **kwargs: Any) -> AsyncIterator[Any]:
            try:
                async with aclosing(fn(*args, **kwargs)) as steps:
                    async for item in steps:
                        yield item
                return
            except pass_through:
                raise
            except Exception as exc:
                leaving = Failure(exc).translate()
            raise leaving

        return async_steps
    if inspect.isgeneratorfunction(fn):

        def steps(*args: Any, **kwargs: Any) -> Generator[Any, Any, Any]:
            try:
                return (yield from fn(*args, **kwargs))
            except pass_through:
                raise
            except Exception as exc:
                leaving = Failure(exc).translate()
            raise leaving

        return steps
    if inspect.iscoroutinefunction(fn):

        async def awaited(*args: Any, **kwargs: Any) -> Any:
            try:
                return await fn(*args, **kwargs)
            except pass_through:
                raise
            except Exception as exc:
                leaving = Failure(exc).translate()
            raise leaving

        return awaited

    def called(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except pass_through:
            raise
        except Exception as exc:
            leaving = Failure(exc).translate()
        raise leaving

    return called


class BoundaryClass:
    """Base of a loom class whose methods a foreign framework calls.

    An adapter declares, on its own base, the methods its framework calls
    (:attr:`BOUNDARY_METHODS`) and its control flow (:attr:`PASS_THROUGH`);
    each of those methods a subclass defines is wrapped by :func:`boundary`
    when the class is created.
    """

    BOUNDARY_METHODS: ClassVar[frozenset[str]] = frozenset()
    PASS_THROUGH: ClassVar[PassThrough] = NO_PASS_THROUGH

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for name in cls.BOUNDARY_METHODS & cls.__dict__.keys():
            setattr(cls, name, boundary(cls.__dict__[name], pass_through=cls.PASS_THROUGH))


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
        if isinstance(exc, Exception):
            Failure(exc).translate()
            return
        loop.default_exception_handler(dict(context))

    def _excepthook(
        self, kind: type[BaseException], exc: BaseException, tb: TracebackType | None
    ) -> None:
        if not isinstance(exc, Exception):
            self._previous_excepthook(kind, exc, tb)
            return
        leaving = Failure(exc).translate()
        self._previous_excepthook(type(leaving), leaving, leaving.__traceback__)

    def _thread_hook(self, args: threading.ExceptHookArgs) -> None:
        exc = args.exc_value
        if not isinstance(exc, Exception):
            self._previous_thread_hook(args)
            return
        leaving = Failure(exc).translate()
        self._previous_thread_hook(
            threading.ExceptHookArgs((type(leaving), leaving, leaving.__traceback__, args.thread))
        )

    def _unraisable_hook(self, args: Any) -> None:
        exc = args.exc_value
        if not isinstance(exc, Exception):
            self._previous_unraisable(args)
            return
        leaving = Failure(exc).translate()
        self._previous_unraisable(
            type(args)((type(leaving), leaving, leaving.__traceback__, None, None))
        )
