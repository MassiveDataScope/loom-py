"""Keep an application open in one task of its own, across the calls that start and stop it."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any, TypeVar

from loom.composition.errors import CompositionErrorCode, CompositionStateError

if TYPE_CHECKING:
    from loom.composition.application import Application

_logger = logging.getLogger(__name__)
_T = TypeVar("_T")

_FAILED_TO_CLOSE = "application failed to close"


class ApplicationHost:
    """Opens an application in a task it owns and closes it in that same task.

    A transport that starts and stops its application in separate calls (a
    worker's signals, a Lambda's Init and shutdown) cannot enter
    :meth:`Application.running` in one and exit it in another: each call runs
    in a task of its own, and an agent runtime, an MCP client or a cancel
    scope refuses to be exited from a task other than the one that entered it.
    The host spawns one owner task on the running loop that enters
    :meth:`~Application.running`, waits until :meth:`stop`, and exits it.

    A host opens its application once; compose a new host to open it again.
    The owner task runs on the loop that called :meth:`start`, so
    :meth:`stop` must run on that same loop. If the loop is cancelled before
    :meth:`stop`, the owner still exits :meth:`~Application.running` in its own
    task. A failure to close that no :meth:`stop` can raise, after a cancelled
    :meth:`start` say, is logged.

    Args:
        application: The application to open.
        check: Run each resource's startup checks before it opens.
    """

    def __init__(self, application: Application, *, check: bool = True) -> None:
        self._application = application
        self._check = check
        self._started = False
        self._owner: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._close_failure: BaseException | None = None

    async def start(self) -> None:
        """Open the application in a new owner task and return once it is open.

        Cancelling the call cancels the opening: what opened is closed in the
        owner task before the cancellation propagates.

        Raises:
            CompositionStateError: When this host was already started, or
                when :meth:`stop` was called before the application finished
                opening; it is closed by then.
            Exception: The failure to open the application, as itself; nothing
                is left open.
        """
        if self._started:
            raise CompositionStateError(CompositionErrorCode.HOST_ALREADY_STARTED.issue())
        self._started = True
        opened: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        owner = asyncio.create_task(self._own(opened))
        owner.add_done_callback(self._record_close_failure)
        self._owner = owner
        try:
            # Shielded: a cancelled caller must not cancel the future the owner
            # reports to, only the owner itself.
            await asyncio.shield(opened)
        except BaseException as error:
            self._owner = None
            if isinstance(error, asyncio.CancelledError):
                owner.cancel()
            await asyncio.wait({owner})
            raise

    async def stop(self) -> None:
        """Close the application in its owner task and wait until it is closed.

        Every call waits for the same close, including one made while
        :meth:`start` is still opening. Stopping a host that never opened, or
        one already closed, does nothing. Cancelling the call does not
        interrupt the closing.

        Raises:
            Exception: The failure to close, as :meth:`Application.running`
                reports it, raised to the first call only.
        """
        owner = self._owner
        if owner is None:
            return
        first = not self._stopping.is_set()
        self._stopping.set()
        try:
            await asyncio.wait({owner})
        except asyncio.CancelledError:
            if first:
                # No one is left to raise it: log it once the owner finishes.
                owner.add_done_callback(lambda _: self._log_failure())
            raise
        if not first:
            return
        # Recorded already: the owner's done-callbacks run in the order they
        # were added, so _record_close_failure ran before this wait returned.
        failure = self._take_close_failure()
        if isinstance(failure, asyncio.CancelledError):
            _logger.warning(_FAILED_TO_CLOSE, exc_info=failure)
        elif failure is not None:
            raise failure

    async def _own(self, opened: asyncio.Future[None]) -> None:
        """Enter the application, report it open, wait for :meth:`stop` and exit."""
        entered = False
        try:
            async with self._application.running(check=self._check):
                entered = True
                if not self._stopping.is_set():
                    opened.set_result(None)
                    await self._stopping.wait()
        except BaseException as error:
            if not opened.done():
                # Not open yet: start() awaits the future and raises the failure.
                if isinstance(error, asyncio.CancelledError):
                    opened.cancel()
                elif entered:
                    opened.set_exception(_stopped_while_opening())
                else:
                    opened.set_exception(error)
                    return
            raise
        if not opened.done():
            opened.set_exception(_stopped_while_opening())

    def _record_close_failure(self, owner: asyncio.Task[None]) -> None:
        """Keep the owner's failure to close for :meth:`stop`, or log it if none is waiting."""
        self._close_failure = _failure_to_close(owner)
        if not self._stopping.is_set():
            self._log_failure()

    def _take_close_failure(self) -> BaseException | None:
        """Return the recorded failure to close and forget it, so it is reported once."""
        failure, self._close_failure = self._close_failure, None
        return failure

    def _log_failure(self) -> None:
        """Log the owner's failure to close, unless it was already reported."""
        failure = self._take_close_failure()
        if failure is not None:
            _logger.warning(_FAILED_TO_CLOSE, exc_info=failure)


class LoopHost:
    """Keeps an application open on an event loop of its own, across synchronous calls.

    For a transport with no lifespan hook, such as a Lambda function: it opens
    the application once at Init and serves each warm invocation from a
    synchronous call. Building the host creates the loop and makes it the
    thread's current one; :meth:`start` opens the application on it in an
    :class:`ApplicationHost`; every call then runs on that same loop, and
    :meth:`close` closes the application and disposes of the loop.

    Args:
        application: The application to open.
    """

    def __init__(self, application: Application) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._host = ApplicationHost(application)
        self._closed = False

    def start(self) -> None:
        """Open the application, with its startup checks, on the host's loop.

        Raises:
            Exception: The failure to open the application, as itself; the
                loop is disposed of by then.
        """
        try:
            self._loop.run_until_complete(self._host.start())
        except BaseException:
            self._dispose()
            raise

    def make_current(self) -> None:
        """Make the host's loop the thread's current one, for code that runs on that loop."""
        asyncio.set_event_loop(self._loop)

    def run(self, coroutine: Coroutine[Any, Any, _T]) -> _T:
        """Run *coroutine* to completion on the host's loop and return its result."""
        self.make_current()
        return self._loop.run_until_complete(coroutine)

    def close(self) -> None:
        """Close the application, then dispose of the loop; a second call does nothing.

        Raises:
            Exception: The failure to close the application; the loop is
                disposed of all the same.
        """
        if self._closed:
            return
        try:
            self._loop.run_until_complete(self._host.stop())
        finally:
            self._dispose()

    def _dispose(self) -> None:
        """Finish the loop's async generators and default executor, close it and unset it."""
        self._closed = True
        loop = self._loop
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
        finally:
            asyncio.set_event_loop(None)
            loop.close()


def _failure_to_close(owner: asyncio.Task[None]) -> BaseException | None:
    """What the finished *owner* failed to close with, if anything.

    :meth:`Application.running` records a failure to close what had opened as
    a note on the error that interrupted the opening, a cancellation included.
    """
    if owner.cancelled():
        try:
            owner.exception()
        except asyncio.CancelledError as error:
            return error if getattr(error, "__notes__", None) else None
        return None
    return owner.exception()


def _stopped_while_opening() -> CompositionStateError:
    return CompositionStateError(CompositionErrorCode.HOST_STOPPED_WHILE_OPENING.issue())
