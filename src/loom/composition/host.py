"""Keep an application open in one task of its own, across the calls that start and stop it."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from loom.composition.application import Application

_logger = logging.getLogger(__name__)

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
        self._failure_claimed = False

    async def start(self) -> None:
        """Open the application in a new owner task and return once it is open.

        Cancelling the call cancels the opening: what opened is closed in the
        owner task before the cancellation propagates.

        Raises:
            RuntimeError: When this host was already started, or when
                :meth:`stop` was called before the application finished
                opening; it is closed by then.
            Exception: The failure to open the application, as itself; nothing
                is left open.
        """
        if self._started:
            raise RuntimeError("ApplicationHost opens its application once; create another host")
        self._started = True
        opened: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        owner = asyncio.create_task(self._own(opened))
        owner.add_done_callback(self._log_unless_stopping)
        self._owner = owner
        try:
            # Shielded: a cancelled caller must not cancel the future the owner
            # reports to, only the owner itself.
            await asyncio.shield(opened)
        except asyncio.CancelledError:
            self._owner = None
            owner.cancel()
            await asyncio.wait({owner})
            raise
        except BaseException:
            self._owner = None
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
                owner.add_done_callback(self._log_failure)
            raise
        if not first:
            return
        failure = self._claim_failure(owner)
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

    def _claim_failure(self, owner: asyncio.Task[None]) -> BaseException | None:
        """The owner's failure to close, handed to one reporter only."""
        if self._failure_claimed:
            return None
        self._failure_claimed = True
        return _failure_to_close(owner)

    def _log_unless_stopping(self, owner: asyncio.Task[None]) -> None:
        """Log a failure to close unless a :meth:`stop` is waiting to raise it."""
        if not self._stopping.is_set():
            self._log_failure(owner)

    def _log_failure(self, owner: asyncio.Task[None]) -> None:
        """Log the owner's failure to close, unless it was already reported."""
        failure = self._claim_failure(owner)
        if failure is not None:
            _logger.warning(_FAILED_TO_CLOSE, exc_info=failure)


def _failure_to_close(owner: asyncio.Task[None]) -> BaseException | None:
    """What the finished *owner* failed to close with, if anything.

    :meth:`Application.running` records a failure to close what had opened as
    a note on the error that interrupted the opening, a cancellation included.
    """
    try:
        owner.result()
    except asyncio.CancelledError as cancelled:
        return cancelled if getattr(cancelled, "__notes__", None) else None
    except BaseException as failure:
        return failure
    return None


def _stopped_while_opening() -> RuntimeError:
    return RuntimeError("ApplicationHost was stopped while opening; the application is closed")
