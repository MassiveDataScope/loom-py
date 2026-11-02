"""The Lambda function handler: one application opened at Init, one event per invocation."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from mangum import Mangum

from loom.composition import Application, ApplicationHost

if TYPE_CHECKING:
    from starlette.applications import Starlette


class LambdaHTTPHandler:
    """Serve a Starlette application to API Gateway events, keeping its application open.

    Built during the execution environment's Init (a module-level
    ``handler = create_app(...)``): it creates the event loop of the thread,
    opens the application on it in an :class:`~loom.composition.ApplicationHost`
    with its startup checks, and leaves it open across warm invocations. A
    failure to open propagates, so the execution environment fails its Init.

    Each call translates one event with Mangum, its ASGI lifespan off, on that
    same loop: a request runs in a task of its own, with its own identity,
    trace id and unit of work.

    Args:
        app: The application serving the requests.
        application: The composed application the requests run against.
    """

    def __init__(self, app: Starlette, application: Application) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        host = ApplicationHost(application)
        try:
            loop.run_until_complete(host.start())
        except BaseException:
            asyncio.set_event_loop(None)
            loop.close()
            raise
        self._loop = loop
        self._host = host
        self._mangum = Mangum(app, lifespan="off")

    def __call__(self, event: dict[str, Any], context: Any) -> dict[str, Any]:
        """Answer one API Gateway HTTP API event."""
        # Mangum runs on the thread's current loop: make sure it is the one
        # the application was opened on.
        asyncio.set_event_loop(self._loop)
        return self._mangum(event, context)

    def close(self) -> None:
        """Close the application and its loop, from a shutdown hook or a test.

        Raises:
            Exception: The failure to close the application; the loop is
                closed all the same.
        """
        try:
            self._loop.run_until_complete(self._host.stop())
        finally:
            asyncio.set_event_loop(None)
            self._loop.close()
