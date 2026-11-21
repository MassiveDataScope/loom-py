"""One admitted run: its context, its conversation loader and its output hook."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import dataclass
from typing import Any, Final

import msgspec

from loom.ai.abc import AgentEvent, Conversation, ErrorEvent, FinalEvent
from loom.ai.compiler._plan import (
    HOOK_MESSAGES_FIELD,
    HOOK_OUTPUT_FIELD,
    AgentPlan,
    CompiledConversation,
    CompiledOutputHook,
    CompiledUseCaseCall,
)
from loom.ai.errors import (
    CONVERSATION_LOAD_FAILED_MESSAGE,
    CONVERSATION_LOAD_TIMEOUT_MESSAGE,
    DENIALS,
    DENIED_MESSAGE,
    HOOK_FAILED_MESSAGE,
    NO_TERMINAL_MESSAGE,
    AgentRunError,
    AgentRunErrorCode,
)
from loom.core.concurrency import discard_outcome
from loom.core.identity import Identity
from loom.core.use_case.invoker import ApplicationInvoker

_logger = logging.getLogger(__name__)

_CANCEL_GRACE_S: Final[float] = 1.0
"""Time a bounded use case cut at its bound gets to observe its cancellation."""


@dataclass(frozen=True, slots=True)
class TurnCallbacks:
    """One admitted run, with the conversation loader and the output hook it executes.

    Args:
        plan: Compiled plan being run.
        identity: Caller the run and its use cases execute as.
        interaction_id: Identifier minted at admission.
        conversation_id: Opaque value the caller supplied, or ``None``.
        invoker: The application's invoker, resolved at start-up; ``None``
            only when no plan declares a hook, a loader or a use case.
    """

    plan: AgentPlan
    identity: Identity
    interaction_id: str
    conversation_id: str | None
    invoker: ApplicationInvoker | None

    def command(self, accepted: frozenset[str], **extra: object) -> dict[str, Any]:
        """Build the command a use case of the run receives, filtered to its Input's names.

        Args:
            accepted: Internal names the Input declares.
            **extra: Names offered beside the run context.

        Returns:
            The payload ``from_payload`` will decode.
        """
        offered: dict[str, Any] = {
            "conversation_id": self.conversation_id,
            "interaction_id": self.interaction_id,
            "subject": self.identity.subject,
            "mechanism": self.identity.mechanism,
            "agent": self.plan.name,
            **extra,
        }
        return {name: value for name, value in offered.items() if name in accepted}

    def hook_command(self, final: FinalEvent, accepted: frozenset[str]) -> dict[str, Any]:
        """Build the command the hook use case receives, filtered to its Input's names.

        Args:
            final: Terminal event of the run, carrying its output and messages.
            accepted: Internal names the Input declares.

        Returns:
            The payload ``from_payload`` will decode; the output is nested
            under ``output`` and the run's new messages under ``messages``.
        """
        plan = self.plan
        extra: dict[str, object] = {
            HOOK_MESSAGES_FIELD: final.messages,
            "provider": plan.inference.provider,
            "model": plan.inference.model,
        }
        if HOOK_OUTPUT_FIELD in accepted:
            extra[HOOK_OUTPUT_FIELD] = plan.output.loom_type.to_builtins(final.output)
        return self.command(accepted, **extra)

    async def conversation(self) -> Conversation | None:
        """Load the conversation the run continues, or ``None`` when it is single-shot.

        Returns:
            The conversation the engine receives, or ``None`` when the plan
            declares no loader or the run carries no ``conversation_id``.

        Raises:
            AgentRunError: ``CONVERSATION_LOAD_TIMEOUT`` when the loader is cut
                at ``tool_timeout_ms`` or raises a ``TimeoutError`` of its own;
                ``CONVERSATION_LOAD_FAILED`` when it raises anything else,
                returns anything but ``bytes | None``, or returns more than
                ``policies.max_history_bytes``; ``UNAUTHORIZED`` on an
                application denial.  Its ``usage`` is ``None``.
        """
        loader = self.plan.conversation
        if loader is None or self.conversation_id is None:
            return None
        try:
            raw = await self.bounded(loader, self.command(loader.accepted))
            history = self.history(loader, raw)
        except TimeoutError as exc:
            raise self.failure(
                exc,
                loader,
                code=AgentRunErrorCode.CONVERSATION_LOAD_TIMEOUT,
                message=CONVERSATION_LOAD_TIMEOUT_MESSAGE,
            ) from exc
        except Exception as exc:
            raise self.failure(
                exc,
                loader,
                code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED,
                message=CONVERSATION_LOAD_FAILED_MESSAGE,
            ) from exc
        return Conversation(conversation_id=self.conversation_id, history=history)

    async def events(self, events: AsyncIterator[AgentEvent]) -> AsyncGenerator[AgentEvent, None]:
        """Forward a supervised stream, running the hook at its terminal event.

        Args:
            events: The supervised event stream of the run.

        Yields:
            The run's events; the terminal one carries ``interaction_id`` and,
            for a ``final`` event, the hook's result or the hook's failure.  A
            stream exhausted without a terminal event ends with a
            ``PROVIDER_UNAVAILABLE`` error.
        """
        async for event in events:
            if type(event) is ErrorEvent:
                yield msgspec.structs.replace(event, interaction_id=self.interaction_id)
                return
            if type(event) is FinalEvent:
                yield await self.terminal(event)
                return
            yield event
        yield ErrorEvent(
            code=AgentRunErrorCode.PROVIDER_UNAVAILABLE,
            message=NO_TERMINAL_MESSAGE.format(agent=self.plan.name),
            interaction_id=self.interaction_id,
        )

    async def terminal(self, final: FinalEvent) -> FinalEvent | ErrorEvent:
        """Run the hook, if any, and produce the stream's terminal event.

        Args:
            final: The engine's terminal event.

        Returns:
            *final* with the run's ``interaction_id`` and the hook's result, or
            the ``HOOK_FAILED`` error event when the hook failed.
        """
        hook: CompiledOutputHook | None = self.plan.on_output
        if hook is None:
            return msgspec.structs.replace(final, interaction_id=self.interaction_id)
        try:
            result = await self.bounded(hook, self.hook_command(final, hook.accepted))
        except Exception as exc:
            error = self.failure(
                exc, hook, code=AgentRunErrorCode.HOOK_FAILED, message=HOOK_FAILED_MESSAGE
            )
            return ErrorEvent.of_failure(
                error, interaction_id=self.interaction_id, usage=final.usage
            )
        return msgspec.structs.replace(
            final, interaction_id=self.interaction_id, hook_result=result
        )

    async def bounded(self, call: CompiledUseCaseCall, command: dict[str, Any]) -> object:
        """Run *call*'s use case shielded from the consumer and bounded by ``tool_timeout_ms``.

        Args:
            call: The compiled hook or loader.
            command: The payload its Input decodes.

        Returns:
            What the use case returned.

        Raises:
            TimeoutError: When the use case does not complete within the bound.
            asyncio.CancelledError: When the consumer was cancelled; re-raised
                once the use case settled within the remaining bound.
            RuntimeError: When the use case ended cancelled without the
                consumer being cancelled.
            LookupError: When the host holds no application invoker.
            Exception: Whatever the use case raised.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.plan.policies.tool_timeout_ms / 1000
        task = asyncio.ensure_future(self._invoke(call, command))
        try:
            async with asyncio.timeout_at(deadline):
                await asyncio.wait({task})
        except asyncio.CancelledError:
            try:
                await self._settle(task, max(deadline - loop.time(), 0.0), call)
            except asyncio.CancelledError:
                task.cancel()
                raise
            raise
        except TimeoutError:
            task.cancel()
            await self._settle(task, _CANCEL_GRACE_S, call)
            raise
        if task.cancelled():
            raise RuntimeError(f"the {call.step} was cancelled internally")
        return task.result()

    def failure(
        self,
        exc: BaseException,
        call: CompiledUseCaseCall,
        *,
        code: AgentRunErrorCode,
        message: str,
    ) -> AgentRunError:
        """Map a failure of *call* to the coded error the caller receives.

        Args:
            exc: What the bounded step raised.
            call: The compiled hook or loader that failed.
            code: Code of the generic failure.
            message: Fixed client text of the generic failure.

        Returns:
            An ``AgentRunError`` keeping its own code and text, ``UNAUTHORIZED``
            for an application denial, else *code* and *message* with the
            detail logged server-side; always carrying ``interaction_id`` and
            no ``usage``.
        """
        if isinstance(exc, AgentRunError):
            return AgentRunError(exc.code, str(exc), interaction_id=self.interaction_id)
        if isinstance(exc, DENIALS):
            return AgentRunError(
                AgentRunErrorCode.UNAUTHORIZED, DENIED_MESSAGE, interaction_id=self.interaction_id
            )
        _logger.exception(
            "%s of agent %r failed (interaction %s)",
            call.step,
            self.plan.name,
            self.interaction_id,
        )
        return AgentRunError(code, message, interaction_id=self.interaction_id)

    def history(self, loader: CompiledConversation, value: object) -> bytes | None:
        """Accept the loader's ``None`` or ``bytes`` within ``max_history_bytes``.

        Args:
            loader: The compiled loader that returned *value*.
            value: What the loader use case returned.

        Returns:
            The history the engine receives, untouched.

        Raises:
            TypeError: *value* is neither ``bytes`` nor ``None``.
            ValueError: *value* is longer than ``policies.max_history_bytes``.
        """
        if value is None:
            return None
        if not isinstance(value, bytes):
            raise TypeError(
                f"{loader.step} returned {type(value).__name__}, expected bytes or None"
            )
        bound = self.plan.policies.max_history_bytes
        if len(value) > bound:
            raise ValueError(
                f"{loader.step} returned {len(value)} bytes, above max_history_bytes ({bound})"
            )
        return value

    async def _invoke(self, call: CompiledUseCaseCall, command: dict[str, Any]) -> object:
        if self.invoker is None:
            raise LookupError("the container holds no ApplicationInvoker")
        return await self.invoker.invoke(call.use_case, params=None, payload=command)

    async def _settle(
        self, task: asyncio.Future[object], bound: float, call: CompiledUseCaseCall
    ) -> None:
        if not task.done():
            try:
                async with asyncio.timeout(bound):
                    await asyncio.wait({task})
            except TimeoutError:
                pass
        if not task.done():
            task.cancel()
            task.add_done_callback(discard_outcome)
            _logger.warning(
                "%s of agent %r still running at its bound after the consumer "
                "left; cancelled (interaction %s)",
                call.step,
                self.plan.name,
                self.interaction_id,
            )
            return
        if task.cancelled():
            return
        if task.exception() is not None:
            _logger.error(
                "%s of agent %r failed after the consumer left (interaction %s)",
                call.step,
                self.plan.name,
                self.interaction_id,
                exc_info=task.exception(),
            )
            return
        _logger.info(
            "%s of agent %r completed after the consumer left (interaction %s)",
            call.step,
            self.plan.name,
            self.interaction_id,
        )
