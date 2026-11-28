"""Conversation-loader use cases exercised by the conversation phase tests.

``LoadConversation`` is the satisfiable loader: its command asks only for
``conversation_id`` and ``subject`` among the names the host offers, plus
an infrastructure-owned ``Internal`` field the proof never demands.  The
other use cases each break exactly one rule of the compile-time proof.
"""

from __future__ import annotations

from typing import Any

from loom.core.command import Command, Internal
from loom.core.identity import Identity
from loom.core.use_case import Caller, Input, UseCase
from loom.core.use_case.keys import use_case_key


class LoadConversationCommand(Command, frozen=True, kw_only=True):
    """Command fed from the run: the thread key plus one context name."""

    conversation_id: str
    subject: str
    loaded_by: Internal[str]


@use_case_key("conversations.load")
class LoadConversation(UseCase[Any, bytes | None]):
    """Loader use case whose Input is satisfiable from a run."""

    async def execute(
        self,
        cmd: LoadConversationCommand = Input(),
        caller: Identity = Caller(),
    ) -> bytes | None:
        return None


class LoadRecentCommand(Command, frozen=True, kw_only=True):
    """Command declaring an offered name but not ``conversation_id``."""

    agent: str


@use_case_key("conversations.load_recent")
class LoadRecent(UseCase[Any, bytes | None]):
    """Refused: the Input cannot know which conversation to load."""

    async def execute(self, cmd: LoadRecentCommand = Input()) -> bytes | None:
        return None


class LoadTenantThreadCommand(Command, frozen=True, kw_only=True):
    """Command demanding a name no run offers."""

    conversation_id: str
    tenant: str


@use_case_key("conversations.load_tenant_thread")
class LoadTenantThread(UseCase[Any, bytes | None]):
    """Refused: ``tenant`` is required but never offered."""

    async def execute(self, cmd: LoadTenantThreadCommand = Input()) -> bytes | None:
        return None


class LoadWithOutputCommand(Command, frozen=True, kw_only=True):
    """Command demanding the hook-only ``output`` name."""

    conversation_id: str
    output: dict[str, Any]


@use_case_key("conversations.load_with_output")
class LoadWithOutput(UseCase[Any, bytes | None]):
    """Refused: ``output`` is offered to hooks, never to loaders."""

    async def execute(self, cmd: LoadWithOutputCommand = Input()) -> bytes | None:
        return None


@use_case_key("conversations.load_by_token")
class LoadByToken(UseCase[Any, bytes | None]):
    """Refused: declares a primitive parameter the run cannot bind."""

    async def execute(
        self,
        token: str,
        cmd: LoadConversationCommand = Input(),
    ) -> bytes | None:
        return None


@use_case_key("conversations.count_threads")
class CountThreads(UseCase[Any, int]):
    """Refused: declares no ``Input()``."""

    async def execute(self, caller: Identity = Caller()) -> int:
        return 0
