"""The execution identity and the ambient identity never disagree (plan 6).

A seeded generator plays a few hundred random trees of executions, each run
by a real :class:`~loom.core.engine.executor.RuntimeExecutor` (no stores), so
the property holds for the lifecycle loom ships, not for a copy of it. Trees
mix nesting, ``gather`` across tasks, raising bodies and cancelled bodies, with
explicit identities and none. At every step the test checks:

- the current execution runs as the expected effective identity (NS2: the one
  given, else the enclosing execution's, else ``None``);
- whenever that identity is not ``None`` it equals ``current_identity()``, and
  whenever it is ``None``, ``current_identity()`` is ``ANONYMOUS`` (F3);
- after each child (finished, raising or cancelled) the parent's execution is
  current again, and after each tree no execution remains and the ambient
  identity is back;
- an assertion failing inside a gathered task fails the test: ``gather``
  collects results, and anything but the planned ``_Boom`` and cancellation
  is raised again.

``hypothesis`` is not a dependency of this repository, so the trees come from
``random.Random`` with fixed seeds: a failure names its seed and replays.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.execution import current_execution
from loom.core.identity import (
    ANONYMOUS,
    Identity,
    current_identity,
    reset_identity,
    set_identity,
)
from loom.core.use_case.use_case import UseCase

_ALICE = Identity(subject="acme-alice", mechanism="test")
_BOB = Identity(subject="acme-bob", mechanism="test")
_AMBIENT = Identity(subject="acme-transport", mechanism="test")
_IDENTITIES = (None, None, _ALICE, _BOB, ANONYMOUS)
_TREES = 300


class _Boom(Exception):
    pass


@dataclass(frozen=True)
class _Run:
    """One execution: an explicit identity (``None`` to inherit) and its body."""

    identity: Identity | None
    body: tuple[_Step, ...] = field(default=())


@dataclass(frozen=True)
class _Gather:
    """Executions run concurrently in their own tasks."""

    runs: tuple[_Run, ...]


@dataclass(frozen=True)
class _Fail:
    """The body raises."""


@dataclass(frozen=True)
class _Cancel:
    """The task running the body is cancelled at its next await."""


_Step = _Run | _Gather | _Fail | _Cancel


class _Body(UseCase[Any, None]):
    """A use case whose ``execute()`` plays the body it is built with."""

    def __init__(self, body: Callable[[], Awaitable[None]]) -> None:
        self._body = body

    async def execute(self) -> None:
        await self._body()


_EXECUTOR = RuntimeExecutor(UseCaseCompiler())


async def _execute(run: _Run) -> None:
    """Run ``run`` through the executor, expecting the effective identity NS2 defines."""
    enclosing = current_execution()
    explicit = run.identity
    effective = explicit if explicit is not None or enclosing is None else enclosing.identity
    await _EXECUTOR.execute(_Body(lambda: _play(run.body, effective)), identity=explicit)


def _check(expected: Identity | None) -> None:
    context = current_execution()
    assert context is not None
    assert context.identity is expected
    assert current_identity() is (ANONYMOUS if expected is None else expected)


async def _play(steps: tuple[_Step, ...], expected: Identity | None) -> None:
    for step in steps:
        _check(expected)
        before = current_execution()
        match step:
            case _Run():
                with suppress(_Boom):
                    await _execute(step)
            case _Gather():
                await _gather(step)
            case _Fail():
                raise _Boom
            case _Cancel():
                task = asyncio.current_task()
                assert task is not None
                task.cancel()
                await asyncio.sleep(0)
        assert current_execution() is before
        _check(expected)


async def _gather(step: _Gather) -> None:
    """Run each execution in its own task; re-raise anything but a planned outcome."""
    tasks = (asyncio.create_task(_execute(run)) for run in step.runs)
    for result in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(result, BaseException) and not isinstance(
            result, (_Boom, asyncio.CancelledError)
        ):
            raise result


def _tree(rng: random.Random, depth: int, *, in_task: bool) -> tuple[_Step, ...]:
    """Draw a body; a cancellation only appears where a task of its own runs it."""
    kinds = ["run", "run", "gather"] if depth else []
    kinds += ["fail"] + (["cancel"] if in_task else [])
    steps: list[_Step] = []
    for _ in range(rng.randint(0, 3)):
        kind = rng.choice(kinds)
        if kind == "run":
            body = _tree(rng, depth - 1, in_task=in_task)
            steps.append(_Run(rng.choice(_IDENTITIES), body))
        elif kind == "gather":
            runs = tuple(
                _Run(rng.choice(_IDENTITIES), _tree(rng, depth - 1, in_task=True))
                for _ in range(rng.randint(1, 3))
            )
            steps.append(_Gather(runs))
        else:
            steps.append(_Fail() if kind == "fail" else _Cancel())
    return tuple(steps)


class TestExecutionAndAmbientIdentity:
    async def test_stay_consistent(self) -> None:
        """Covers nesting, gather, failure and cancellation; fixed seeds replay a failure."""
        for seed in range(_TREES):
            rng = random.Random(seed)
            root = _Run(rng.choice(_IDENTITIES), _tree(rng, 3, in_task=False))
            ambient = set_identity(_AMBIENT)
            try:
                with suppress(_Boom):
                    await _execute(root)
                assert current_execution() is None
                assert current_identity() is _AMBIENT
            except AssertionError as exc:
                exc.add_note(f"seed {seed}")
                raise
            finally:
                reset_identity(ambient)
