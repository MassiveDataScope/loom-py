"""The execution identity and the ambient identity never disagree (plan 6).

A seeded generator plays a few hundred random trees of executions, run by a
helper shaped like the executor's lifecycle (NS2): the effective identity is
computed once, then installed with ``set_identity`` (``ANONYMOUS`` when it is
``None``, F3) and ``enter`` and reset in ``finally``. Trees mix nesting,
``gather`` across tasks, raising bodies and cancelled bodies. At every step the test checks:

- the current execution runs as the expected effective identity;
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
from contextlib import suppress
from dataclasses import dataclass, field

from loom.core.execution import current_execution, enter, leave
from loom.core.identity import (
    ANONYMOUS,
    Identity,
    current_identity,
    reset_identity,
    set_identity,
)

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


async def _execute(run: _Run) -> None:
    """Play ``run`` the way ``RuntimeExecutor._execute`` will run an execution (NS2)."""
    enclosing = current_execution()
    explicit = run.identity
    effective = explicit if explicit is not None or enclosing is None else enclosing.identity
    identity_token = set_identity(ANONYMOUS if effective is None else effective)
    token = enter(effective)
    try:
        await _play(run.body, effective)
    finally:
        leave(token)
        reset_identity(identity_token)


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
