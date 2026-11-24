"""The acting identity: one carrier; transports install it, work fixes it (spec 011, T09a)."""

from __future__ import annotations

import asyncio
import logging
import math
import time

import pytest

from loom.core.config import ConfigError
from loom.core.errors import DelegationExpired
from loom.core.execution import ExecutionScope
from loom.core.identity import (
    ANONYMOUS,
    Acting,
    Delegation,
    Identity,
    IdentityFixed,
    captured_delegation,
    configure_delegation,
    current_caller,
    current_identity,
    reset_identity,
    set_identity,
)
from loom.core.identity import context as identity_context
from loom.core.identity.context import IdentityEvent, IdentityKey
from loom.core.log_names import LogField
from tests.helpers.delegation import acting_as, assert_new_delegation
from tests.helpers.loggers import CapturedLog

_ALICE = Identity(subject="alice", mechanism="test")
_PROMOTED_ALICE = Identity(subject="alice", mechanism="test", roles=("admin",))
_BOB = Identity(subject="bob", mechanism="test")
_LATER = time.time() + 3600
_PASSED = time.time() - 1
_WORK = object()
"""Stands for the work fixing the identity: opaque to the identity layer."""


def _switched(switches: CapturedLog) -> list[tuple[object, object, object]]:
    return [
        (
            fields[IdentityKey.SWITCHED_FROM],
            fields[IdentityKey.SWITCHED_TO],
            fields[LogField.USE_CASE],
        )
        for event, fields in switches.calls
        if event == IdentityEvent.CALLER_SWITCHED
    ]


class TestTheCarrier:
    def test_defaults_to_the_anonymous_identity_and_no_caller(self) -> None:
        """Never ``None`` for ``current_identity``; no caller is ``None``, not ``ANONYMOUS``."""
        assert current_identity() is ANONYMOUS
        assert current_caller() is None
        assert captured_delegation() == (None, None)

    def test_installs_the_caller_with_the_deadline_it_resumes(self) -> None:
        token = set_identity(_ALICE, until=_LATER)
        try:
            assert (current_identity(), current_caller()) == (_ALICE, _ALICE)
            assert captured_delegation() == Delegation(_ALICE, _LATER)
        finally:
            reset_identity(token)

    def test_a_caller_with_no_deadline_starts_a_delegation_of_the_configured_window(
        self, window: float
    ) -> None:
        before = time.time()
        token = set_identity(_ALICE)
        try:
            _, deadline = captured_delegation()
        finally:
            reset_identity(token)

        assert_new_delegation(deadline, before, window)

    @pytest.mark.parametrize("until", [10 * 3600.0, math.inf], ids=["far", "infinite"])
    def test_a_resumed_deadline_never_reaches_past_one_window(
        self, window: float, until: float
    ) -> None:
        """A tampered or skewed deadline is clamped to ``now + window``."""
        with acting_as(_ALICE, until=time.time() + until):
            _, deadline = captured_delegation()

        assert deadline is not None
        assert deadline <= time.time() + window

    def test_no_caller_has_no_deadline(self) -> None:
        token = set_identity(None, until=_LATER)
        try:
            assert (current_identity(), current_caller()) == (ANONYMOUS, None)
            assert captured_delegation() == (None, None)
        finally:
            reset_identity(token)

    def test_reset_restores_the_previous_identity(self) -> None:
        outer = set_identity(_ALICE)
        try:
            reset_identity(set_identity(_BOB, until=_LATER))
            assert current_identity() is _ALICE
        finally:
            reset_identity(outer)
        assert current_caller() is None

    async def test_concurrent_tasks_never_see_each_others_identity(self) -> None:
        """Two concurrent callers must not cross: the leak this design exists to prevent."""
        seen: dict[str, str] = {}

        async def _run(identity: Identity) -> None:
            token = set_identity(identity)
            try:
                await asyncio.sleep(0)
                seen[identity.subject] = current_identity().subject
            finally:
                reset_identity(token)

        await asyncio.gather(_run(_ALICE), _run(_BOB))

        assert seen == {"alice": "alice", "bob": "bob"}


class TestATransportSwitch:
    """Outside work a transport may replace the caller; replacing a different one is logged."""

    def test_replacing_a_different_caller_is_logged(self, switches: CapturedLog) -> None:
        outer = set_identity(_ALICE)
        try:
            reset_identity(set_identity(_BOB))
        finally:
            reset_identity(outer)

        assert _switched(switches) == [("alice", "bob", "transport")]

    @pytest.mark.parametrize(
        ("acting", "installed"),
        [(None, _ALICE), (_ALICE, _ALICE)],
        ids=["over-no-caller", "the-same-caller"],
    )
    def test_installing_over_no_caller_or_the_same_one_is_no_switch(
        self, switches: CapturedLog, acting: Identity | None, installed: Identity
    ) -> None:
        outer = set_identity(acting)
        try:
            reset_identity(set_identity(installed))
        finally:
            reset_identity(outer)

        assert switches.calls == []

    def test_a_failing_report_leaves_the_previous_caller_installed(
        self,
    ) -> None:
        """The switch is not half done: the caller it would replace still acts."""

        class AcmeBrokenHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                raise RuntimeError("acme logger down")

        broken = AcmeBrokenHandler()
        log = logging.getLogger(identity_context.__name__)
        log.addHandler(broken)
        try:
            with acting_as(_ALICE):
                with pytest.raises(RuntimeError, match="acme logger down"):
                    set_identity(_BOB)
                assert current_caller() is _ALICE
        finally:
            log.removeHandler(broken)


class TestAFixedIdentity:
    """Once work starts under an identity, only an explicit switch changes it."""

    def test_set_identity_is_refused_naming_the_explicit_switch(self) -> None:
        with ExecutionScope(_ALICE):
            with pytest.raises(IdentityFixed, match=r"executor\.run\(UseCase, identity=\.\.\.\)"):
                set_identity(_BOB)
            assert current_caller() is _ALICE

    def test_a_transport_sets_it_again_once_the_work_ended(self) -> None:
        with ExecutionScope(_ALICE):
            pass

        transport = set_identity(_BOB)
        try:
            assert current_caller() is _BOB
        finally:
            reset_identity(transport)


class TestPinnedFor:
    """``pinned_for(E, work, what)`` over the acting ``(C, D, work)`` when an execution opens."""

    @pytest.mark.parametrize(
        ("given", "logged"),
        [(_ALICE, []), (_PROMOTED_ALICE, [("alice", "alice", "AcmeUseCase")])],
        ids=["the-caller-acting", "the-same-subject-with-other-roles"],
    )
    def test_the_same_subject_keeps_the_deadline(
        self, switches: CapturedLog, given: Identity, logged: list[object]
    ) -> None:
        with acting_as(_ALICE, until=_LATER):
            pinned = Acting.current().pinned_for(given, _WORK, "AcmeUseCase")

        assert (pinned.effective, pinned.caller, pinned.deadline, pinned.work) == (
            given,
            given,
            _LATER,
            _WORK,
        )
        assert _switched(switches) == logged

    @pytest.mark.parametrize("deadline", [_PASSED, math.nan], ids=["passed", "not-a-number"])
    @pytest.mark.parametrize(
        "given", [_ALICE, _PROMOTED_ALICE], ids=["the-caller-acting", "the-same-subject"]
    )
    def test_the_same_subject_past_its_deadline_is_refused(
        self, switches: CapturedLog, given: Identity, deadline: float
    ) -> None:
        with acting_as(_ALICE, until=deadline):
            acting = Acting.current()
            with pytest.raises(DelegationExpired, match="system identity"):
                acting.pinned_for(given, _WORK, "AcmeUseCase")

        assert [event for event, _ in switches.calls] == [IdentityEvent.DELEGATION_EXPIRED]

    def test_another_subject_is_a_logged_switch_with_a_new_delegation(
        self, switches: CapturedLog, window: float
    ) -> None:
        with acting_as(_ALICE, until=_PASSED):
            before = time.time()
            pinned = Acting.current().pinned_for(_BOB, _WORK, "AcmeUseCase")

        assert pinned.caller is _BOB
        assert_new_delegation(pinned.deadline, before, window)
        assert _switched(switches) == [("alice", "bob", "AcmeUseCase")]

    def test_no_caller_given_under_one_is_a_logged_switch_with_no_deadline(
        self, switches: CapturedLog
    ) -> None:
        """Alice's deadline has passed: none is kept, so the work is not refused."""
        with acting_as(_ALICE, until=_PASSED):
            pinned = Acting.current().pinned_for(None, _WORK, "AcmeUseCase")

        assert (pinned.effective, pinned.caller, pinned.deadline) == (ANONYMOUS, None, math.inf)
        pinned.refuse_expired()
        assert _switched(switches) == [("alice", None, "AcmeUseCase")]

    def test_a_caller_given_with_nothing_acting_is_the_transport_s_and_no_switch(
        self, switches: CapturedLog
    ) -> None:
        Acting.current().pinned_for(_ALICE, _WORK, "AcmeUseCase")

        assert switches.calls == []

    def test_a_caller_given_inside_work_with_none_is_a_switch_from_no_one(
        self, switches: CapturedLog
    ) -> None:
        outer = Acting.current().pinned_for(None, _WORK, "AcmeOuter")

        outer.pinned_for(_ALICE, _WORK, "AcmeUseCase")

        assert _switched(switches) == [(None, "alice", "AcmeUseCase")]


@pytest.mark.usefixtures("window")
class TestConfigureDelegation:
    def test_sets_the_window_a_request_s_delegation_lasts(self) -> None:
        before = time.time()
        configure_delegation(42.0)

        with acting_as(_ALICE):
            assert_new_delegation(captured_delegation().until, before, 42.0)

    @pytest.mark.parametrize(
        "seconds",
        [math.inf, 1e400, math.nan, 0, -1.0],
        ids=["infinite", "overflowing", "nan", "zero", "negative"],
    )
    def test_refuses_anything_but_a_finite_number_above_zero(self, seconds: float) -> None:
        with pytest.raises(ConfigError, match="identity.max_delegation_seconds"):
            configure_delegation(seconds)

        assert identity_context._max_delegation_seconds == 60.0  # pyright: ignore[reportPrivateUsage]

    def test_accepts_a_fraction_of_a_second(self) -> None:
        configure_delegation(1.5)

        assert identity_context._max_delegation_seconds == 1.5  # pyright: ignore[reportPrivateUsage]


class TestCapturedDelegation:
    """What a dispatch captures: the authenticated caller and its deadline, a plain read."""

    @pytest.mark.parametrize("caller", [None, ANONYMOUS], ids=["no-caller", "anonymous"])
    def test_no_authenticated_caller_is_captured_as_no_caller(
        self, caller: Identity | None
    ) -> None:
        with acting_as(caller, until=_LATER):
            assert captured_delegation() == Delegation(None, None)

    def test_an_authenticated_caller_is_captured_with_its_deadline(self) -> None:
        with acting_as(_ALICE, until=_LATER):
            assert captured_delegation() == Delegation(_ALICE, _LATER)


class TestRefuseExpired:
    """The check of code acting for the caller outside an execution: RLS and audit."""

    @pytest.mark.parametrize(
        ("caller", "deadline"), [(None, None), (_ALICE, _LATER)], ids=["no-caller", "within"]
    )
    def test_passes_with_no_caller_and_within_the_deadline(
        self, caller: Identity | None, deadline: float | None
    ) -> None:
        with acting_as(caller, until=deadline):
            Acting.current().refuse_expired()

    @pytest.mark.parametrize("deadline", [_PASSED, math.nan], ids=["passed", "not-a-number"])
    def test_refuses_past_the_deadline_logging_the_guidance(
        self, switches: CapturedLog, deadline: float
    ) -> None:
        with acting_as(_ALICE, until=deadline):
            acting = Acting.current()
            with pytest.raises(DelegationExpired, match="system identity"):
                acting.refuse_expired()

        assert switches.calls == [(IdentityEvent.DELEGATION_EXPIRED, {})]
