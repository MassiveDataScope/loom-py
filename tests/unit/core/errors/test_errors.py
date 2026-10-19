from __future__ import annotations

import math
from typing import Any

import pytest

from loom.core.errors import (
    Conflict,
    DomainError,
    Forbidden,
    InsufficientAuthentication,
    LoomError,
    NotFound,
    RateLimited,
    RuleViolation,
    RuleViolations,
    SystemError,
    Unauthenticated,
)
from loom.core.errors.codes import ErrorCode

# ---------------------------------------------------------------------------
# LoomError base
# ---------------------------------------------------------------------------


class TestLoomError:
    def test_stores_message_and_code(self) -> None:
        err = LoomError("something broke", code="broken")
        assert err.message == "something broke"
        assert err.code == "broken"

    def test_is_exception(self) -> None:
        assert isinstance(LoomError("msg", code="x"), Exception)

    def test_str_is_message(self) -> None:
        assert str(LoomError("readable message", code="x")) == "readable message"


# ---------------------------------------------------------------------------
# Hierarchy — one parametrized test replaces eight individual issubclass checks
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "subclass,base",
    [
        (DomainError, LoomError),
        (SystemError, LoomError),
        (NotFound, DomainError),
        (Forbidden, DomainError),
        (Conflict, DomainError),
        (RateLimited, DomainError),
        (InsufficientAuthentication, Unauthenticated),
        (RuleViolation, DomainError),
        (RuleViolations, DomainError),
    ],
)
def test_error_hierarchy(subclass: type, base: type) -> None:
    assert issubclass(subclass, base)


def test_domain_error_not_system_error() -> None:
    assert not issubclass(DomainError, SystemError)


# ---------------------------------------------------------------------------
# Error codes — one parametrized test replaces per-class code checks
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "factory,expected_code",
    [
        (lambda: NotFound("User", id=1), ErrorCode.NOT_FOUND),
        (lambda: Forbidden(), ErrorCode.FORBIDDEN),
        (lambda: Conflict("x"), ErrorCode.CONFLICT),
        (lambda: RateLimited(1), ErrorCode.RATE_LIMITED),
        (lambda: InsufficientAuthentication(), ErrorCode.INSUFFICIENT_AUTHENTICATION),
        (lambda: SystemError("x"), ErrorCode.SYSTEM_ERROR),
        (lambda: RuleViolation("f", "m"), ErrorCode.RULE_VIOLATION),
        (lambda: RuleViolations([]), ErrorCode.RULE_VIOLATIONS),
    ],
)
def test_error_codes(factory: Any, expected_code: ErrorCode) -> None:
    assert factory().code == expected_code


# ---------------------------------------------------------------------------
# NotFound
# ---------------------------------------------------------------------------


class TestNotFound:
    def test_attributes(self) -> None:
        err = NotFound("User", id=42)
        assert err.entity == "User"
        assert err.id == 42

    def test_message_format(self) -> None:
        err = NotFound("Product", id="abc-123")
        assert "Product" in err.message
        assert "abc-123" in err.message

    def test_catchable_as_loom_error(self) -> None:
        with pytest.raises(LoomError):
            raise NotFound("User", id=1)


# ---------------------------------------------------------------------------
# Forbidden
# ---------------------------------------------------------------------------


class TestForbidden:
    def test_default_message(self) -> None:
        assert "Forbidden" in Forbidden().message

    def test_custom_message(self) -> None:
        assert Forbidden("You cannot update other users").message == "You cannot update other users"

    def test_no_reason_and_nothing_missing_by_default(self) -> None:
        error = Forbidden()
        assert (error.reason, error.missing) == (None, ())

    def test_a_reason_and_the_missing_permissions_travel_with_the_refusal(self) -> None:
        error = Forbidden(
            "Granting this needs a recent MFA sign-in",
            reason="mfa_required",
            missing=["grants.manage", "users.manage"],
        )
        assert error.reason == "mfa_required"
        assert error.missing == ("grants.manage", "users.manage")
        assert error.code == ErrorCode.FORBIDDEN

    def test_missing_given_as_an_iterator_is_kept(self) -> None:
        assert Forbidden("no", missing=iter(["grants.manage"])).missing == ("grants.manage",)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"reason": ""},
            {"reason": "  "},
            {"reason": 5},
            {"missing": "grants.manage"},
            {"missing": b"ab"},
            {"missing": ["ok", ""]},
            {"missing": [1]},
        ],
        ids=[
            "empty-reason",
            "blank-reason",
            "reason-not-a-string",
            "string-as-missing",
            "bytes-as-missing",
            "empty-permission",
            "not-a-string",
        ],
    )
    def test_a_malformed_reason_or_missing_is_refused(self, kwargs: dict[str, object]) -> None:
        with pytest.raises(ValueError):
            Forbidden("no", **kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# SystemError — must NOT be catchable as DomainError
# ---------------------------------------------------------------------------


def test_system_error_not_catchable_as_domain_error() -> None:
    caught = False
    try:
        raise SystemError("boom")
    except DomainError:
        caught = True
    except SystemError:
        pass
    assert not caught


# ---------------------------------------------------------------------------
# RuleViolation
# ---------------------------------------------------------------------------


class TestRuleViolation:
    def test_field_and_message(self) -> None:
        err = RuleViolation("email", "Invalid format")
        assert err.field == "email"
        assert err.message == "Invalid format"

    def test_str_contains_field_and_message(self) -> None:
        err = RuleViolation("email", "Invalid format")
        assert "email" in str(err)
        assert "Invalid format" in str(err)

    def test_catchable_as_domain_error(self) -> None:
        with pytest.raises(DomainError):
            raise RuleViolation("x", "y")

    def test_backward_compat_import_from_rule_module(self) -> None:
        from loom.core.use_case.rule import RuleViolation as RuleViolationFromRule

        assert RuleViolationFromRule is RuleViolation


# ---------------------------------------------------------------------------
# RuleViolations
# ---------------------------------------------------------------------------


class TestRuleViolations:
    def test_stores_violations_as_tuple(self) -> None:
        v1 = RuleViolation("email", "bad")
        v2 = RuleViolation("name", "empty")
        err = RuleViolations([v1, v2])
        assert err.violations == (v1, v2)

    def test_message_joins_violations(self) -> None:
        err = RuleViolations([RuleViolation("email", "bad"), RuleViolation("name", "empty")])
        assert "email" in err.message
        assert "name" in err.message

    def test_catchable_as_domain_error(self) -> None:
        error = RuleViolations([RuleViolation("f", "m")])
        with pytest.raises(DomainError):
            raise error

    def test_backward_compat_import_from_rule_module(self) -> None:
        from loom.core.use_case.rule import RuleViolations as RVFromRule

        assert RVFromRule is RuleViolations


# ---------------------------------------------------------------------------
# Executor uses NotFound (integration smoke)
# ---------------------------------------------------------------------------


class TestExecutorUsesNotFound:
    async def test_missing_entity_raises_not_found(self) -> None:
        from unittest.mock import AsyncMock

        from loom.core.engine.compiler import UseCaseCompiler
        from loom.core.engine.executor import RuntimeExecutor
        from loom.core.use_case.markers import LoadById
        from loom.core.use_case.use_case import UseCase

        class _Entity:
            pass

        class _UC(UseCase[Any, None]):
            async def execute(
                self,
                eid: int,
                entity: _Entity = LoadById(_Entity, by="eid"),
            ) -> None:
                pass

        repo = AsyncMock()
        repo.get_by_id = AsyncMock(return_value=None)

        executor = RuntimeExecutor(UseCaseCompiler())
        uc = _UC()
        with pytest.raises(NotFound) as exc_info:
            await executor.execute(
                uc,
                params={"eid": 7},
                dependencies={_Entity: repo},
            )

        assert exc_info.value.entity == "_Entity"
        assert exc_info.value.id == 7
        assert exc_info.value.code == ErrorCode.NOT_FOUND


class TestRateLimited:
    @pytest.mark.parametrize(("seconds", "expected"), [(0.2, 1), (1.5, 2), (2, 2), (0, 1), (-3, 1)])
    def test_retry_after_is_whole_seconds_rounded_up(self, seconds: float, expected: int) -> None:
        assert RateLimited(seconds).retry_after == expected

    @pytest.mark.parametrize("seconds", [math.nan, math.inf, -math.inf])
    def test_a_non_finite_delay_is_refused(self, seconds: float) -> None:
        with pytest.raises(ValueError, match="retry_after"):
            RateLimited(seconds)


class TestInsufficientAuthentication:
    def test_carries_the_step_up_requirements(self) -> None:
        error = InsufficientAuthentication(max_age=300, acr_values=["urn:mfa", "phr"])

        assert (error.max_age, error.acr_values) == (300, ("urn:mfa", "phr"))

    def test_accepts_a_max_age_of_zero(self) -> None:
        assert InsufficientAuthentication(max_age=0).max_age == 0

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"max_age": -1},
            {"max_age": True},
            {"acr_values": "urn:mfa"},
            {"acr_values": [""]},
            {"acr_values": ["urn mfa"]},
            {"acr_values": ['urn"mfa']},
            {"acr_values": ["urn\\mfa"]},
            {"acr_values": ["urn:mfá"]},
            {"acr_values": [1]},
            {"acr_values": b"urn"},
            {"message": 5},
        ],
        ids=[
            "negative-age",
            "bool-age",
            "single-str",
            "blank",
            "space",
            "quote",
            "backslash",
            "non-ascii",
            "int-item",
            "bytes",
            "non-str-message",
        ],
    )
    def test_refuses_what_a_challenge_cannot_carry(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(ValueError, match="InsufficientAuthentication"):
            InsufficientAuthentication(**kwargs)
