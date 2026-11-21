"""Every framework error survives pickling with its class and fields."""

from __future__ import annotations

import pickle
from typing import Any

import pytest

from loom.core.errors import (
    BoundaryValidationError,
    Conflict,
    DelegationExpired,
    Forbidden,
    IdentityFixed,
    InsufficientAuthentication,
    JobFailedError,
    JobTimeoutError,
    LoomError,
    MissingCaller,
    NotFound,
    PostCommitError,
    RateLimited,
    RuleViolationError,
    RuleViolationsError,
    StoresNotCommittedError,
    SystemError,
    Unauthenticated,
    UnknownJob,
    UnsupportedQuery,
)
from loom.core.errors.codes import ErrorCode


class SlottedError(LoomError):
    __slots__ = ("extra",)

    def __init__(self, *, extra: int) -> None:
        self.extra = extra
        super().__init__("slotted", code=ErrorCode.SYSTEM_ERROR)


class TestErrorPickling:
    @pytest.mark.parametrize(
        "error",
        [
            pytest.param(
                PostCommitError(committed=True, failures=(ValueError("a"), OSError("b"))),
                id="post-commit",
            ),
            pytest.param(NotFound("User", id=42), id="not-found"),
            pytest.param(RateLimited(3.5), id="rate-limited"),
            pytest.param(RuleViolationError("name", "required"), id="rule-violation"),
            pytest.param(
                RuleViolationsError([RuleViolationError("name", "required")]), id="violations"
            ),
            pytest.param(UnsupportedQuery("no joins", backend="dynamo"), id="unsupported-query"),
            pytest.param(MissingCaller("Report", "caller", "Caller"), id="missing-caller"),
            pytest.param(Forbidden(reason="no", missing=["a"]), id="forbidden"),
            pytest.param(InsufficientAuthentication(max_age=60), id="insufficient-auth"),
            pytest.param(UnknownJob("Acme"), id="unknown-job"),
            pytest.param(JobTimeoutError("job-1", 2.0), id="job-timeout"),
            pytest.param(JobFailedError("job-1", ValueError("x")), id="job-failed"),
            pytest.param(StoresNotCommittedError("audit rolled back"), id="stores-not-committed"),
            pytest.param(BoundaryValidationError("bad", (("f", "m"),)), id="boundary"),
            pytest.param(Unauthenticated(), id="unauthenticated"),
            pytest.param(DelegationExpired(), id="delegation-expired"),
            pytest.param(IdentityFixed(), id="identity-fixed"),
            pytest.param(Conflict("taken"), id="conflict"),
            pytest.param(SlottedError(extra=3), id="slotted"),
            pytest.param(SystemError("boom"), id="system-error"),
        ],
    )
    def test_keeps_class_args_and_fields(self, error: Exception) -> None:
        rebuilt: Any = pickle.loads(pickle.dumps(error))

        assert type(rebuilt) is type(error)
        assert rebuilt.args == error.args
        assert repr(rebuilt.__getstate__()) == repr(error.__getstate__())
