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
    MissingCaller,
    NotFound,
    PostCommitError,
    RateLimited,
    RuleViolation,
    RuleViolations,
    StoresNotCommitted,
    SystemError,
    Unauthenticated,
    UnknownJob,
    UnsupportedQuery,
)


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
            pytest.param(RuleViolation("name", "required"), id="rule-violation"),
            pytest.param(RuleViolations([RuleViolation("name", "required")]), id="violations"),
            pytest.param(UnsupportedQuery("no joins", backend="dynamo"), id="unsupported-query"),
            pytest.param(MissingCaller("Report", "caller", "Caller"), id="missing-caller"),
            pytest.param(Forbidden(reason="no", missing=["a"]), id="forbidden"),
            pytest.param(InsufficientAuthentication(max_age=60), id="insufficient-auth"),
            pytest.param(UnknownJob("Acme"), id="unknown-job"),
            pytest.param(JobTimeoutError("job-1", 2.0), id="job-timeout"),
            pytest.param(JobFailedError("job-1", ValueError("x")), id="job-failed"),
            pytest.param(StoresNotCommitted("audit rolled back"), id="stores-not-committed"),
            pytest.param(BoundaryValidationError("bad", (("f", "m"),)), id="boundary"),
            pytest.param(Unauthenticated(), id="unauthenticated"),
            pytest.param(DelegationExpired(), id="delegation-expired"),
            pytest.param(IdentityFixed(), id="identity-fixed"),
            pytest.param(Conflict("taken"), id="conflict"),
            pytest.param(SystemError("boom"), id="system-error"),
        ],
    )
    def test_keeps_class_args_and_fields(self, error: Exception) -> None:
        rebuilt: Any = pickle.loads(pickle.dumps(error))

        assert type(rebuilt) is type(error)
        assert rebuilt.args == error.args
        assert repr(vars(rebuilt)) == repr(vars(error))
