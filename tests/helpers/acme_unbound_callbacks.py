"""A job callback whose dependency nothing binds, listed as one module to be found by the scan."""

from __future__ import annotations

from loom.core.job.callback import JobCallback

MODULE = __name__
"""The entry that makes the scan find the callback below."""


class AcmeNeedsUnboundCallback(JobCallback):
    """Injects a service nothing binds."""

    def __init__(self, missing: AcmeUnbound) -> None:
        self.missing = missing


class AcmeUnbound:
    """A class with a required parameter no binding serves."""

    def __init__(self, name: str) -> None:
        self.name = name
