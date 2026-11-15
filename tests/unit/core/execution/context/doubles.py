"""Doubles shared by the execution context tests."""

from __future__ import annotations

from loom.core.identity import Identity

ACME_ALICE = Identity(subject="acme-alice", mechanism="test")
ACME_BOB = Identity(subject="acme-bob", mechanism="test")


class AcmePerExecutionValue:
    """A per-execution value."""


class AcmeBuiltSession:
    """Built per execution."""


class PlannedFailure(Exception):
    pass
