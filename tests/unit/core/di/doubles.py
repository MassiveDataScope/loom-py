"""Doubles shared by more than one dependency-injection subpackage."""

from __future__ import annotations

from tests.unit.core.di.graph.acme import AcmeInput


class AcmeSession:
    """Bound per execution."""


class AcmeTakesInput:
    def __init__(self, payload: object = AcmeInput()) -> None:
        self.payload = payload
