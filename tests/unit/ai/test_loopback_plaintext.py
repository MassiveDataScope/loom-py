"""Plaintext is acceptable only where the traffic cannot leave the machine.

Requiring TLS from a developer's own MCP server buys nothing — there is no
network to intercept — and costs a self-signed certificate before anything
can be tried locally. The exception is narrow and deliberately audible: the
same URL on any other host is refused, so a configuration that works on a
laptop and fails in staging says so at start-up.
"""

from __future__ import annotations

import logging

import pytest

from loom.ai.config import A2AConfig
from loom.ai.errors import AgentCompilationError
from loom.core.model.redaction import Dsn


@pytest.mark.parametrize(
    "url", ["http://localhost:8000", "http://127.0.0.1:9000", "http://[::1]:9000"]
)
def test_acepta_plaintext_en_loopback(url: str) -> None:
    """Every spelling of "this machine" is the same exception."""
    assert A2AConfig(base_url=Dsn(url), expose=("triage",)) is not None


@pytest.mark.parametrize("url", ["http://api.example.com", "http://192.168.1.10:8000"])
def test_rechaza_plaintext_fuera_de_loopback(url: str) -> None:
    """A private address is still a network: the refusal stands."""
    address = Dsn(url)
    with pytest.raises(AgentCompilationError):
        A2AConfig(base_url=address, expose=("triage",))


def test_el_permiso_es_audible_no_silencioso(caplog: pytest.LogCaptureFixture) -> None:
    """An exception nobody hears is how a local-only config reaches staging."""
    with caplog.at_level(logging.WARNING):
        A2AConfig(base_url=Dsn("http://localhost:8000"), expose=("triage",))
    assert "local-only" in caplog.text


def test_https_no_avisa_de_nada(caplog: pytest.LogCaptureFixture) -> None:
    """The warning marks the exception, so the normal case must stay quiet."""
    with caplog.at_level(logging.WARNING):
        A2AConfig(base_url=Dsn("https://api.example.com"), expose=("triage",))
    assert caplog.text == ""
