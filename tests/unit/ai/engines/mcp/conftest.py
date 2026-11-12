"""No proxy reaches these tests from the machine running them.

A toolset behind a proxy keeps fastmcp's stock transport, so a proxy variable
or a system proxy setting on the machine would change which transport every
test here builds. A test that wants a proxy sets one itself.
"""

from __future__ import annotations

import os
import urllib.request

import pytest


@pytest.fixture(autouse=True)
def _no_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ):
        if key.lower().endswith("_proxy"):
            monkeypatch.delenv(key)
    monkeypatch.setattr(urllib.request, "getproxies", urllib.request.getproxies_environment)
