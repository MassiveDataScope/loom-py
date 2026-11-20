"""No handle or grant view takes an identity: each reads the execution identity when called."""

from __future__ import annotations

import inspect

import pytest

from loom.ai.execution._grants import McpGrantView, SqlGrantView
from loom.ai.execution._handle import _AgentHandle, _McpHandle  # pyright: ignore[reportPrivateUsage]

_HANDLES = (McpGrantView, SqlGrantView, _AgentHandle, _McpHandle)


def _callables(handle: type) -> list[tuple[str, object]]:
    return [
        (name, member)
        for name, member in vars(handle).items()
        if callable(member) and (name == "__init__" or not name.startswith("_"))
    ]


class TestHandleSignatures:
    @pytest.mark.parametrize("handle", _HANDLES, ids=[handle.__name__ for handle in _HANDLES])
    def test_takes_no_identity_parameter(self, handle: type) -> None:
        taking = [
            name
            for name, member in _callables(handle)
            if "identity" in inspect.signature(member).parameters  # type: ignore[arg-type]
        ]

        assert taking == []

    @pytest.mark.parametrize("handle", _HANDLES, ids=[handle.__name__ for handle in _HANDLES])
    def test_keeps_no_identity_attribute(self, handle: type) -> None:
        source = inspect.getsource(handle)

        assert "self._identity" not in source
