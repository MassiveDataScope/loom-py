"""Contract tests for loom.core.compiler."""

from __future__ import annotations

import loom.core.compiler as compiler_module
from loom.core.compiler import CompilerProtocol


class _IntCompiler:
    def __init__(self) -> None:
        self.requests: list[str] = []

    def compile(self, request: str) -> int:
        self.requests.append(request)
        return len(request)


class TestCompilerProtocol:
    def test_module_exports_only_the_protocol(self) -> None:
        assert compiler_module.__all__ == ["CompilerProtocol"]

    def test_compiler_protocol_is_structurally_satisfied(self) -> None:
        compiler: CompilerProtocol[str, int] = _IntCompiler()

        assert compiler.compile("abc") == 3
        assert isinstance(compiler, _IntCompiler)
        assert compiler.requests == ["abc"]  # type: ignore[attr-defined]
