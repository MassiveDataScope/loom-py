"""A sink node is usable in a ``Process`` under ``mypy --strict``, without a cast.

The protocol exists so anyone can add a destination without importing loom or
inheriting from anything. That promise is a typing promise, so it is checked by
running the type checker over a snippet, not by asserting at run time.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

_BUILT_IN_SINKS = textwrap.dedent("""
    from loom.core.model import LoomStruct
    from loom.streaming import Backend, FromTopic, IntoTable, IntoTopic, Process, StreamFlow

    class Ev(LoomStruct):
        id: str

    table: StreamFlow[Ev, Ev] = StreamFlow(
        name="t",
        source=FromTopic("in", payload=Ev),
        process=Process(IntoTable(payload=Ev, table="t", backend=Backend.CLICKHOUSE, name="s")),
    )
    topic: StreamFlow[Ev, Ev] = StreamFlow(
        name="k",
        source=FromTopic("in", payload=Ev),
        process=Process(IntoTopic("out", payload=Ev)),
    )
""")

_THIRD_PARTY_SINK = textwrap.dedent("""
    from collections.abc import Sequence
    from dataclasses import dataclass
    from typing import Any, ClassVar

    from loom.core.model import LoomStruct
    from loom.streaming import FromTopic, Process, StreamFlow

    class Ev(LoomStruct):
        id: str

    class JsonlPartition:
        def write_batch(self, items: Sequence[Ev]) -> int:
            return len(items)

        def close(self) -> int:
            return 0

    @dataclass(frozen=True)
    class IntoJsonl:
        payload: type[Ev]
        name: str = "jsonl"
        router_branch_safe: ClassVar[bool] = True

        def build_partition(
            self,
            config: Any,
            worker_index: int,
            worker_count: int,
            bridge: Any = None,
            session_manager: Any = None,
        ) -> JsonlPartition:
            return JsonlPartition()

    flow: StreamFlow[Ev, Ev] = StreamFlow(
        name="own",
        source=FromTopic("in", payload=Ev),
        process=Process(IntoJsonl(payload=Ev)),
    )
""")


@pytest.mark.slow
@pytest.mark.parametrize(
    ("snippet", "case"),
    [(_BUILT_IN_SINKS, "built-in"), (_THIRD_PARTY_SINK, "third-party")],
    ids=["built_in", "third_party"],
)
def test_un_sink_pasa_mypy_strict_dentro_de_un_process(
    snippet: str, case: str, tmp_path: Path
) -> None:
    """Neither a built-in sink nor one written outside loom needs a cast."""
    module = tmp_path / "snippet.py"
    module.write_text(snippet, encoding="utf-8")

    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "mypy", "--strict", str(module)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"{case} sink does not type-check:\n{result.stdout}"
