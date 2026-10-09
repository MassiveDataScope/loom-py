"""FR-003 ratchet: outside ``_loom_type.py`` no module of ``loom.ai``, ``loom.core.cache`` or
``loom.rest`` calls a msgspec or pydantic conversion primitive on a boundary value; every
remaining call site is allow-listed with its measured count until its slice removes it.
"""

from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2] / "src" / "loom"
_PACKAGES = ("ai", "core/cache", "core/command", "rest")
_TARGETS = frozenset(
    {
        "msgspec.convert",
        "msgspec.json.Decoder",
        "msgspec.json.Encoder",
        "msgspec.to_builtins",
        "msgspec.json.encode",
        "msgspec.json.decode",
        "pydantic.TypeAdapter",
        "pydantic.type_adapter.TypeAdapter",
    }
)

_ALLOWED: dict[tuple[str, str], int] = {
    ("ai/a2a/_rpc.py", "msgspec.json.Decoder"): 1,
    ("ai/compiler/_fingerprint.py", "msgspec.json.encode"): 1,
    ("ai/declarative/_envelope.py", "msgspec.json.decode"): 1,
    ("ai/describe.py", "msgspec.to_builtins"): 1,
    ("ai/engines/pydantic_ai/_events.py", "msgspec.json.decode"): 1,
    ("ai/engines/pydantic_ai/_returns.py", "msgspec.json.encode"): 1,
    ("ai/fastapi/endpoints.py", "msgspec.json.Decoder"): 1,
    ("ai/fastapi/endpoints.py", "msgspec.json.decode"): 1,
    ("ai/fastapi/response.py", "msgspec.json.Encoder"): 1,
    ("ai/remote_auth.py", "msgspec.convert"): 1,
    ("ai/runtime/_grants.py", "msgspec.json.encode"): 1,
    ("core/cache/calls.py", "msgspec.json.encode"): 1,
    ("core/cache/calls.py", "msgspec.to_builtins"): 1,
    ("core/cache/keys.py", "msgspec.json.encode"): 1,
    ("core/cache/repository.py", "msgspec.convert"): 1,
    ("core/cache/repository.py", "msgspec.to_builtins"): 1,
    ("core/cache/result_codec.py", "msgspec.to_builtins"): 1,
    ("rest/_body.py", "msgspec.json.encode"): 1,
    ("rest/auth/middleware.py", "msgspec.json.encode"): 1,
    ("rest/fastapi/response.py", "msgspec.json.encode"): 1,
    ("rest/fastapi/router_runtime.py", "msgspec.json.decode"): 1,
    ("rest/fastapi/sql.py", "msgspec.json.Decoder"): 1,
    ("rest/fastapi/sql.py", "msgspec.json.Encoder"): 1,
    ("rest/rest_adapter.py", "msgspec.to_builtins"): 1,
}


def _import_bindings(tree: ast.Module) -> dict[str, str]:
    bindings: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname is None:
                    bindings[alias.name.split(".")[0]] = alias.name.split(".")[0]
                else:
                    bindings[alias.asname] = alias.name
        elif isinstance(node, ast.ImportFrom) and node.module is not None and node.level == 0:
            for alias in node.names:
                bindings[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return bindings


def _dotted_parts(func: ast.expr) -> list[str] | None:
    parts: list[str] = []
    while isinstance(func, ast.Attribute):
        parts.append(func.attr)
        func = func.value
    if not isinstance(func, ast.Name):
        return None
    parts.append(func.id)
    parts.reverse()
    return parts


def _resolved_call_name(node: ast.Call, bindings: dict[str, str]) -> str | None:
    parts = _dotted_parts(node.func)
    if parts is None:
        return None
    head = bindings.get(parts[0], parts[0])
    return ".".join([head, *parts[1:]])


def _count_target_calls(path: Path) -> Counter[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    bindings = _import_bindings(tree)
    counts: Counter[str] = Counter()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _resolved_call_name(node, bindings)
        if name in _TARGETS:
            counts[name] += 1
    return counts


def _observed_calls() -> dict[tuple[str, str], int]:
    observed: dict[tuple[str, str], int] = {}
    for package in _PACKAGES:
        for path in sorted((_SRC / package).rglob("*.py")):
            relative = path.relative_to(_SRC).as_posix()
            for name, count in sorted(_count_target_calls(path).items()):
                observed[(relative, name)] = count
    return observed


def _format_diff(observed: dict[tuple[str, str], int]) -> list[str]:
    lines: list[str] = []
    for key in sorted(set(observed) | set(_ALLOWED)):
        seen = observed.get(key, 0)
        listed = _ALLOWED.get(key, 0)
        if seen != listed:
            lines.append(f"{key}: observed {seen} vs listed {listed}")
    return lines


def test_boundary_conversion_calls_are_contained() -> None:
    diff = _format_diff(_observed_calls())
    assert not diff, "\n".join(["boundary call allow-list is out of date:", *diff])
