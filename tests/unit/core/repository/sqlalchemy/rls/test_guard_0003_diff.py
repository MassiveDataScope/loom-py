"""Each function revision 3 replaces differs from its previous body only by a reviewed delta.

A replaced guard function is a copy with a change in it; read alone, the copy
hides the change. This test diffs every ``CREATE OR REPLACE FUNCTION`` in
``0003.sql`` against the last earlier definition and pins the digest of that
diff. A failure prints the diff, so changing a replaced function means reading
exactly what changed and updating its digest here.
"""

from __future__ import annotations

import difflib
import hashlib
import re
from pathlib import Path

import pytest

_GUARD = Path(__file__).parents[6] / "src/loom/core/repository/sqlalchemy/rls/guard"
_HEADER = re.compile(r"^CREATE (OR REPLACE )?FUNCTION (\w+)\(", re.MULTILINE)


def _definitions(sql: str) -> dict[str, tuple[bool, str]]:
    """Map each function name to (replaces, body text) in one revision file."""
    found: dict[str, tuple[bool, str]] = {}
    starts = list(_HEADER.finditer(sql))
    for match, following in zip(starts, [*starts[1:], None], strict=True):
        end = following.start() if following else len(sql)
        body = sql[match.start() : end]
        body = body[: body.find("$$;") + 3] if "$$;" in body else body
        found[match.group(2)] = (bool(match.group(1)), body)
    return found


def _previous() -> dict[str, str]:
    earlier: dict[str, str] = {}
    for name in ("0001.sql", "0002.sql"):
        earlier.update({k: v for k, (_, v) in _definitions((_GUARD / name).read_text()).items()})
    return earlier


_REPLACED = {
    name: body
    for name, (replaces, body) in _definitions((_GUARD / "0003.sql").read_text()).items()
    if replaces
}

EXPECTED_DIFFS = {
    "assert_scoped_schema": "7048347eb71d1da1790f26cd59ad18a08b48e9e9ce7b507121896b2e08faf89a",
    "grant_bypass": "df474254039d75c5619066e0eca545c0db5a5daa0a48b2d67f1bfd70ea27ac72",
    "configure": "157024470e40f048e8b9610b08a20a0f6d29a7fde880d3d27ff254f309335cbc",
    "on_sql_drop": "a0d9459c68e0a3442a9501ac9a3b2d132c3877cb36418d026c315337f0488db2",
    "owner_functions": "e2510df74dae525ce39e4112e73929283c84ab58c129877a73d992f60516bcbe",
    "role_specs": "85189491b4bd0ea6abd723cd2383be7110c38174008e06016f75ff5828bf056b",
    "secure_guard": "e5db369a8f600c4f6e18f81080c4759aa84c8de36969c55fac722401f8efd1ba",
    "unexpected_memberships": "66b1a5a985544260ef87a9a1bf4653f9288ea05316b7217b6a31215881d4af44",
}


def _diff(name: str) -> str:
    before = _previous()[name].splitlines()
    after = _REPLACED[name].splitlines()
    lines = difflib.unified_diff(before, after, lineterm="", n=0)
    return "\n".join(line for line in lines if not line.startswith(("---", "+++")))


def test_the_replaced_functions_are_exactly_the_reviewed_set() -> None:
    assert sorted(_REPLACED) == sorted(EXPECTED_DIFFS)


def test_every_replaced_function_replaces_an_earlier_definition() -> None:
    assert set(_REPLACED) <= set(_previous())


@pytest.mark.parametrize("name", sorted(EXPECTED_DIFFS))
def test_a_replaced_function_changes_only_by_its_reviewed_delta(name: str) -> None:
    delta = _diff(name)
    digest = hashlib.sha256(delta.encode()).hexdigest()

    review = f"{name} changed; review the delta and pin {digest}:\n{delta}"
    assert digest == EXPECTED_DIFFS[name], review
