"""Grammar-aware resolver references in unresolved configuration strings."""

from __future__ import annotations

import pytest

from loom.core.config import ConfigError
from loom.core.config.resolver import referenced


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (r"\${secrets:ignored} ${ssm:/live}", {"ssm": ["/live"]}),
        ("${secrets:a,b}", {"secrets": ["a"]}),
        ("before ${secrets: ' /quoted/path ' , other} after", {"secrets": [" /quoted/path "]}),
        ('${ssm:  "/spaced"  }', {"ssm": ["/spaced"]}),
        ("${oc.env:X} ${db.url}", {}),
    ],
)
def test_referenced_uses_omegaconf_grammar(text: str, expected: dict[str, list[str]]) -> None:
    assert referenced(text, {"secrets", "ssm"}) == expected


def test_referenced_refuses_nested_interpolation() -> None:
    with pytest.raises(ConfigError, match="nests a placeholder"):
        referenced("${secrets:/path/${oc.env:X}}", {"secrets", "ssm"})
