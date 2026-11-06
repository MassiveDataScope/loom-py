"""Grammar-aware resolver references in unresolved configuration strings."""

from __future__ import annotations

import pytest

from loom.core.config import ConfigError
from loom.core.config.resolver import referenced


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (r"\${secrets:ignored} ${ssm:/live}", {"ssm": ["/live"]}),
        (r"literal \${secrets:ignored} only", {}),
        ("${secrets:/app/db.host}", {"secrets": ["/app/db.host"]}),
        ("${ssm:/app/%ENV%/url}", {"ssm": ["/app/%ENV%/url"]}),
        ("${secrets:/a} and ${secrets:/b} and ${ssm:/c}", {"secrets": ["/a", "/b"], "ssm": ["/c"]}),
        ("${oc.env:X,${secrets:/fallback}}", {"secrets": ["/fallback"]}),
        ("plain text without placeholders", {}),
        ("${secrets:a,b}", {"secrets": ["a"]}),
        ("before ${secrets: ' /quoted/path ' , other} after", {"secrets": [" /quoted/path "]}),
        ('${ssm:  "/spaced"  }', {"ssm": ["/spaced"]}),
        ("${oc.env:X} ${db.url}", {}),
    ],
)
def test_referenced_uses_omegaconf_grammar(text: str, expected: dict[str, list[str]]) -> None:
    assert referenced(text, {"secrets", "ssm"}) == expected


@pytest.mark.parametrize(
    "text",
    [
        "${secrets:/path/${oc.env:X}}",
        "${secrets:${ssm:/name}}",
        "${ssm:/first,${oc.env:SECOND}}",
    ],
)
def test_referenced_refuses_nested_interpolation(text: str) -> None:
    with pytest.raises(ConfigError, match="nests a placeholder in a (secrets|ssm) key"):
        referenced(text, {"secrets", "ssm"})


def test_the_grammar_entry_points_loom_relies_on_exist() -> None:
    """An omegaconf upgrade that moves the parser fails here, not in a deploy."""
    from omegaconf.grammar.gen.OmegaConfGrammarParser import OmegaConfGrammarParser
    from omegaconf.grammar_parser import parse
    from omegaconf.grammar_visitor import GrammarVisitor

    tree = parse("${secrets:/x}", parser_rule="configValue")
    assert hasattr(OmegaConfGrammarParser, "InterpolationResolverContext")
    assert hasattr(OmegaConfGrammarParser, "InterpolationContext")
    assert callable(GrammarVisitor(lambda _k, _m: None, lambda **_: None, None).visitElement)
    assert tree.getChildCount() > 0
