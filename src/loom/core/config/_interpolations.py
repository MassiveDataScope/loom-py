"""The placeholders of a configuration string, read by OmegaConf's own grammar.

This module is the one place loom touches the generated ANTLR parser behind
OmegaConf (``omegaconf.grammar``; written against omegaconf 2.3). It reads a
string the way the loader would, without resolving anything, so the deploy
manifest can name the secrets and parameters a configuration fetches.
``tests/unit/core/config/test_referenced.py`` pins every grammar path the
manifest relies on; an omegaconf upgrade that moves the parser fails there.
"""

from __future__ import annotations

from collections.abc import Collection, Iterator
from typing import Any

from loom.core.config.errors import ConfigError


def referenced(text: str, names: Collection[str]) -> dict[str, list[str]]:
    """The first argument of every ``${name:...}`` of *text* whose *name* is one of *names*.

    OmegaConf's own grammar reads *text*, so the surrounding literal text, an
    escaped ``\\${``, quotes and argument separators mean what they mean to
    the loader; nothing is resolved.

    Returns:
        The arguments by resolver name, as the resolver would receive them,
        in order of appearance; a name nothing references is absent.

    Raises:
        ConfigError: When a selected resolver's arguments nest another
            interpolation, whose value the caller cannot know as written.
    """
    if "${" not in text:
        return {}
    # Local imports keep omegaconf out of the import of ``loom.core.config``.
    from omegaconf.grammar.gen.OmegaConfGrammarParser import OmegaConfGrammarParser as Grammar
    from omegaconf.grammar_parser import parse
    from omegaconf.grammar_visitor import GrammarVisitor

    literal = GrammarVisitor(lambda _key, _memo: None, lambda **_kwargs: None, None)
    found: dict[str, list[str]] = {}
    for node in _parse_nodes(parse(text, parser_rule="configValue")):
        if not isinstance(node, Grammar.InterpolationResolverContext):
            continue
        call: Any = node  # the generated parser carries no useful types
        name = call.resolverName().getText()
        if name not in names:
            continue
        sequence = call.sequence()
        if any(isinstance(n, Grammar.InterpolationContext) for n in _parse_nodes(sequence)):
            raise ConfigError(f"nests a placeholder in a {name} key")
        arguments = [] if sequence is None else sequence.element()
        first = str(literal.visitElement(arguments[0])) if arguments else ""
        found.setdefault(name, []).append(first)
    return found


def _parse_nodes(tree: Any) -> Iterator[Any]:
    """*tree* and every node under it, parents first; nothing for ``None``."""
    if tree is None:
        return
    yield tree
    for index in range(tree.getChildCount()):
        yield from _parse_nodes(tree.getChild(index))


__all__ = ["referenced"]
