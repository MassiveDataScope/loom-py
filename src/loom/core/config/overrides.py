"""``LOOM__SECTION__KEY`` variables: the last layer of an application's configuration.

Precedence, lowest first: defaults in code, ``base.yaml`` (or the first
explicit file), the overlay of the environment (or each later file), then
the ``LOOM__*`` variables. A double underscore separates the levels, as in
ASP.NET Core and pydantic-settings: ``LOOM__OBSERVABILITY__LOG__CONFIG__LEVEL``
sets ``observability.log.config.level``.

Each level is letters, digits and underscores, and matches the key the
configuration already declares at that place, whatever its case; a level the
configuration does not declare is added in lower case, and a section type
that forbids unknown fields refuses it. The first level must name a section
the configuration declares or loom reads
(:class:`~loom.core.config.keys.ConfigKey`); anything else is a startup error
naming the variable. Two variables for the same key, or for a key and one of
its parents, are refused.

``checks`` and ``app.rest.auth`` cannot be overridden: silencing a deploy
check, the check modules and the authentication mechanism are declared in the
versioned files, where they are reviewed.

Values are literals. They parse as an OmegaConf dotlist (YAML 1.1): ``5`` is
an int, ``false``, ``no`` and ``off`` are booleans, ``[a, b]`` is a list,
``{a: 1}`` a mapping, an empty value is null and `` #`` starts a comment;
quote a value to keep it a string (``'"1.10"'``). A list is replaced whole;
its items cannot be addressed one by one. A value holding ``${`` is refused,
so an override cannot reach another variable or a secret; the value is
parsed first and every string in it is checked, so a YAML escape cannot
smuggle one in.

Do not pass secrets through these variables: one line, at WARNING since it is
written before the logger is configured, names every overridden key. Values
are never logged nor carried in an error.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final

from loom.core.config.context import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.config.keys import ConfigKey
from loom.core.config.loader import load_config
from loom.core.environment import OVERRIDE_PREFIX, override_variables

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from loom.core.config.resolver import ConfigResolver

_logger = logging.getLogger(__name__)

_SEPARATOR: Final = "__"
_LEVEL: Final = re.compile(r"[A-Za-z0-9_]+")
_INTERPOLATION: Final = "${"
_REVIEWED_ONLY: Final = (("checks",), ("app", "rest", "auth"))
_KNOWN_SECTIONS: Final = frozenset(str(key).split(".", 1)[0] for key in ConfigKey)


def load_application_config(
    *paths: str,
    resolvers: Sequence[ConfigResolver] = (),
    keyed: Sequence[str] = (),
) -> ConfigContext:
    """Load an application's configuration files, then apply the ``LOOM__*`` variables.

    Args:
        *paths: Files merged left to right, as :func:`load_config` merges them.
        resolvers: Resolvers for ``${name:key}`` placeholders.
        keyed: Dotted paths of the collections merged by key.

    Returns:
        The merged configuration.

    Raises:
        ConfigError: When a file cannot be loaded, or a ``LOOM__*`` variable
            names no known section or a reviewed one, has an empty or
            malformed level, collides with another variable, reaches inside
            an interpolated key, holds ``${``, or does not parse or merge.
            The error names the variable, never the value.
    """
    config = load_config(*paths, resolvers=resolvers, keyed=keyed)
    return ConfigContext(_apply_overrides(config, override_variables()))


def _apply_overrides(config: DictConfig, variables: Mapping[str, str]) -> DictConfig:
    from omegaconf import OmegaConf

    if not variables:
        return config
    written = OmegaConf.to_container(config, resolve=False)
    keys = {name: _dotted_key(written, name) for name in variables}
    _refuse_collisions(keys)
    merged: Any = config
    for name, key in sorted(keys.items(), key=lambda item: item[1]):
        merged = _merged(merged, name, key, _parsed(name, key, variables[name]))
    _logger.warning(
        "configuration keys overridden by %s variables: %s",
        OVERRIDE_PREFIX,
        ", ".join(sorted(keys.values())),
    )
    result: DictConfig = merged
    return result


def _parsed(name: str, key: str, value: str) -> Any:
    """Parse one override into a dotlist layer, refusing any ``${`` it carries."""
    import yaml
    from omegaconf import OmegaConf
    from omegaconf.errors import OmegaConfBaseException

    try:
        layer = OmegaConf.from_dotlist([f"{key}={value}"])
        written = OmegaConf.to_container(layer, resolve=False)
    except (OmegaConfBaseException, yaml.YAMLError):
        # From None: the parser's exception, if chained, would quote the value.
        raise ConfigError(f"{name} does not parse as a value for {key!r}") from None
    if any(_INTERPOLATION in text for text in _strings(written)):
        raise ConfigError(f"{name} holds an interpolation; override values are literals")
    _refuse_reviewed(name, written)
    return layer


def _merged(config: Any, name: str, key: str, layer: Any) -> Any:
    from omegaconf import OmegaConf
    from omegaconf.errors import OmegaConfBaseException

    try:
        return OmegaConf.merge(config, layer)
    except OmegaConfBaseException:
        # From None: the merge error, if chained, would quote the value.
        raise ConfigError(f"{name} does not merge into {key!r}") from None


def _strings(node: Any) -> list[str]:
    if isinstance(node, str):
        return [node]
    if isinstance(node, dict):
        return [text for value in node.values() for text in _strings(value)]
    if isinstance(node, list):
        return [text for value in node for text in _strings(value)]
    return []


def _refuse_reviewed(name: str, layer: Any) -> None:
    for reviewed in _REVIEWED_ONLY:
        if _reaches(layer, reviewed):
            raise ConfigError(
                f"{name} overrides {'.'.join(reviewed)!r}, which is declared only in the "
                "configuration files, where it is reviewed"
            )


def _reaches(node: Any, path: tuple[str, ...]) -> bool:
    """Whether the parsed layer *node* sets anything at or under *path*, any case."""
    for part in path:
        if not isinstance(node, dict):
            return False
        node = next((v for k, v in node.items() if str(k).casefold() == part), _ABSENT)
        if node is _ABSENT:
            return False
    return True


_ABSENT: Final = object()


def _refuse_collisions(keys: Mapping[str, str]) -> None:
    ordered = sorted(keys.items(), key=lambda item: item[1].casefold())
    for (first, key), (second, other) in zip(ordered, ordered[1:], strict=False):
        folded, other_folded = key.casefold(), other.casefold()
        if folded == other_folded or other_folded.startswith(f"{folded}."):
            raise ConfigError(f"{first} and {second} set the same key or one inside the other")


def _dotted_key(written: Any, name: str) -> str:
    levels = name[len(OVERRIDE_PREFIX) :].split(_SEPARATOR)
    if not all(_LEVEL.fullmatch(level) for level in levels):
        raise ConfigError(
            f"{name} has an empty or malformed level; a level is letters, digits and "
            "underscores, separated by '__'"
        )
    node: Any = written
    keys: list[str] = []
    for level in levels:
        if isinstance(node, str) and _INTERPOLATION in node:
            raise ConfigError(
                f"{name} reaches inside {'.'.join(keys)!r}, which is an interpolation; "
                "override the whole key in the files instead"
            )
        key = _declared(node, level)
        keys.append(key)
        node = node.get(key) if isinstance(node, dict) else None
    section = keys[0]
    if section not in _KNOWN_SECTIONS and not (isinstance(written, dict) and section in written):
        raise ConfigError(
            f"{name} names no known configuration section {section!r}; "
            "check the variable for a typo"
        )
    return ".".join(keys)


def _declared(node: Any, level: str) -> str:
    if isinstance(node, dict):
        for key in node:
            if str(key).casefold() == level.casefold():
                return str(key)
    return level.lower()


__all__ = ["load_application_config"]
