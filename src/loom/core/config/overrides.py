"""``LOOM__SECTION__KEY`` variables: the last layer of an application's configuration.

Precedence, lowest first: defaults in code, ``base.yaml`` (or the first
explicit file), the overlay of the environment (or each later file), then
the ``LOOM__*`` variables. A double underscore separates the levels, as in
ASP.NET Core and pydantic-settings: ``LOOM__OBSERVABILITY__LOG__CONFIG__LEVEL``
sets ``observability.log.config.level``.

Each level matches the key the configuration already declares at that place,
whatever its case; a level the configuration does not declare is added in
lower case, and the section structs then refuse an unknown field. The first
level must name a section the configuration declares or loom knows
(:class:`~loom.core.config.keys.ConfigKey`, ``checks``); anything else is a
startup error naming the variable, so a typo is never ignored.

Values parse as an OmegaConf dotlist: ``5`` is an int, ``false`` a bool,
``[a, b]`` a list and ``{a: 1}`` a mapping. A list is replaced whole; its
items cannot be addressed one by one.

Secrets never go through these variables: they are logged by key on every
start. One line, at WARNING since it is written before the logger is
configured, names the overridden keys; their values are never logged.
"""

from __future__ import annotations

import logging
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
_KNOWN_SECTIONS: Final = frozenset({str(key).split(".", 1)[0] for key in ConfigKey} | {"checks"})


def load_application_config(
    *paths: str,
    resolvers: Sequence[ConfigResolver] = (),
    keyed: Sequence[str] = (),
) -> ConfigContext:
    """Load an application's configuration files, then apply the ``LOOM__*`` variables.

    Every loom entry point loads its configuration through this function.

    Args:
        *paths: Files merged left to right, as :func:`load_config` merges them.
        resolvers: Resolvers for ``${name:key}`` placeholders.
        keyed: Dotted paths of the collections merged by key.

    Returns:
        The merged configuration.

    Raises:
        ConfigError: When a file cannot be loaded, or a ``LOOM__*`` variable
            names no known section, has an empty level, or does not merge.
    """
    config = load_config(*paths, resolvers=resolvers, keyed=keyed)
    return ConfigContext(apply_overrides(config, override_variables()))


def apply_overrides(config: DictConfig, variables: Mapping[str, str]) -> DictConfig:
    """Merge *variables* (``LOOM__*`` name to value) over *config* and log their keys.

    Raises:
        ConfigError: When a variable names no known section, has an empty
            level, or its value does not merge into *config*.
    """
    import yaml
    from omegaconf import OmegaConf
    from omegaconf.errors import OmegaConfBaseException

    if not variables:
        return config
    keys = {name: _dotted_key(config, name) for name in sorted(variables)}
    merged: Any = config
    for name, key in keys.items():
        try:
            layer = OmegaConf.from_dotlist([f"{key}={variables[name]}"])
            merged = OmegaConf.merge(merged, layer)
        except (OmegaConfBaseException, yaml.YAMLError) as exc:
            # The variable is what the operator acts on; the value is not
            # echoed, as the parser's message would.
            raise ConfigError(f"{name} does not merge into {key!r}: {type(exc).__name__}") from exc
    _logger.warning(
        "configuration keys overridden by %s variables: %s",
        OVERRIDE_PREFIX,
        ", ".join(keys.values()),
    )
    result: DictConfig = merged
    return result


def _dotted_key(config: DictConfig, name: str) -> str:
    levels = name[len(OVERRIDE_PREFIX) :].split(_SEPARATOR)
    if not all(levels):
        raise ConfigError(f"{name} has an empty level; separate levels with one '__'")
    node: Any = config
    keys: list[str] = []
    for level in levels:
        key = _declared(node, level)
        keys.append(key)
        node = node.get(key) if _is_mapping(node) and key in node else None
    if keys[0] not in _KNOWN_SECTIONS and not _declared_in(config, levels[0]):
        raise ConfigError(
            f"{name} names no known configuration section {keys[0]!r}; "
            "check the variable for a typo"
        )
    return ".".join(keys)


def _declared(node: Any, level: str) -> str:
    if _is_mapping(node):
        for key in node:
            if str(key).casefold() == level.casefold():
                return str(key)
    return level.lower()


def _declared_in(config: DictConfig, level: str) -> bool:
    return any(str(key).casefold() == level.casefold() for key in config)


def _is_mapping(node: Any) -> bool:
    from omegaconf import DictConfig

    return isinstance(node, DictConfig)


__all__ = ["apply_overrides", "load_application_config"]
