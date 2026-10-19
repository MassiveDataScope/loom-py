"""``LOOM__SECTION__KEY`` variables: the last layer of an application's configuration.

Precedence, lowest first: defaults in code, ``base.yaml`` (or the first
explicit file), the overlay of the environment (or each later file), then
the ``LOOM__*`` variables. A double underscore separates the levels, as in
ASP.NET Core and pydantic-settings: ``LOOM__OBSERVABILITY__LOG__CONFIG__LEVEL``
sets ``observability.log.config.level``.

Each level matches the key the configuration already declares at that place,
whatever its case; a level the configuration does not declare is added in
lower case, and a section type that forbids unknown fields refuses it. The
first level must name a section the configuration declares or loom reads
(:class:`~loom.core.config.keys.ConfigKey` and the sections a package
registers with :func:`register_config_sections`); anything else is a startup
error naming the variable.

The ``checks`` section cannot be overridden: silencing a deploy check or
changing the modules that register checks is declared in the versioned
files, where it is reviewed.

Values are literals. They parse as an OmegaConf dotlist (YAML 1.1): ``5`` is
an int, ``false``, ``no`` and ``off`` are booleans, ``[a, b]`` is a list,
``{a: 1}`` a mapping, an empty value is null and `` #`` starts a comment;
quote a value to keep it a string (``'"1.10"'``). A list is replaced whole;
its items cannot be addressed one by one. A value holding ``${`` is refused,
so an override cannot reach another variable or a secret.

Do not pass secrets through these variables: one line, at WARNING since it is
written before the logger is configured, names every overridden key. Values
are never logged nor carried in an error.
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
_INTERPOLATION: Final = "${"
_REVIEWED_ONLY: Final = frozenset({"checks"})
_known_sections: set[str] = {str(key).split(".", 1)[0] for key in ConfigKey}


def register_config_sections(*names: str) -> None:
    """Declare top-level sections a package reads, so ``LOOM__`` may name them.

    Args:
        *names: Section names, as written in the configuration.
    """
    _known_sections.update(names)


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
            is refused (see :func:`apply_overrides`).
    """
    config = load_config(*paths, resolvers=resolvers, keyed=keyed)
    return ConfigContext(apply_overrides(config, override_variables()))


def apply_overrides(config: DictConfig, variables: Mapping[str, str]) -> DictConfig:
    """Merge *variables* (``LOOM__*`` name to value) over *config* and log their keys.

    The key path is walked on the configuration as written, so no placeholder
    is resolved while the overrides are applied.

    Raises:
        ConfigError: When a variable names no known section or the ``checks``
            section, has an empty level, reaches inside an interpolated key,
            holds ``${``, or its value does not parse or merge. The error
            names the variable, never the value.
    """
    from omegaconf import OmegaConf

    if not variables:
        return config
    written = OmegaConf.to_container(config, resolve=False)
    keys = {name: _dotted_key(written, name) for name in sorted(variables)}
    merged: Any = config
    for name, key in keys.items():
        merged = _merge(merged, name, key, variables[name])
    _logger.warning(
        "configuration keys overridden by %s variables: %s",
        OVERRIDE_PREFIX,
        ", ".join(keys.values()),
    )
    result: DictConfig = merged
    return result


def _merge(config: Any, name: str, key: str, value: str) -> Any:
    import yaml
    from omegaconf import OmegaConf
    from omegaconf.errors import OmegaConfBaseException

    if _INTERPOLATION in value:
        raise ConfigError(f"{name} holds an interpolation; override values are literals")
    try:
        return OmegaConf.merge(config, OmegaConf.from_dotlist([f"{key}={value}"]))
    except (OmegaConfBaseException, yaml.YAMLError):
        # Raised from None: the parser's message, chained as a cause, quotes the value.
        raise ConfigError(f"{name} does not parse or merge into {key!r}") from None


def _dotted_key(written: Any, name: str) -> str:
    levels = name[len(OVERRIDE_PREFIX) :].split(_SEPARATOR)
    if not all(levels):
        raise ConfigError(f"{name} has an empty level; separate levels with one '__'")
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
    if section.casefold() in _REVIEWED_ONLY:
        raise ConfigError(
            f"{name} overrides the {section!r} section, which is declared only in the "
            "configuration files, where it is reviewed"
        )
    if section not in _known_sections and not (isinstance(written, dict) and section in written):
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


__all__ = ["apply_overrides", "load_application_config", "register_config_sections"]
