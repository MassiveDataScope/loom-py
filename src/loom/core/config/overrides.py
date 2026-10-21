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

An override sets one leaf value. A value that is a mapping, or a list
holding one at any depth, is refused, and so is a target the files declare
that way, whatever the new value (null, a scalar or a list): an override
cannot replace a structure. Lists of scalars are leaves.

In production and staging an override may set only an allowlisted key: the
operational keys loom lists in :data:`OVERRIDABLE` (log levels, worker and
pool sizes, driver timeouts), plus the keys the reviewed files list under
``config.overridable``. Every other key is a startup error naming the
variable. An entry is a dotted key; ``*`` stands for one level but never the
first, and an entry inside ``checks`` or ``config`` is refused: both are read
as written, so the files could not route them to the environment otherwise.
The entries are checked at every load, in any environment and with or
without a variable. In
development and test any leaf may be overridden. ``config`` itself can never
be overridden, in any environment.

An allowlisted key also changes every ``${...}`` placeholder in the files
that references it, so the files are reviewed with that in mind; a key the
files set from ``${oc.env:...}`` is controlled by the process environment
because the files chose so.

Values are literals. They parse as an OmegaConf dotlist (YAML 1.1): ``5`` is
an int, ``false``, ``no`` and ``off`` are booleans, ``[a, b]`` is a list,
``{a: 1}`` a mapping, an empty value is null and `` #`` starts a comment;
quote a value to keep it a string (``'"1.10"'``). A list is replaced whole;
its items cannot be addressed one by one. A value holding ``${`` is refused,
so an override cannot reach another variable or a secret; the value is
parsed first and every string in it is checked, so a YAML escape cannot
smuggle one in.

Do not pass secrets through these variables: one line, at WARNING since it is
written before the logger is configured, names every overridden key. This
module never logs or raises with an override's value.
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
from loom.core.environment import (
    OVERRIDE_PREFIX,
    Environment,
    override_variables,
    resolve_environment,
)

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from loom.core.config.resolver import ConfigResolver

_logger = logging.getLogger(__name__)

_SEPARATOR: Final = "__"
_LEVEL: Final = re.compile(r"\w+", re.ASCII)  # ASCII: \w is [A-Za-z0-9_]
_INTERPOLATION: Final = "${"
_ENTRY: Final = re.compile(r"(\w+|\*)(\.(\w+|\*))*", re.ASCII)
_ALLOWLIST_SECTION: Final = "config"
_ALLOWLIST_FIELD: Final = "overridable"
_ALLOWLIST_KEY: Final = f"{_ALLOWLIST_SECTION}.{_ALLOWLIST_FIELD}"
_PROTECTED: Final = ((_ALLOWLIST_SECTION,), ("checks",), ("persistence", "cursors"))
"""Keys no ``config.overridable`` entry may open, with everything below them.

``config`` and ``checks`` are read as written, so the files cannot route them to
the environment any other way. ``persistence.cursors`` decides whether, and with
which key, cursor tokens are signed, which no per-deploy variable may change."""

OVERRIDABLE: Final = (
    "observability.log.config.level",
    "observability.log.config.named_levels.*",
    "celery.worker_concurrency",
    "celery.worker_max_tasks_per_child",
    "celery.worker_prefetch_multiplier",
    "celery.runtime.shutdown_timeout_ms",
    "streaming.runtime.workers_per_process",
    "streaming.runtime.epoch_interval_ms",
    "streaming.runtime.force_shutdown_timeout_ms",
    "sql.connections.*.pool_size",
    "sql.connections.*.executor_threads",
    "sql.connections.*.connect_timeout",
    "sql.connections.*.send_receive_timeout",
    "database.pool.size",
    "database.pool.max_overflow",
    "database.pool.timeout",
    "database.pool.recycle",
    "persistence.dynamodb.max_pool_connections",
    "persistence.dynamodb.connect_timeout",
    "persistence.dynamodb.read_timeout",
    "persistence.dynamodb.max_attempts",
)
"""Keys a ``LOOM__`` variable may set in production and staging; ``*`` is one level."""
_KNOWN_SECTIONS: Final = frozenset(str(key).split(".", 1)[0] for key in ConfigKey)


def load_application_config(
    *paths: str,
    resolvers: Sequence[ConfigResolver] = (),
    keyed: Sequence[str] = (),
    environment: Environment | str | None = None,
) -> ConfigContext:
    """Load an application's configuration files, then apply the ``LOOM__*`` variables.

    Args:
        *paths: Files merged left to right, as :func:`load_config` merges them.
        resolvers: Resolvers for ``${name:key}`` placeholders.
        keyed: Dotted paths of the collections merged by key.
        environment: Environment deciding which keys an override may set,
            resolved like :func:`~loom.core.environment.resolve_environment`
            when ``None``.

    Returns:
        The merged configuration.

    Raises:
        ConfigError: When a file cannot be loaded; when ``config.overridable``
            is not a literal list of dotted keys or an entry opens a protected
            key, checked at every load; or when a ``LOOM__*`` variable names
            no known section, a key off the allowlist in a deployed
            environment or ``config``, has an empty or malformed level,
            collides with another variable, replaces a declared structure,
            reaches inside an interpolated key, holds ``${``, or does not
            parse or merge. The error names the variable, never the value.
    """
    config = load_config(*paths, resolvers=resolvers, keyed=keyed)
    resolved = resolve_environment(environment)
    return ConfigContext(_apply_overrides(config, override_variables(), resolved))


def _apply_overrides(
    config: DictConfig, variables: Mapping[str, str], environment: Environment
) -> DictConfig:
    from omegaconf import OmegaConf

    written = OmegaConf.to_container(config, resolve=False)
    # Validated with or without variables, so a bad entry fails in development.
    declared = _declared_allowlist(written)
    if not variables:
        return config
    keys = {name: _leaf_key(written, name) for name in variables}
    _refuse_collisions(keys)
    for name, key in keys.items():
        if key.split(".", 1)[0].casefold() == _ALLOWLIST_SECTION:
            raise ConfigError(
                f"{name} overrides {key!r}; {_ALLOWLIST_KEY} is declared only in the "
                "configuration files, where it is reviewed"
            )
        if environment.is_deployed:
            _require_allowlisted(name, key, (*OVERRIDABLE, *declared), environment)
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
    """Parse one override into a dotlist layer holding a single literal leaf."""
    from omegaconf import OmegaConf

    try:
        layer = OmegaConf.from_dotlist([f"{key}={value}"])
        written = OmegaConf.to_container(layer, resolve=False)
    except Exception:
        # Any parse failure, a YAML tag that fails to construct included; from
        # None, since the chained exception's message may quote the value.
        raise ConfigError(f"{name} does not parse as a value for {key!r}") from None
    if any(_INTERPOLATION in text for text in _strings(written)):
        raise ConfigError(f"{name} holds an interpolation; override values are literals")
    leaf: Any = written
    for level in key.split("."):
        leaf = leaf[level]
    if _holds_mapping(leaf):
        raise ConfigError(f"{name} is or holds a mapping; an override sets one leaf value")
    return layer


def _holds_mapping(node: Any) -> bool:
    """Whether *node* is a mapping or a list holding one, at any depth."""
    if isinstance(node, dict):
        return True
    return isinstance(node, list) and any(_holds_mapping(item) for item in node)


def _merged(config: Any, name: str, key: str, layer: Any) -> Any:
    from omegaconf import OmegaConf
    from omegaconf.errors import OmegaConfBaseException

    try:
        return OmegaConf.merge(config, layer)
    except OmegaConfBaseException:
        # From None: chained, the merge error would print with the traceback,
        # and its message may quote the value.
        raise ConfigError(f"{name} does not merge into {key!r}") from None


def _strings(node: Any) -> list[str]:
    if isinstance(node, str):
        return [node]
    if isinstance(node, dict):
        return [text for value in node.values() for text in _strings(value)]
    if isinstance(node, list):
        return [text for value in node for text in _strings(value)]
    return []


def _refuse_collisions(keys: Mapping[str, str]) -> None:
    ordered = sorted(keys.items(), key=lambda item: item[1].casefold())
    for (first, key), (second, other) in zip(ordered, ordered[1:], strict=False):
        folded, other_folded = key.casefold(), other.casefold()
        if folded == other_folded or other_folded.startswith(f"{folded}."):
            raise ConfigError(f"{first} and {second} set the same key or one inside the other")


def _leaf_key(written: Any, name: str) -> str:
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
    key = ".".join(keys)
    if _holds_mapping(node):
        raise ConfigError(
            f"{name} targets {key!r}, which the files declare as a structure; an override "
            "sets one leaf value"
        )
    return key


def _declared_allowlist(written: Any) -> tuple[str, ...]:
    """Return ``config.overridable`` as the files declare it, a literal list of dotted keys."""
    section = written.get(_ALLOWLIST_SECTION) if isinstance(written, dict) else None
    entries = section.get(_ALLOWLIST_FIELD, []) if isinstance(section, dict) else []
    valid = isinstance(entries, list) and all(
        isinstance(entry, str) and _ENTRY.fullmatch(entry) for entry in entries
    )
    if not valid:
        raise ConfigError(
            f"{_ALLOWLIST_KEY} must be a literal list of dotted keys, '*' standing for one level"
        )
    for entry in entries:
        _refuse_protected(entry)
    return tuple(entries)


def _refuse_protected(entry: str) -> None:
    """Refuse an entry with ``*`` first, or one that can match a key under a protected one."""
    levels = entry.casefold().split(".")
    opens_protected = any(
        len(levels) >= len(protected)
        and all(level in ("*", part) for level, part in zip(levels, protected, strict=False))
        for protected in _PROTECTED
    )
    if levels[0] == "*" or opens_protected:
        raise ConfigError(
            f"{_ALLOWLIST_KEY} entry {entry!r} opens a key no override may set: '*' as the "
            "first level, checks, config or persistence.cursors"
        )


def _require_allowlisted(
    name: str, key: str, allowlist: Sequence[str], environment: Environment
) -> None:
    levels = key.casefold().split(".")
    for entry in allowlist:
        parts = entry.casefold().split(".")
        if len(parts) == len(levels) and all(
            part in ("*", level) for part, level in zip(parts, levels, strict=True)
        ):
            return
    raise ConfigError(
        f"{name} sets {key!r}, which is not overridable in {str(environment)!r}: loom "
        f"allows its operational keys and those listed under {_ALLOWLIST_KEY} in the "
        "reviewed configuration files"
    )


def _declared(node: Any, level: str) -> str:
    if isinstance(node, dict):
        for key in node:
            if str(key).casefold() == level.casefold():
                return str(key)
    return level.lower()


__all__ = ["OVERRIDABLE", "load_application_config"]
