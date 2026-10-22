"""Configuration loader backed by omegaConf.

omegaConf is the single source of truth for configuration.  It reads and
merges YAML files, resolves ``${oc.env:VAR}`` interpolations, and returns a
live :class:`omegaconf.DictConfig`.  The framework does **not** impose any
particular shape on the config — the user owns the structure.

For sections that benefit from strict typing, use :func:`section` to
extract and validate a subtree into a user-defined struct or dataclass via
``msgspec.convert``.

Cloud URIs
----------
:func:`load_config` accepts cloud storage URIs (``s3://``, ``gs://``,
``abfss://``, ``r2://`` …) in addition to local filesystem paths.  Cloud
files are fetched via ``fsspec`` at parse time, which means the config is
always resolved against the current state of object storage — no baking
into images or wheels.

Cloud files honour the ``includes`` directive exactly like local files:
relative entries resolve against the including URI, and entries may point
to any scheme.

Custom resolvers
----------------
Pass :class:`~loom.core.config.resolver.ConfigResolver` implementations to
resolve ``${prefix:key}`` placeholders at parse time::

    cfg = load_config("s3://bucket/prod.yaml", resolvers=[SsmResolver("eu-west-1")])

YAML ``includes`` directive
---------------------------
A config file may declare a top-level ``includes`` list to merge other
files before its own values.  Entries are resolved relative to the declaring
file, on any scheme, and may be globs (``*``, ``?``, ``[``); glob matches
are merged in lexicographic order of their resolved path.  The declaring
file always takes precedence over its includes.  Includes are resolved
recursively; circular references and entries matching no file raise
:class:`ConfigError`.

Example::

    # config/app.yaml
    includes:
      - base.yaml            # relative to config/
      - tables/*.yaml        # every YAML under config/tables/
      - s3://bucket/shared.yaml

    app:
      name: my-service  # overrides anything in base.yaml or secrets.yaml

Function-level composition is still available::

    cfg = load_config("config/base.yaml", "config/production.yaml")

Typed sections
--------------
:func:`section` binds a subtree to a user-defined type::

    class DatabaseConfig(msgspec.Struct, kw_only=True):
        url: str
        pool_size: int = 5

    db = section(cfg, "database", DatabaseConfig)

Keyed collections
-----------------
``keyed=`` names dotted paths whose mapping form merges by key across the
files of an ``includes`` composition.  A key declared by two files of the
same composition, or a list form mixed with a mapping form, raises
:class:`ConfigError` naming the files involved.  Explicit layers passed to
:func:`load_config` override keys as usual; only the form mismatch is
reported across them::

    cfg = load_config("config/app.yaml", keyed=("storage.tables",))
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NamedTuple, TypeVar

import msgspec

from loom.core.config._includes import (
    canonical_key,
    expand_include,
    failure_detail,
    is_cloud_uri,
    resolve_include,
)
from loom.core.config.errors import ConfigError

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from loom.core.config.resolver import ConfigResolver

T = TypeVar("T")

Provenance = dict[str, dict[str, str]]
"""Keyed path -> key -> file declaring that key."""


class _Layer(NamedTuple):
    """One loaded file: its label, config and the provenance of its keyed entries."""

    label: str
    cfg: Any
    provenance: Provenance


@dataclass(frozen=True)
class _LoadState:
    """Invariants shared by every file loaded in one :func:`load_config` call.

    Attributes:
        omega_conf: OmegaConf module.
        keyed: Dotted paths of the collections merged by key.
        cache: Layers already loaded, by canonical key, so a file reached
            through several include branches is parsed once.
    """

    omega_conf: Any
    keyed: Sequence[str]
    cache: dict[str, _Layer]


def _ensure_omegaconf() -> Any:
    """Import OmegaConf or raise a clear ConfigError."""
    try:
        from omegaconf import OmegaConf

        return OmegaConf
    except ImportError as exc:
        raise ConfigError(
            "omegaconf is required for load_config. "
            "Install it with: pip install loom-kernel[config]"
        ) from exc


def _fetch_cloud_content(uri: str, label: str) -> str:
    """Fetch raw YAML text from a cloud URI via fsspec.

    Args:
        uri: Cloud storage URI (``s3://``, ``gs://``, ``abfss://``, …).
        label: Text naming the file in error messages.

    Returns:
        Raw YAML string.

    Raises:
        ConfigError: When the fetch fails.
    """
    import fsspec

    try:
        with fsspec.open(uri, mode="r", encoding="utf-8") as fh:
            return str(fh.read())  # pyright: ignore[reportAttributeAccessIssue]
    except Exception as exc:
        raise ConfigError(
            f"Failed to fetch config from {label!r}: {failure_detail(exc, label)}"
        ) from exc


def _parse_cloud_file(uri: str, label: str, omega_conf: Any) -> Any:
    """Fetch and parse a YAML file from a cloud URI.

    Args:
        uri: Cloud storage URI.
        label: Text naming the file in error messages.
        omega_conf: OmegaConf module.

    Returns:
        Parsed :class:`omegaconf.DictConfig`.

    Raises:
        ConfigError: On fetch failure or parse error.
    """
    content = _fetch_cloud_content(uri, label)
    try:
        return omega_conf.create(content)
    except Exception as exc:
        raise ConfigError(f"Failed to parse config from {label!r}: {exc}") from exc


def _parse_local_file(path: str, label: str, omega_conf: Any) -> Any:
    """Parse a YAML file from the local filesystem.

    Args:
        path: Absolute or relative local path to the YAML file.
        label: Text naming the file in error messages.
        omega_conf: OmegaConf module.

    Returns:
        Parsed :class:`omegaconf.DictConfig`.

    Raises:
        ConfigError: On missing file or parse error.
    """
    try:
        return omega_conf.load(path)
    except FileNotFoundError as exc:
        raise ConfigError(f"Configuration file not found: {label!r}") from exc
    except Exception as exc:
        raise ConfigError(
            f"Failed to parse configuration file {label!r}: {failure_detail(exc, label)}"
        ) from exc


def _load_file(uri: str, label: str, seen: set[str], state: _LoadState) -> _Layer:
    """Load a YAML file from any scheme, resolving its ``includes`` recursively.

    Args:
        uri: Local path or cloud URI of the YAML file.
        label: Text naming the file in error messages: the path as passed to
            :func:`load_config`, or the include entry as written in YAML.
        seen: Canonical keys of the files already in the call stack, used to
            detect circular references.
        state: Invariants of the enclosing :func:`load_config` call.

    Returns:
        The file label, its merged :class:`omegaconf.DictConfig` (includes
        merged in, ``includes`` key stripped) and the provenance of every
        keyed entry reachable from it.

    Raises:
        ConfigError: On missing file, fetch failure, parse error, unmatched
            include, circular include, duplicate keyed entry, or mixed
            list and mapping forms of a keyed collection.
    """
    key = canonical_key(uri)
    if key in seen:
        raise ConfigError(f"Circular include detected: {label!r} is already being loaded.")
    if key in state.cache:
        return state.cache[key]

    omega_conf = state.omega_conf
    cfg = (
        _parse_cloud_file(uri, label, omega_conf)
        if is_cloud_uri(uri)
        else _parse_local_file(uri, label, omega_conf)
    )
    entries = _include_entries(omega_conf.select(cfg, "includes", default=None), omega_conf)
    own = omega_conf.masked_copy(cfg, [k for k in cfg if k != "includes"])
    layers = _load_includes(key, label, entries, seen | {key}, state)
    layers.append(_Layer(label, own, _own_provenance(label, own, state.keyed, omega_conf)))
    provenance = _check_keyed(layers, state.keyed, omega_conf)
    state.cache[key] = _Layer(label, _merge_layers(layers, omega_conf), provenance)
    return state.cache[key]


def _include_entries(raw_includes: Any, omega_conf: Any) -> list[tuple[str, str]]:
    """Pair every ``includes`` entry as written with its resolved text.

    Args:
        raw_includes: The ``includes`` node of a parsed file, or ``None``.
        omega_conf: OmegaConf module.

    Returns:
        ``(literal, resolved)`` per entry, in declaration order.  The literal
        keeps interpolations such as ``${secrets:/x}`` unexpanded so error
        messages can name the entry without echoing resolved values.
    """
    if raw_includes is None:
        return []
    literals = omega_conf.to_container(raw_includes, resolve=False)
    return [(str(literal), str(raw_includes[index])) for index, literal in enumerate(literals)]


def _load_includes(
    base: str,
    label: str,
    entries: Sequence[tuple[str, str]],
    seen: set[str],
    state: _LoadState,
) -> list[_Layer]:
    """Load every file designated by the ``includes`` entries of one file.

    Args:
        base: Canonical key of the file declaring the entries.
        label: Label of the file declaring the entries.
        entries: ``(literal, resolved)`` include entries.
        seen: Canonical keys of the files in the call stack, including the
            declaring file.
        state: Invariants of the enclosing :func:`load_config` call.

    Returns:
        One loaded layer per matched file, in include order and, within a
        glob, in lexicographic order of resolved path.

    Raises:
        ConfigError: Any loading error, naming the entry per :func:`_labels`
            and suffixed with the declaring file's label.
    """
    layers: list[_Layer] = []
    for literal, resolved in entries:
        pattern = resolve_include(base, resolved)
        try:
            for match, match_label in _labels(literal, pattern):
                layers.append(_load_file(match, match_label, seen, state))
        except ConfigError as exc:
            raise ConfigError(f"{exc} (included from {label!r})") from exc
    return layers


def _labels(literal: str, pattern: str) -> list[tuple[str, str]]:
    """Expand one include entry and pick the label of each matched file.

    An entry holding an interpolation (``${``) is named by its literal text
    everywhere, so a resolved secret never reaches an error message.  A
    plain entry is named by its resolved path, and each glob match by its
    own path.

    Args:
        literal: Include entry as written in YAML.
        pattern: The entry resolved and joined to the declaring file.

    Returns:
        ``(matched path, label)`` per matched file.

    Raises:
        ConfigError: When the entry matches no file or cannot be listed.
    """
    if "${" in literal:
        return [(match, literal) for match in expand_include(pattern, literal)]
    return [(match, match) for match in expand_include(pattern, pattern)]


def _merge_layers(layers: Sequence[_Layer], omega_conf: Any) -> Any:
    """Merge the configs of ``layers`` left-to-right.

    Args:
        layers: Loaded layers in precedence order, later overriding earlier.
        omega_conf: OmegaConf module.

    Returns:
        The merged :class:`omegaconf.DictConfig`.
    """
    return omega_conf.merge(*(layer.cfg for layer in layers))


def _own_provenance(label: str, cfg: Any, keyed: Sequence[str], omega_conf: Any) -> Provenance:
    """Record ``label`` as the declaring file of every keyed entry ``cfg`` holds.

    Args:
        label: Label of the file whose own keys are recorded.
        cfg: Config of that file without its includes.
        keyed: Dotted paths of the collections merged by key.
        omega_conf: OmegaConf module.

    Returns:
        Provenance covering the keyed paths present as mappings in ``cfg``.
    """
    provenance: Provenance = {}
    for path in keyed:
        node = _select_keyed(label, cfg, path, omega_conf)
        if omega_conf.is_dict(node):
            provenance[path] = dict.fromkeys((str(k) for k in node), label)
    return provenance


def _check_keyed(layers: Sequence[_Layer], keyed: Sequence[str], omega_conf: Any) -> Provenance:
    """Validate the keyed collections across the layers of one composition.

    Args:
        layers: Included layers in order, followed by the declaring file.
        keyed: Dotted paths of the collections merged by key.
        omega_conf: OmegaConf module.

    Returns:
        The union of the layers' provenance.

    Raises:
        ConfigError: When a key is declared by two files, or the collection
            is a list in one layer and a mapping in another.
    """
    provenance: Provenance = {}
    for path in keyed:
        _check_form(path, layers, omega_conf)
        provenance[path] = _check_duplicates(path, layers)
    return provenance


def _check_form(path: str, layers: Sequence[_Layer], omega_conf: Any) -> None:
    """Reject ``path`` being a list in one layer and a mapping in another.

    Args:
        path: Dotted path of the keyed collection.
        layers: Layers to inspect.
        omega_conf: OmegaConf module.

    Raises:
        ConfigError: Naming the first file using each form.
    """
    list_file: str | None = None
    mapping_file: str | None = None
    for layer in layers:
        node = _select_keyed(layer.label, layer.cfg, path, omega_conf)
        if list_file is None and omega_conf.is_list(node):
            list_file = layer.label
        if mapping_file is None and omega_conf.is_dict(node):
            mapping_file = layer.label
    if list_file is None or mapping_file is None:
        return
    raise ConfigError(
        f"{path} is a list in {list_file!r} and a mapping in {mapping_file!r}; "
        "use one form within a composition"
    )


def _select_keyed(label: str, cfg: Any, path: str, omega_conf: Any) -> Any:
    """Select the node at keyed ``path`` in ``cfg``, or ``None`` when absent.

    Args:
        label: Label of the file ``cfg`` was loaded from.
        cfg: Config to inspect.
        path: Dotted path of the keyed collection.
        omega_conf: OmegaConf module.

    Returns:
        The selected node, or ``None`` when the path is absent.

    Raises:
        ConfigError: When the path cannot be selected, e.g. its parent is not
            a container or the node is an unresolvable interpolation.
    """
    from omegaconf.errors import OmegaConfBaseException

    try:
        return omega_conf.select(cfg, path, default=None)
    except OmegaConfBaseException as exc:
        raise ConfigError(f"Cannot read keyed collection {path!r} in {label!r}: {exc}") from exc


def _check_duplicates(path: str, layers: Sequence[_Layer]) -> dict[str, str]:
    """Reject a key of ``path`` declared by two files across ``layers``.

    The same file reached through several include branches counts as one
    declaration.

    Args:
        path: Dotted path of the keyed collection.
        layers: Layers whose provenance is merged.

    Returns:
        Key to declaring file across every layer.

    Raises:
        ConfigError: Naming the key and both declaring files.
    """
    declared: dict[str, str] = {}
    for layer in layers:
        for key, declaring in layer.provenance.get(path, {}).items():
            if declared.get(key, declaring) != declaring:
                raise ConfigError(
                    f"{path}[{key!r}] is declared in {declared[key]!r} and in {declaring!r}"
                )
            declared[key] = declaring
    return declared


def _register_resolvers(resolvers: Sequence[Any], omega_conf: Any) -> None:
    """Register the given resolvers with OmegaConf before parsing.

    Each resolver replaces any earlier registration of the same name.

    Args:
        resolvers: Sequence of :class:`~loom.core.config.resolver.ConfigResolver`
            implementations.
        omega_conf: OmegaConf module.
    """
    for resolver in resolvers:
        omega_conf.register_new_resolver(
            resolver.name, resolver.resolve, replace=True, use_cache=True
        )


def load_config(
    *config_files: str,
    resolvers: Sequence[ConfigResolver] = (),
    keyed: Sequence[str] = (),
) -> DictConfig:
    """Load and merge one or more YAML config files into a DictConfig.

    Accepts local filesystem paths and cloud storage URIs
    (``s3://``, ``gs://``, ``abfss://``, ``r2://`` …).  Cloud files are
    fetched via ``fsspec`` at call time.

    Files are merged **left-to-right**: values in later files override those
    in earlier ones.  ``${oc.env:VAR}`` interpolations are resolved by
    OmegaConf.  Custom ``resolvers`` are registered before parsing so their
    ``${name:key}`` placeholders resolve during the same pass.

    Any file may declare a top-level ``includes`` list to pull in
    additional files before its own values.  Entries are relative to the
    declaring file, may live on any scheme, and may be globs.  Circular
    includes and entries matching no file raise :class:`ConfigError`.

    ``keyed`` names collections whose mapping form merges by key within an
    ``includes`` composition: a key declared by two files of the same
    composition raises :class:`ConfigError`, as does mixing a list form
    with a mapping form.  Explicit ``config_files`` layers override keys
    left-to-right; only the form mismatch is reported across them.

    The framework does not impose any shape on the resulting config — the
    user owns the structure entirely.  Use :func:`section` to extract typed
    sub-objects where desired.

    Args:
        *config_files: One or more local paths or cloud URIs.
        resolvers: Optional sequence of
            :class:`~loom.core.config.resolver.ConfigResolver` instances.
            Each resolver registers a ``${name:key}`` placeholder resolved
            at parse time (e.g. from AWS SSM or Azure Key Vault).
        keyed: Dotted paths (e.g. ``"storage.tables"``) of the collections
            merged by key.

    Returns:
        Merged :class:`omegaconf.DictConfig` with interpolation support.

    Raises:
        ConfigError: If no files are provided, a file is not found, parsing
            fails, an include matches no file, a circular include is detected,
            a keyed collection holds a duplicate key or mixes list and mapping
            forms, omegaconf is not installed, or a cloud URI fetch fails.

    Example — single local file with inline includes::

        cfg = load_config("config.yaml")

    Example — explicit multi-file composition::

        cfg = load_config("config/base.yaml", "config/production.yaml")
        db_url = cfg.database.url

    Example — cloud URI::

        cfg = load_config("s3://my-bucket/config/prod.yaml")

    Example — with custom resolver::

        cfg = load_config("config/prod.yaml", resolvers=[SsmResolver("eu-west-1")])

    Example — keyed collections::

        cfg = load_config("config/app.yaml", keyed=("storage.tables", "storage.files"))
    """
    if not config_files:
        raise ConfigError("load_config requires at least one config file.")

    omega_conf = _ensure_omegaconf()
    _register_resolvers(resolvers, omega_conf)

    state = _LoadState(omega_conf=omega_conf, keyed=keyed, cache={})
    layers = [_load_file(path, path, set(), state) for path in config_files]
    for path in keyed:
        _check_form(path, layers, omega_conf)
    return _merge_layers(layers, omega_conf)  # type: ignore[no-any-return]


def section(cfg: DictConfig, key: str, target_type: type[T]) -> T:
    """Extract and validate a config section as a typed object.

    Navigates ``cfg`` by ``key`` (dot-notation supported, e.g.
    ``"database.primary"``), resolves omegaConf interpolations, and converts
    the result to ``target_type`` via ``msgspec.convert``.

    Works with any type supported by ``msgspec.convert``: ``msgspec.Struct``
    subclasses, ``dataclasses``, ``TypedDict``, plain dicts, etc.  A key that
    names a scalar leaf (``"api.token"``) converts that value, e.g. to ``str``.

    Args:
        cfg: Root :class:`omegaconf.DictConfig` returned by :func:`load_config`.
        key: Dot-separated path to the desired section or scalar leaf (e.g.
            ``"database"`` or ``"services.cache"``).
        target_type: Type to convert the section into.

    Returns:
        Validated instance of ``target_type``.

    Raises:
        ConfigError: If the key is absent, the section cannot be resolved,
            or ``msgspec`` validation fails.

    Example::

        class DatabaseConfig(msgspec.Struct, kw_only=True):
            url: str
            pool_size: int = 5

        db = section(cfg, "database", DatabaseConfig)
    """
    omega_conf = _ensure_omegaconf()

    node: Any = cfg
    for part in key.split("."):
        try:
            node = node[part]
        except Exception as exc:
            raise ConfigError(
                f"Config section not found: {key!r}  (failed at segment {part!r})"
            ) from exc

    if not omega_conf.is_config(node):
        return _convert_section(node, key, target_type)
    try:
        data = omega_conf.to_container(node, resolve=True)
    except Exception as exc:
        raise ConfigError(
            f"Failed to resolve config section {key!r} "
            f"(check ${{oc.env:VAR}} interpolations): {exc}"
        ) from exc

    return _convert_section(data, key, target_type)


def _convert_section(data: Any, key: str, target_type: type[T]) -> T:
    """Convert resolved section data to ``target_type`` leniently.

    Raises:
        ConfigError: When ``msgspec`` validation fails.
    """
    try:
        from loom.core.config.convert import convert_config

        return convert_config(data, target_type, strict=False)
    except msgspec.ValidationError as exc:
        raise ConfigError(
            f"Config section {key!r} failed validation as {target_type.__name__!r}: {exc}"
        ) from exc
