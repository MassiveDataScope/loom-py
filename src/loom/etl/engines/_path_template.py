"""Path template resolution for file sources and targets.

``FromFile`` / ``IntoFile`` paths (and ``storage.files`` URI templates)
support ``{field_name}`` placeholders resolved from the concrete params
instance at runtime. Placeholders use :meth:`str.format` semantics, so
attribute access (``{run_date.month}``) and format specs
(``{run_date:%Y%m%d}``) are supported. Literal braces are escaped by
doubling (``{{`` / ``}}``).

Shared by the Polars and Spark backends — the template is data, not
backend behavior.
"""

from __future__ import annotations

import string
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from loom.etl.storage._file_locator import FileLocator


def extract_template_fields(path: str) -> tuple[str, ...]:
    """Return the root params-field names referenced by *path* placeholders.

    Args:
        path: File path or URI, possibly containing ``{field}`` placeholders.

    Returns:
        Deduplicated root field names in first-appearance order. Empty when
        the path has no placeholders.

    Raises:
        ValueError: When a placeholder is positional (``{}`` / ``{0}``) —
            only named params fields can be resolved.
    """
    fields: dict[str, None] = {}
    for _literal, field_name, _spec, _conv in string.Formatter().parse(path):
        if field_name is None:
            continue
        root = field_name.split(".")[0].split("[")[0]
        if root == "" or root.isdigit():
            raise ValueError(
                f"File path template {path!r} uses a positional placeholder; "
                "only named placeholders matching params fields are supported "
                "(e.g. '{run_date}')."
            )
        fields[root] = None
    return tuple(fields)


def resolve_path_template(path: str, params_instance: Any) -> str:
    """Substitute ``{field}`` placeholders in *path* from *params_instance*.

    Args:
        path:            File path or URI, possibly containing placeholders.
        params_instance: Concrete params for the current run; each root
                         placeholder must be an attribute of it.

    Returns:
        The path with all placeholders substituted. Paths without
        placeholders are returned unchanged.

    Raises:
        ValueError: When a placeholder has no matching attribute on the
            params instance, or *params_instance* is ``None`` while the
            path contains placeholders.
    """
    fields = extract_template_fields(path)
    if not fields:
        return path
    if params_instance is None:
        raise ValueError(
            f"File path template {path!r} requires params to resolve "
            f"{list(fields)!r}, but no params instance was provided."
        )
    missing = [name for name in fields if not hasattr(params_instance, name)]
    if missing:
        available = sorted(name for name in dir(params_instance) if not name.startswith("_"))
        raise ValueError(
            f"File path template {path!r} references unknown params "
            f"field(s) {missing!r}. Available fields: {available!r}."
        )
    mapping = {name: getattr(params_instance, name) for name in fields}
    return path.format(**mapping)


def resolve_file_uri(
    path: str,
    *,
    is_alias: bool,
    locator: FileLocator | None,
    params_instance: Any,
    role: str,
) -> str:
    """Return the physical URI for a file source/target spec.

    Alias resolution happens first (``storage.files`` lookup via *locator*),
    then ``{field}`` placeholders are substituted from *params_instance* —
    so environment-specific URI templates can also be parameterized.

    Args:
        path:            Literal path/template, or logical alias name.
        is_alias:        Whether *path* is a ``storage.files`` alias.
        locator:         :class:`~loom.etl.storage.FileLocator` or ``None``.
        params_instance: Concrete params for the current run.
        role:            Declaration name for error messages
                         (``"FromFile"`` / ``"IntoFile"``).

    Raises:
        ValueError: When the alias has no configured locator, or template
            resolution fails (see :func:`resolve_path_template`).
    """
    if is_alias:
        if locator is None:
            raise ValueError(
                f"{role}.alias({path!r}) requires storage.files to be configured. "
                "Set storage.files in your config YAML."
            )
        path = locator.locate(path).uri_template
    return resolve_path_template(path, params_instance)


__all__ = ["extract_template_fields", "resolve_file_uri", "resolve_path_template"]
