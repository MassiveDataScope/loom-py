"""FileLocator — protocol and built-in implementation for file URI resolution.

Decouples logical file aliases (e.g. ``"events_raw"``) from physical storage
URIs and cloud credentials.  Backends receive an optional locator at
construction time; when present, ``FromFile.alias()`` / ``IntoFile.alias()``
specs are resolved through it at runtime.

Built-in implementation
-----------------------
* :class:`MappingFileLocator` — explicit ``alias → FileLocation`` mapping
  built from :attr:`~loom.etl.StorageConfig.files`.

Usage
-----
File aliases are declared in the storage config::

    storage:
      files:
        - name: events_raw
          path:
            uri: s3://raw-bucket/events/
            storage_options:
              AWS_REGION: eu-west-1
        - name: exports_daily
          path:
            uri: s3://exports-bucket/daily/

And consumed in pipelines via :meth:`~loom.etl.FromFile.alias` and
:meth:`~loom.etl.IntoFile.alias`::

    events = FromFile.alias("events_raw", format=Format.CSV)
    target = IntoFile.alias("exports_daily", format=Format.PARQUET)

The runner resolves the alias to the physical URI at job startup —
the pipeline never hard-codes storage paths.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from loom.core.errors import UnknownKey


@dataclass(frozen=True)
class FileLocation:
    """Physical storage address for one file route.

    Args:
        uri_template: Full URI or URI template.  Supports ``{field_name}``
            placeholders consistent with :class:`~loom.etl.FromFile` path
            templates.
        storage_options: Cloud credentials / connection settings passed
            verbatim to the underlying I/O layer.
    """

    uri_template: str
    storage_options: dict[str, str] = field(default_factory=dict)


@runtime_checkable
class FileLocator(Protocol):
    """Protocol for resolving a logical file alias to a physical
    :class:`FileLocation`.

    Implement this to support custom file routing strategies.

    Example::

        class MyFileLocator:
            def locate(self, name: str) -> FileLocation:
                return FileLocation(uri_template=f"s3://my-bucket/{name}/")
    """

    def locate(self, name: str) -> FileLocation:
        """Resolve *name* to its physical storage location.

        Args:
            name: Logical file alias declared via
                :meth:`~loom.etl.FromFile.alias` or
                :meth:`~loom.etl.IntoFile.alias`.

        Returns:
            :class:`FileLocation` with full URI template and credentials.

        Raises:
            UnknownKey: When *name* is not registered.
        """
        ...


class MappingFileLocator:
    """Resolve file aliases via an explicit ``alias → FileLocation`` mapping.

    Built automatically by
    :meth:`~loom.etl.StorageConfig.to_file_locator` from the
    ``storage.files`` configuration block.

    Args:
        mapping: ``alias → FileLocation`` dict.

    Raises:
        UnknownKey: On :meth:`locate` when the alias is not in *mapping*.

    Example::

        locator = MappingFileLocator(
            mapping={
                "events_raw": FileLocation(
                    uri_template="s3://raw/events/",
                    storage_options={"AWS_REGION": "eu-west-1"},
                ),
                "exports_daily": FileLocation(
                    uri_template="s3://exports/daily/",
                ),
            }
        )
    """

    def __init__(self, mapping: dict[str, FileLocation]) -> None:
        self._mapping = mapping

    def locate(self, name: str) -> FileLocation:
        """Resolve *name* from the mapping.

        Args:
            name: Logical file alias.

        Returns:
            :class:`FileLocation` for the alias.

        Raises:
            UnknownKey: When *name* is not registered.  The error message
                lists available aliases to aid debugging.
        """
        location = self._mapping.get(name)
        if location is None:
            available = sorted(self._mapping)
            raise UnknownKey(
                f"No file route configured for alias {name!r}. "
                f"Available aliases: {available}. "
                "Define it under storage.files in your config YAML."
            )
        return location


__all__ = ["FileLocation", "FileLocator", "MappingFileLocator"]
