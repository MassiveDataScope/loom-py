"""Resolution and glob expansion of ``includes`` entries on any scheme.

Local entries use the standard library only; cloud entries import
``fsspec`` lazily, so local configurations load without the extra.
"""

from __future__ import annotations

import glob
import os
import posixpath
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from loom.core.config.errors import ConfigError

_CLOUD_SCHEMES = frozenset({"s3", "gs", "gcs", "abfss", "abfs", "az", "r2", "memory"})
_CONFIG_SUFFIXES = frozenset({".yaml", ".yml"})


def is_cloud_uri(uri: str) -> bool:
    """Return ``True`` when *uri* uses a cloud storage scheme.

    Args:
        uri: Local path or URI.

    Returns:
        Whether the scheme belongs to a cloud storage backend.
    """
    return urlsplit(str(uri).strip()).scheme.lower() in _CLOUD_SCHEMES


def read_text(uri: str) -> str:
    """Return the UTF-8 text of a local file or a cloud object.

    The one reader behind :func:`~loom.core.config.load_config` and the
    ``file`` resolver, so both accept the same paths and schemes. Errors
    propagate unchanged: each caller decides what its message may carry.

    Args:
        uri: Local path or cloud URI (see :func:`is_cloud_uri`).

    Returns:
        The decoded text.
    """
    if is_cloud_uri(uri):
        import fsspec  # noqa: PLC0415 - the config extra; local files load without it

        with fsspec.open(uri, mode="r", encoding="utf-8") as fh:
            return str(fh.read())  # pyright: ignore[reportAttributeAccessIssue]
    return Path(uri).read_text(encoding="utf-8")


def resolve_include(base: str, entry: str) -> str:
    """Resolve an ``includes`` entry against the file that declares it.

    Args:
        base: Path or URI of the declaring file.
        entry: Resolved include entry text: a URI, an absolute path, or a
            path relative to the declaring file.

    Returns:
        The entry as is when it carries a scheme or is absolute; otherwise
        the entry joined to the parent of ``base`` and normalised.
    """
    if is_cloud_uri(entry) or os.path.isabs(entry):
        return entry
    if not is_cloud_uri(base):
        return os.path.normpath(os.path.join(os.path.dirname(base), entry))
    parts = urlsplit(base)
    joined = posixpath.normpath(posixpath.join(posixpath.dirname(parts.path), entry))
    return urlunsplit((parts.scheme, parts.netloc, joined, "", ""))


def expand_config_glob(pattern: str) -> list[str]:
    """Expand a configuration file pattern on the local or a cloud filesystem.

    Args:
        pattern: Resolved local path or cloud URI, optionally containing glob
            characters (``*``, ``?``, ``[``).

    Returns:
        ``[pattern]`` for a plain entry; for a glob, the matching regular
        ``.yaml``/``.yml`` files sorted lexicographically by resolved path.

    Raises:
        ConfigError: When a glob matches no configuration file or the cloud
            backend fails to list it.
    """
    return expand_include(pattern, pattern)


def failure_detail(exc: BaseException, label: str) -> str:
    """Return the text of *exc* safe to append to an error naming *label*.

    A label holding an interpolation (``${``) stands for a resolved value
    that must not be echoed; backend messages and YAML marks embed the
    resolved path, so only the exception type is returned for them.

    Args:
        exc: Underlying failure.
        label: Text naming the file in the loom error message.

    Returns:
        ``str(exc)`` for a plain label, the exception class name otherwise.
    """
    return type(exc).__name__ if "${" in label else str(exc)


def expand_include(pattern: str, label: str) -> list[str]:
    """Expand *pattern* like :func:`expand_config_glob`, naming *label* in errors.

    Args:
        pattern: Resolved local path or cloud URI, optionally a glob.
        label: Text identifying the entry in error messages, typically the
            include entry as written in YAML before interpolation.

    Returns:
        ``[pattern]`` for a plain entry; for a glob, the matching regular
        ``.yaml``/``.yml`` files sorted lexicographically by resolved path.

    Raises:
        ConfigError: When a glob matches no configuration file or the cloud
            backend fails to list it; the message names ``label``.
    """
    if not glob.has_magic(pattern):
        return [pattern]
    matches = (
        _expand_cloud_glob(pattern, label) if is_cloud_uri(pattern) else _expand_local_glob(pattern)
    )
    if not matches:
        raise ConfigError(f"Include {label!r} matches no configuration file")
    return sorted(matches)


def canonical_key(uri: str) -> str:
    """Return the identity of a file used for circular-include detection.

    Args:
        uri: Local path or cloud URI.

    Returns:
        ``scheme://netloc/normalised-path`` for cloud URIs, with an empty
        netloc taken from the first path segment so that ``scheme:///a/b``
        and ``scheme://a/b`` agree; the resolved absolute path for local
        files.
    """
    if not is_cloud_uri(uri):
        return str(Path(uri).resolve())
    parts = urlsplit(uri)
    netloc, path = parts.netloc, parts.path
    if not netloc and path.startswith("/"):
        netloc, _, rest = path.lstrip("/").partition("/")
        path = f"/{rest}"
    return urlunsplit((parts.scheme, netloc, posixpath.normpath(path), "", ""))


def _is_config_file(path: str) -> bool:
    """Return ``True`` when *path* has a YAML suffix."""
    return os.path.splitext(path)[1].lower() in _CONFIG_SUFFIXES


def _expand_local_glob(pattern: str) -> list[str]:
    """Return the regular YAML files matching a local glob pattern."""
    return [p for p in glob.glob(pattern) if os.path.isfile(p) and _is_config_file(p)]


def _expand_cloud_glob(uri: str, label: str) -> list[str]:
    """Return the regular YAML files matching a cloud glob, as full URIs."""
    from fsspec.core import url_to_fs

    try:
        fs, path = url_to_fs(uri)
        entries = fs.glob(path, detail=True)
    except Exception as exc:
        raise ConfigError(
            f"Failed to expand include {label!r}: {failure_detail(exc, label)}"
        ) from exc
    return [
        str(fs.unstrip_protocol(name))
        for name, info in entries.items()
        if info.get("type") == "file" and _is_config_file(name)
    ]
