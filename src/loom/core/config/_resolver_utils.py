"""Shared utilities for config resolvers (SSM, Secrets Manager, etc.)."""

from __future__ import annotations

import json
import os
import re
from typing import Any

from loom.core.config.aws import aws_section
from loom.core.config.context import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.config.keys import ConfigKey
from loom.core.config.loader import convert_section
from loom.core.connections.aws import AwsClientConfig, aws_island
from loom.core.plugins.optional import MissingExtraError

_AWS_EXTRA = "config-ssm"


def aws_client(service: str, region: str | None, *, resolver: str, root: Any) -> Any:
    """Build one boto3 client through the AWS island from the ``aws:`` section as written.

    The resolvers are the ones resolving the placeholders, so they read
    ``aws:`` of the configuration *root* unresolved; *region*, when given,
    overrides its region.

    Args:
        service: Service name, ``"ssm"`` or ``"secretsmanager"``.
        region: AWS region, or ``None`` to take ``aws.region`` or let boto3
            resolve its own.
        resolver: Resolver class name, for the messages.
        root: The configuration being resolved, or ``None`` outside one.

    Returns:
        The client; typed ``Any`` because boto3 ships no stubs.

    Raises:
        ConfigError: When boto3 is not installed, naming the extra, or
            ``aws:`` is invalid or holds a placeholder.
    """
    try:
        island = aws_island(extra=_AWS_EXTRA)
    except MissingExtraError as exc:
        raise ConfigError(
            f"boto3 is required for {resolver}. "
            "Install it with: pip install loom-kernel[config-ssm]"
        ) from exc
    raw = None if root is None else ConfigContext(root).raw(ConfigKey.AWS)
    for key, value in raw.items() if isinstance(raw, dict) else ():
        if isinstance(value, str) and "${" in value:
            raise ConfigError(
                f"{ConfigKey.AWS}.{key} is a placeholder, but {resolver} reads "
                f"{ConfigKey.AWS} as written: write the value, or leave it out for boto3 "
                "to read its environment (AWS_REGION, AWS_ENDPOINT_URL)"
            )
    written = aws_section(raw)
    settings = written if region is None else {**written, "region": region}
    config = convert_section(settings, ConfigKey.AWS, AwsClientConfig)
    return island.build_aws_client(service, config)


CLIENT = ("client",)
"""Key of the client in a resolver's cache; a resolver key is a string."""


def root_cache(root: Any, name: str, unbound: dict[Any, Any]) -> dict[Any, Any]:
    """Return the cache of resolver *name* for configuration *root*, *unbound* outside one.

    OmegaConf caches no resolver that receives the root, so the client and
    the values live in its cache of the root: one per configuration.
    """
    if root is None:
        return unbound
    from omegaconf import OmegaConf

    cache: dict[Any, Any] = OmegaConf.get_cache(root)[name]
    return cache


_ENV_VAR_PATTERN = re.compile(r"%([A-Z][A-Z0-9_]*)%")


def _expand_env_vars(key: str) -> str:
    """Expand ``%VAR_NAME%`` placeholders in *key* using environment variables.

    Only uppercase identifiers matching ``[A-Z][A-Z0-9_]*`` are expanded.
    The ``%VAR%`` syntax avoids conflicts with OmegaConf's ANTLR grammar,
    which rejects bare ``{`` inside ``${resolver:...}`` interpolations.

    Args:
        key: Resolver path, possibly containing ``%VAR_NAME%`` tokens.

    Returns:
        Path with all tokens replaced by their environment variable values.

    Raises:
        ConfigError: When a referenced variable is absent from the environment.
    """
    if not _ENV_VAR_PATTERN.search(key):
        return key

    def _replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        value = os.environ.get(var_name)
        if value is None:
            raise ConfigError(
                f"Env var {var_name!r} not found in environment "
                f"(referenced in resolver path {key!r})"
            )
        return value

    return _ENV_VAR_PATTERN.sub(_replace, key)


def _split_resolver_key(key: str) -> tuple[str, list[str]]:
    """Split a resolver key into the base path and JSON navigation keys.

    The first dot after the last '/' separator marks the start of a JSON key
    path. Both SSM and Secrets Manager use '/' as path separator, so a dot
    is unambiguously a JSON navigation token.

    Note: ARN-style SecretIds (``arn:aws:secretsmanager:...``) are not
    supported for dot-notation — use short names instead.

    **Dot-notation comes from the key as written.** Callers must split
    *before* expanding ``%VAR%`` tokens and expand only the returned base
    path, so an expanded environment value can never introduce a JSON
    separator. Example: ``/app/svc/%ENV%`` with ``ENV=db.prod`` fetches
    ``/app/svc/db.prod`` whole, and ``/app/%ENV%/db.host`` fetches
    ``/app/prod/db`` and navigates ``host``. A dot is a navigation token
    only when the operator typed it in the key.

    Args:
        key: Unexpanded resolver key, e.g. ``"/app/%ENV%/db_config.host"``.

    Returns:
        A 2-tuple ``(base_path, json_keys)`` where *base_path* is the path
        to expand and fetch and *json_keys* is a list of dict keys to
        navigate (empty list when no dot-notation is present).
    """
    if not key:
        return key, []
    last_slash = key.rfind("/")
    after_last_slash = key[last_slash + 1 :] if last_slash != -1 else key
    dot_pos = after_last_slash.find(".")
    if dot_pos == -1:
        return key, []
    base_end = (last_slash + 1 + dot_pos) if last_slash != -1 else dot_pos
    json_tail = key[base_end + 1 :]
    return key[:base_end], [k for k in json_tail.split(".") if k]


def _navigate_json(raw: str, keys: list[str], path: str) -> object:
    """Parse *raw* as JSON and navigate to the value at *keys*.

    Args:
        raw: Raw string value from the resolver backend.
        keys: Ordered list of dict keys to traverse.
        path: Key as written in the configuration (unexpanded ``%VAR%``
            tokens), used only in error messages. Neither error chains a
            cause: the decode error carries the raw value and the
            key-not-found branch has none.

    Returns:
        The value at the navigated position. May be any JSON-compatible type.

    Raises:
        ConfigError: When *raw* is not valid JSON, or a key is absent.
    """
    try:
        data: object = json.loads(raw)
    except json.JSONDecodeError as exc:
        # Suppress chain (from None) — JSONDecodeError.doc holds the raw secret
        # and would be exposed via __cause__ in tracebacks / APM serialisation.
        raise ConfigError(
            f"Resolver path {path!r} value is not valid JSON "
            f"(required for key navigation): {exc.msg} at position {exc.pos}"
        ) from None
    current: object = data
    for k in keys:
        if not isinstance(current, dict) or k not in current:
            raise ConfigError(f"Key {k!r} not found at resolver path {path!r}")
        current = current[k]
    return current


__all__ = ["_expand_env_vars", "_split_resolver_key", "_navigate_json"]
