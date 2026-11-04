"""AWS Secrets Manager resolver for loom configuration.

Resolves ``${secrets:/path/to/secret}`` placeholders in OmegaConf configs
by fetching values from AWS Secrets Manager at parse time.

Example::

    from loom.core.config import load_config
    from loom.core.config.secrets import SecretsManagerResolver

    cfg = load_config("config/prod.yaml", resolvers=[SecretsManagerResolver("eu-west-1")])
"""

from __future__ import annotations

import functools
import logging
from typing import Any

from loom.core.config._resolver_utils import (
    _aws_error_code,
    _expand_env_vars,
    _navigate_json,
    _split_resolver_key,
    aws_client,
    cached,
)
from loom.core.config.errors import ConfigError

logger = logging.getLogger(__name__)


def _fetch_secret(client: Any, name: str, *, label: str) -> str:
    """Fetch a string secret value from AWS Secrets Manager.

    Args:
        client: Boto3 secretsmanager client.
        name: Secret name or short ARN, with ``%VAR%`` tokens already
            expanded. ARN-style names are supported for plain fetches but
            not for dot-notation JSON navigation.
        label: Key as written in the configuration (unexpanded); used only
            in error messages so that they never carry expanded values.

    Returns:
        The secret value as a string.

    Raises:
        ConfigError: When the secret is binary, or on any API error. An API
            error message carries the AWS error code (or the exception type
            name when absent), never the expanded *name*; its chained cause
            may carry the expanded *name*.
    """
    try:
        result = client.get_secret_value(SecretId=name)
    except Exception as exc:
        raise ConfigError(
            f"Failed to fetch Secrets Manager secret {label!r}: {_aws_error_code(exc)}"
        ) from exc
    if "SecretString" not in result:
        raise ConfigError(
            f"Secrets Manager secret {label!r} is binary — only string secrets are supported"
        )
    return str(result["SecretString"])


class SecretsManagerResolver:
    """Resolves AWS Secrets Manager paths for use with :func:`~loom.core.config.load_config`.

    Fetches secret values from AWS Secrets Manager. The boto3 client is
    created lazily on first use and reused across calls.

    Env-var tokens in the form ``%VAR_NAME%`` (uppercase letters, digits,
    and underscores only) are expanded from ``os.environ`` before the
    request is made.

    Keys are trusted deployment configuration: they come from the
    deployment's own config files, never from request input. The resolver
    logs and reports the key as written (with its ``%VAR%`` tokens), never
    the expanded path. Two channels remain for a caller that lets untrusted
    input reach a resolver key: a missing variable raises
    ``ConfigError("Env var 'X' not found ...")``, a deterministic existence
    oracle for any attacker-chosen variable name, and the chained cause
    (``__cause__``) of a fetch error may carry the expanded path echoed by
    the AWS client. The client is also called with the expanded path, so
    botocore's DEBUG request logging reproduces it — keep botocore at INFO
    or above in production.

    Args:
        region: AWS region name. Passed directly to ``boto3.client``.
            Defaults to ``None``: ``aws.region`` of the configuration, else
            boto3's own resolution chain (env vars, instance metadata, etc.).

    Example::

        resolver = SecretsManagerResolver("eu-west-1")
        value = resolver.resolve("/myapp/%ENV%/db_password")
    """

    def __init__(self, region: str | None = None) -> None:
        self._region = region
        self._client: Any = None

    @property
    def name(self) -> str:
        """OmegaConf resolver prefix.

        Returns:
            The string ``"secrets"``.
        """
        return "secrets"

    def _get_client(self, root: Any) -> Any:
        """Return the boto3 secretsmanager client, creating it on first call.

        Returns:
            A boto3 secretsmanager client instance.

        Raises:
            ConfigError: When boto3 is not installed, or the ``aws:`` section of
                *root* is invalid.
        """
        if self._client is None:
            self._client = aws_client(
                "secretsmanager", self._region, resolver="SecretsManagerResolver", root=root
            )
        return self._client

    def resolve(self, key: str, _root_: Any = None) -> object:
        """Resolve an AWS Secrets Manager path to its stored value.

        Splits the dot-notation tail off *key* as written, expands
        ``%VAR_NAME%`` tokens in the remaining base path from the
        environment, then fetches the secret from AWS Secrets Manager.

        Args:
            key: Secret path, optionally containing ``%VAR_NAME%``
                placeholders that are replaced with environment variable values.
                Supports dot-notation for JSON key navigation: ``/path/secret.key``
                fetches ``/path/secret`` and returns ``secret["key"]``. Dots are read
                from the key as written, so an expanded value containing a dot
                is part of the path and never a navigation separator.
            _root_: The configuration OmegaConf is resolving, or ``None``;
                its ``aws:`` section, as written, configures the client, and
                the value is fetched once per configuration.

        Returns:
            Resolved value. A plain string for secrets without dot-notation;
            a structured value (string, int, dict, etc.) when dot-notation
            navigates into a JSON secret.

        Raises:
            ConfigError: When *key* is empty, an env-var placeholder is
                missing, boto3 is not installed, the secret is binary,
                or the API call fails.
        """
        return cached(_root_, self.name, key, functools.partial(self._fetch, root=_root_))

    def _fetch(self, key: str, root: Any) -> object:
        if not key:
            raise ConfigError("Secrets Manager key must not be empty")
        base, json_keys = _split_resolver_key(key)
        path = _expand_env_vars(base)
        logger.info("secrets_resolver: fetching %s", key)
        raw = _fetch_secret(self._get_client(root), path, label=key)
        if not json_keys:
            return raw
        return _navigate_json(raw, json_keys, key)


__all__ = ["SecretsManagerResolver"]
