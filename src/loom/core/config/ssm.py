"""AWS SSM Parameter Store resolver for loom configuration.

Resolves ``${ssm:/path/to/parameter}`` placeholders in OmegaConf configs
by fetching values from AWS Systems Manager Parameter Store at parse time.

Example::

    from loom.core.config import load_config
    from loom.core.config.ssm import SsmResolver

    cfg = load_config("config/prod.yaml", resolvers=[SsmResolver("eu-west-1")])
"""

from __future__ import annotations

import logging
from typing import Any

from loom.core.config._resolver_utils import (
    CLIENT,
    _aws_error_code,
    _expand_env_vars,
    _navigate_json,
    _split_resolver_key,
    aws_client,
    root_cache,
)
from loom.core.config.errors import ConfigError

logger = logging.getLogger(__name__)


def _fetch_parameter(client: Any, name: str, with_decryption: bool, *, label: str) -> str:
    """Fetch a single parameter value from AWS SSM.

    Args:
        client: Boto3 SSM client.
        name: Fully-qualified SSM parameter name, with ``%VAR%`` tokens
            already expanded.
        with_decryption: Whether to decrypt SecureString parameters.
        label: Key as written in the configuration (unexpanded); used only
            in error messages so that they never carry expanded values.

    Returns:
        The parameter value as a string.

    Raises:
        ConfigError: On any SSM API error. The message carries the AWS error
            code (or the exception type name when absent), never the expanded
            *name*. The chained cause may carry the expanded *name*.
    """
    try:
        result = client.get_parameter(Name=name, WithDecryption=with_decryption)
    except Exception as exc:
        raise ConfigError(
            f"Failed to fetch SSM parameter {label!r}: {_aws_error_code(exc)}"
        ) from exc
    return str(result["Parameter"]["Value"])


class SsmResolver:
    """Resolves SSM Parameter Store paths for use with :func:`~loom.core.config.load_config`.

    Fetches parameter values from AWS Systems Manager Parameter Store.
    The boto3 client is created on first use, once per configuration.

    Env-var tokens in the form ``%VAR_NAME%`` (uppercase letters, digits,
    and underscores only) are expanded from ``os.environ`` before the
    SSM request is made.

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
        with_decryption: Whether to decrypt SecureString parameters.
            Defaults to ``True``.

    Example::

        resolver = SsmResolver("eu-west-1")
        value = resolver.resolve("/myapp/%ENV%/db_password")
    """

    def __init__(
        self,
        region: str | None = None,
        *,
        with_decryption: bool = True,
    ) -> None:
        self._region = region
        self._with_decryption = with_decryption
        self._unbound: dict[Any, Any] = {}

    @property
    def name(self) -> str:
        """Resolver name used as the OmegaConf placeholder prefix.

        Returns:
            The string ``"ssm"``.
        """
        return "ssm"

    def resolve(self, key: str, _root_: Any = None) -> object:
        """Resolve an SSM parameter path to its stored value.

        Splits the dot-notation tail off *key* as written, expands
        ``%VAR_NAME%`` tokens in the remaining base path from the
        environment, then fetches the parameter from AWS SSM Parameter Store.

        Args:
            key: SSM parameter path, optionally containing ``%VAR_NAME%``
                placeholders that are replaced with environment variable values.
                Supports dot-notation for JSON key navigation: ``/path/param.key``
                fetches ``/path/param`` and returns ``param["key"]``. Dots are read
                from the key as written, so an expanded value containing a dot
                is part of the path and never a navigation separator.
            _root_: The configuration being resolved; its ``aws:`` section,
                as written, configures the client. Client and values are
                kept per configuration.

        Returns:
            Resolved value. A plain string for parameters without dot-notation;
            a structured value (string, int, dict, etc.) when dot-notation
            navigates into a JSON parameter.

        Raises:
            ConfigError: When *key* is empty, an env-var placeholder is missing,
                boto3 is not installed, or the SSM API call fails.
        """
        cache = root_cache(_root_, self.name, self._unbound)
        if key in cache:
            return cache[key]
        if not key:
            raise ConfigError("SSM key must not be empty")
        base, json_keys = _split_resolver_key(key)
        ssm_path = _expand_env_vars(base)
        logger.info("ssm_resolver: fetching %s", key)
        if CLIENT not in cache:
            cache[CLIENT] = aws_client("ssm", self._region, resolver="SsmResolver", root=_root_)
        raw = _fetch_parameter(cache[CLIENT], ssm_path, self._with_decryption, label=key)
        cache[key] = _navigate_json(raw, json_keys, key) if json_keys else raw
        return cache[key]
