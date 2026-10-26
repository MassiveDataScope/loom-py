"""Configuration loading utilities for Loom applications.

Provides an omegaConf-backed loader that supports multiple YAML files and
typed section extraction.  The framework imposes no shape on the config
object — the user owns the structure.

Example::

    from loom.core.config import load_config, section

    cfg = load_config("config/base.yaml", "config/local.yaml")
    db = section(cfg, "database", DatabaseConfig)
"""

from loom.core.config._includes import expand_config_glob, is_cloud_uri
from loom.core.config.binder import StructBinder
from loom.core.config.configurable import ConfigBinding, Configurable
from loom.core.config.context import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.config.keys import ConfigKey
from loom.core.config.loader import load_config, section
from loom.core.config.observability import OtelConfig
from loom.core.config.resolver import (
    ConfigResolver,
    default_resolvers,
    merge_resolvers,
    with_default_resolvers,
)
from loom.core.config.retired import reject_retired_keys
from loom.core.config.secrets import SecretsManagerResolver
from loom.core.config.ssm import SsmResolver

__all__ = [
    "ConfigBinding",
    "ConfigContext",
    "ConfigError",
    "ConfigKey",
    "ConfigResolver",
    "Configurable",
    "OtelConfig",
    "SecretsManagerResolver",
    "SsmResolver",
    "StructBinder",
    "default_resolvers",
    "expand_config_glob",
    "is_cloud_uri",
    "load_config",
    "merge_resolvers",
    "reject_retired_keys",
    "section",
    "with_default_resolvers",
]
