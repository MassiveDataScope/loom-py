"""Prefect entrypoint for ETLs declared in YAML.

Prefect loads ``loom.prefect.deploy.entrypoint.<attribute>``, where
``<attribute>`` is the ETL name with hyphens replaced by underscores, and gets
the flow rebuilt from the declaration file named by ``LOOM_ETL_CONFIG``.
Names starting with ``_`` raise ``AttributeError`` as in any module; a public
name raises ``ConfigError`` when the variable is unset or the file declares no
ETL for it.
"""

from __future__ import annotations

from typing import Any

from loom.core.config import ConfigError
from loom.core.environment import loom_variable
from loom.prefect._meta import LOOM_ETL_CONFIG
from loom.prefect.deploy._yaml_etls import EtlDeclaration, load_declaration
from loom.prefect.flow import build_etl_flow


def build_flow(declaration: EtlDeclaration) -> Any:
    """Build the Prefect flow of a YAML declaration, sourced from this module.

    Args:
        declaration: Resolved ETL declaration.

    Returns:
        A ``@prefect.flow``-decorated callable with ``__loom_etl_meta__``
        attached.
    """
    return build_etl_flow(
        name=declaration.name,
        pipeline=declaration.pipeline,
        params_type=declaration.params_type,
        settings=declaration.settings,
        config_path=declaration.config_uri,
        source_file=__file__,
        storage_config_path=declaration.storage_config_path,
    )


def __getattr__(attribute: str) -> Any:
    if attribute.startswith("_"):
        raise AttributeError(attribute)
    config_uri = loom_variable(LOOM_ETL_CONFIG)
    if not config_uri:
        raise ConfigError(
            f"{LOOM_ETL_CONFIG} is not set; it must name the YAML file declaring "
            f"the ETL behind entrypoint attribute {attribute!r}"
        )
    return build_flow(load_declaration(config_uri, attribute))


__all__ = ["LOOM_ETL_CONFIG", "build_flow"]
