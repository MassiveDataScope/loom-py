"""Cross-package metadata bridging the flow factory and the deployer.

The ``etl_flow()`` factory attaches a frozen ``ETLFlowMeta`` to each
decorated flow at ``__loom_etl_meta__``. The deployer reads it back to
build the matching Prefect Deployment. The type lives here, next to
``loom.prefect._flow_yaml``, so that ``loom.prefect.flow`` imports nothing
from ``loom.prefect.deploy``; the only cross-package edge is deploy → flow.
"""

from __future__ import annotations

from typing import Any

from loom.core.environment import Environment
from loom.core.model import LoomFrozenStruct

# Attribute name used to attach discovery metadata to each flow.
LOOM_ETL_META_ATTR = "__loom_etl_meta__"

# Storage YAML path baked into the worker image, read at flow-run time.
DEFAULT_STORAGE_CONFIG_PATH = "/app/config.yaml"

# Environment variable naming the YAML file a YAML-declared ETL is rebuilt from.
LOOM_ETL_CONFIG = "LOOM_ETL_CONFIG"


class FlowTrigger(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Run a deployment each time another deployment completes a run.

    Args:
        after: The upstream deployment, as ``<flow>/<deployment>`` or, for a
            loom ETL whose flow and deployment share its name, that name.
        inherit_params: Parameters copied from the completed run; every
            other parameter takes this deployment's default.
    """

    after: str
    inherit_params: tuple[str, ...] = ()

    @property
    def upstream_flow(self) -> str:
        """Name of the flow the upstream deployment belongs to."""
        flow, _, _ = self.after.partition("/")
        return flow

    @property
    def upstream_deployment(self) -> str:
        """Name of the upstream deployment."""
        _, _, deployment = self.after.partition("/")
        return deployment or self.after


class ETLFlowMeta(LoomFrozenStruct, frozen=True, kw_only=True):
    """Per-flow metadata consumed by :func:`discover_and_deploy_etls`.

    Args:
        name: Logical ETL name (used as the Prefect flow name AND the
            deployment name).
        config_path: Resolved absolute path of the per-ETL YAML, or the cloud
            URI verbatim.
        source_file: Absolute path to the user's flow module (``__file__``).
        correlation_field: Name of the parameter used as correlation
            value, or ``None`` for random suffixes.
        schedule: The ``schedule`` block from the YAML, or ``None``.
        raw_params: Default parameter mapping pre-bound at deploy time.
        pool_config: Per-environment work-pool overrides
            (``Environment → {"work_pool", "job_variables"}``).
        tags: Extra deployment tags from the YAML, appended after ``name``.
        trigger: The ``trigger`` block from the YAML, or ``None``.
    """

    name: str
    config_path: str
    source_file: str
    correlation_field: str | None
    schedule: dict[str, Any] | None
    raw_params: dict[str, Any]
    pool_config: dict[Environment, dict[str, Any]]
    tags: tuple[str, ...] = ()
    trigger: FlowTrigger | None = None


__all__ = [
    "DEFAULT_STORAGE_CONFIG_PATH",
    "LOOM_ETL_CONFIG",
    "LOOM_ETL_META_ATTR",
    "ETLFlowMeta",
    "FlowTrigger",
]
