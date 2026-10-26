"""loom.prefect — Prefect integration for ETL observability.

Provides the building blocks to declare per-ETL Prefect flows backed by a
worker (``prefect-aws.ECSWorker`` in production, ``prefect-docker``
locally). Each ETL declares one ``etl_flow(...)`` instance at module
scope in a per-ETL ``.py`` file; CI/CD registers all of them as Prefect
deployments via ``discover_and_deploy_etls(...)``.

Install the optional extra to get ``prefect`` as a dependency::

    pip install loom-kernel[prefect]

Public API
----------
- :class:`FlowCtx` — operational context (correlation_id, run_id, run options).
- :func:`etl_flow` — per-ETL flow factory with synthesized signature.
- :func:`backfill_flow` / :data:`BackfillChunk` — chunked backfill flow
  factory (hour/day/month/year partitions plus a finalize pass).
- :func:`discover_and_deploy_etls` — walks a flows package and registers
  one Prefect deployment per ETL.
- :func:`deploy_flow` — registers the Prefect deployment of one flow built
  by a loom flow factory.
- :class:`ManifestStore` / :class:`RunManifest` / :class:`StepEntry` —
  ephemeral retry manifest types.
- :class:`S3JsonManifestStore` — S3-backed manifest implementation.
- :class:`PrefectObserver` — ``LifecycleObserver`` bridging loom events
  to the Prefect run logger.
- :func:`resolve_placeholder` — date/datetime DSL used by parameter
  defaults (``${today}``, ``${now-1h}``, ...).
- :class:`FlowTrigger` — the per-flow YAML ``trigger`` block: run after
  another deployment completes, inheriting some of its parameters.

Architecture contract
---------------------
``loom.core`` and ``loom.etl`` never import from ``loom.prefect`` or
``prefect``. Container provisioning belongs to the Prefect worker, not
to loom.
"""

from loom.prefect._config import FlowConfig
from loom.prefect._config import _load_flow_config as load_flow_config
from loom.prefect._ctx import FlowCtx
from loom.prefect._meta import FlowTrigger
from loom.prefect._placeholders import resolve_placeholder
from loom.prefect.deploy import deploy_flow, discover_and_deploy_etls
from loom.prefect.flow import BackfillChunk, backfill_flow, etl_flow, maintenance_flow
from loom.prefect.manifest import (
    ManifestStore,
    RunManifest,
    S3JsonManifestStore,
    StepEntry,
)
from loom.prefect.observer import PrefectObserver, PrefectTaskRunObserver

__all__ = [
    "BackfillChunk",
    "FlowConfig",
    "FlowCtx",
    "FlowTrigger",
    "ManifestStore",
    "PrefectObserver",
    "PrefectTaskRunObserver",
    "RunManifest",
    "S3JsonManifestStore",
    "StepEntry",
    "backfill_flow",
    "deploy_flow",
    "discover_and_deploy_etls",
    "etl_flow",
    "maintenance_flow",
    "load_flow_config",
    "resolve_placeholder",
]
