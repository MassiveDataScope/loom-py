"""Register one Prefect deployment per ETL, from a flows package or from YAML."""

from __future__ import annotations

import importlib
import os
import pkgutil
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from prefect import Flow
from prefect.events import DeploymentEventTrigger

from loom.prefect._meta import LOOM_ETL_CONFIG, LOOM_ETL_META_ATTR, ETLFlowMeta, FlowTrigger
from loom.prefect.deploy import entrypoint
from loom.prefect.deploy._schedule import build_cron_schedule
from loom.prefect.deploy._yaml_etls import EtlDeclaration, read_declarations


def discover_and_deploy_etls(
    *,
    flows_package: str | None = None,
    config: str | None = None,
    work_pool: str = "loom-fargate",
    env: str = "prod",
) -> list[str]:
    """Register one Prefect Deployment per ETL.

    ETLs come either from ``flows_package`` (one module per ETL calling
    ``etl_flow``) or from ``config`` (YAML declarations rebuilt at run time
    through :mod:`loom.prefect.deploy.entrypoint`). Each deployment targets
    ``work_pool`` (an ECS-typed pool in prod, a docker-typed pool in local
    dev) and carries the per-ETL ``job_variables`` from the YAML's
    ``environments.<env>`` block.

    The ``config`` path exports ``LOOM_ETL_CONFIG`` in the process
    environment around each ``from_source`` call, so it is not thread-safe.

    Args:
        flows_package: Dotted package containing one module per ETL.
        config: Local path, cloud URI or glob of ETL declaration files.
        work_pool: Prefect work pool name. Default matches the prod ECS
            pool; override with ``loom-docker`` for local dev.
        env: Environment key used to pull ``job_variables`` from each
            ETL's YAML.

    Returns:
        List of deployment ids.

    Raises:
        ValueError: When both or neither of ``flows_package`` and ``config``
            are given, or when ``flows_package`` is not a package.
        ConfigError: When a declaration in ``config`` is invalid or sets
            ``LOOM_ETL_CONFIG`` under ``job_variables.env``; raised before
            any deployment is registered.
    """
    if (flows_package is None) == (config is None):
        raise ValueError("pass exactly one of flows_package= or config=")
    if flows_package is not None:
        return _deploy_package(flows_package, work_pool, env)
    return [_deploy_declaration(d, work_pool, env) for d in read_declarations(str(config))]


def deploy_flow(flow: Flow[..., Any], *, work_pool: str = "loom-fargate", env: str = "prod") -> str:
    """Register the Prefect Deployment of one flow built by a loom flow factory.

    Registers exactly what :func:`discover_and_deploy_etls` registers for the
    same flow found in a ``flows_package``: the deployment takes the flow's
    name, tags, default parameters, schedule and trigger, and the work-pool
    settings of ``environments.<env>`` in its YAML. Use it when the flows
    are not one module attribute per ETL, for instance built on demand by a
    module ``__getattr__``, or to deploy a single ETL by name.

    The deployment's entrypoint is ``<source_file>:<flow function>``, with
    ``source_file`` taken relative to the ``working_dir`` job variable
    (``/app/src`` by default), so the worker must be able to rebuild the
    flow by importing that attribute from that file.

    Args:
        flow: A flow returned by :func:`~loom.prefect.etl_flow`,
            :func:`~loom.prefect.backfill_flow` or
            :func:`~loom.prefect.maintenance_flow`.
        work_pool: Prefect work pool name, used when the YAML sets no
            ``work_pool`` for ``env``.
        env: Environment key used to pull ``job_variables`` from the flow's
            YAML.

    Returns:
        The deployment id.

    Raises:
        TypeError: When ``flow`` was not built by a loom flow factory.

    Example::

        from loom.prefect import deploy_flow, etl_flow

        daily_orders = etl_flow(name="daily-orders", ...)
        deployment_id = deploy_flow(daily_orders, work_pool="loom-docker", env="dev")
    """
    meta = getattr(flow, LOOM_ETL_META_ATTR, None)
    if not isinstance(meta, ETLFlowMeta):
        raise TypeError(
            f"deploy_flow() takes a flow built by etl_flow, backfill_flow or "
            f"maintenance_flow; {flow!r} carries no loom ETL metadata."
        )
    return _deploy_single(flow, meta, work_pool, env)


def _deploy_package(flows_package: str, work_pool: str, env: str) -> list[str]:
    pkg = importlib.import_module(flows_package)
    pkg_path = getattr(pkg, "__path__", None)
    if pkg_path is None:
        raise ValueError(f"{flows_package!r} is not a package")

    deployment_ids: list[str] = []
    for module_info in pkgutil.iter_modules(pkg_path):
        module = importlib.import_module(f"{flows_package}.{module_info.name}")
        for attr in vars(module).values():
            meta = getattr(attr, LOOM_ETL_META_ATTR, None)
            if meta is None:
                continue
            deployment_ids.append(_deploy_single(attr, meta, work_pool, env))
    return deployment_ids


@dataclass(frozen=True)
class _DeploymentPlan:
    """Work-pool settings of one deployment, resolved from ``meta.pool_config``."""

    work_pool: str
    working_dir: str
    image: str | None
    job_variables: dict[str, Any]


def _deploy_declaration(declaration: EtlDeclaration, work_pool: str, env: str) -> str:
    flow_obj = entrypoint.build_flow(declaration)
    meta = getattr(flow_obj, LOOM_ETL_META_ATTR)
    plan = _with_recorded_env(_plan(meta, work_pool, env), declaration.config_uri)
    with _exported(LOOM_ETL_CONFIG, declaration.config_uri):
        sourced = flow_obj.from_source(
            source=plan.working_dir,
            entrypoint=f"{entrypoint.__name__}.{declaration.attribute}",
        )
    return _register(sourced, meta, plan)


def _with_recorded_env(plan: _DeploymentPlan, config_uri: str) -> _DeploymentPlan:
    """Record the declaration file under ``job_variables.env`` for the worker."""
    user_env = plan.job_variables.get("env") or {}
    env = {**user_env, LOOM_ETL_CONFIG: config_uri}
    # ``replace`` copies fields added later; the cast Sonar asks for fails mypy.
    return replace(plan, job_variables={**plan.job_variables, "env": env})


@contextmanager
def _exported(name: str, value: str) -> Iterator[None]:
    """Set ``name`` in the process environment, restoring the previous state on exit."""
    previous = os.environ.get(name)
    os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


def _deploy_single(flow_obj: Any, meta: ETLFlowMeta, work_pool: str, env: str) -> str:
    """Register one deployment of a flow defined in a user module.

    Prefect 3 quirk: ``Flow.deploy()`` does NOT accept an ``entrypoint``
    kwarg. For flows with synthesised signatures (our case — ``flow.fn``
    points back at this module), the canonical override is
    ``flow.from_source(source=..., entrypoint=...).deploy(image=..., ...)``.
    """
    plan = _plan(meta, work_pool, env)
    sourced = flow_obj.from_source(
        source=plan.working_dir,
        entrypoint=_file_entrypoint(flow_obj, meta, plan.working_dir),
    )
    return _register(sourced, meta, plan)


def _plan(meta: ETLFlowMeta, work_pool: str, env: str) -> _DeploymentPlan:
    """Resolve the pool, image and job variables of ``meta`` for ``env``."""
    pool_env_config = meta.pool_config.get(env, {})
    job_variables = dict(pool_env_config.get("job_variables") or {})
    image = job_variables.pop("image", None)
    return _DeploymentPlan(
        work_pool=pool_env_config.get("work_pool") or work_pool,
        working_dir=job_variables.get("working_dir", "/app/src"),
        image=image,
        job_variables=job_variables,
    )


def _register(sourced: Any, meta: ETLFlowMeta, plan: _DeploymentPlan) -> str:
    kwargs: dict[str, Any] = {
        "name": meta.name,
        "work_pool_name": plan.work_pool,
        "build": False,
        "push": False,
        "tags": [meta.name, *meta.tags],
        "parameters": dict(meta.raw_params),
        "job_variables": plan.job_variables,
        "enforce_parameter_schema": False,
    }
    if plan.image is not None:
        kwargs["image"] = plan.image
    schedule = build_cron_schedule(meta.schedule)
    if schedule is not None:
        kwargs["schedules"] = [schedule]
    if meta.trigger is not None:
        kwargs["triggers"] = [_completion_trigger(meta.trigger)]
    return str(sourced.deploy(**kwargs))


def _completion_trigger(trigger: FlowTrigger) -> DeploymentEventTrigger:
    """Start the deployment when a run of the upstream deployment completes.

    Each inherited parameter is rendered from the completed run's stored
    parameters, which loom records resolved, so placeholders arrive as dates.
    """
    inherited = {
        name: {"__prefect_kind": "jinja", "template": f"{{{{ flow_run.parameters['{name}'] }}}}"}
        for name in trigger.inherit_params
    }
    # Prefect requires every entry of the list to match a related resource; a
    # deployment name alone is only unique within its flow.
    return DeploymentEventTrigger(
        expect={"prefect.flow-run.Completed"},
        match_related=[
            {"prefect.resource.role": "flow", "prefect.resource.name": trigger.upstream_flow},
            {
                "prefect.resource.role": "deployment",
                "prefect.resource.name": trigger.upstream_deployment,
            },
        ],
        parameters=inherited or None,
    )


def _file_entrypoint(flow_obj: Any, meta: ETLFlowMeta, working_dir: str) -> str:
    """Return ``<path relative to working_dir>:<flow function>``.

    The source file captured at factory time may live under a different
    absolute prefix on the deploy host, so the path is re-anchored on the
    ``working_dir`` name when ``relative_to`` fails.
    """
    flow_file = Path(meta.source_file)
    try:
        relative = flow_file.relative_to(Path(working_dir))
    except ValueError:
        parts = flow_file.parts
        anchor = next(
            (i for i, p in enumerate(parts) if p == Path(working_dir).name),
            None,
        )
        relative = Path(*parts[anchor + 1 :]) if anchor is not None else Path(flow_file.name)
    return f"{relative.as_posix()}:{flow_obj.fn.__name__}"


__all__ = ["deploy_flow", "discover_and_deploy_etls"]
