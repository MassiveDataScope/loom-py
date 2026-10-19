"""Register one Prefect deployment per ETL, from a flows package or from YAML."""

from __future__ import annotations

import importlib
import os
import pkgutil
import sys
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from types import ModuleType
from typing import Any

from prefect import Flow
from prefect.events import DeploymentEventTrigger
from prefect.exceptions import ScriptError
from prefect.utilities.importtools import load_script_as_module

from loom.core.environment import Environment, resolve_environment
from loom.prefect._meta import LOOM_ETL_CONFIG, LOOM_ETL_META_ATTR, ETLFlowMeta, FlowTrigger
from loom.prefect.deploy import entrypoint
from loom.prefect.deploy._schedule import build_cron_schedule
from loom.prefect.deploy._yaml_etls import EtlDeclaration, read_declarations
from loom.prefect.flow import flow_attribute_name


def discover_and_deploy_etls(
    *,
    flows_package: str | None = None,
    config: str | None = None,
    work_pool: str = "loom-fargate",
    env: Environment | str | None = None,
) -> list[str]:
    """Register one Prefect Deployment per ETL.

    ETLs come either from ``flows_package`` (one module per ETL calling
    ``etl_flow``) or from ``config`` (YAML declarations rebuilt at run time
    through :mod:`loom.prefect.deploy.entrypoint`). Each deployment targets
    ``work_pool`` (an ECS-typed pool in prod, a docker-typed pool in local
    dev) and carries the per-ETL ``job_variables`` from the YAML's
    ``environments.<env>`` block. Each deployment records the environment
    as its ``env`` parameter, so its runs resolve to it.

    The ``config`` path exports ``LOOM_ETL_CONFIG`` in the process
    environment around each ``from_source`` call, so it is not thread-safe.

    Args:
        flows_package: Dotted package containing one module per ETL.
        config: Local path, cloud URI or glob of ETL declaration files.
        work_pool: Prefect work pool name. Default matches the prod ECS
            pool; override with ``loom-docker`` for local dev.
        env: Environment to deploy for: picks ``environments.<env>`` from
            each ETL's YAML. ``None`` resolves it like any loom process
            (``LOOM_ENV``, else production).

    Returns:
        List of deployment ids.

    Raises:
        ValueError: When both or neither of ``flows_package`` and ``config``
            are given, when ``flows_package`` is not a package, or when one of
            its flows has no work pool for ``env``, has a source file that
            cannot be imported or is not exposed under its attribute name, as
            :func:`deploy_flow` checks; raised before any deployment is
            registered.
        ConfigError: When ``env`` names no environment, or a declaration in
            ``config`` is invalid or sets
            ``LOOM_ETL_CONFIG`` under ``job_variables.env``; raised before
            any deployment is registered.
    """
    if (flows_package is None) == (config is None):
        raise ValueError("pass exactly one of flows_package= or config=")
    environment = resolve_environment(env)
    if flows_package is not None:
        return _deploy_package(flows_package, work_pool, environment)
    return [_deploy_declaration(d, work_pool, environment) for d in read_declarations(str(config))]


def deploy_flow(
    flow: Flow[..., Any],
    *,
    env: Environment | str | None = None,
    work_pool: str | None = None,
) -> str:
    """Register the Prefect Deployment of one flow built by a loom flow factory.

    Registers exactly what :func:`discover_and_deploy_etls` registers for the
    same flow found in a ``flows_package``: the deployment takes the flow's
    name, tags, default parameters, schedule and trigger, and the work-pool
    settings of ``environments.<env>`` in its YAML. Use it when the flows
    are not one module attribute per ETL, for instance built on demand by a
    module ``__getattr__``, or to deploy a single ETL by name.

    The deployment's entrypoint is ``<source_file>:<attribute>``, with
    ``source_file`` taken relative to the ``working_dir`` job variable
    (``/app/src`` by default) and ``attribute`` the flow name with hyphens
    replaced by underscores. The worker rebuilds the flow by importing that
    attribute, so ``deploy_flow`` checks the module exposes it, through a
    plain attribute or a module ``__getattr__``, before registering. The
    module already imported from ``source_file`` is used; otherwise the file
    is imported the way the worker imports it, running its top level.

    Args:
        flow: A flow returned by :func:`~loom.prefect.etl_flow`,
            :func:`~loom.prefect.backfill_flow` or
            :func:`~loom.prefect.maintenance_flow`.
        env: Environment to deploy for: picks ``work_pool`` and
            ``job_variables`` from ``environments.<env>`` of the flow's YAML
            and becomes the deployment's ``env`` parameter. ``None`` resolves
            it like any loom process (``LOOM_ENV``, else production).
        work_pool: Prefect work pool name, used when the YAML sets no
            ``work_pool`` for ``env``.

    Returns:
        The deployment id.

    Raises:
        TypeError: When ``flow`` was not built by a loom flow factory, or was
            built from a YAML declaration, which
            ``discover_and_deploy_etls(config=...)`` deploys.
        ConfigError: When ``env`` names no environment.
        ValueError: When neither ``work_pool`` nor the YAML names a work pool
            for ``env``, when ``source_file`` cannot be imported, or when it
            does not expose the flow under its attribute name.

    Example::

        from loom.prefect import deploy_flow, etl_flow

        daily_orders = etl_flow(name="daily-orders", ...)
        deployment_id = deploy_flow(daily_orders, env="development", work_pool="loom-docker")
    """
    meta = getattr(flow, LOOM_ETL_META_ATTR, None)
    if not isinstance(meta, ETLFlowMeta):
        raise TypeError(
            f"deploy_flow() takes a flow built by etl_flow, backfill_flow or "
            f"maintenance_flow; {flow!r} carries no loom ETL metadata."
        )
    if _is_declared_in_yaml(meta):
        raise TypeError(
            f"deploy_flow() cannot deploy {meta.name!r}: it was built from a YAML "
            f"declaration; deploy it with discover_and_deploy_etls(config=...)."
        )
    plan = _plan(meta, work_pool, resolve_environment(env))
    _check_exposed(meta)
    return _deploy_single(flow, meta, plan)


def _is_declared_in_yaml(meta: ETLFlowMeta) -> bool:
    """Whether the flow was built by :mod:`loom.prefect.deploy.entrypoint`."""
    return os.path.realpath(meta.source_file) == os.path.realpath(entrypoint.__file__)


def _check_exposed(meta: ETLFlowMeta) -> None:
    """Refuse a flow the worker cannot import from ``meta.source_file``."""
    attribute = flow_attribute_name(meta.name)
    module = _source_module(meta.source_file)
    message = (
        f"{meta.source_file} does not expose the flow {meta.name!r} as attribute "
        f"{attribute!r}; the worker imports the flow from that attribute."
    )
    try:
        exposed = getattr(module, attribute, None)
    except Exception as exc:
        raise ValueError(message) from exc
    exposed_meta = getattr(exposed, LOOM_ETL_META_ATTR, None)
    if not isinstance(exposed_meta, ETLFlowMeta) or exposed_meta.name != meta.name:
        raise ValueError(message)


def _source_module(source_file: str) -> ModuleType:
    """Return the module loaded from ``source_file``, importing it as the worker does."""
    loaded = _loaded_module_name(source_file)
    if loaded is not None:
        return sys.modules[loaded]
    try:
        return load_script_as_module(source_file)
    except ScriptError as exc:
        failed = _loaded_module_name(source_file)
        if failed is not None:
            del sys.modules[failed]
        raise ValueError(f"{source_file} is not an importable Python file: {exc}") from exc


def _loaded_module_name(source_file: str) -> str | None:
    """Return the ``sys.modules`` key of the module loaded from ``source_file``.

    Paths are compared after resolving symlinks, so a module imported through
    a symlinked directory matches the file it resolves to.
    """
    source = os.path.realpath(source_file)
    for name, module in list(sys.modules.items()):
        module_file = getattr(module, "__file__", None)
        if module_file is not None and os.path.realpath(module_file) == source:
            return name
    return None


def _deploy_package(flows_package: str, work_pool: str, env: Environment) -> list[str]:
    pkg = importlib.import_module(flows_package)
    pkg_path = getattr(pkg, "__path__", None)
    if pkg_path is None:
        raise ValueError(f"{flows_package!r} is not a package")

    flows = list(_package_flows(flows_package, pkg_path))
    plans = [_plan(meta, work_pool, env) for _, meta in flows]
    for _, meta in flows:
        _check_exposed(meta)
    return [
        _deploy_single(flow, meta, plan) for (flow, meta), plan in zip(flows, plans, strict=True)
    ]


def _package_flows(
    flows_package: str, pkg_path: Iterable[str]
) -> Iterator[tuple[Any, ETLFlowMeta]]:
    """Yield each loom flow, with its metadata, of the modules of ``flows_package``."""
    for module_info in pkgutil.iter_modules(pkg_path):
        module = importlib.import_module(f"{flows_package}.{module_info.name}")
        for attr in vars(module).values():
            meta = getattr(attr, LOOM_ETL_META_ATTR, None)
            if meta is not None:
                yield attr, meta


@dataclass(frozen=True)
class _DeploymentPlan:
    """Work-pool settings of one deployment, resolved from ``meta.pool_config``."""

    work_pool: str
    working_dir: str
    image: str | None
    job_variables: dict[str, Any]
    environment: Environment


def _deploy_declaration(declaration: EtlDeclaration, work_pool: str, env: Environment) -> str:
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


def _deploy_single(flow_obj: Any, meta: ETLFlowMeta, plan: _DeploymentPlan) -> str:
    """Register one deployment of a flow defined in a user module.

    Prefect 3 quirk: ``Flow.deploy()`` does NOT accept an ``entrypoint``
    kwarg. For flows with synthesised signatures (our case — ``flow.fn``
    points back at this module), the canonical override is
    ``flow.from_source(source=..., entrypoint=...).deploy(image=..., ...)``.
    """
    sourced = flow_obj.from_source(
        source=plan.working_dir,
        entrypoint=_file_entrypoint(meta, plan.working_dir),
    )
    return _register(sourced, meta, plan)


def _plan(meta: ETLFlowMeta, work_pool: str | None, env: Environment) -> _DeploymentPlan:
    """Resolve the pool, image and job variables of ``meta`` for ``env``."""
    pool_env_config = meta.pool_config.get(env, {})
    resolved_pool = pool_env_config.get("work_pool") or work_pool
    if not resolved_pool:
        raise ValueError(
            f"no work pool for flow {meta.name!r} in env {str(env)!r}: pass work_pool= "
            f"or set environments.{env}.work_pool in its YAML."
        )
    job_variables = dict(pool_env_config.get("job_variables") or {})
    image = job_variables.pop("image", None)
    return _DeploymentPlan(
        work_pool=resolved_pool,
        working_dir=job_variables.get("working_dir", "/app/src"),
        image=image,
        job_variables=job_variables,
        environment=env,
    )


def _register(sourced: Any, meta: ETLFlowMeta, plan: _DeploymentPlan) -> str:
    kwargs: dict[str, Any] = {
        "name": meta.name,
        "work_pool_name": plan.work_pool,
        "build": False,
        "push": False,
        "tags": [meta.name, *meta.tags],
        "parameters": {**meta.raw_params, "env": plan.environment.value},
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


def _file_entrypoint(meta: ETLFlowMeta, working_dir: str) -> str:
    """Return ``<path relative to working_dir>:<flow attribute>``.

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
    return f"{relative.as_posix()}:{flow_attribute_name(meta.name)}"


__all__ = ["deploy_flow", "discover_and_deploy_etls"]
