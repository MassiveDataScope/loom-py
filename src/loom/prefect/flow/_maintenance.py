"""Prefect flow factory for MaintenanceStep — mirrors etl_flow() structure."""

from __future__ import annotations

import os
from typing import Any

import msgspec

from loom.core.checks import REST_CHECKS, enforce_deploy_checks
from loom.core.environment import resolve_environment
from loom.core.observability import ObservabilityRuntime, Scope
from loom.etl.maintenance._runner import MaintenanceRunner
from loom.etl.maintenance._step import MaintenanceStep
from loom.etl.runner.config_loader import _load_context, _parse_sections
from loom.prefect._summary import set_run_summary
from loom.prefect.flow._assemble import assemble_flow, load_flow_settings
from loom.prefect.flow._common import prefect_flow_run_id, prefect_run_anchor
from loom.prefect.flow._params import record_run_params, resolve_run_params
from loom.prefect.flow._signature import synthesise_flow_signature
from loom.prefect.observer._logging_bridge import install_log_bridge, uninstall_log_bridge


def maintenance_flow(
    *,
    name: str,
    step: type[MaintenanceStep[Any]],
    params_type: type[msgspec.Struct],
    config_path: str,
    source_file: str,
    storage_config_path: str = "/app/config.yaml",
) -> Any:
    """Build a Prefect flow for a :class:`~loom.etl.maintenance.MaintenanceStep`.

    Mirrors :func:`~loom.prefect.etl_flow` so the deployment machinery
    (schedule, tags, notifications, work-pool config) works identically.

    Maintenance flows do not support retries at the Prefect level (vacuum and
    compaction are idempotent but long-running; retries should be triggered
    manually by the operator if needed).  They also do not use a
    ``ManifestStore`` — each run starts fresh.

    Args:
        name: Logical flow name shown in the Prefect UI.
        step: :class:`~loom.etl.maintenance.MaintenanceStep` subclass to run.
        params_type: ``msgspec.Struct`` whose fields become typed flow kwargs.
        config_path: Path to the per-flow YAML (schedule, params, tags, …).
        source_file: ``__file__`` of the calling module (needed by Prefect
            ``from_source`` to locate the flow on disk).
        storage_config_path: Path to the loom storage YAML read at runtime
            inside the container. Defaults to ``/app/config.yaml``.

    Returns:
        A ``@prefect.flow``-decorated callable with ``__loom_etl_meta__``
        attached for the deployer.
    """
    settings = load_flow_settings(config_path)

    def _flow_body(**kwargs: Any) -> None:
        install_log_bridge(prefect_flow_run_id())
        try:
            environment = resolve_environment(kwargs.pop("env", None))
            params = resolve_run_params(kwargs, params_type, anchor=prefect_run_anchor())
            decoded = msgspec.structs.asdict(params)
            record_run_params(decoded)
            actual_path = os.environ.get("LOOM_STORAGE_CONFIG_PATH") or storage_config_path
            context = _load_context(actual_path)
            storage_config, observability_config = _parse_sections(context)
            observability = ObservabilityRuntime.from_config(
                observability_config, environment=environment
            )
            enforce_deploy_checks(context, environment, skip=(REST_CHECKS,))
            with observability.span(Scope.MAINTENANCE, step.__name__):
                report = MaintenanceRunner.from_config(storage_config).run(step, params=params)
                set_run_summary(_maintenance_summary(report, decoded))
                report.raise_if_errors()
        finally:
            uninstall_log_bridge()

    return assemble_flow(
        name=name,
        body=_flow_body,
        signature=synthesise_flow_signature(params_type),
        params_type=params_type,
        settings=settings,
        config_path=config_path,
        source_file=source_file,
    )


def _maintenance_summary(report: Any, params: dict[str, Any]) -> str:
    """Format a one-line summary from a MaintenanceReport.

    Examples:
        ``5 tables — vacuum ✓  compact ✓  dry_run: false``
        ``3/5 tables failed: raw.events, staging.snapshots``
    """
    from loom.etl.maintenance._runner import MaintenanceReport  # noqa: PLC0415

    if not isinstance(report, MaintenanceReport):
        return ""

    total = len(report.results)
    failed = [r for r in report.results if not r.ok]

    dry_run = params.get("dry_run")
    dry_run_tag = f"  dry_run: {str(dry_run).lower()}" if dry_run is not None else ""

    if failed:
        refs = ", ".join(r.table_ref for r in failed)
        return f"{len(failed)}/{total} tables failed: {refs}{dry_run_tag}"

    # Collect op names that ran across all tables
    op_names: list[str] = []
    seen: set[str] = set()
    for result in report.results:
        for op_name in result.op_results:
            if op_name not in seen:
                op_names.append(op_name)
                seen.add(op_name)
    ops_str = "  ".join(f"{n} ✓" for n in op_names) if op_names else "no ops"
    return f"{total} tables — {ops_str}{dry_run_tag}"


__all__ = ["maintenance_flow"]
