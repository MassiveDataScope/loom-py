"""Runtime body of the per-ETL Prefect flow."""

from __future__ import annotations

import os
from typing import Any
from uuid import UUID, uuid4

import msgspec

from loom.etl.compiler import flatten_step_names
from loom.etl.compiler._plan import PipelinePlan, iter_processes, iter_steps_in_process
from loom.etl.pipeline import ETLPipeline
from loom.etl.runner import ETLRunner
from loom.prefect._ctx import FlowCtx
from loom.prefect._summary import set_run_summary
from loom.prefect.flow._common import prefect_flow_run_id, prefect_run_anchor
from loom.prefect.flow._params import record_run_params, resolve_run_params
from loom.prefect.flow._run_name import compute_correlation_id
from loom.prefect.flow._runtime import (
    build_observers,
    load_or_init_manifest,
    maybe_delete_manifest,
)
from loom.prefect.flow._stages import known_process_names, validate_stage_names
from loom.prefect.manifest import ManifestStore, RunManifest, completed_steps
from loom.prefect.observer._logging_bridge import (
    install_log_bridge,
    uninstall_log_bridge,
)


def build_flow_body(
    *,
    flow_name: str,
    pipeline: type[ETLPipeline[Any]],
    params_type: type[msgspec.Struct],
    plan: PipelinePlan,
    correlation_field: str | None,
    storage_config_path: str,
    manifest_store: ManifestStore | None,
) -> Any:
    """Return the ``_flow_body(**kwargs)`` callable bound to the factory state.

    The returned callable is what Prefect runs inside the
    ``@prefect.flow`` wrapper; ``etl_flow()`` decorates it after
    attaching a synthesised ``__signature__``.

    Args:
        flow_name: Logical ETL name (used in correlation_id and run_id).
        pipeline: ``ETLPipeline`` subclass to execute.
        params_type: Struct used to decode the bound parameters.
        plan: Pre-compiled pipeline plan.
        correlation_field: Parameter whose value seeds the correlation_id.
        storage_config_path: Loom storage YAML path. Overridable at
            runtime via ``LOOM_STORAGE_CONFIG_PATH``.
        manifest_store: Cross-attempt resume backend, or ``None``.

    Returns:
        The flow body callable. Returns ``None`` on success, raises on
        runner failure.
    """

    known_processes = known_process_names(plan)

    def _flow_body(**kwargs: Any) -> None:
        flow_run_id = prefect_flow_run_id()
        install_log_bridge(flow_run_id)
        try:
            _run(flow_run_id, kwargs)
        finally:
            uninstall_log_bridge()

    def _run(flow_run_id: UUID | None, kwargs: dict[str, Any]) -> None:
        # "env" names the deployment target the flow was deployed for (its
        # work pool); notifications read it from the flow parameters, so the
        # context does not carry it.
        kwargs.pop("env", None)
        explicit_correlation = kwargs.pop("correlation_id", None)
        processes = validate_stage_names(kwargs.pop("processes", None), known_processes)
        params_obj = resolve_run_params(kwargs, params_type, anchor=prefect_run_anchor())
        decoded = msgspec.structs.asdict(params_obj)
        record_run_params(decoded)
        ctx = FlowCtx(
            correlation_id=(
                explicit_correlation
                or compute_correlation_id(flow_name, correlation_field, decoded)
            ),
            run_id=f"{flow_name}-{uuid4().hex[:8]}",
            processes=processes,
        )

        actual_config_path = os.environ.get("LOOM_STORAGE_CONFIG_PATH") or storage_config_path

        manifest = load_or_init_manifest(manifest_store, ctx.correlation_id)
        pending = _resolve_pending(plan, ctx.processes, manifest)

        if not pending:
            maybe_delete_manifest(manifest_store, ctx.correlation_id)
            return

        observers = build_observers(flow_run_id, manifest_store, manifest)
        _invoke_runner(
            actual_config_path,
            pipeline,
            params_obj,
            pending,
            ctx,
            observers,
            plan,
        )
        set_run_summary(_etl_summary(plan, pending))
        maybe_delete_manifest(manifest_store, ctx.correlation_id)

    return _flow_body


def _resolve_pending(
    plan: PipelinePlan,
    processes: tuple[str, ...] | None,
    manifest: RunManifest,
) -> list[str]:
    done = completed_steps(manifest)
    all_step_names = flatten_step_names(plan, processes)
    return [s for s in all_step_names if s not in done]


def _invoke_runner(
    config_path: str,
    pipeline: type[ETLPipeline[Any]],
    params_obj: Any,
    pending: list[str],
    ctx: FlowCtx,
    observers: list[Any],
    plan: PipelinePlan,
) -> None:
    import prefect  # noqa: PLC0415

    runner = ETLRunner.from_yaml(config_path, extra_observers=observers)
    pending_set = set(pending)

    for proc in iter_processes(plan):
        proc_step_names = [s.step_type.__name__ for s in iter_steps_in_process(proc)]
        proc_pending = [s for s in proc_step_names if s in pending_set]
        if not proc_pending:
            continue

        @prefect.task(name=proc.process_type.__name__)
        def _run_proc(steps: list[str] = proc_pending) -> None:
            runner.run(
                pipeline,
                params_obj,
                include=steps,
                run_id=ctx.run_id,
                correlation_id=ctx.correlation_id,
            )

        _run_proc()


def _etl_summary(plan: PipelinePlan, pending: list[str]) -> str:
    """Build a one-line completion summary from the pipeline plan.

    Example: ``StagingProcess → 3 steps ✓   PreparedProcess → 3 steps ✓``
    """
    pending_set = set(pending)
    parts = []
    for proc in iter_processes(plan):
        step_names = [s.step_type.__name__ for s in iter_steps_in_process(proc)]
        total = len(step_names)
        ran = sum(1 for s in step_names if s not in pending_set)
        mark = "✓" if ran == total else f"{ran}/{total}"
        parts.append(f"{proc.process_type.__name__} → {total} steps {mark}")
    return "   ".join(parts)


__all__ = ["build_flow_body"]
