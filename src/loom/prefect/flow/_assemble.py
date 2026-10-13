"""Deploy-time flow assembly shared by every flow factory.

``etl_flow``, ``maintenance_flow`` and ``backfill_flow`` all read the same
per-flow YAML settings and produce the same artefact: a ``@prefect.flow``
decorated body with :class:`~loom.prefect._meta.ETLFlowMeta` attached for the
deployer. This module owns that boilerplate so a change to the decoration or
the metadata contract is made in exactly one place.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, get_type_hints

import msgspec
import prefect

from loom.prefect._config import FlowConfig, flow_config_from_mapping
from loom.prefect._flow_yaml import (
    extract_pool_config,
    extract_trigger,
    read_yaml,
    resolve_config_uri,
)
from loom.prefect._meta import LOOM_ETL_META_ATTR, ETLFlowMeta, FlowTrigger
from loom.prefect.flow._common import coerce_tags
from loom.prefect.flow._hooks import make_notification_hooks, pause_schedule_on_failure
from loom.prefect.flow._run_name import make_run_name_callback
from loom.prefect.notify import Notifier, build_notifiers

_INHERITABLE_TYPES: frozenset[Any] = frozenset({str, date, datetime})


@dataclass(frozen=True)
class FlowSettings:
    """Per-flow YAML settings consumed by :func:`assemble_flow`."""

    correlation_field: str | None
    schedule: dict[str, Any] | None
    raw_params: dict[str, Any]
    pool_config: dict[str, dict[str, Any]]
    tags: tuple[str, ...]
    notifiers: tuple[Notifier, ...]
    retry_policy: FlowConfig
    trigger: FlowTrigger | None = None


def load_flow_settings(config_path: str) -> FlowSettings:
    """Read the per-flow YAML into the settings every factory needs.

    Args:
        config_path: Path or URI of the per-flow YAML (schedule, params, tags, …).

    Returns:
        Parsed :class:`FlowSettings`.
    """
    return flow_settings_from_mapping(read_yaml(config_path))


def flow_settings_from_mapping(raw_cfg: Mapping[str, Any]) -> FlowSettings:
    """Build the settings every factory needs from an already-loaded YAML body.

    Args:
        raw_cfg: Top-level mapping of a per-flow YAML document.

    Returns:
        Parsed :class:`FlowSettings`.
    """
    return FlowSettings(
        correlation_field=raw_cfg.get("correlation_field"),
        schedule=raw_cfg.get("schedule"),
        raw_params=dict(raw_cfg.get("params") or {}),
        pool_config=extract_pool_config(raw_cfg),
        tags=coerce_tags(raw_cfg.get("tags")),
        notifiers=build_notifiers(raw_cfg.get("notifications")),
        retry_policy=flow_config_from_mapping(raw_cfg.get("retry")),
        trigger=extract_trigger(raw_cfg),
    )


def flow_attribute_name(name: str) -> str:
    """Derive the Python attribute name a flow body carries for *name*.

    Args:
        name: Logical flow name, free-form (hyphens allowed).

    Returns:
        *name* with hyphens replaced by underscores.
    """
    return name.replace("-", "_")


def assemble_flow(
    *,
    name: str,
    body: Callable[..., None],
    signature: inspect.Signature,
    params_type: type[msgspec.Struct],
    settings: FlowSettings,
    config_path: str,
    source_file: str,
    correlation_field: str | None = None,
    retries: int | None = None,
    retry_delay_seconds: int | None = None,
) -> Any:
    """Decorate *body* as a Prefect flow and attach the deployer metadata.

    Args:
        name: Logical flow name (Prefect flow name AND deployment name).
        body: The ``**kwargs`` flow-body callable produced by a factory.
        signature: Synthesised ``inspect.Signature`` exposed to Prefect.
        params_type: Struct the flow's parameters decode into.
        settings: Parsed per-flow YAML settings.
        config_path: Path or URI of the per-flow YAML. A local path is
            resolved into the metadata; a cloud URI is recorded verbatim.
        source_file: ``__file__`` of the user's flow module.
        correlation_field: Parameter whose value seeds the run name and the
            correlation id, or ``None`` for timestamp/random naming.
        retries: Prefect flow retries; ``None`` disables retries.
        retry_delay_seconds: Delay before the server reschedules the run. The
            container exits while it waits, so an hour-scale delay is free.

    Returns:
        The ``@prefect.flow``-decorated callable with ``__loom_etl_meta__``
        attached at :data:`~loom.prefect._meta.LOOM_ETL_META_ATTR`.

    Raises:
        ValueError: When the trigger inherits a parameter *params_type* does
            not declare.
    """
    check_inherited_params(name, settings.trigger, params_type)
    safe_name = flow_attribute_name(name)
    flow_body: Any = body
    flow_body.__signature__ = signature
    flow_body.__name__ = safe_name
    flow_body.__qualname__ = safe_name

    failure_hooks, completion_hooks = make_notification_hooks(name, settings.notifiers)
    decorated = prefect.flow(
        name=name,
        flow_run_name=make_run_name_callback(name, correlation_field),
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
        validate_parameters=False,
        on_failure=[pause_schedule_on_failure, *failure_hooks],
        on_completion=completion_hooks or None,
    )(flow_body)
    setattr(
        decorated,
        LOOM_ETL_META_ATTR,
        ETLFlowMeta(
            name=name,
            config_path=resolve_config_uri(config_path),
            source_file=str(Path(source_file).resolve()),
            correlation_field=correlation_field,
            schedule=settings.schedule,
            raw_params=settings.raw_params,
            pool_config=settings.pool_config,
            tags=settings.tags,
            trigger=settings.trigger,
        ),
    )
    return decorated


def check_inherited_params(
    name: str, trigger: FlowTrigger | None, params_type: type[msgspec.Struct]
) -> None:
    """Refuse a trigger inheriting a parameter it cannot carry.

    The trigger renders each inherited value as text, so only a field
    declared exactly ``str``, ``date`` or ``datetime`` decodes back to what
    the upstream run held. An optional field is refused as well: a null
    upstream value renders as the text ``"None"``, which a date field
    rejects and a text field takes silently.

    Every flow factory runs this check when it builds the flow; calling it
    directly validates a trigger without building one, e.g. in CI.

    Args:
        name: Flow name, quoted in the error message.
        trigger: The flow's trigger, or ``None``, which passes.
        params_type: Struct the flow's parameters decode into.

    Raises:
        ValueError: When an entry of ``trigger.inherit_params`` is not a field
            of *params_type*, or is not declared ``str``, ``date`` or
            ``datetime``.
    """
    if trigger is None:
        return
    fields = {field.name for field in msgspec.structs.fields(params_type)}
    hints = get_type_hints(params_type)
    for param in trigger.inherit_params:
        if param not in fields:
            raise ValueError(
                f"flow {name!r}: trigger.inherit_params names {param!r}, "
                f"not a field of {params_type.__name__}"
            )
        if hints.get(param) not in _INHERITABLE_TYPES:
            raise ValueError(
                f"flow {name!r}: trigger.inherit_params names {param!r}, declared "
                f"{hints.get(param)!r}; the trigger passes values as text, so only "
                "str, date and datetime fields can be inherited"
            )


__all__ = [
    "FlowSettings",
    "assemble_flow",
    "check_inherited_params",
    "flow_attribute_name",
    "flow_settings_from_mapping",
    "load_flow_settings",
]
