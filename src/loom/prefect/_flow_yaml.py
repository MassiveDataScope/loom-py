"""Per-flow YAML loading shared by the flow factories and the deployer.

Lives outside both ``loom.prefect.flow`` and ``loom.prefect.deploy`` so that
the flow package imports nothing from the deploy package.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import msgspec

from loom.core.config import is_cloud_uri, load_config
from loom.core.config.overrides import apply_overrides, register_config_sections
from loom.core.environment import Environment, override_variables, parse_environment
from loom.prefect._meta import FlowTrigger

_ENV_INTERPOLATION = re.compile(r"\$\{oc\.[^}]*\}")

# The top-level sections loom reads from a per-flow YAML. A flow process loads
# its storage configuration with the same LOOM__ variables, so they are known
# sections there too.
FLOW_SECTIONS = (
    "etl",
    "etls",
    "pipeline",
    "params_type",
    "storage_config_path",
    "correlation_field",
    "schedule",
    "params",
    "tags",
    "environments",
    "retry",
    "notifications",
    "trigger",
)
register_config_sections(*FLOW_SECTIONS)


def read_yaml(config_path: str) -> dict[str, Any]:
    """Load a per-flow YAML via ``loom.core.config.load_config``.

    The ``LOOM__*`` variables merge last, as for an application's
    configuration (see :mod:`loom.core.config.overrides`).
    ``${oc.*}`` interpolations are resolved against the host env at load
    time; loom placeholders (``${now}``, ``${today-1d}``, …) are kept
    verbatim so :func:`resolve_placeholder` resolves them at flow-run
    time. ``s3://``/``gs://``/… URIs are honoured.

    Raises:
        ValueError: When the top level is not a mapping.
        loom.core.config.errors.ConfigError: Underlying load failures.
    """
    from omegaconf import DictConfig, OmegaConf  # noqa: PLC0415

    _ensure_mapping_top_level(config_path)
    merged = apply_overrides(load_config(config_path, keyed=("etls",)), override_variables())
    if not isinstance(merged, DictConfig):
        raise ValueError(f"{config_path}: top-level YAML must be a mapping")
    container = OmegaConf.to_container(merged, resolve=False)
    if not isinstance(container, dict):
        raise ValueError(f"{config_path}: top-level YAML must be a mapping")
    container.pop("includes", None)
    return {str(k): _resolve_oc_only(v) for k, v in container.items()}


def resolve_config_uri(config_path: str) -> str:
    """Return a cloud URI verbatim and a local path resolved to an absolute one.

    Args:
        config_path: Local path (optionally a glob) or cloud URI.

    Returns:
        The resolved absolute path of a local entry, or the cloud URI verbatim.
    """
    if is_cloud_uri(config_path):
        return config_path
    return str(Path(config_path).resolve())


def extract_pool_config(raw_cfg: Mapping[str, Any]) -> dict[Environment, dict[str, Any]]:
    """Return per-environment work-pool overrides from the per-flow YAML.

    Args:
        raw_cfg: Top-level YAML mapping returned by :func:`read_yaml`.

    Returns:
        Mapping ``Environment → {"work_pool": str | None,
        "job_variables": dict}``. Empty when the YAML has no
        ``environments`` block.

    Raises:
        ConfigError: When a key of ``environments`` names no
            :class:`~loom.core.environment.Environment`.
    """
    environments = raw_cfg.get("environments") or {}
    out: dict[Environment, dict[str, Any]] = {}
    for env_name, env_block in environments.items():
        environment = parse_environment(str(env_name), source="the environments block")
        if not isinstance(env_block, dict):
            continue
        out[environment] = {
            "work_pool": env_block.get("work_pool"),
            "job_variables": env_block.get("job_variables", {}) or {},
        }
    return out


def extract_trigger(raw_cfg: Mapping[str, Any]) -> FlowTrigger | None:
    """Return the ``trigger`` block of the per-flow YAML, or ``None``.

    Raises:
        ValueError: When the block does not decode into a
            :class:`~loom.prefect._meta.FlowTrigger` (unknown key, wrong
            type) or ``after`` does not name a deployment.
    """
    raw = raw_cfg.get("trigger")
    if raw is None:
        return None
    try:
        trigger = msgspec.convert(raw, type=FlowTrigger)
    except msgspec.ValidationError as exc:
        raise ValueError(f"trigger: {exc}") from exc
    parts = trigger.after.split("/")
    if len(parts) > 2 or not all(part and part == part.strip() for part in parts):
        raise ValueError(
            f"trigger.after: expected '<flow>/<deployment>' or a deployment name, "
            f"got {trigger.after!r}"
        )
    return trigger


def _ensure_mapping_top_level(config_path: str) -> None:
    """Reject a non-mapping document; unreadable or malformed files are left to ``load_config``."""
    import yaml  # noqa: PLC0415
    from omegaconf import DictConfig, OmegaConf  # noqa: PLC0415

    try:
        raw = OmegaConf.load(config_path)
    except (OSError, yaml.YAMLError):
        return
    if not isinstance(raw, DictConfig):
        raise ValueError(f"{config_path}: top-level YAML must be a mapping")


def _resolve_oc_only(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _resolve_oc_only(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_oc_only(v) for v in value]
    if isinstance(value, str) and _ENV_INTERPOLATION.search(value):
        return _resolve_string(value)
    return value


def _resolve_string(value: str) -> Any:
    from omegaconf import OmegaConf  # noqa: PLC0415
    from omegaconf.errors import OmegaConfBaseException  # noqa: PLC0415

    try:
        node = OmegaConf.create({"_": value})
        resolved = OmegaConf.to_container(node, resolve=True)
    except OmegaConfBaseException:
        return value
    if isinstance(resolved, dict):
        return resolved.get("_", value)
    return value


__all__ = ["extract_pool_config", "extract_trigger", "read_yaml", "resolve_config_uri"]
