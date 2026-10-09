"""Agents in ETL steps: the Polars implementation of the agent runner port.

An island of the ``etl-polars`` extra: :mod:`loom.etl` never imports it, and
:meth:`loom.etl.ETLRunner.from_yaml` loads it by module path only when the
config declares an ``ai:`` section.
"""

from __future__ import annotations

from pathlib import Path

from loom.ai.config import AiConfig
from loom.ai.etl._runner import PolarsAgentRunner
from loom.core.config import ConfigContext, ConfigKey

__all__ = ["PolarsAgentRunner", "agent_runner"]


def agent_runner(context: ConfigContext, *, root: Path) -> PolarsAgentRunner:
    """Build the agent runner of a pipeline config's ``ai:`` section.

    Args:
        context: Config the runner was built from; must declare ``ai:``.
        root: Directory the ``ai.specs`` globs are resolved against.

    Returns:
        The runner the ETL compiler and executor are given.
    """
    return PolarsAgentRunner(context.section(ConfigKey.AI, AiConfig), root=root)
