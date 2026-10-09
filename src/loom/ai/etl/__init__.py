"""Agents in ETL steps: the Polars implementation of the agent runner port.

An island of the ``etl-polars`` extra: :mod:`loom.etl` never imports it, and
:meth:`loom.etl.ETLRunner.from_yaml` loads it by module path only when the
config declares an ``ai:`` section.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

from loom.ai.config import AiConfig
from loom.ai.etl._runner import PolarsAgentRunner
from loom.core.config import ConfigContext, ConfigKey

__all__ = ["PolarsAgentRunner", "agent_runner"]


_SPECS_ROOT: Final = f"{ConfigKey.AI}.root"


def agent_runner(context: ConfigContext, *, root: Path) -> PolarsAgentRunner:
    """Build the agent runner of a pipeline config's ``ai:`` section.

    The ``ai.specs`` globs resolve against ``ai.root``, a path relative to
    *root*, or against *root* itself when ``ai.root`` is absent; no glob may
    leave that directory.

    Args:
        context: Config the runner was built from; must declare ``ai:``.
        root: Directory holding the config file.

    Returns:
        The runner the ETL compiler and executor are given.
    """
    specs_root = context.section_optional(_SPECS_ROOT, str)
    return PolarsAgentRunner(
        context.section(ConfigKey.AI, AiConfig),
        root=root if specs_root is None else (root / specs_root).resolve(),
    )
