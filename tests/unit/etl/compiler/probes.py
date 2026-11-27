"""Aliases of the compile probes the ETL compiler tests share."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from loom.etl.compiler import StepPlan

CompileTarget = Callable[[Any], StepPlan]
