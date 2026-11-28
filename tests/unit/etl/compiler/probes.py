"""The compile probes the ETL compiler tests share: a declaration in, its compiled plan out."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from loom.etl.compiler import StepPlan

CompileDeclaration = Callable[[Any], StepPlan]
