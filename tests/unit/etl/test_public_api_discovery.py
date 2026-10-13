"""Public API discoverability tests for ETL package modules."""

from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize(
    "module_name",
    [
        "loom.etl",
        "loom.etl.compiler",
        "loom.etl.executor",
        "loom.etl.lineage",
        "loom.etl.lineage.sinks",
        "loom.etl.maintenance",
        "loom.etl.declarative",
        "loom.etl.declarative.target",
        "loom.etl.pipeline",
        "loom.etl.runner",
        "loom.etl.schema",
        "loom.etl.storage",
        "loom.etl.checkpoint",
        "loom.etl.testing",
    ],
)
def test_all_exports_are_resolvable(module_name: str) -> None:
    module = importlib.import_module(module_name)
    exported = getattr(module, "__all__", ())
    for name in exported:
        assert getattr(module, name) is not None


def test_lazy_modules_raise_attribute_error_for_unknown_symbol() -> None:
    pipeline_module = importlib.import_module("loom.etl.pipeline")

    with pytest.raises(AttributeError, match="has no attribute"):
        _ = pipeline_module.DOES_NOT_EXIST


def test_top_level_aliases_are_discoverable() -> None:
    etl = importlib.import_module("loom.etl")

    for symbol in (
        "ETLParams",
        "ETLStep",
        "ETLProcess",
        "ETLPipeline",
        "ETLRunner",
        "FromTable",
        "IntoTable",
        "TableRef",
        "StepSQL",
    ):
        assert hasattr(etl, symbol)
