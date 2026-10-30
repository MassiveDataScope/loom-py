"""Every entry point refuses, at composition, a use case that reads a model nothing serves.

``compose_application`` hands ``factory.verify`` the models the compiled plans
read through markers, so the API and the worker raise the same typed
:class:`~loom.core.use_case.errors.UnservedModelError` before serving.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from loom.composition import (
    AppConfig,
    Application,
    CompositionSource,
    Recipe,
    compose_application,
)
from loom.core.config import ConfigContext
from loom.core.discovery.base import DiscoveryResult
from loom.core.environment import Environment
from loom.core.model import BaseModel, ColumnField
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence import SERVER_PROFILE
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from loom.core.use_case import Exists, LoadById, UnservedModelError, UseCase, UseCaseErrorCode


class _Invoice(BaseModel):
    __tablename__ = "unserved_model_invoices"

    id: int = ColumnField(primary_key=True, autoincrement=True)


class _ReadInvoice(UseCase[object, int]):
    async def execute(
        self, invoice_id: int, invoice: _Invoice = LoadById(_Invoice, by="invoice_id")
    ) -> int:
        return invoice.id


class _InvoiceExists(UseCase[object, bool]):
    async def execute(
        self, invoice_id: int, known: bool = Exists(_Invoice, from_param="invoice_id", against="id")
    ) -> bool:
        return known


class _Serving:
    """What the API asks for, minus HTTP: these use cases, with or without the model."""

    def __init__(self, *use_cases: type[UseCase[Any, Any]], models: bool) -> None:
        self._use_cases = use_cases
        self._models = (_Invoice,) if models else ()

    def recipe(self, source: CompositionSource) -> Recipe:
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=DiscoveryResult(
                models=self._models, use_cases=cast(Any, self._use_cases), interfaces=()
            ),
            backend=SQLAlchemyBackend(),
        )

    def bind(self, application: Application) -> Application:
        return application


def _source(tmp_path: Path) -> CompositionSource:
    return CompositionSource(
        config=ConfigContext.from_dict(
            {
                "database": {"url": "sqlite+aiosqlite:///:memory:"},
                "persistence": {"cursors": {"signing": "ephemeral"}},
            }
        ),
        environment=Environment.TEST,
        code_path=tmp_path,
        config_root=tmp_path,
        app=AppConfig(name="api"),
        observability=ObservabilityRuntime.noop(),
    )


@pytest.mark.parametrize(
    ("use_case", "parameter"), [(_ReadInvoice, "invoice"), (_InvoiceExists, "known")]
)
def test_a_marker_over_an_undiscovered_model_fails_the_composition(
    tmp_path: Path, use_case: type[UseCase[Any, Any]], parameter: str
) -> None:
    with pytest.raises(UnservedModelError) as caught:
        compose_application(_source(tmp_path), _Serving(use_case, models=False))

    issue = caught.value.issue
    assert issue.code is UseCaseErrorCode.UNSERVED_MODEL
    assert (issue.component, issue.field) == (use_case.__qualname__, parameter)
    assert caught.value.model is _Invoice


def test_a_discovered_model_composes(tmp_path: Path) -> None:
    application = compose_application(
        _source(tmp_path), _Serving(_ReadInvoice, _InvoiceExists, models=True)
    )

    assert application.kernel.container.has_repo_mapping(_Invoice)
