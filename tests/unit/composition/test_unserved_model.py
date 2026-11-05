"""Every entry point refuses, at composition, a use case that reads a model nothing serves.

``compose_application`` hands ``factory.verify`` the models the compiled plans
read through markers, so the API and the worker raise the same typed
:class:`~loom.core.use_case.errors.UnservedModelError` before serving.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom.composition import (
    AppConfig,
    CompositionSource,
    compose_application,
)
from loom.core.config import ConfigContext
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case import Exists, LoadById, UnservedModelError, UseCase, UseCaseErrorCode
from tests.helpers.composition import Serving, composition_source


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


def _source(tmp_path: Path) -> CompositionSource:
    return composition_source(
        ConfigContext.from_dict(
            {
                "stores": {
                    "default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///:memory:"}
                },
                "persistence": {"cursors": {"signing": "ephemeral"}},
            }
        ),
        app=AppConfig(name="api"),
        code_path=tmp_path,
        config_root=tmp_path,
    )


@pytest.mark.parametrize(
    ("use_case", "parameter"), [(_ReadInvoice, "invoice"), (_InvoiceExists, "known")]
)
def test_a_marker_over_an_undiscovered_model_fails_the_composition(
    tmp_path: Path, use_case: type[UseCase[Any, Any]], parameter: str
) -> None:
    source = _source(tmp_path)
    adapter = Serving(use_cases=(use_case,))
    with pytest.raises(UnservedModelError) as caught:
        compose_application(source, adapter)

    issue = caught.value.issue
    assert issue.code is UseCaseErrorCode.UNSERVED_MODEL
    assert (issue.component, issue.field) == (use_case.__qualname__, parameter)
    assert caught.value.model is _Invoice


def test_a_discovered_model_composes(tmp_path: Path) -> None:
    application = compose_application(
        _source(tmp_path), Serving(_Invoice, use_cases=(_ReadInvoice, _InvoiceExists))
    )

    assert application.kernel.container.has_repo_mapping(_Invoice)
