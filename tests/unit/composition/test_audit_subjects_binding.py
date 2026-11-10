"""AuditSubjects default binding precedence."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import cast

import pytest

from loom.composition import AppConfig, compose_application
from loom.composition.build import _register_audit_subjects
from loom.core.audit import ALL_SUBJECTS, OWN, AuditSubjects
from loom.core.audit.subjects import OwnSubjectsOnly
from loom.core.authz.product import (
    AuthzProduct,
    clear_authz_product,
    load_authz_product,
    register_authz_product,
)
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.identity import Identity
from loom.core.persistence.stores import StoreKey
from tests.helpers.composition import Serving, composition_source


class ProductHook:
    catalog = None
    delegate = None
    elevations: dict[str, object] = {}

    async def audit_subjects(self, identity: Identity, store: StoreKey) -> object:
        return ALL_SUBJECTS


class BoundHook:
    async def audit_subjects(self, identity: Identity, store: StoreKey) -> object:
        return frozenset({"bound"})


@pytest.fixture(autouse=True)
def _clear_product() -> Iterator[None]:
    clear_authz_product()
    yield
    clear_authz_product()


def test_explicit_binding_wins_over_authz_product() -> None:
    container = LoomContainer()
    bound = BoundHook()
    container.register_instance(AuditSubjects, bound)
    register_authz_product(cast(AuthzProduct, ProductHook()))
    _register_audit_subjects(container, load_authz_product())
    assert container.resolve(AuditSubjects) is bound


def test_authz_product_wins_over_own_default() -> None:
    container = LoomContainer()
    product = ProductHook()
    register_authz_product(cast(AuthzProduct, product))
    _register_audit_subjects(container, load_authz_product())
    assert container.resolve(AuditSubjects) is product


def test_no_product_registers_own_default() -> None:
    container = LoomContainer()
    _register_audit_subjects(container, load_authz_product())
    hook = container.resolve(AuditSubjects)
    result = asyncio.run(hook.audit_subjects(Identity(subject="alice"), StoreKey("ledger")))
    assert result is OWN


def test_synchronous_product_hook_is_refused() -> None:
    class SynchronousHook(ProductHook):
        def audit_subjects(  # pyright: ignore[reportIncompatibleMethodOverride]
            self, identity: Identity, store: StoreKey
        ) -> object:
            return ALL_SUBJECTS

    with pytest.raises(ConfigError, match="must be async"):
        _register_audit_subjects(LoomContainer(), cast(AuthzProduct, SynchronousHook()))


def test_composition_registers_default_audit_subjects() -> None:
    source = composition_source(
        ConfigContext.from_dict({}), app=AppConfig(name="audit", job_service="inline")
    )

    application = compose_application(source, Serving())

    assert isinstance(application.kernel.container.resolve(AuditSubjects), OwnSubjectsOnly)
