"""A module or override may not add or replace a framework-owned key.

The check compares each reserved key's binding before and after the module
runs, so a key bound earlier and left alone passes.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.composition import Application
from loom.composition._reserved import refusing_reserved
from loom.composition.errors import ReservedBindingError
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.sql import SqlQueryService
from tests.unit.composition.pipeline.components import binds_sql_service

Module = Callable[[LoomContainer], None]


def _defaults_sql_service(container: LoomContainer) -> None:
    container.register_default(SqlQueryService, object, scope=Scope.APPLICATION)


def _leaves_everything(container: LoomContainer) -> None:
    del container


def _binds_observability(container: LoomContainer) -> None:
    container.register(ObservabilityRuntime, ObservabilityRuntime.noop, scope=Scope.APPLICATION)


def _run(module: Module, container: LoomContainer) -> None:
    (refusing,) = refusing_reserved([module], (DEFAULT_STORE,))
    refusing(container)


def _sql_bound() -> LoomContainer:
    container = LoomContainer()
    container.register(SqlQueryService, object, scope=Scope.APPLICATION)
    return container


def test_a_module_adding_a_reserved_key_is_refused_with_its_message() -> None:
    with pytest.raises(ReservedBindingError) as caught:
        _run(binds_sql_service, LoomContainer())

    assert str(caught.value) == str(
        ReservedBindingError.for_key(binds_sql_service, SqlQueryService)
    )


def test_a_module_replacing_a_reserved_key_is_refused() -> None:
    with pytest.raises(ReservedBindingError, match="SqlQueryService"):
        _run(binds_sql_service, _sql_bound())


@pytest.mark.parametrize("module", [_leaves_everything, _defaults_sql_service])
def test_a_module_leaving_a_bound_reserved_key_alone_passes(module: Module) -> None:
    container = _sql_bound()
    before = container.bindings()[SqlQueryService]

    _run(module, container)

    assert container.bindings()[SqlQueryService] is before


def test_an_override_of_the_sql_service_is_refused_unwrapped(
    overridden: Callable[..., Application],
) -> None:
    with pytest.raises(ReservedBindingError) as caught:
        overridden(binds_sql_service)

    assert type(caught.value) is ReservedBindingError
    assert str(caught.value) == str(
        ReservedBindingError.for_key(binds_sql_service, SqlQueryService)
    )


def test_a_module_replacing_a_seeded_reserved_key_is_refused(
    overridden: Callable[..., Application],
) -> None:
    with pytest.raises(ReservedBindingError) as caught:
        overridden(modules=(_binds_observability,))

    assert str(caught.value) == str(
        ReservedBindingError.for_key(_binds_observability, ObservabilityRuntime)
    )
