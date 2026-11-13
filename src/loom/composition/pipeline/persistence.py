"""Build store wiring and resources before kernel registration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from loom.composition.recipe import Recipe
from loom.composition.resources import Resource, ResourceKind
from loom.composition.source import CompositionSource
from loom.core.config import ConfigError, ConfigKey
from loom.core.persistence import BackendRuntime, PersistenceWiring
from loom.core.persistence.stores import DEFAULT_STORE, DEFAULT_STORE_PATH, StoreKey
from loom.core.repository.abc import CursorCodec
from loom.core.uow.abc import UnitOfWorkFactory

if TYPE_CHECKING:
    from loom.core.config import ConfigContext


@dataclass(frozen=True, slots=True, kw_only=True)
class Persisted:
    wirings: dict[StoreKey, PersistenceWiring]
    store_resources: tuple[Resource, ...]

    @property
    def factories(self) -> dict[StoreKey, UnitOfWorkFactory]:
        """The unit-of-work factory of every store that has one."""
        return {key: w.uow_factory for key, w in self.wirings.items() if w.uow_factory is not None}


def persist(source: CompositionSource, recipe: Recipe) -> Persisted:
    ctx = source.config
    discovered = recipe.discovered
    catalog = source.catalog
    stores = catalog.models_by_store(discovered.models)
    backends = {key: source.backend(key, default=recipe.default_backend) for key in stores}
    default_label = backends[DEFAULT_STORE].name
    if default_label in stores:
        raise ConfigError(
            f"{ConfigKey.STORES}.{default_label} would report its readiness under the "
            f"key of {DEFAULT_STORE_PATH}, named after its backend; rename the store"
        )
    cursor_codec = _cursor_codec(ctx, recipe)
    wirings: dict[StoreKey, PersistenceWiring] = {}
    store_resources: list[Resource] = []
    for key, models in stores.items():
        runtime = BackendRuntime(
            recipe.profile,
            pool_metrics=recipe.pool_metrics,
            connection_only=recipe.connection_only,
            catalog=catalog,
            store=key,
            cursor_codec=cursor_codec,
        )
        wiring = wirings[key] = backends[key].build(ctx, models, runtime)
        wiring.prepare_models(models)
        label = default_label if key == DEFAULT_STORE else key
        store_resources.append(_store_resource(key, wiring, label=label))
    return Persisted(wirings=wirings, store_resources=tuple(store_resources))


def _cursor_codec(ctx: ConfigContext, recipe: Recipe) -> CursorCodec | None:
    """The application's one cursor codec, shared by every store; none without repositories.

    Built before any store: an invalid ``persistence.cursors`` fails with no
    engine or client built.
    """
    if recipe.connection_only or not recipe.discovered.models:
        return None
    return CursorCodec.from_context(ctx)


def _store_resource(key: StoreKey, wiring: PersistenceWiring, *, label: str) -> Resource:
    """The resource of store *key*, whose readiness is reported under *label*.

    The default store keeps its backend's name as the label: ``/health`` has
    always reported it under that key.
    """
    return Resource(
        kind=ResourceKind.STORE,
        name=key,
        manager=wiring.lifespan_init,
        check=wiring.startup_checks,
        readiness=wiring.readiness,
        readiness_key=label,
    )
