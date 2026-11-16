"""The repositories of every store, looked up by model or by table name."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, TypeVar, cast

from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel
from loom.core.model.introspection import get_table_name
from loom.core.stores.catalog import StoreKey
from loom.core.stores.repository.capabilities import RepoFor

ModelT = TypeVar("ModelT", bound=BaseModel)


class RepositoryCatalog:
    """Resolves the repository bound for a model, or for a table name, at run time.

    Inject the repository itself when the model is known when the component
    is written; use the catalog only when the model is chosen at run time.

    Args:
        container: Where the repositories are bound.
        models: The models each store serves.
    """

    def __init__(
        self,
        container: LoomContainer,
        models: Mapping[StoreKey, Sequence[type[BaseModel]]],
    ) -> None:
        self._container = container
        self._stores = tuple(models)
        self._tables: dict[str, dict[StoreKey, type[BaseModel]]] = {}
        for store, served in models.items():
            for model in served:
                self._tables.setdefault(get_table_name(model), {})[store] = model

    def __getitem__(self, model: type[ModelT]) -> RepoFor[ModelT]:
        """Return the repository bound for *model*.

        Args:
            model: A model a store serves.

        Returns:
            The repository the container serves for *model*, an override
            included.

        Raises:
            KeyError: When no store serves *model*.
        """
        if not self._container.has_repo_mapping(model):
            raise KeyError(f"No store serves {model.__qualname__}")
        return cast(RepoFor[ModelT], self._container.resolve_repo(model))

    def for_table(self, table: str, *, store: StoreKey | None = None) -> RepoFor[Any]:
        """Return the repository of the model stored in *table*.

        Args:
            table: The ``__tablename__`` of a served model.
            store: The store to look in; without it every store is searched.

        Returns:
            The repository bound for the model.

        Raises:
            KeyError: When no searched store serves a model of *table*.
            ValueError: When *store* is omitted and several stores serve *table*.
        """
        by_store = self._tables.get(table, {})
        if store is None:
            searched = self._stores
            if len(by_store) > 1:
                raise ValueError(
                    f"Table {table!r} is served by stores {', '.join(by_store)}; pass store="
                )
            model = next(iter(by_store.values()), None)
        else:
            searched = (store,)
            model = by_store.get(store)
        if model is None:
            raise KeyError(f"No model stored in table {table!r}; searched {', '.join(searched)}")
        return self[model]
