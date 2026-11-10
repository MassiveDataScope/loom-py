"""Store-keyed constructor injection through UseCaseFactory."""

from __future__ import annotations

from typing import Annotated, Any

import pytest

from loom.core.config import ConfigError
from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.persistence import stores
from loom.core.persistence.stores import StoreKey, store_binding
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase


class Reader:
    pass


class ArchiveRead(UseCase[Any, str]):
    def __init__(self, reader: Reader) -> None:
        self.reader = reader

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class DefaultRead(UseCase[Any, str]):
    def __init__(self, reader: Reader) -> None:
        self.reader = reader

    async def execute(self, **kwargs: Any) -> str:
        return "ok"


def test_annotated_dependency_uses_named_store_binding() -> None:
    ArchiveRead.__init__.__annotations__["reader"] = Annotated[
        Reader, stores.InStore(StoreKey("archive"))
    ]
    container = LoomContainer()
    default_reader = Reader()
    archive_reader = Reader()
    container.register_instance(Reader, default_reader)
    container.register_instance(store_binding(Reader, StoreKey("archive")), archive_reader)

    factory = UseCaseFactory(container)
    factory.register(ArchiveRead)
    factory.verify()

    assert factory.build(ArchiveRead).reader is archive_reader


def test_annotated_default_store_uses_plain_binding() -> None:
    DefaultRead.__init__.__annotations__["reader"] = Annotated[
        Reader, stores.InStore(StoreKey("default"))
    ]
    container = LoomContainer()
    reader = Reader()
    container.register_instance(Reader, reader)

    factory = UseCaseFactory(container)
    factory.register(DefaultRead)
    factory.verify()

    assert factory.build(DefaultRead).reader is reader


def test_verify_names_missing_store_binding() -> None:
    ArchiveRead.__init__.__annotations__["reader"] = Annotated[
        Reader, stores.InStore(StoreKey("archive"))
    ]
    factory = UseCaseFactory(LoomContainer())
    factory.register(ArchiveRead)

    with pytest.raises(ResolutionError, match=r"ArchiveRead injects reader: .*archive"):
        factory.verify()


def test_store_key_accepts_valid_name() -> None:
    assert stores.store_key("archive_2") == StoreKey("archive_2")


@pytest.mark.parametrize("invalid", ["", "2archive", "archive-store", "é", 42, None])
def test_store_key_rejects_invalid_name(invalid: object) -> None:
    with pytest.raises(ConfigError, match="not a store key"):
        stores.store_key(invalid)
