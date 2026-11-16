"""Unit tests for UnitOfWork and UnitOfWorkFactory protocols."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock

import pytest

from loom.core.stores.uow.abc import UnitOfWork, UnitOfWorkFactory

# ---------------------------------------------------------------------------
# Fake implementations (satisfy protocol structurally)
# ---------------------------------------------------------------------------


class FakeUoW:
    """Minimal UnitOfWork implementation for protocol verification."""

    is_transactional: ClassVar[bool] = True

    async def begin(self) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...

    async def __aenter__(self) -> FakeUoW:
        await self.begin()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.commit()


class FakeUoWFactory:
    def create(self) -> FakeUoW:
        return FakeUoW()


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestUnitOfWorkProtocols:
    def test_fake_uow_satisfies_protocol(self) -> None:
        assert isinstance(FakeUoW(), UnitOfWork)

    def test_fake_factory_satisfies_protocol(self) -> None:
        assert isinstance(FakeUoWFactory(), UnitOfWorkFactory)

    def test_mock_uow_satisfies_protocol(self) -> None:
        mock = MagicMock(spec=UnitOfWork)
        mock.begin = AsyncMock()
        mock.commit = AsyncMock()
        mock.rollback = AsyncMock()
        mock.__aenter__ = AsyncMock(return_value=mock)
        mock.__aexit__ = AsyncMock(return_value=None)
        mock.is_transactional = True
        assert isinstance(mock, UnitOfWork)

    def test_factory_create_returns_uow(self) -> None:
        factory = FakeUoWFactory()
        uow = factory.create()
        assert isinstance(uow, UnitOfWork)

    def test_factory_create_returns_new_instance_each_time(self) -> None:
        factory = FakeUoWFactory()
        uow1 = factory.create()
        uow2 = factory.create()
        assert uow1 is not uow2

    @pytest.mark.asyncio
    async def test_uow_context_manager_commit_on_success(self) -> None:
        committed = False
        rolled_back = False

        class TrackingUoW:
            is_transactional: ClassVar[bool] = True

            async def begin(self) -> None: ...

            async def commit(self) -> None:
                nonlocal committed
                committed = True

            async def rollback(self) -> None:
                nonlocal rolled_back
                rolled_back = True

            async def __aenter__(self) -> TrackingUoW:
                await self.begin()
                return self

            async def __aexit__(self, *args: object) -> None:
                if args[0] is None:
                    await self.commit()
                else:
                    await self.rollback()

        async with TrackingUoW():
            pass

        assert committed is True
        assert rolled_back is False

    @pytest.mark.asyncio
    async def test_uow_context_manager_rollback_on_exception(self) -> None:
        rolled_back = False

        class TrackingUoW:
            is_transactional: ClassVar[bool] = True

            async def begin(self) -> None: ...
            async def commit(self) -> None: ...

            async def rollback(self) -> None:
                nonlocal rolled_back
                rolled_back = True

            async def __aenter__(self) -> TrackingUoW:
                return self

            async def __aexit__(self, exc_type: Any, *args: Any) -> None:
                if exc_type is not None:
                    await self.rollback()

        uow = TrackingUoW()
        with pytest.raises(ValueError):
            async with uow:
                raise ValueError("boom")

        assert rolled_back is True
