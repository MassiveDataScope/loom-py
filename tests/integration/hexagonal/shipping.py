"""Acme shipping's application layer: its ports, its use case and the job running it.

``TrackShipment`` names two ports and nothing else: where a shipment's status
comes from (``ShipmentQueries``) and who words the customer notice
(``CustomerNotifier``). Nothing here knows which transport calls it or what
implements a port. ``TrackShipment`` reads no repository, so its model
parameter is ``Any``: the first parameter of ``UseCase`` names the main
repository's model, not the input.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import msgspec

from loom.core.job.job import Job
from loom.core.use_case import UseCase
from loom.core.use_case.invoker import ApplicationInvoker


class ShipmentReport(msgspec.Struct, frozen=True):
    """Where a shipment is, and the notice its customer receives."""

    shipment_id: str
    status: str
    notice: str


class ShipmentQueries(ABC):
    """Reads the status of a shipment."""

    @abstractmethod
    async def status_of(self, shipment_id: str) -> str:
        """Return where *shipment_id* is."""


class CustomerNotifier(ABC):
    """Words the notice a customer receives about a shipment."""

    @abstractmethod
    async def notice_for(self, shipment_id: str, status: str) -> str:
        """Return the notice telling the customer that *shipment_id* is *status*."""


class TrackShipment(UseCase[Any, ShipmentReport]):
    """Report where a shipment is, with the notice for its customer."""

    def __init__(self, queries: ShipmentQueries, notifier: CustomerNotifier) -> None:
        self._queries = queries
        self._notifier = notifier

    async def execute(self, shipment_id: str) -> ShipmentReport:
        status = await self._queries.status_of(shipment_id)
        notice = await self._notifier.notice_for(shipment_id, status)
        return ShipmentReport(shipment_id=shipment_id, status=status, notice=notice)


class TrackShipmentJob(Job[str]):
    """Track a shipment from a queue, as the caller who enqueued it."""

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self, shipment_id: str) -> str:
        report: ShipmentReport = await self._app.invoke(
            TrackShipment, params={"shipment_id": shipment_id}
        )
        return report.notice
