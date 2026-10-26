"""Bytewax source runtime for MongoDB CDC."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Protocol, cast

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from pymongo.errors import OperationFailure

from loom.core.connections.mongo import mongo_island
from loom.core.model.redaction import reveal_for_driver
from loom.streaming.compiler._plan import CompiledMongoCDCSource
from loom.streaming.core._message import Message
from loom.streaming.mongo._event import MongoCDCEvent
from loom.streaming.mongo._normalize import build_mongo_cdc_message

_logger = logging.getLogger(__name__)
_MAX_BATCH_SIZE = 500
_MAX_RESTART_ATTEMPTS = 5


class _ChangeStream(Protocol):
    """Minimal interface for a PyMongo change stream cursor."""

    def try_next(self) -> object | None:
        """Return the next available change document, or None if none is ready."""
        ...

    def close(self) -> None:
        """Release resources held by the change stream."""
        ...


class _DatabaseLike(Protocol):
    """Minimal interface for a PyMongo client, database, or collection.

    Covers the subset of the PyMongo API used at runtime:
    - ``__getitem__`` to access databases or collections by name.
    - ``watch`` to open a change stream.
    - ``close`` to release the underlying connection.
    """

    def __getitem__(self, name: str) -> _DatabaseLike:
        """Return a child database or collection by name."""
        ...

    def watch(
        self,
        pipeline: list[dict[str, object]] | None = None,
        **kwargs: object,
    ) -> _ChangeStream:
        """Open and return a change stream on this client, database, or collection."""
        ...

    def close(self) -> None:
        """Close the underlying connection."""
        ...


class MongoCDCPartition(StatefulSourcePartition[Message[MongoCDCEvent], dict[str, object] | None]):
    """Single MongoDB change-stream partition backed by PyMongo."""

    def __init__(
        self,
        source: CompiledMongoCDCSource,
        resume_state: dict[str, object] | None,
    ) -> None:
        self._source = source
        self._resume_token = resume_state
        self._restart_count: int = 0
        self._skip_count: int = 0
        self._client: _DatabaseLike = _build_mongo_client(source)
        self._stream: _ChangeStream = _open_change_stream(self._client, source, resume_state)

    def next_batch(self) -> list[Message[MongoCDCEvent]]:
        """Poll up to _MAX_BATCH_SIZE change events when available."""
        messages: list[Message[MongoCDCEvent]] = []
        while len(messages) < _MAX_BATCH_SIZE:
            try:
                change = self._stream.try_next()
            except OperationFailure as exc:
                if not _should_restart_from_now(self._source, exc):
                    raise
                change = self._restart_from_now()
            except Exception as exc:
                _logger.warning(
                    "mongo_cdc_event_skipped resume_token=%s error_type=%s reason=%s",
                    self._resume_token,
                    type(exc).__name__,
                    str(exc),
                )
                self._skip_count += 1
                break
            if change is None:
                break
            if not isinstance(change, Mapping):
                raise TypeError("Mongo change stream yielded a non-mapping event.")
            try:
                message = build_mongo_cdc_message(change)
            except Exception as exc:
                _logger.warning(
                    "mongo_cdc_message_build_skipped resume_token=%s error_type=%s reason=%s",
                    self._resume_token,
                    type(exc).__name__,
                    str(exc),
                )
                self._skip_count += 1
                continue
            self._resume_token = message.payload.resume_token
            messages.append(message)
        return messages

    def snapshot(self) -> dict[str, object] | None:
        """Return the current Mongo resume token."""
        return self._resume_token

    def close(self) -> None:
        self._stream.close()
        self._client.close()

    def _restart_from_now(self) -> object | None:
        self._restart_count += 1
        _logger.error(
            "Mongo CDC oplog expired; restarting from now (attempt %d/%d)",
            self._restart_count,
            _MAX_RESTART_ATTEMPTS,
        )
        if self._restart_count > _MAX_RESTART_ATTEMPTS:
            raise RuntimeError(
                f"MongoCDC stream restarted {self._restart_count} times due to oplog expiry;"
                " manual intervention required"
            )
        self._stream.close()
        self._resume_token = None
        self._stream = _open_change_stream(self._client, self._source, None)
        return self._stream.try_next()


class MongoCDCSource(FixedPartitionedSource[Message[MongoCDCEvent], dict[str, object] | None]):
    """Bytewax source wrapping a MongoDB change stream."""

    _PARTITION_KEY = "mongo_cdc"

    def __init__(self, source: CompiledMongoCDCSource) -> None:
        self._source = source

    def list_parts(self) -> list[str]:
        """Return the fixed Mongo CDC partition key."""
        return [self._PARTITION_KEY]

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: dict[str, object] | None,
    ) -> MongoCDCPartition:
        """Build the single Mongo CDC source partition."""
        del step_id
        if for_part != self._PARTITION_KEY:
            raise KeyError(f"Unknown Mongo CDC partition {for_part!r}.")
        return MongoCDCPartition(self._source, resume_state)


def _build_mongo_client(source: CompiledMongoCDCSource) -> _DatabaseLike:
    """Build a PyMongo client for one compiled Mongo source.

    Raises:
        MissingExtraError: When the ``mongo`` extra is not installed.
    """
    client = mongo_island().build_mongo_client(source.settings, datetime_conversion="DATETIME_AUTO")
    return cast(_DatabaseLike, client)


def _open_change_stream(
    client: _DatabaseLike,
    source: CompiledMongoCDCSource,
    resume_state: dict[str, object] | None,
) -> _ChangeStream:
    """Open a Mongo change stream for one compiled source."""
    database = client[source.settings.database]
    watch_options = _resolve_watch_options(source.watch_options, resume_state)
    pipeline = _build_pipeline(source.collections, source.watch_options)
    if len(source.collections) == 1:
        return database[source.collections[0]].watch(pipeline, **watch_options)
    return database.watch(pipeline, **watch_options)


def _build_pipeline(
    collections: tuple[str, ...],
    watch_options: Mapping[str, object],
) -> list[dict[str, object]]:
    """Build the Mongo change-stream pipeline without mutating watch options."""
    stages: list[dict[str, object]] = []
    if len(collections) > 1:
        stages.append({"$match": {"ns.coll": {"$in": list(collections)}}})
    extra_pipeline = watch_options.get("pipeline")
    if isinstance(extra_pipeline, list):
        stages.extend(stage for stage in extra_pipeline if isinstance(stage, dict))
    return stages


def _resolve_watch_options(
    watch_options: Mapping[str, object],
    resume_state: dict[str, object] | None,
) -> dict[str, object]:
    """Resolve watch kwargs for one Mongo change stream open."""
    revealed = cast("dict[str, object]", reveal_for_driver(watch_options))
    resolved = {key: value for key, value in revealed.items() if key != "pipeline"}
    if resume_state is not None:
        resolved["resume_after"] = resume_state
    return resolved


def _should_restart_from_now(source: CompiledMongoCDCSource, exc: Exception) -> bool:
    if source.settings.on_oplog_expired != "restart_from_now":
        return False
    return _is_oplog_expired_error(exc)


def _is_oplog_expired_error(exc: Exception) -> bool:
    code = getattr(exc, "code", None)
    details = getattr(exc, "details", None)
    code_name = details.get("codeName") if isinstance(details, Mapping) else None
    if code == 286 or code_name == "ChangeStreamHistoryLost":
        return True
    message = str(exc).lower()
    return (
        "changestreamhistorylost" in message
        or "resume point may no longer be in the oplog" in message
        or "resume of change stream was not possible" in message
    )


__all__ = ["MongoCDCPartition", "MongoCDCSource"]
