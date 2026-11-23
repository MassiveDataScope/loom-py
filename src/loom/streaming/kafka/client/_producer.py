"""Raw Kafka producer backed by confluent-kafka."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Literal

from confluent_kafka import Message as _RawMessage
from confluent_kafka import Producer as _Producer

from loom.core.observability.event import LifecycleEvent, Scope
from loom.core.observability.observability import Observability
from loom.streaming.kafka._config import ProducerSettings
from loom.streaming.kafka._errors import KafkaDeliveryError
from loom.streaming.kafka._message import HEADER_CORRELATION_ID, HEADER_TRACE_ID
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka.client._protocol import DeliveryCallback


class KafkaProducerClient:
    """Confluent-backed raw Kafka producer.

    Sends ``KafkaRecord[bytes]`` to Kafka. All values must already be
    serialized to bytes before calling :meth:`send`.

    Args:
        settings: Typed producer settings.
        delivery_callback: Optional callback notified on delivery success
            or failure.
        observer: Optional observability observer.
    """

    def __init__(
        self,
        settings: ProducerSettings,
        delivery_callback: DeliveryCallback | None = None,
        obs: Observability | None = None,
    ) -> None:
        self._producer = _Producer(settings.to_confluent_config())
        self._delivery_callback = delivery_callback
        self._obs = obs
        self._pending_delivery_error: KafkaDeliveryError | None = None
        self._delivery_error_lock = threading.Lock()

    def send(self, record: KafkaRecord[bytes]) -> None:
        """Produce one raw byte record.

        Args:
            record: Kafka record with a ``bytes`` value.

        Raises:
            KafkaDeliveryError: If Kafka rejects the produce call.
        """
        headers: list[tuple[str, str | bytes | None]] | None = (
            list(record.headers.items()) if record.headers else None
        )
        callback = self._build_delivery_callback(record)
        try:
            if record.timestamp_ms is None:
                self._producer.produce(
                    topic=record.topic,
                    key=_serialize_key(record.key),
                    value=record.value,
                    headers=headers,
                    on_delivery=callback,
                )
            else:
                self._producer.produce(
                    topic=record.topic,
                    key=_serialize_key(record.key),
                    value=record.value,
                    headers=headers,
                    timestamp=record.timestamp_ms,
                    on_delivery=callback,
                )
            self._producer.poll(0.0)
        except Exception as exc:
            error = KafkaDeliveryError(str(exc))
            if self._obs is not None:
                self._obs.emit(
                    LifecycleEvent.exception(
                        scope=Scope.TRANSPORT,
                        name="kafka_produce",
                        trace_id=_header_trace_id(record.headers),
                        correlation_id=_header_correlation_id(record.headers),
                        error=exc,
                        meta={"topic": record.topic},
                    )
                )
            _notify_delivery(self._delivery_callback, record, error)
            raise error from exc

    def flush(self, timeout_ms: int | None = None) -> None:
        """Flush pending records and materialize delivery failures.

        Args:
            timeout_ms: Optional maximum flush wait in milliseconds.

        Raises:
            KafkaDeliveryError: If flush fails or a pending delivery error
                exists. Pending delivery errors are consumed when raised, so a
                later ``flush`` call will not raise the same error again.
        """
        try:
            if timeout_ms is None:
                self._producer.flush()
            else:
                self._producer.flush(timeout_ms / 1000)
        except Exception as exc:
            raise KafkaDeliveryError(str(exc)) from exc
        self._raise_pending_delivery_error()

    def close(self) -> None:
        """Flush and close the producer.

        Raises:
            KafkaDeliveryError: If pending delivery failures remain.
        """
        self.flush()

    def __enter__(self) -> KafkaProducerClient:
        """Return self for context-manager usage."""
        return self

    def __exit__(self, *exc: object) -> Literal[False]:
        """Flush and close the producer on context exit."""
        try:
            self.close()
        except Exception:
            if exc[0] is None:
                raise
        return False

    def _build_delivery_callback(
        self,
        record: KafkaRecord[bytes],
    ) -> Callable[[object | None, _RawMessage | None], None]:
        def _callback(error: object | None, _: _RawMessage | None) -> None:
            delivery_error = None if error is None else KafkaDeliveryError(str(error))
            if delivery_error is not None:
                with self._delivery_error_lock:
                    self._pending_delivery_error = delivery_error
            if self._obs is not None:
                if delivery_error is None:
                    self._obs.emit(
                        LifecycleEvent.end(
                            scope=Scope.TRANSPORT,
                            name="kafka_produce",
                            trace_id=_header_trace_id(record.headers),
                            correlation_id=_header_correlation_id(record.headers),
                            meta={"topic": record.topic},
                        )
                    )
                else:
                    self._obs.emit(
                        LifecycleEvent.exception(
                            scope=Scope.TRANSPORT,
                            name="kafka_produce",
                            trace_id=_header_trace_id(record.headers),
                            correlation_id=_header_correlation_id(record.headers),
                            error=delivery_error,
                            meta={"topic": record.topic},
                        )
                    )
            _notify_delivery(self._delivery_callback, record, delivery_error)

        return _callback

    def _raise_pending_delivery_error(self) -> None:
        with self._delivery_error_lock:
            error = self._pending_delivery_error
            if error is None:
                return
            self._pending_delivery_error = None
        raise error


def _notify_delivery(
    callback: DeliveryCallback | None,
    record: KafkaRecord[bytes],
    error: KafkaDeliveryError | None,
) -> None:
    if callback is not None:
        callback(record, error)


def _serialize_key(key: bytes | str | None) -> bytes | None:
    if key is None:
        return None
    if isinstance(key, bytes):
        return key
    return key.encode("utf-8")


def _header_trace_id(headers: dict[str, bytes]) -> str | None:
    raw = headers.get(HEADER_TRACE_ID)
    return raw.decode() if raw is not None else None


def _header_correlation_id(headers: dict[str, bytes]) -> str | None:
    raw = headers.get(HEADER_CORRELATION_ID)
    return raw.decode() if raw is not None else None
