"""Kafka transport contracts for Loom streaming."""

from loom.streaming.kafka._codec import KafkaCodec, MsgspecCodec
from loom.streaming.kafka._config import (
    ConsumerSettings,
    KafkaConfigValue,
    KafkaSecuritySettings,
    KafkaSettings,
    ProducerSettings,
    load_kafka_settings,
    resolve_consumer_topics,
    resolve_producer_topic,
)
from loom.streaming.kafka._errors import (
    KafkaClientError,
    KafkaCommitError,
    KafkaConfigurationError,
    KafkaConsumerError,
    KafkaDeliveryError,
    KafkaDeserializationError,
    KafkaPollError,
    KafkaProducerError,
    KafkaSerializationError,
)
from loom.streaming.kafka._key_resolver import FixedKey, PartitionKeyResolver, PreserveKey
from loom.streaming.kafka._message import (
    ContentType,
    MessageDescriptor,
    MessageEnvelope,
    MessageMetadata,
    SchemaRef,
    build_message,
)
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka._wire import (
    DecodeError,
    DecodeOk,
    DecodeResult,
    envelope_to_message,
    try_decode_multi_record,
    try_decode_record,
)
from loom.streaming.kafka.client._consumer import KafkaConsumerClient
from loom.streaming.kafka.client._producer import KafkaProducerClient
from loom.streaming.kafka.client._protocol import KafkaConsumer, KafkaProducer
from loom.streaming.kafka.message._consumer import KafkaMessageConsumer
from loom.streaming.kafka.message._producer import KafkaMessageProducer
from loom.streaming.kafka.message._protocol import MessageConsumer, MessageProducer

__all__ = [
    "ContentType",
    "ConsumerSettings",
    "DecodeError",
    "DecodeOk",
    "DecodeResult",
    "FixedKey",
    "KafkaClientError",
    "KafkaCodec",
    "KafkaCommitError",
    "KafkaConfigValue",
    "KafkaConfigurationError",
    "KafkaConsumer",
    "KafkaConsumerClient",
    "KafkaConsumerError",
    "KafkaDeliveryError",
    "KafkaDeserializationError",
    "KafkaMessageConsumer",
    "KafkaMessageProducer",
    "KafkaPollError",
    "KafkaProducer",
    "KafkaProducerClient",
    "KafkaProducerError",
    "KafkaRecord",
    "KafkaSecuritySettings",
    "KafkaSettings",
    "KafkaSerializationError",
    "MessageConsumer",
    "MessageDescriptor",
    "MessageEnvelope",
    "MessageMetadata",
    "MessageProducer",
    "MsgspecCodec",
    "PartitionKeyResolver",
    "PreserveKey",
    "ProducerSettings",
    "resolve_consumer_topics",
    "resolve_producer_topic",
    "SchemaRef",
    "build_message",
    "envelope_to_message",
    "load_kafka_settings",
    "try_decode_multi_record",
    "try_decode_record",
]
