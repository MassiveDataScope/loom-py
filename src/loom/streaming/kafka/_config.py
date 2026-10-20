"""Typed Kafka configuration contracts."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal, TypeAlias

import msgspec

from loom.core.config import ConfigResolver
from loom.core.config.overrides import load_application_config
from loom.core.model import LoomFrozenStruct
from loom.core.routing import DefaultingRouteResolver, LogicalRef

KafkaConfigValue: TypeAlias = str | int | float | bool


class KafkaSecuritySettings(
    LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True
):
    """Optional Kafka security settings.

    Attributes:
        protocol: Kafka security protocol.
        sasl_mechanism: Optional SASL mechanism.
        sasl_username: Optional SASL username.
        sasl_password: Optional SASL password.
        ssl_ca_location: Optional CA file path.
    """

    protocol: Literal["PLAINTEXT", "SSL", "SASL_SSL", "SASL_PLAINTEXT"]
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    ssl_ca_location: str | None = None

    def to_confluent_config(self) -> dict[str, KafkaConfigValue]:
        """Compile security settings to Confluent-compatible keys.

        Returns:
            String-keyed Confluent security configuration mapping.
        """

        return _without_none(
            {
                "security.protocol": self.protocol,
                "sasl.mechanism": self.sasl_mechanism,
                "sasl.username": self.sasl_username,
                "sasl.password": self.sasl_password,
                "ssl.ca.location": self.ssl_ca_location,
            }
        )


class ProducerSettings(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Typed Kafka producer settings.

    Attributes:
        brokers: Kafka broker addresses.
        client_id: Producer client identifier.
        topic: Optional default physical topic for a logical output.
        security: Optional security configuration.
        extra: Optional extra Confluent settings.
    """

    brokers: tuple[str, ...]
    client_id: str | None = None
    topic: str | None = None
    security: KafkaSecuritySettings | None = None
    extra: dict[str, KafkaConfigValue] = msgspec.field(default_factory=dict)

    def to_confluent_config(self) -> dict[str, KafkaConfigValue]:
        """Compile settings to a Confluent-compatible config mapping.

        Returns:
            String-keyed Confluent configuration mapping.

        Raises:
            ValueError: If ``extra`` redefines a typed configuration key.
        """

        managed = {
            "bootstrap.servers": _broker_list(self.brokers),
            **_without_none({"client.id": self.client_id}),
            **_security_config(self.security),
        }
        return _merge_extra_config(managed, self.extra)


class ConsumerSettings(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Typed Kafka consumer settings.

    Attributes:
        brokers: Kafka broker addresses.
        group_id: Consumer group identifier.
        topics: Topics to subscribe to.
        auto_offset_reset: Offset reset policy.
        poll_timeout_ms: Maximum milliseconds to block waiting for a message on
            each poll call.  Higher values reduce CPU usage when the topic is
            idle; lower values decrease end-to-end latency.  Defaults to 100.
        batch_size: Maximum records returned by one batch consume call.
            Defaults to 500.
        poll_backoff_ms: Milliseconds to back off after an empty batch consume
            before polling again.  Defaults to 50.
        commit_keepalive_ms: Maximum milliseconds between offset commits per
            partition.  Member-less consumer groups expire committed offsets
            after ``offsets.retention.minutes`` (default 7 days), so idle
            partitions re-commit their watermark periodically to refresh
            retention.  Defaults to 30 minutes.
        delivery: Explicit delivery semantics.  ``None`` (default) keeps the
            legacy resolution derived from ``enable_auto_commit``.
        enable_auto_commit: Whether Kafka should auto-commit offsets.
            Deprecated in favor of ``delivery`` but honored for the whole 1.x
            line.  ``None`` means unset.
        security: Optional security configuration.
        extra: Optional extra Confluent settings.
    """

    brokers: tuple[str, ...]
    group_id: str
    topics: tuple[str, ...]
    auto_offset_reset: Literal["earliest", "latest"] = "earliest"
    poll_timeout_ms: int = 100
    batch_size: int = 500
    poll_backoff_ms: int = 50
    commit_keepalive_ms: int = 1_800_000
    delivery: Literal["at_least_once", "at_most_once"] | None = None
    enable_auto_commit: bool | None = None
    security: KafkaSecuritySettings | None = None
    extra: dict[str, KafkaConfigValue] = msgspec.field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate batch consumption parameters.

        Raises:
            ValueError: If ``batch_size`` or ``poll_backoff_ms`` is not
                greater than zero.
        """
        if self.batch_size < 1:
            raise ValueError("ConsumerSettings.batch_size must be greater than zero.")
        if self.poll_backoff_ms < 1:
            raise ValueError("ConsumerSettings.poll_backoff_ms must be greater than zero.")
        if self.commit_keepalive_ms < 1:
            raise ValueError("ConsumerSettings.commit_keepalive_ms must be greater than zero.")

    def effective_delivery(self) -> Literal["at_least_once", "at_most_once"]:
        """Resolve the effective delivery semantics.

        Returns:
            ``delivery`` when set explicitly; otherwise the legacy resolution
            from ``enable_auto_commit``: ``False`` maps to ``"at_least_once"``
            while ``True`` or unset maps to ``"at_most_once"``.
        """
        if self.delivery is not None:
            return self.delivery
        if self.enable_auto_commit is False:
            return "at_least_once"
        return "at_most_once"

    def to_confluent_config(self) -> dict[str, KafkaConfigValue]:
        """Compile settings to a Confluent-compatible config mapping.

        ``enable.auto.commit`` is derived from :meth:`effective_delivery`:
        ``"at_most_once"`` maps to ``True`` and ``"at_least_once"`` to
        ``False``.

        Returns:
            String-keyed Confluent configuration mapping.

        Raises:
            ValueError: If ``extra`` redefines a typed configuration key.
        """

        managed = {
            "bootstrap.servers": _broker_list(self.brokers),
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.effective_delivery() == "at_most_once",
            **_security_config(self.security),
        }
        return _merge_extra_config(managed, self.extra)


class KafkaSettings(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Typed Kafka settings loaded from a YAML config section.

    Args:
        producer: Optional producer settings.
        consumer: Optional consumer settings.
        producers: Named producer settings keyed by logical output reference.
        consumers: Named consumer settings keyed by logical input reference.
    """

    producer: ProducerSettings | None = None
    consumer: ConsumerSettings | None = None
    producers: dict[str, ProducerSettings] = msgspec.field(default_factory=dict)
    consumers: dict[str, ConsumerSettings] = msgspec.field(default_factory=dict)

    def producer_for(self, ref: str | LogicalRef) -> ProducerSettings:
        """Resolve producer settings by logical reference with default fallback.

        Args:
            ref: Logical output reference.

        Returns:
            Specific producer settings when present, otherwise the common
            producer settings.

        Raises:
            KeyError: If neither a specific nor common producer is configured.
        """

        resolver = DefaultingRouteResolver(
            default=self.producer,
            overrides=self.producers,
            kind="Kafka producer config",
        )
        return resolver.resolve(ref)

    def consumer_for(self, ref: str | LogicalRef) -> ConsumerSettings:
        """Resolve consumer settings by logical reference with default fallback.

        Args:
            ref: Logical input reference.

        Returns:
            Specific consumer settings when present, otherwise the common
            consumer settings.

        Raises:
            KeyError: If neither a specific nor common consumer is configured.
        """

        resolver = DefaultingRouteResolver(
            default=self.consumer,
            overrides=self.consumers,
            kind="Kafka consumer config",
        )
        return resolver.resolve(ref)


def resolve_producer_topic(ref: str | LogicalRef, settings: ProducerSettings) -> str:
    """Resolve physical producer topic from settings or logical fallback.

    Args:
        ref: Logical output reference used as fallback only when the config
            does not define ``topic``.
        settings: Resolved producer settings.

    Returns:
        Physical topic name.
    """

    logical_ref = ref.ref if isinstance(ref, LogicalRef) else ref
    return settings.topic or logical_ref


def resolve_consumer_topics(
    ref: str | LogicalRef,
    settings: ConsumerSettings,
) -> tuple[str, ...]:
    """Resolve physical consumer topics from settings or logical fallback.

    Args:
        ref: Logical input reference used as fallback only when the config does
            not define ``topics``.
        settings: Resolved consumer settings.

    Returns:
        Physical topic names.
    """

    if settings.topics:
        return settings.topics
    logical_ref = ref.ref if isinstance(ref, LogicalRef) else ref
    return (logical_ref,)


def load_kafka_settings(
    *config_files: str,
    section_name: str = "kafka",
    resolvers: Sequence[ConfigResolver] = (),
) -> KafkaSettings:
    """Load Kafka settings from YAML as an application's configuration.

    The ``LOOM__*`` variables merge last (see :mod:`loom.core.config.overrides`).

    Args:
        *config_files: One or more local paths or cloud URIs.
        section_name: Dot-separated config section path. Defaults to
            ``"kafka"``.
        resolvers: Optional core config resolvers for custom placeholders.

    Returns:
        Validated Kafka settings.

    Raises:
        loom.core.config.ConfigError: If files cannot be loaded, the section
            is missing, or the section fails validation.
    """

    return load_application_config(*config_files, resolvers=resolvers).section(
        section_name, KafkaSettings
    )


def _merge_extra_config(
    config: dict[str, KafkaConfigValue],
    extra: dict[str, KafkaConfigValue],
) -> dict[str, KafkaConfigValue]:
    """Merge extra Kafka config without allowing silent key override.

    Args:
        config: Base typed configuration.
        extra: Extra user-provided configuration values.

    Raises:
        ValueError: If ``extra`` redefines a typed configuration key.
    """

    duplicate_keys = set(config).intersection(extra)
    if duplicate_keys:
        ordered_keys = ", ".join(sorted(duplicate_keys))
        raise ValueError(f"extra contains keys already managed by typed settings: {ordered_keys}")
    return {**config, **extra}


def _broker_list(brokers: tuple[str, ...]) -> str:
    """Return Confluent bootstrap server string."""
    return ",".join(brokers)


def _security_config(security: KafkaSecuritySettings | None) -> dict[str, KafkaConfigValue]:
    """Return security config when configured."""
    if security is None:
        return {}
    return security.to_confluent_config()


def _without_none(
    values: dict[str, KafkaConfigValue | None],
) -> dict[str, KafkaConfigValue]:
    """Return a copy without unset optional values."""
    return {key: value for key, value in values.items() if value is not None}
