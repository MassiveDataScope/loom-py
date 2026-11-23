"""Factory that maps the YAML ``notifications:`` block to Notifier instances."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from loom.connections.aws import AwsClients
from loom.core.errors import InvalidValue
from loom.core.model.redaction import Dsn
from loom.prefect.notify._port import Notifier
from loom.prefect.notify._slack import SlackNotifier
from loom.prefect.notify._sns import SnsNotifier


def _build_slack(cfg: dict[str, Any]) -> Notifier | None:
    webhook = str(cfg.get("webhook_url") or "").strip()
    if not webhook:
        return None
    return SlackNotifier(
        webhook_url=Dsn(webhook),
        on_failure=bool(cfg.get("on_failure", True)),
        on_completion=bool(cfg.get("on_completion", False)),
        channel=cfg.get("channel"),
    )


def _build_sns(
    cfg: dict[str, Any], aws: Mapping[str, Any] | None = None, clients: AwsClients | None = None
) -> Notifier | None:
    topic_arn = (cfg.get("topic_arn") or "").strip()
    if not topic_arn:
        return None
    return SnsNotifier(
        topic_arn=topic_arn,
        on_failure=bool(cfg.get("on_failure", True)),
        on_completion=bool(cfg.get("on_completion", False)),
        aws=aws,
        clients=clients,
    )


_BUILDERS: dict[str, Callable[[dict[str, Any]], Notifier | None]] = {
    "slack": _build_slack,
    "sns": _build_sns,
}


def build_notifiers(
    block: Iterable[dict[str, Any]] | None,
    *,
    aws: Mapping[str, Any] | None = None,
    clients: AwsClients | None = None,
) -> tuple[Notifier, ...]:
    """Parse the YAML ``notifications:`` list into Notifier instances.

    Args:
        block: List of dicts from the YAML. ``None`` or empty returns ``()``.
        aws: The ``aws:`` section of the YAML, which the built-in ``sns``
            notifier's client inherits.
        clients: The AWS clients every ``sns`` notifier takes its client from;
            one set is shared by the notifiers of the block when none is given.

    Returns:
        Tuple of constructed notifiers.

    Raises:
        ValueError: When an entry omits ``kind`` or references an unknown
            notifier kind.
    """
    if not block:
        return ()
    clients = clients or AwsClients()
    out: list[Notifier] = []
    for entry in block:
        kind = entry.get("kind")
        if not kind:
            raise InvalidValue("notifications: every entry must declare 'kind'")
        builder = _BUILDERS.get(kind)
        if builder is None:
            raise InvalidValue(
                f"notifications: unknown notifier kind. Available: {sorted(_BUILDERS)}"
            )
        instance = _build_sns(entry, aws, clients) if builder is _build_sns else builder(entry)
        if instance is not None:
            out.append(instance)
    return tuple(out)


def register_notifier(kind: str, builder: Callable[[dict[str, Any]], Notifier | None]) -> None:
    """Register a new notifier kind for ``build_notifiers``.

    Args:
        kind: Identifier used in YAML ``kind:`` field.
        builder: Callable that turns a config dict into a Notifier.
    """
    _BUILDERS[kind] = builder


__all__ = ["build_notifiers", "register_notifier"]
