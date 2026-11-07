"""The person an AWS event names travels as ``loom:on_behalf_of``, never what a publisher wrote."""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.core.config import ConfigError
from loom.core.identity import ON_BEHALF_OF_ATTRIBUTE
from loom.serverless.aws.lambda_.functions import (
    EventBridgeTrigger,
    S3Trigger,
    _check_event_trigger,
)
from loom.serverless.aws.lambda_.handler import _event_reader
from loom.serverless.aws.lambda_.triggers import eventbridge, s3

PRINCIPAL = "AWS:AIDAEXAMPLE"
ARN = "arn:aws:iam::123456789012:user/ana"
IDENTITY = {"subject": "svc:importer", "roles": ["importer"]}


def _s3_event(**record: Any) -> dict[str, Any]:
    return {
        "Records": [
            {
                "eventName": "ObjectCreated:Put",
                "eventTime": "2026-11-07T10:00:00Z",
                "s3": {"bucket": {"name": "raw"}, "object": {"key": "in/a.csv", "size": 3}},
                **record,
            }
        ]
    }


def _attributes(
    trigger: Any, decoders: dict[str, Any], event: dict[str, Any], *, direct: bool = True
) -> dict[str, str]:
    read = _event_reader("importer", trigger, decoders, trusts_requester=direct)
    ((envelope, _),) = read(event)
    assert envelope.identity is not None
    assert envelope.identity["subject"] == "svc:importer"
    return dict(envelope.identity.get("attributes") or {})


def test_an_s3_uploader_is_the_person_the_system_identity_acts_for() -> None:
    trigger = msgspec.convert({"job": "Import", "identity": IDENTITY}, S3Trigger)
    event = _s3_event(userIdentity={"principalId": PRINCIPAL})

    attributes = _attributes(trigger, {"s3": s3.decode}, event)

    assert attributes[ON_BEHALF_OF_ATTRIBUTE] == PRINCIPAL


def test_a_queued_event_names_nobody_whatever_its_body_claims() -> None:
    trigger = msgspec.convert({"job": "Import", "identity": IDENTITY}, S3Trigger)
    forged = _s3_event(userIdentity={"principalId": PRINCIPAL})

    attributes = _attributes(trigger, {"s3": s3.decode}, forged, direct=False)

    assert ON_BEHALF_OF_ATTRIBUTE not in attributes


def test_an_s3_record_without_uploader_delegates_for_nobody() -> None:
    trigger = msgspec.convert({"job": "Import", "identity": IDENTITY}, S3Trigger)

    attributes = _attributes(trigger, {"s3": s3.decode}, _s3_event())

    assert ON_BEHALF_OF_ATTRIBUTE not in attributes


def test_an_aws_service_event_names_the_caller_aws_recorded() -> None:
    trigger = msgspec.convert({"job": "Import", "identity": IDENTITY}, EventBridgeTrigger)
    event = {"id": "e-1", "source": "aws.s3", "detail": {"userIdentity": {"arn": ARN}}}

    attributes = _attributes(trigger, {"eventbridge": eventbridge.decode}, event)

    assert attributes[ON_BEHALF_OF_ATTRIBUTE] == ARN


def test_a_custom_event_cannot_claim_a_person() -> None:
    trigger = msgspec.convert({"job": "Import", "identity": IDENTITY}, EventBridgeTrigger)
    event = {"id": "e-1", "source": "orders", "detail": {"userIdentity": {"arn": ARN}}}

    attributes = _attributes(trigger, {"eventbridge": eventbridge.decode}, event)

    assert ON_BEHALF_OF_ATTRIBUTE not in attributes


@pytest.mark.parametrize("name", [ON_BEHALF_OF_ATTRIBUTE, "loom:anything"])
def test_configuration_cannot_write_a_reserved_attribute(name: str) -> None:
    identity = {**IDENTITY, "attributes": {name: "ana"}}
    trigger = msgspec.convert({"job": "Import", "identity": identity}, S3Trigger)

    with pytest.raises(ConfigError, match="loom:"):
        _check_event_trigger("lambda.functions.importer.trigger", trigger)
