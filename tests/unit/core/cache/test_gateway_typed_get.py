"""``CacheGateway.get_value``/``multi_get_values`` route through ``LoomType`` now.

Both reads used to call ``msgspec.convert`` directly; they now resolve the
requested type through ``loom_type_of(type).from_builtins`` instead, which is
what lets a pydantic model be requested from the cache exactly like a
``msgspec.Struct`` always could. A non-strict ``msgspec.Struct`` — the shape
the index payloads use — keeps working because ``loom_type_of`` never checks
strictness.
"""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.core.cache import CacheConfig, CacheGateway
from loom.core.model.redaction import RedactedMap

from ._doubles import SERIALIZED_BACKEND

pydantic = pytest.importorskip("pydantic")


class StrictModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    name: str


class LaxStruct(msgspec.Struct):
    """Non-strict struct, the shape a cached index payload is read back as."""

    name: str


def _gateway(alias: str, backend: dict[str, Any]) -> CacheGateway:
    CacheGateway.apply_config(
        CacheConfig(aiocache_alias=alias, aiocache_config=RedactedMap({alias: backend}))
    )
    return CacheGateway(alias=alias)


class TestGetValue:
    async def test_a_strict_pydantic_model_round_trips(self) -> None:
        gateway = _gateway("typed-get-pydantic", SERIALIZED_BACKEND)
        await gateway.set_value("k", {"name": "a"})

        value = await gateway.get_value("k", type=StrictModel)

        assert value == StrictModel(name="a")

    async def test_a_non_strict_struct_round_trips(self) -> None:
        gateway = _gateway("typed-get-lax-struct", SERIALIZED_BACKEND)
        await gateway.set_value("k", {"name": "a"})

        value = await gateway.get_value("k", type=LaxStruct)

        assert value == LaxStruct(name="a")


class TestMultiGetValues:
    async def test_pydantic_values_round_trip_with_a_miss_kept_as_none(self) -> None:
        gateway = _gateway("typed-multi-get-pydantic", SERIALIZED_BACKEND)
        await gateway.multi_set_values([("a", {"name": "x"}), ("b", {"name": "y"})])

        values = await gateway.multi_get_values(["a", "b", "missing"], type=StrictModel)

        assert values == [StrictModel(name="x"), StrictModel(name="y"), None]
