"""``state`` decode, its own cap and the normalised mapping (T304, T305).

``_decode_state`` is the seam ``ai/fastapi/endpoints.py`` parses a request's
raw ``state`` bytes through, exactly once, against one agent's declared
shape (FR-008). These tests exercise it directly, without a live ASGI
client, because the seam is a plain function with no transport dependency.
"""

from __future__ import annotations

import msgspec
import pytest
from pydantic import BaseModel, ConfigDict

from loom.ai._transport import TransportError
from loom.ai.abc import StateShape
from loom.ai.fastapi.endpoints import _decode_state
from loom.core.model import loom_type, msgspec_type

_AGENT = "appraiser"


class _StateWithDefault(msgspec.Struct, forbid_unknown_fields=True):
    """State shape carrying one required field and one declared default."""

    marca: str
    km: int = 0


_SCHEMA_SHAPE = StateShape(schema={"type": "object"}, loom_type=msgspec_type(_StateWithDefault))
_OPEN_SHAPE = StateShape(schema=None, loom_type=None)


class _PydanticStateWithDefault(BaseModel):
    """Pydantic analogue of ``_StateWithDefault``."""

    model_config = ConfigDict(extra="forbid")

    marca: str
    km: int = 0


_PYDANTIC_SCHEMA_SHAPE = StateShape(
    schema={"type": "object"}, loom_type=loom_type(_PydanticStateWithDefault)
)


def _raw(payload: bytes) -> msgspec.Raw:
    """Wrap *payload* as ``msgspec.Raw`` without validating it as JSON.

    Mirrors what ``_AgentRunRequest.state`` actually holds: the shared
    request decoder stops at the field without parsing it, so a value this
    function passes through may be syntactically invalid JSON — exactly the
    case :func:`_decode_state` must still catch.
    """
    return msgspec.Raw(payload)


class TestNoStateGiven:
    def test_returns_none_regardless_of_the_declared_shape(self) -> None:
        assert _decode_state(_AGENT, _SCHEMA_SHAPE, msgspec.Raw()) is None
        assert _decode_state(_AGENT, None, msgspec.Raw()) is None

    def test_an_explicit_json_null_is_also_no_state_given(self) -> None:
        """``_AgentRunRequest`` cannot tell an omitted field from an explicit ``null``."""
        assert _decode_state(_AGENT, _SCHEMA_SHAPE, _raw(b"null")) is None


class TestDecodedStateAgainstNoDeclaredShape:
    def test_refuses_with_422_naming_the_agent(self) -> None:
        payload = _raw(b'{"marca": "civic"}')
        with pytest.raises(TransportError) as excinfo:
            _decode_state(_AGENT, None, payload)

        assert excinfo.value.status_code == 422
        assert excinfo.value.code == "STATE_NOT_DECLARED"
        assert _AGENT in excinfo.value.message


class TestStateAgainstASchemaShape:
    """Shared behaviour of a declared schema shape, regardless of its library."""

    @pytest.fixture(params=[_SCHEMA_SHAPE, _PYDANTIC_SCHEMA_SHAPE], ids=["msgspec", "pydantic"])
    def shape(self, request: pytest.FixtureRequest) -> StateShape:
        return request.param  # type: ignore[no-any-return]

    def test_decodes_and_normalises_the_declared_defaults(self, shape: StateShape) -> None:
        """FR-009, AC-011: the omitted 'km' reaches the mapping as its declared default.

        The measurement this pins (from 'What the spike measured'): a payload
        omitting a defaulted field renders as the empty value from the
        caller's own raw mapping ('marca=civic, km=') and as the declared
        default from the normalised one ('marca=civic, km=0'). This is the
        test that would have caught the raw-mapping design.
        """
        state = _decode_state(_AGENT, shape, _raw(b'{"marca": "civic"}'))

        assert state == {"marca": "civic", "km": 0}

    def test_malformed_bytes_are_refused_with_422_before_any_provider_call(
        self, shape: StateShape
    ) -> None:
        payload = _raw(b"not json")
        with pytest.raises(TransportError) as excinfo:
            _decode_state(_AGENT, shape, payload)

        assert excinfo.value.status_code == 422
        assert excinfo.value.code == "INVALID_STATE"

    def test_an_undeclared_field_is_refused_not_dropped(self, shape: StateShape) -> None:
        """The compiled decoder forbids unknown fields, matching the output side (FR-017)."""
        payload = _raw(b'{"marca": "civic", "extra": 1}')
        with pytest.raises(TransportError) as excinfo:
            _decode_state(_AGENT, shape, payload)

        assert excinfo.value.status_code == 422
        assert excinfo.value.code == "INVALID_STATE"

    def test_a_present_but_incomplete_msgspec_payload_is_refused_not_a_raw_crash(self) -> None:
        """A required field missing from a present payload is ``ValidationError``.

        msgspec-specific: ``msgspec.ValidationError`` subclasses
        ``msgspec.DecodeError``, which this function already catches, so a
        payload present but missing ``marca`` is refused the same coded way
        as malformed bytes rather than raising uncaught.
        """
        payload = _raw(b"{}")
        with pytest.raises(TransportError) as excinfo:
            _decode_state(_AGENT, _SCHEMA_SHAPE, payload)

        assert excinfo.value.status_code == 422
        assert excinfo.value.code == "INVALID_STATE"


class TestStateAgainstTheOpenDictShape:
    def test_decodes_a_plain_mapping_with_no_defaults_to_apply(self) -> None:
        state = _decode_state(_AGENT, _OPEN_SHAPE, _raw(b'{"anything": "goes"}'))

        assert state == {"anything": "goes"}

    def test_malformed_bytes_are_refused_with_422(self) -> None:
        payload = _raw(b"not json")
        with pytest.raises(TransportError) as excinfo:
            _decode_state(_AGENT, _OPEN_SHAPE, payload)

        assert excinfo.value.status_code == 422
        assert excinfo.value.code == "INVALID_STATE"
