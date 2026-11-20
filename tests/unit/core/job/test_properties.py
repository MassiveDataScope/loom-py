"""Properties of the job retry policy and wire envelope over generated input."""

from __future__ import annotations

import msgspec
from hypothesis import given
from hypothesis import strategies as st

from loom.core.job.envelope import decode_job_envelope, encode_job_message
from loom.core.job.job import Job
from loom.core.job.policy import Backoff, Countdown, Seconds


class _AnyJob(Job[None]):
    def execute(self) -> None:
        return None


_wire_values = st.one_of(st.none(), st.booleans(), st.integers(), st.text(max_size=20))
_wire_mappings = st.dictionaries(st.text(max_size=12), _wire_values, max_size=6)


class TestCountdownProperties:
    @given(seconds=st.integers(min_value=-2000, max_value=2000))
    def test_the_rule_declared_in_the_type_and_the_check_agree(self, seconds: int) -> None:
        try:
            Countdown(seconds)
            checked = True
        except ValueError:
            checked = False
        try:
            msgspec.convert(seconds, Seconds)
            declared = True
        except msgspec.ValidationError:
            declared = False

        assert checked is declared


class TestBackoffProperties:
    @given(
        first=st.integers(min_value=0, max_value=1200),
        factor=st.integers(min_value=1, max_value=8),
        cap=st.integers(min_value=0, max_value=900),
        attempt=st.integers(min_value=1, max_value=30),
    )
    def test_unjittered_delay_is_exponential_and_capped(
        self, first: int, factor: int, cap: int, attempt: int
    ) -> None:
        backoff = Backoff(first_s=first, factor=factor, max_s=cap, jitter=False)

        assert backoff.delay(attempt).seconds == min(cap, first * factor ** (attempt - 1))


class TestEnvelopeProperties:
    @given(payload=_wire_mappings, params=_wire_mappings, trace_id=st.text(max_size=20))
    def test_wire_fields_survive_encoding(
        self, payload: dict[str, object], params: dict[str, object], trace_id: str
    ) -> None:
        body = encode_job_message(
            _AnyJob, payload=payload, params=params, identity=None, trace_id=trace_id
        )

        envelope = decode_job_envelope(body.decode())

        assert (envelope.payload, envelope.params, envelope.trace_id) == (payload, params, trace_id)
