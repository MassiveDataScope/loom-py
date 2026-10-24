"""Model binding value type shared by the compiler and the engine.

:class:`InferenceTarget` lives in its own module rather than in
``loom.ai.config`` because it is a value type of the pillar: the compiler
embeds it in the plan and the engine consumes it, so it is not a
config-parsing detail.

Secret containment (data-model invariant 4, FR-018): ``credentials_ref`` and
``options`` never leave the process in clear text.  The mechanism is
fail-closed — on construction both values are rewrapped into types msgspec
refuses to encode, so ``msgspec.json.encode`` (and ``msgspec.to_builtins``,
``msgspec.msgpack.encode``) raise ``TypeError`` instead of leaking them, and
``repr``/``str`` redact them.  Rejecting the encode was chosen over silent
redaction because a plan that reaches a wire encoder with a secret reference
aboard is a bug worth surfacing, not smoothing over.  Decoding through the
config loader is unaffected: ``msgspec.convert`` builds the struct from plain
values and ``__post_init__`` wraps them afterwards.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any, Final, Literal, get_args

from msgspec import field, structs

from loom.core.model import LoomFrozenStruct
from loom.core.model.redaction import Dsn

OutputMode = Literal["tool", "native"]
"""How the engine asks the model for the structured answer.

``prompted`` is deliberately absent: the engine strips markdown fences before
validating a prompted answer while loom decodes the raw text part, so a fenced
answer would pass the engine and fail loom.

This is the single source of truth for the set: adding a member here makes
every exhaustive dispatch over it (``_spec.build_output_type``) fail type
checking until it handles the new mode, instead of degrading to a default at
run time.
"""

OUTPUT_MODES: Final[tuple[OutputMode, ...]] = get_args(OutputMode)
"""Values ``InferenceTarget.output_mode`` accepts, derived from :data:`OutputMode`."""

_REDACTED: Final = "<redacted>"
"""Placeholder shown in place of a secret-bearing value (FR-018)."""


class _RedactedRef(str):
    """Secret reference that redacts itself and refuses msgspec encoding.

    A ``str`` subclass behaves as the reference everywhere in Python, while
    msgspec rejects encoding non-exact ``str`` types, which is exactly the
    fail-closed behaviour invariant 4 requires.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return _REDACTED


class _RedactedOptions(Mapping[str, Any]):
    """Options mapping that redacts itself and refuses msgspec encoding.

    msgspec encodes ``dict`` but not arbitrary mappings, so wrapping the
    decoded ``dict`` makes any direct encode of the struct raise instead of
    leaking vendor settings.
    """

    __slots__ = ("_raw",)

    def __init__(self, raw: Mapping[str, Any]) -> None:
        self._raw: dict[str, Any] = dict(raw)

    def __getitem__(self, key: str) -> Any:
        return self._raw[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._raw)

    def __len__(self) -> int:
        return len(self._raw)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _RedactedOptions):
            return self._raw == other._raw
        if isinstance(other, Mapping):
            return self._raw == dict(other)
        return NotImplemented

    def __repr__(self) -> str:
        return _REDACTED


class InferenceTarget(LoomFrozenStruct, frozen=True, kw_only=True):
    """One resolved model binding for a model role (``ai.models.<role>``).

    ``repr``/``str`` show ``provider``, ``model``, ``region``, ``endpoint``,
    ``output_mode`` and ``streaming``, but never the values of
    ``credentials_ref`` or ``options`` — the plan
    carries this struct, so an unredacted repr in a start-up traceback is the
    concrete leak path.  Encoding the struct with msgspec raises when either
    secret-bearing field is set (see the module docstring for the rationale).

    Attributes:
        provider: Provider identifier (``bedrock``, ``openai``, ...).
        model: Vendor model id.
        region: Region for regional providers such as Bedrock.
        endpoint: Gateway or compatible endpoint URL.
        output_mode: How the engine asks the model for the structured
            answer (``tool`` or ``native``, see :data:`OutputMode`).
            ``None`` leaves the choice to the engine.  Typed ``str`` rather
            than :data:`OutputMode` because msgspec validates a ``Literal``
            during the decode, before ``__post_init__``: an unknown value
            would surface as a raw ``ValidationError`` instead of the
            ``OUTPUT_MODE_UNKNOWN`` issue naming the role.  The set is
            enforced by ``loom.ai.config._validate_model_binding``.
        streaming: Whether the engine asks the provider for its answers as a
            stream. ``False`` asks for each answer whole instead -- for a
            model whose streamed tool calls arrive damaged -- and the
            model's text then reaches the caller as one delta per text part
            rather than many. ``AgentEngine.run`` has always been served
            whole; only its streamed entry points change with this flag.
        credentials_ref: Reference resolved by the existing secrets resolver.
            Never a literal secret (FR-018).
        options: Vendor-specific settings.  Confined here; never reaches the
            artifact.
    """

    provider: str
    model: str
    region: str | None = None
    endpoint: Dsn | None = None
    output_mode: str | None = None
    streaming: bool = True
    credentials_ref: str | None = None
    options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.credentials_ref is not None:
            structs.force_setattr(self, "credentials_ref", _RedactedRef(self.credentials_ref))
        if self.options:
            structs.force_setattr(self, "options", _RedactedOptions(self.options))

    def __repr__(self) -> str:
        credentials = _REDACTED if self.credentials_ref is not None else None
        options = _REDACTED if self.options else "{}"
        return (
            f"InferenceTarget(provider={self.provider!r},"
            f" model={self.model!r},"
            f" region={self.region!r},"
            f" endpoint={self.endpoint!r},"
            f" output_mode={self.output_mode!r},"
            f" streaming={self.streaming!r},"
            f" credentials_ref={credentials},"
            f" options={options})"
        )

    __str__ = __repr__
