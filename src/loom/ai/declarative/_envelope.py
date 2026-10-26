"""Two-phase decoding of authored agent artifacts.

Phase 1 reads *only* the envelope — ``spec_version`` — with a permissive struct.
Phase 2 decodes the whole payload with the struct registered for that version,
strictly, so an unknown field is a failure rather than a dropped value.

Splitting the two phases is what makes forward compatibility diagnosable: an
artifact written for a future version is reported as
:data:`~loom.ai.errors.AgentErrorCode.SPEC_VERSION_UNSUPPORTED` instead of
producing a pile of unrelated field errors.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Final, Protocol, TypeVar

import msgspec

from loom.ai.errors import (
    AgentCompilationError,
    AgentCompilationIssue,
    AgentErrorCode,
)

from ._v1 import AgentSpecV1

AgentSpec = AgentSpecV1
"""Artifact type produced by the currently supported spec versions."""

LATEST_SPEC_VERSION: Final[int] = 1
"""Spec version this release writes and considers current."""

SUPPORTED_SPEC_VERSIONS: Final[Mapping[int, type[AgentSpec]]] = MappingProxyType({1: AgentSpecV1})
"""Registry mapping each readable spec version to its artifact struct."""

ANONYMOUS_SOURCE: Final[str] = "<bytes>"
"""Origin reported for artifacts decoded from bytes with no file behind them."""

_T = TypeVar("_T")


class _PayloadDecoder(Protocol):
    """Decodes artifact bytes of one serialization format into a typed struct."""

    def __call__(self, buf: bytes, *, type: type[_T]) -> _T: ...


class DecodedSpec(msgspec.Struct, frozen=True, kw_only=True):
    """One successfully decoded artifact and its non-fatal findings.

    Args:
        spec:        The decoded artifact.
        issues:      Non-fatal issues raised while decoding, such as a
            deprecation notice for a superseded but still readable spec version.
        source_path: File the artifact was read from, when there is one. A
            ``./`` skill library resolves against this path, so an artifact
            decoded from bare bytes cannot use one.
    """

    spec: AgentSpec
    issues: tuple[AgentCompilationIssue, ...] = ()
    source_path: str | None = None


class _Envelope(msgspec.Struct, frozen=True, kw_only=True):
    """Phase-1 view of an artifact: the version field and nothing else."""

    spec_version: int | None = None


_FIELD_PATH_RE: Final[re.Pattern[str]] = re.compile(r" - at `\$\.?(.+)`$")
_UNKNOWN_FIELD_RE: Final[re.Pattern[str]] = re.compile(r"unknown field `([^`]+)`")

_IssueBuilder = Callable[[str, str, str | None], AgentCompilationIssue]


def _unknown_field_issue(component: str, message: str, field: str | None) -> AgentCompilationIssue:
    match = _UNKNOWN_FIELD_RE.search(message)
    name = match.group(1) if match else (field or "<unknown>")
    return AgentErrorCode.SPEC_UNKNOWN_FIELD.issue(component=component, field=name)


def _agent_name_issue(component: str, message: str, field: str | None) -> AgentCompilationIssue:
    del field
    return AgentErrorCode.AGENT_NAME_INVALID.issue(component=component, reason=message)


def _malformed_issue(component: str, message: str, field: str | None) -> AgentCompilationIssue:
    issue = AgentErrorCode.SPEC_MALFORMED.issue(component=component, reason=message)
    return msgspec.structs.replace(issue, field=field)


_ISSUE_BUILDERS: Final[Mapping[AgentErrorCode, _IssueBuilder]] = MappingProxyType(
    {
        AgentErrorCode.SPEC_UNKNOWN_FIELD: _unknown_field_issue,
        AgentErrorCode.AGENT_NAME_INVALID: _agent_name_issue,
        AgentErrorCode.SPEC_MALFORMED: _malformed_issue,
    }
)

_FIELD_CODES: Final[Mapping[str, AgentErrorCode]] = MappingProxyType(
    {"name": AgentErrorCode.AGENT_NAME_INVALID}
)


def _field_path(message: str) -> str | None:
    match = _FIELD_PATH_RE.search(message)
    return match.group(1) if match else None


def _validation_code(message: str, field: str | None) -> AgentErrorCode:
    if _UNKNOWN_FIELD_RE.search(message):
        return AgentErrorCode.SPEC_UNKNOWN_FIELD
    return _FIELD_CODES.get(field or "", AgentErrorCode.SPEC_MALFORMED)


def _issue_from_validation_error(component: str, message: str) -> AgentCompilationIssue:
    field = _field_path(message)
    builder = _ISSUE_BUILDERS[_validation_code(message, field)]
    return builder(component, message, field)


def _envelope_version(data: bytes, decoder: _PayloadDecoder, source: str) -> int:
    try:
        envelope = decoder(data, type=_Envelope)
    except msgspec.ValidationError as exc:
        raise AgentCompilationError([_malformed_issue(source, str(exc), "spec_version")]) from exc
    except msgspec.DecodeError as exc:
        raise AgentCompilationError(
            [AgentErrorCode.SPEC_MALFORMED.issue(component=source, reason=str(exc))]
        ) from exc

    if envelope.spec_version is None:
        raise AgentCompilationError([AgentErrorCode.SPEC_VERSION_MISSING.issue(component=source)])
    return envelope.spec_version


def _version_issues(
    version: int,
    source: str,
    versions: Mapping[int, type[AgentSpec]],
) -> tuple[AgentCompilationIssue, ...]:
    latest = max(versions)
    if version not in versions:
        raise AgentCompilationError(
            [
                AgentErrorCode.SPEC_VERSION_UNSUPPORTED.issue(
                    component=source, found=version, supported=sorted(versions)
                )
            ]
        )
    issues = (
        [
            AgentErrorCode.SPEC_VERSION_DEPRECATED.issue(
                component=source, found=version, latest=latest
            )
        ]
        if version < latest
        else []
    )
    return tuple(issues)


def _decode_payload(
    data: bytes,
    decoder: _PayloadDecoder,
    struct: type[AgentSpec],
    source: str,
) -> AgentSpec:
    try:
        return decoder(data, type=struct)
    except msgspec.ValidationError as exc:
        raise AgentCompilationError([_issue_from_validation_error(source, str(exc))]) from exc
    except msgspec.DecodeError as exc:
        raise AgentCompilationError(
            [AgentErrorCode.SPEC_MALFORMED.issue(component=source, reason=str(exc))]
        ) from exc


def decode_artifact(
    data: bytes,
    decoder: _PayloadDecoder,
    *,
    source: str,
    versions: Mapping[int, type[AgentSpec]],
) -> DecodedSpec:
    """Run the two-phase decode over ``data`` using a serialization-specific decoder.

    Args:
        data:     Raw artifact bytes.
        decoder:  Callable decoding those bytes into a given struct type, such
            as ``msgspec.json.decode`` or ``msgspec.yaml.decode``.
        source:   Human-readable origin, reported as every issue's component
            and kept as the artifact's ``source_path`` unless it is
            :data:`ANONYMOUS_SOURCE`.
        versions: Registry of readable spec versions.

    Returns:
        The decoded artifact together with its non-fatal issues.

    Raises:
        AgentCompilationError: If the envelope or the payload is unusable.
    """
    version = _envelope_version(data, decoder, source)
    issues = _version_issues(version, source, versions)
    spec = _decode_payload(data, decoder, versions[version], source)
    path = source if source != ANONYMOUS_SOURCE else None
    return DecodedSpec(spec=spec, issues=issues, source_path=path)


def _decode_json_payload(buf: bytes, *, type: type[_T]) -> _T:
    return msgspec.json.decode(buf, type=type)


def decode_spec(
    data: bytes,
    *,
    source: str = ANONYMOUS_SOURCE,
    versions: Mapping[int, type[AgentSpec]] = SUPPORTED_SPEC_VERSIONS,
) -> DecodedSpec:
    """Decode JSON artifact bytes into the struct of the version they declare.

    Args:
        data:     Raw JSON bytes of a single artifact.
        source:   Human-readable origin used as every issue's component and,
            when it is a real path, as the artifact's ``source_path``.
        versions: Registry of readable spec versions. Overriding it is how the
            deprecation path is exercised without shipping a fictitious future
            version: a registry whose maximum key is above the artifact's
            version turns that artifact into a deprecated-but-readable one.

    Returns:
        The decoded artifact together with its non-fatal issues, such as a
        deprecation notice.

    Raises:
        AgentCompilationError: If ``spec_version`` is absent, unsupported, or
            the payload does not decode as the declared version.

    Example:
        >>> decoded = decode_spec(raw_bytes, source="agents/triage.agent.yaml")
        >>> decoded.spec.name
        'triage'
    """
    return decode_artifact(data, _decode_json_payload, source=source, versions=versions)
