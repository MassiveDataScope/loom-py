"""Authored agent artifacts: Tier-1 structs, envelope decoding and loading.

This package is the only supported entry point for artifacts a human or a
generator writes. It is import-light on purpose: no optional extra is imported
at module level, so ``import loom.ai.declarative`` works on a base install even
though YAML artifacts need PyYAML.

Example:
    >>> specs = load_specs(["agents/*.agent.yaml"])
    >>> [decoded.spec.name for decoded in specs]
    ['triage']
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Final, TypeVar

from loom.ai._paths import escapes, is_within
from loom.ai.errors import (
    AgentCompilationError,
    AgentCompilationIssue,
    agent_specs_escape_root,
    spec_malformed,
)

from ._envelope import (
    LATEST_SPEC_VERSION,
    SUPPORTED_SPEC_VERSIONS,
    AgentSpec,
    DecodedSpec,
    decode_artifact,
    decode_spec,
)
from ._schema import SCHEMA_FILENAMES, agent_spec_json_schema, agent_spec_schema_path
from ._v1 import (
    DEPS_TYPE_PATTERN,
    INSTRUCTION_NAME_PATTERN,
    NATIVE_TOOLS,
    A2ACapability,
    AgentSpecV1,
    CapabilitySpec,
    ConversationSpec,
    InstructionBlock,
    JsonSchemaOutput,
    McpCapability,
    NativeCapability,
    NativeToolName,
    OutputHookSpec,
    OutputSpec,
    PolicySpec,
    PythonCapability,
    SkillsCapability,
    SqlCapability,
    TypeRefOutput,
    UsecaseCapability,
)

__all__ = [
    "DEPS_TYPE_PATTERN",
    "INSTRUCTION_NAME_PATTERN",
    "NATIVE_TOOLS",
    "SCHEMA_FILENAMES",
    "SUPPORTED_SPEC_VERSIONS",
    "LATEST_SPEC_VERSION",
    "A2ACapability",
    "AgentSpec",
    "AgentSpecV1",
    "CapabilitySpec",
    "ConversationSpec",
    "DecodedSpec",
    "InstructionBlock",
    "JsonSchemaOutput",
    "McpCapability",
    "NativeCapability",
    "NativeToolName",
    "OutputHookSpec",
    "OutputSpec",
    "PolicySpec",
    "PythonCapability",
    "SkillsCapability",
    "SqlCapability",
    "TypeRefOutput",
    "UsecaseCapability",
    "agent_spec_json_schema",
    "agent_spec_schema_path",
    "decode_spec",
    "load_specs",
]

_T = TypeVar("_T")

_YAML_MISSING_HINT: Final[str] = (
    "PyYAML is required to load .yaml agent artifacts: install it with "
    "'pip install pyyaml', or write the artifact as JSON"
)


def _decode_yaml_payload(buf: bytes, *, type: type[_T]) -> _T:
    # Lazy import: PyYAML is not a base dependency, and importing it at module
    # level would break ``import loom.ai.declarative`` on a bare install.
    from msgspec import yaml as msgspec_yaml

    return msgspec_yaml.decode(buf, type=type)


def _load_yaml(data: bytes, source: str) -> DecodedSpec:
    return decode_artifact(
        data,
        _decode_yaml_payload,
        source=source,
        versions=SUPPORTED_SPEC_VERSIONS,
    )


def _load_json(data: bytes, source: str) -> DecodedSpec:
    return decode_spec(data, source=source)


_LOADERS: Final[Mapping[str, Callable[[bytes, str], DecodedSpec]]] = MappingProxyType(
    {
        ".yaml": _load_yaml,
        ".yml": _load_yaml,
        ".json": _load_json,
    }
)


def _glob(pattern: str, root: Path) -> tuple[set[Path], list[AgentCompilationIssue]]:
    if escapes(pattern):
        return set(), [agent_specs_escape_root(pattern)]
    matches = {path for path in root.glob(pattern) if path.is_file()}
    if not all(is_within(path, root) for path in matches):
        return set(), [agent_specs_escape_root(pattern)]
    return matches, []


def _resolve_paths(patterns: Sequence[str], root: Path) -> tuple[Path, ...]:
    matches: set[Path] = set()
    issues: list[AgentCompilationIssue] = []
    for pattern in patterns:
        found, pattern_issues = _glob(pattern, root)
        matches.update(found)
        issues.extend(pattern_issues)
    if issues:
        raise AgentCompilationError(issues)
    return tuple(sorted(matches))


def _load_one(path: Path) -> DecodedSpec:
    source = str(path)
    loader = _LOADERS.get(path.suffix.lower())
    if loader is None:
        raise AgentCompilationError(
            [spec_malformed(source, f"unsupported artifact extension '{path.suffix}'")]
        )
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise AgentCompilationError(
            [spec_malformed(source, f"cannot read artifact: {exc}")]
        ) from exc
    try:
        return loader(data, source)
    except ImportError as exc:
        raise AgentCompilationError([spec_malformed(source, _YAML_MISSING_HINT)]) from exc


def load_specs(
    patterns: Sequence[str],
    root: Path | str = ".",
) -> tuple[DecodedSpec, ...]:
    """Load every artifact matching a set of globs.

    Patterns are resolved relative to ``root`` and may not leave it: an
    absolute pattern, a ``..`` segment or a match whose resolved path lies
    outside ``root`` is refused before any artifact is read. Matches are
    de-duplicated across patterns and returned sorted by path, so the result
    is deterministic.
    ``.yaml``/``.yml`` files decode as YAML, ``.json`` files as JSON, and any
    other extension is a failure.

    Failures from every file are accumulated and reported once, so a broken
    artifact does not hide the ones after it. Non-fatal findings ride on each
    returned :class:`DecodedSpec`. Duplicate agent names are *not* checked here:
    that is a later compilation phase over the whole application.

    Args:
        patterns: Glob patterns, relative to ``root``.
        root:     Directory the patterns are resolved against.

    Returns:
        One decoded artifact per matched file, ordered by path.

    Raises:
        AgentCompilationError: With ``AGENT_SPECS_ESCAPE_ROOT`` for every
            pattern that leaves ``root``; otherwise aggregating every fatal
            issue found across all matched files.

    Example:
        >>> load_specs(["*.agent.yaml"], root="agents")
        (DecodedSpec(spec=AgentSpecV1(...), issues=()),)
    """
    decoded: list[DecodedSpec] = []
    issues: list[AgentCompilationIssue] = []
    for path in _resolve_paths(patterns, Path(root)):
        try:
            decoded.append(_load_one(path))
        except AgentCompilationError as exc:
            issues.extend(exc.issues)
    if issues:
        raise AgentCompilationError(issues)
    return tuple(decoded)
