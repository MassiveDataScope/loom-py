"""Regression corpus for authored agent artifacts of spec version 1.

Every fixture under ``fixtures/corpus_v1`` is an append-only piece of evidence that a
v1 artifact keeps decoding forever. These tests pin two independent guarantees for
each entry: it decodes through the public loader into a v1 struct, and it validates
against the schema Loom publishes for editors and generators
(``agent_spec_json_schema(1)``).

The committed ``contracts/agent-spec-v1.schema.json`` file is deliberately *not* used
here: it is behind the structs in two documented places, and validating the corpus
against it would fail for artifacts that are perfectly legal v1. That gap is the
subject of ``test_schema_emission.py``.

A coverage test closes the format: the corpus must exercise every capability kind, every
output kind and both skill-library forms, so a new kind cannot enter the format without a
fixture.

The layout is one directory per agent, named after the agent and holding its ``agent.yaml``
plus, when it packages skills, its own ``skills/`` library. A *shared* library lives under
``fixtures/skills_root`` and is what a bare ``library:`` name resolves against. Every
``SKILL.md`` shipped here is loaded through ``pydantic-ai-harness`` itself, so a manifest the
real loader rejects cannot sit in the corpus pretending to be evidence.
"""

from __future__ import annotations

import importlib.util
import warnings
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator

from loom.ai.declarative import AgentSpecV1, DecodedSpec, agent_spec_json_schema, load_specs

_HARNESS_MISSING = pytest.mark.skipif(
    importlib.util.find_spec("pydantic_ai_harness") is None,
    reason="pydantic-ai-harness pins its own pydantic-ai release; the floor job runs without it",
)

_CORPUS_DIR: Path = Path(__file__).parent / "fixtures" / "corpus_v1"
_CORPUS_PATTERN: str = "*/agent.yaml"

_CAPABILITY_KINDS: frozenset[str] = frozenset(
    {"usecase", "sql", "mcp", "skills", "python", "a2a", "native"}
)
_OUTPUT_KINDS: frozenset[str] = frozenset({"json_schema", "type_ref"})

_SKILLS_ROOT_DIR: Path = Path(__file__).parent / "fixtures" / "skills_root"
_SKILL_MANIFEST: str = "SKILL.md"
_LOCAL_LIBRARY_PREFIX: str = "./"


def _corpus_paths() -> tuple[Path, ...]:
    """List the corpus artifacts, sorted so parametrisation is deterministic."""
    return tuple(sorted(_CORPUS_DIR.glob(_CORPUS_PATTERN)))


def _corpus_ids() -> tuple[str, ...]:
    """Name each parametrised case after the agent directory holding it."""
    return tuple(path.parent.name for path in _corpus_paths())


def _load_corpus() -> tuple[DecodedSpec, ...]:
    """Decode the whole corpus through the public loader."""
    return load_specs([_CORPUS_PATTERN], _CORPUS_DIR)


def _read_artifact(path: Path) -> Any:
    """Read one artifact as the plain document a JSON Schema validator consumes."""
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _corpus_capabilities() -> tuple[tuple[Path, dict[str, Any]], ...]:
    """Pair every declared capability with the artifact declaring it."""
    return tuple(
        (path, capability)
        for path in _corpus_paths()
        for capability in _read_artifact(path).get("capabilities", ())
    )


def _declared_capability_kinds() -> set[str]:
    """Collect every capability kind declared anywhere in the corpus."""
    return {capability["kind"] for _, capability in _corpus_capabilities()}


def _declared_skill_libraries() -> set[str]:
    """Collect every ``skills.library`` value declared anywhere in the corpus."""
    return {
        capability["library"]
        for _, capability in _corpus_capabilities()
        if capability["kind"] == "skills"
    }


def _skill_libraries() -> tuple[Path, ...]:
    """List every skill-library directory the corpus ships, private and shared."""
    private = (path.parent / "skills" for path in _corpus_paths())
    shared = _SKILLS_ROOT_DIR.iterdir() if _SKILLS_ROOT_DIR.is_dir() else iter(())
    return tuple(sorted(path for path in (*private, *shared) if path.is_dir()))


def _skill_library_ids() -> tuple[str, ...]:
    """Name each library case after the directory holding it."""
    return tuple(f"{path.parent.name}/{path.name}" for path in _skill_libraries())


def _declared_output_kinds() -> set[str]:
    """Collect every output kind declared anywhere in the corpus."""
    return {_read_artifact(path)["output"]["kind"] for path in _corpus_paths()}


def test_the_corpus_is_not_empty_once_the_pattern_resolves() -> None:
    """A silently empty corpus would make every per-file test vacuously green."""
    assert _corpus_paths() != ()


@pytest.mark.parametrize("path", _corpus_paths(), ids=_corpus_ids())
def test_the_artifact_decodes(path: Path) -> None:
    """Each corpus entry decodes through ``load_specs`` into the v1 struct."""
    decoded = load_specs([path.relative_to(_CORPUS_DIR).as_posix()], _CORPUS_DIR)

    assert [type(entry.spec) for entry in decoded] == [AgentSpecV1]


@pytest.mark.parametrize("path", _corpus_paths(), ids=_corpus_ids())
def test_the_artifact_validates_against_the_emitted_schema(
    path: Path,
) -> None:
    """Each corpus entry validates against the schema Loom publishes for v1."""
    validator = Draft202012Validator(agent_spec_json_schema(1))

    assert list(validator.iter_errors(_read_artifact(path))) == []


def test_decoding_reports_no_issues_for_the_current_v1_corpus() -> None:
    """The corpus targets the current spec version, so nothing is deprecated."""
    assert [entry.issues for entry in _load_corpus()] == [()] * len(_corpus_paths())


def test_the_corpus_covers_every_capability_kind() -> None:
    """Every capability kind of the format has at least one fixture."""
    assert _declared_capability_kinds() == set(_CAPABILITY_KINDS)


def test_the_corpus_covers_every_output_kind() -> None:
    """Every output kind of the format has at least one fixture."""
    assert _declared_output_kinds() == set(_OUTPUT_KINDS)


def test_each_artifact_lives_in_a_directory_named_after_it() -> None:
    """The layout is one directory per agent, named after the agent."""
    mismatched = [
        path.parent.name
        for path in _corpus_paths()
        if _read_artifact(path)["name"] != path.parent.name
    ]

    assert mismatched == []


def test_the_corpus_covers_both_skill_library_forms() -> None:
    """Both resolution forms are exercised: beside the artifact and shared."""
    libraries = _declared_skill_libraries()
    local = {library for library in libraries if library.startswith(_LOCAL_LIBRARY_PREFIX)}
    shared = libraries - local

    assert (bool(local), bool(shared)) == (True, True)


def test_no_capability_declares_a_url() -> None:
    """Artifacts name what they reach; addresses are deployment facts."""
    located = [
        (path.parent.name, key)
        for path, capability in _corpus_capabilities()
        for key, value in capability.items()
        if isinstance(value, str) and "://" in value
    ]

    assert located == []


@pytest.mark.parametrize("library", _skill_libraries(), ids=_skill_library_ids())
def test_the_library_contains_only_skill_packages(library: Path) -> None:
    """Only immediate children are discovered, so every child must be one skill."""
    unusable = [
        child.name
        for child in sorted(library.iterdir())
        if not (child.is_dir() and (child / _SKILL_MANIFEST).is_file())
    ]

    assert unusable == []


@_HARNESS_MISSING
@pytest.mark.parametrize("library", _skill_libraries(), ids=_skill_library_ids())
def test_the_library_loads_with_the_harness(library: Path) -> None:
    """Constructing ``Skills`` is the assertion: it parses and validates every manifest.

    A malformed frontmatter, a ``name`` disagreeing with its directory, an empty
    description or a name the loader does not expose all raise here, and the
    ``error`` filter turns the over-long-description warning into a failure too.
    """
    from pydantic_ai_harness.skills import Skills

    expected = sorted(child.name for child in library.iterdir() if child.is_dir())

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        Skills(library)
        selected = Skills(library, include=expected)

    assert selected.include == frozenset(expected)
