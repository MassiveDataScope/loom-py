"""Unit tests for the shared entry-point loader.

These tests pin the public contract of ``loom.core.plugins.entrypoints``:
duplicate-name policies, ``None`` on a miss, the not-found error, the
API-version handshake and one read of the installed distributions per group.
"""

from __future__ import annotations

import logging

import pytest

from loom.core.plugins import entrypoints as entrypoints_module
from loom.core.plugins.entrypoints import (
    ApiVersionMismatchError,
    ApiVersionRequirement,
    DuplicateEntryPointError,
    DuplicatePolicy,
    EntryPointError,
    EntryPointNotFoundError,
    check_api_version,
    list_entry_points,
    load_entry_point,
    select_entry_point,
)

_GROUP = "loom.ai.engines"
_NAME = "pydantic-ai"
_LOGGER_NAME = "loom.core.plugins.entrypoints"


class _FakeDist:
    """Minimal stand-in for ``importlib.metadata.Distribution``."""

    def __init__(self, name: str) -> None:
        self.name = name


class _FakeEntryPoint:
    """Minimal stand-in for ``importlib.metadata.EntryPoint``."""

    def __init__(self, name: str, dist_name: str | None, target: object) -> None:
        self.name = name
        self.group = _GROUP
        self.dist = None if dist_name is None else _FakeDist(dist_name)
        self._target = target

    def load(self) -> object:
        return self._target


class _FakeEntryPoints:
    """Stand-in for the ``EntryPoints`` collection returned by ``entry_points()``."""

    def __init__(self, entries: tuple[_FakeEntryPoint, ...]) -> None:
        self._entries = entries

    def select(self, *, group: str) -> tuple[_FakeEntryPoint, ...]:
        return tuple(entry for entry in self._entries if entry.group == group)


class _Engine:
    """Loadable target used as the entry-point value."""

    def __init__(self, label: str) -> None:
        self.label = label


def _install(
    monkeypatch: pytest.MonkeyPatch,
    entries: tuple[_FakeEntryPoint, ...],
) -> None:
    """Replace ``entry_points`` inside the loader module with a fake collection."""
    monkeypatch.setattr(
        entrypoints_module,
        "entry_points",
        lambda: _FakeEntryPoints(entries),
    )


def _duplicates() -> tuple[_FakeEntryPoint, ...]:
    return (
        _FakeEntryPoint(_NAME, "loom-engine-alpha", _Engine("alpha")),
        _FakeEntryPoint(_NAME, "loom-engine-beta", _Engine("beta")),
    )


class TestDuplicatePolicyError:
    def test_raises_duplicate_error_when_two_distributions_share_the_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, _duplicates())

        with pytest.raises(DuplicateEntryPointError):
            select_entry_point(_GROUP, _NAME, on_duplicate="error")

    def test_error_message_names_all_distributions_on_conflict(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, _duplicates())

        with pytest.raises(DuplicateEntryPointError) as excinfo:
            select_entry_point(_GROUP, _NAME, on_duplicate="error")

        message = str(excinfo.value)
        assert all(
            token in message for token in ("loom-engine-alpha", "loom-engine-beta", _GROUP, _NAME)
        )

    def test_raises_duplicate_error_when_one_distribution_is_unknown(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        entries = (
            _FakeEntryPoint(_NAME, None, _Engine("alpha")),
            _FakeEntryPoint(_NAME, "loom-engine-beta", _Engine("beta")),
        )
        _install(monkeypatch, entries)

        with pytest.raises(DuplicateEntryPointError):
            select_entry_point(_GROUP, _NAME, on_duplicate="error")

    def test_load_entry_point_propagates_duplicate_error_when_the_policy_is_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, _duplicates())

        with pytest.raises(DuplicateEntryPointError):
            load_entry_point(_GROUP, _NAME, on_duplicate="error")


class TestDuplicatePolicyWarnFirst:
    def test_returns_the_first_entry_point_when_the_policy_is_warn_first(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        entries = _duplicates()
        _install(monkeypatch, entries)

        selected = select_entry_point(_GROUP, _NAME, on_duplicate="warn_first")

        assert selected is entries[0]

    def test_emits_a_warning_when_duplicates_exist_under_warn_first(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _install(monkeypatch, _duplicates())

        with caplog.at_level(logging.WARNING, logger=_LOGGER_NAME):
            select_entry_point(_GROUP, _NAME, on_duplicate="warn_first")

        assert [record.levelno for record in caplog.records] == [logging.WARNING]

    def test_warning_names_all_distributions_and_the_chosen_one(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _install(monkeypatch, _duplicates())

        with caplog.at_level(logging.WARNING, logger=_LOGGER_NAME):
            select_entry_point(_GROUP, _NAME, on_duplicate="warn_first")

        message = caplog.text
        assert all(
            token in message for token in ("loom-engine-alpha", "loom-engine-beta", _GROUP, _NAME)
        )

    def test_load_entry_point_returns_the_first_objects_value_when_duplicates_exist(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        entries = _duplicates()
        _install(monkeypatch, entries)

        loaded = load_entry_point(_GROUP, _NAME, on_duplicate="warn_first")

        assert loaded is entries[0].load()


class TestWithoutDuplicates:
    @pytest.mark.parametrize(
        "on_duplicate",
        [
            pytest.param("error", id="returns_the_sole_entry_point_when_the_policy_is_error"),
            pytest.param(
                "warn_first", id="returns_the_sole_entry_point_when_the_policy_is_warn_first"
            ),
        ],
    )
    def test_returns_the_sole_entry_point_whatever_the_policy(
        self,
        on_duplicate: DuplicatePolicy,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        entry = _FakeEntryPoint(_NAME, "loom-engine-alpha", _Engine("alpha"))
        _install(monkeypatch, (entry,))

        assert select_entry_point(_GROUP, _NAME, on_duplicate=on_duplicate) is entry

    def test_emits_no_warning_when_there_are_no_duplicates(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        _install(monkeypatch, (_FakeEntryPoint(_NAME, "loom-engine-alpha", _Engine("alpha")),))

        with caplog.at_level(logging.WARNING, logger=_LOGGER_NAME):
            select_entry_point(_GROUP, _NAME, on_duplicate="warn_first")

        assert caplog.records == []

    def test_selecting_ignores_entry_points_with_a_different_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        wanted = _FakeEntryPoint(_NAME, "loom-engine-alpha", _Engine("alpha"))
        other = _FakeEntryPoint("other-engine", "loom-engine-beta", _Engine("beta"))
        _install(monkeypatch, (other, wanted))

        assert select_entry_point(_GROUP, _NAME, on_duplicate="error") is wanted


class TestMissingEntryPoint:
    def test_select_returns_none_when_no_entry_point_matches(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, (_FakeEntryPoint("other-engine", "loom-engine-beta", _Engine("b")),))

        assert select_entry_point(_GROUP, _NAME, on_duplicate="error") is None

    def test_select_returns_none_when_the_group_is_empty(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, ())

        assert select_entry_point(_GROUP, _NAME, on_duplicate="warn_first") is None

    def test_load_raises_not_found_when_no_entry_point_matches(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, ())

        with pytest.raises(EntryPointNotFoundError):
            load_entry_point(_GROUP, _NAME, on_duplicate="error")

    def test_not_found_message_names_the_group_and_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, ())

        with pytest.raises(EntryPointNotFoundError) as excinfo:
            load_entry_point(_GROUP, _NAME, on_duplicate="error")

        message = str(excinfo.value)
        assert _GROUP in message
        assert _NAME in message


class TestApiVersionHandshake:
    ATTRIBUTE = "LOOM_AI_ENGINE_API"

    def _requirement(self) -> ApiVersionRequirement:
        return ApiVersionRequirement(attribute=self.ATTRIBUTE, supported=frozenset({1}))

    def _install_target(self, monkeypatch: pytest.MonkeyPatch, target: object) -> None:
        _install(monkeypatch, (_FakeEntryPoint(_NAME, "loom-engine-alpha", target),))

    def test_returns_the_loaded_object_when_the_version_is_supported(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        engine = _Engine("alpha")
        engine.LOOM_AI_ENGINE_API = 1  # type: ignore[attr-defined]
        self._install_target(monkeypatch, engine)

        loaded = load_entry_point(
            _GROUP,
            _NAME,
            on_duplicate="error",
            api_version=self._requirement(),
        )

        assert loaded is engine

    def test_raises_mismatch_when_the_version_attribute_is_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._install_target(monkeypatch, _Engine("alpha"))
        requirement = self._requirement()

        with pytest.raises(ApiVersionMismatchError):
            load_entry_point(
                _GROUP,
                _NAME,
                on_duplicate="error",
                api_version=requirement,
            )

    def test_mismatch_message_names_the_attribute_and_supported_versions_when_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._install_target(monkeypatch, _Engine("alpha"))
        requirement = self._requirement()

        with pytest.raises(ApiVersionMismatchError) as excinfo:
            load_entry_point(
                _GROUP,
                _NAME,
                on_duplicate="error",
                api_version=requirement,
            )

        message = str(excinfo.value)
        assert self.ATTRIBUTE in message
        assert "1" in message

    def test_raises_mismatch_when_the_version_is_unsupported(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        engine = _Engine("alpha")
        engine.LOOM_AI_ENGINE_API = 99  # type: ignore[attr-defined]
        self._install_target(monkeypatch, engine)
        requirement = self._requirement()

        with pytest.raises(ApiVersionMismatchError):
            load_entry_point(
                _GROUP,
                _NAME,
                on_duplicate="error",
                api_version=requirement,
            )

    def test_mismatch_message_names_the_seen_value_when_the_version_is_unsupported(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        engine = _Engine("alpha")
        engine.LOOM_AI_ENGINE_API = 99  # type: ignore[attr-defined]
        self._install_target(monkeypatch, engine)
        requirement = self._requirement()

        with pytest.raises(ApiVersionMismatchError) as excinfo:
            load_entry_point(
                _GROUP,
                _NAME,
                on_duplicate="error",
                api_version=requirement,
            )

        message = str(excinfo.value)
        assert all(token in message for token in (self.ATTRIBUTE, "99", "1"))

    def test_raises_mismatch_when_the_declared_version_is_a_bool(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        engine = _Engine("alpha")
        engine.LOOM_AI_ENGINE_API = True  # type: ignore[attr-defined]
        self._install_target(monkeypatch, engine)
        requirement = self._requirement()

        with pytest.raises(ApiVersionMismatchError):
            load_entry_point(
                _GROUP,
                _NAME,
                on_duplicate="error",
                api_version=requirement,
            )

    def test_skips_version_validation_when_no_handshake_is_requested(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        engine = _Engine("alpha")
        self._install_target(monkeypatch, engine)

        assert load_entry_point(_GROUP, _NAME, on_duplicate="error") is engine


class TestErrorHierarchy:
    def test_all_errors_derive_from_entry_point_error(self) -> None:
        assert all(
            issubclass(error, EntryPointError)
            for error in (
                EntryPointNotFoundError,
                DuplicateEntryPointError,
                ApiVersionMismatchError,
            )
        )

    def test_api_version_requirement_is_immutable(self) -> None:
        requirement = ApiVersionRequirement(
            attribute="LOOM_AI_ENGINE_API",
            supported=frozenset({1}),
        )

        with pytest.raises((AttributeError, TypeError)):
            requirement.attribute = "OTHER"  # type: ignore[misc]


class TestNotFoundErrorReporting:
    """A host must be able to name what is installed without re-scanning."""

    def test_not_found_error_carries_group_and_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, ())

        with pytest.raises(EntryPointNotFoundError) as excinfo:
            load_entry_point(_GROUP, _NAME, on_duplicate="error")

        assert (excinfo.value.group, excinfo.value.name) == (_GROUP, _NAME)

    def test_not_found_error_carries_the_registered_names_sorted_and_deduplicated(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(
            monkeypatch,
            (
                _FakeEntryPoint("zeta-engine", "loom-engine-zeta", _Engine("zeta")),
                _FakeEntryPoint("alpha-engine", "loom-engine-alpha", _Engine("alpha")),
                _FakeEntryPoint("alpha-engine", "loom-engine-beta", _Engine("beta")),
            ),
        )

        with pytest.raises(EntryPointNotFoundError) as excinfo:
            load_entry_point(_GROUP, _NAME, on_duplicate="warn_first")

        assert excinfo.value.available == ("alpha-engine", "zeta-engine")

    def test_not_found_error_carries_an_empty_tuple_when_nothing_is_registered(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, ())

        with pytest.raises(EntryPointNotFoundError) as excinfo:
            load_entry_point(_GROUP, _NAME, on_duplicate="error")

        assert excinfo.value.available == ()

    def test_not_found_message_names_the_registered_names(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, (_FakeEntryPoint("other-engine", "loom-engine-beta", _Engine("b")),))

        with pytest.raises(EntryPointNotFoundError) as excinfo:
            load_entry_point(_GROUP, _NAME, on_duplicate="error")

        assert "other-engine" in str(excinfo.value)


class TestDuplicateErrorReporting:
    """FR-021 messages name every claiming distribution; so must the exception."""

    def test_duplicate_error_carries_group_and_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, _duplicates())

        with pytest.raises(DuplicateEntryPointError) as excinfo:
            select_entry_point(_GROUP, _NAME, on_duplicate="error")

        assert (excinfo.value.group, excinfo.value.name) == (_GROUP, _NAME)

    def test_duplicate_error_carries_the_claiming_distributions_in_registration_order(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, _duplicates())

        with pytest.raises(DuplicateEntryPointError) as excinfo:
            select_entry_point(_GROUP, _NAME, on_duplicate="error")

        assert excinfo.value.distributions == ("loom-engine-alpha", "loom-engine-beta")

    def test_duplicate_error_names_a_distribution_less_entry_point_as_unknown(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(
            monkeypatch,
            (
                _FakeEntryPoint(_NAME, None, _Engine("alpha")),
                _FakeEntryPoint(_NAME, "loom-engine-beta", _Engine("beta")),
            ),
        )

        with pytest.raises(DuplicateEntryPointError) as excinfo:
            select_entry_point(_GROUP, _NAME, on_duplicate="error")

        assert excinfo.value.distributions == ("<unknown distribution>", "loom-engine-beta")


class TestCheckApiVersion:
    """Hosts that must construct the plugin first reuse the same handshake."""

    REQUIREMENT = ApiVersionRequirement(
        attribute="LOOM_AI_ENGINE_API",
        supported=frozenset({1}),
    )

    def test_accepts_an_object_declaring_a_supported_version(self) -> None:
        engine = _Engine("alpha")
        engine.LOOM_AI_ENGINE_API = 1  # type: ignore[attr-defined]

        assert check_api_version(engine, self.REQUIREMENT) is None

    def test_rejects_an_object_declaring_an_unsupported_version(self) -> None:
        engine = _Engine("alpha")
        engine.LOOM_AI_ENGINE_API = 99  # type: ignore[attr-defined]

        with pytest.raises(ApiVersionMismatchError):
            check_api_version(engine, self.REQUIREMENT)

    def test_mismatch_error_carries_the_declared_value(self) -> None:
        engine = _Engine("alpha")
        engine.LOOM_AI_ENGINE_API = 99  # type: ignore[attr-defined]

        with pytest.raises(ApiVersionMismatchError) as excinfo:
            check_api_version(engine, self.REQUIREMENT)

        assert excinfo.value.declared == 99

    def test_mismatch_error_carries_none_when_the_attribute_is_absent(self) -> None:
        engine = _Engine("alpha")

        with pytest.raises(ApiVersionMismatchError) as excinfo:
            check_api_version(engine, self.REQUIREMENT)

        assert excinfo.value.declared is None

    def test_mismatch_error_carries_the_requirement_it_was_checked_against(self) -> None:
        engine = _Engine("alpha")

        with pytest.raises(ApiVersionMismatchError) as excinfo:
            check_api_version(engine, self.REQUIREMENT)

        assert excinfo.value.requirement is self.REQUIREMENT


class TestReadOncePerGroup:
    def test_reads_the_installed_distributions_once_per_group(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        reads: list[str] = []

        def counted() -> _FakeEntryPoints:
            reads.append("entry_points")
            return _FakeEntryPoints((_FakeEntryPoint(_NAME, "loom-engine-alpha", _Engine("a")),))

        monkeypatch.setattr(entrypoints_module, "entry_points", counted)

        for _ in range(3):
            load_entry_point(_GROUP, _NAME, on_duplicate="error")
        list_entry_points("loom.persistence.backends")

        assert len(reads) == 2
