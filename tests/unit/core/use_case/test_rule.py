from __future__ import annotations

from typing import Any, cast

import pytest

from loom.core.command import Command, Patch
from loom.core.use_case import F
from loom.core.use_case.rule import Rule, RuleViolation, RuleViolations


class CreateUser(Command, frozen=True):
    email: str
    name: str


class UpdateUser(Command, frozen=True):
    email: Patch[str | None]
    name: Patch[str | None]


def email_not_disposable(command: CreateUser, fields_set: frozenset[str]) -> None:
    del fields_set
    if command.email.endswith("@throwaway.com"):
        raise RuleViolation("email", "Disposable emails not allowed")


def email_format_rule_patch(cmd: UpdateUser, fields_set: frozenset[str]) -> None:
    if "email" not in fields_set:
        return
    if isinstance(cmd.email, str) and "@" not in cmd.email:
        raise RuleViolation("email", "Invalid email format")


def is_disposable(command: Command, _fields_set: frozenset[str]) -> bool:
    user = cast(CreateUser, command)
    return user.email.endswith("@throwaway.com")


def email_format_error_or_none(email: str) -> str | None:
    if "@" not in email:
        return "Invalid email format"
    return None


def patch_email_format_error_or_none(email: str | None) -> str | None:
    if isinstance(email, str) and "@" not in email:
        return "Invalid email format"
    return None


def forbid_user_one(
    _command: Command,
    _fields_set: frozenset[str],
    user_id: str,
) -> bool:
    return user_id == "1"


class TestRuleViolation:
    def test_exposes_field_and_message(self) -> None:
        exc = RuleViolation("email", "Invalid email")

        assert exc.field == "email"
        assert exc.message == "Invalid email"
        assert str(exc) == "email: Invalid email"


class TestRuleViolations:
    def test_accumulates_multiple_violations(self) -> None:
        v1 = RuleViolation("email", "Invalid email")
        v2 = RuleViolation("name", "Name required")
        exc = RuleViolations([v1, v2])

        assert len(exc.violations) == 2
        assert exc.violations[0].field == "email"
        assert exc.violations[1].field == "name"

    def test_violations_stored_as_tuple(self) -> None:
        v1 = RuleViolation("email", "Bad")
        exc = RuleViolations([v1])

        assert isinstance(exc.violations, tuple)

    def test_str_joins_violation_messages(self) -> None:
        v1 = RuleViolation("email", "Invalid")
        v2 = RuleViolation("name", "Required")
        exc = RuleViolations([v1, v2])

        assert str(exc) == "email: Invalid; name: Required"


class TestRule:
    def test_passes_on_valid_input(self) -> None:
        cmd = CreateUser(email="valid@example.com", name="Alice")
        email_not_disposable(cmd, frozenset({"email", "name"}))

    def test_raises_violation_on_invalid_input(self) -> None:
        cmd = CreateUser(email="test@throwaway.com", name="Bob")
        fields_set = frozenset({"email", "name"})

        with pytest.raises(RuleViolation) as exc_info:
            email_not_disposable(cmd, fields_set)

        assert exc_info.value.field == "email"
        assert exc_info.value.message == "Disposable emails not allowed"

    def test_fields_set_enables_patch_aware_validation(self) -> None:
        """Rule can skip validation when field is absent in patch."""

        cmd, fields_set = UpdateUser.from_payload({"name": "NewName"})
        assert cmd.email is None
        email_format_rule_patch(cmd, fields_set)


class TestRuleDsl:
    def test_check_raises_returned_message_as_violation(self) -> None:
        rule = Rule.check(
            F(CreateUser).email,
            via=email_format_error_or_none,
        )

        cmd = CreateUser(email="invalid", name="Alice")
        fields_set = frozenset({"email", "name"})

        with pytest.raises(RuleViolation) as exc_info:
            rule(cmd, fields_set)

        assert exc_info.value.field == "email"
        assert exc_info.value.message == "Invalid email format"

    def test_check_passes_when_evaluator_returns_none(self) -> None:
        rule = Rule.check(F(CreateUser).email, via=email_format_error_or_none)

        rule(CreateUser(email="a@b.com", name="Alice"), frozenset({"email", "name"}))

    def test_check_raises_returned_rule_violation(self) -> None:
        violation = RuleViolation("contact", "Contact rejected")
        rule = Rule.check(F(CreateUser).email, via=lambda _email: violation)

        with pytest.raises(RuleViolation) as exc_info:
            rule(CreateUser(email="a@b.com", name="Alice"), frozenset({"email", "name"}))

        assert exc_info.value is violation

    @pytest.mark.parametrize("outcome", [True, False])
    def test_check_refuses_boolean_outcome(self, outcome: bool) -> None:
        rule = Rule.check(F(CreateUser).email, via=cast(Any, lambda _email: outcome))

        with pytest.raises(TypeError, match=r"field 'email'.*returned bool.*or None"):
            rule(CreateUser(email="a@b.com", name="Alice"), frozenset({"email", "name"}))

    def test_check_when_present_for_patch_field(self) -> None:
        rule = Rule.check(
            F(UpdateUser).email,
            via=patch_email_format_error_or_none,
        ).when_present(F(UpdateUser).email)

        cmd_missing, fields_missing = UpdateUser.from_payload({"name": "Alice"})
        rule(cmd_missing, fields_missing)

        cmd_invalid, fields_invalid = UpdateUser.from_payload({"email": "invalid"})
        with pytest.raises(RuleViolation):
            rule(cmd_invalid, fields_invalid)

    def test_check_when_present_supports_or_expression(self) -> None:
        rule = Rule.check(
            F(UpdateUser).email,
            via=patch_email_format_error_or_none,
        ).when_present(F(UpdateUser).email | F(UpdateUser).name)

        cmd_missing, fields_missing = UpdateUser.from_payload({})
        rule(cmd_missing, fields_missing)

        cmd_invalid, fields_invalid = UpdateUser.from_payload({"email": "invalid"})
        with pytest.raises(RuleViolation):
            rule(cmd_invalid, fields_invalid)

    def test_forbid_blocks_condition(self) -> None:
        rule = Rule.forbid(
            is_disposable,
            message="Disposable emails not allowed",
        )

        cmd = CreateUser(email="spam@throwaway.com", name="Spammer")
        fields_set = frozenset({"email", "name"})

        with pytest.raises(RuleViolation):
            rule(cmd, fields_set)

    def test_forbid_from_params_uses_runtime_context(self) -> None:
        rule = Rule.forbid(
            forbid_user_one,
            message="user id is blocked",
        ).from_params("user_id")

        cmd = CreateUser(email="ok@example.com", name="Alice")
        fields_set = frozenset({"email", "name"})

        with pytest.raises(RuleViolation) as exc_info:
            rule(cmd, fields_set, {"user_id": "1"})

        assert exc_info.value.field == "user_id"

    def test_forbid_from_params_missing_context_raises(self) -> None:
        rule = Rule.forbid(
            forbid_user_one,
            message="user id is blocked",
        ).from_params("user_id")

        cmd = CreateUser(email="ok@example.com", name="Alice")
        fields_set = frozenset({"email", "name"})

        with pytest.raises(ValueError, match="runtime context"):
            rule(cmd, fields_set)

    def test_require_present_for_non_patch_field(self) -> None:
        rule = Rule.require_present(F(CreateUser).email)
        rule(CreateUser(email="a@b.com", name="Alice"), frozenset({"email", "name"}))

    def test_require_present_for_patch_field_uses_fields_set(self) -> None:
        rule = Rule.require_present(F(UpdateUser).email)
        cmd, fields_set = UpdateUser.from_payload({"name": "Alice"})

        with pytest.raises(RuleViolation) as exc_info:
            rule(cmd, fields_set)

        assert exc_info.value.field == "email"

    def test_tdd_check_without_build_should_be_directly_callable(self) -> None:
        """Rule.check(...) returns a directly callable rule."""
        rule = Rule.check(
            F(CreateUser).email,
            via=email_format_error_or_none,
        )

        cmd = CreateUser(email="invalid", name="Alice")
        fields_set = frozenset({"email", "name"})

        with pytest.raises(RuleViolation):
            rule(cmd, fields_set)
