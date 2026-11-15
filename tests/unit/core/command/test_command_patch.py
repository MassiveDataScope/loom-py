from __future__ import annotations

from loom.core.command import Command, Internal, Patch


class AcmePatchUser(Command, frozen=True):
    email: Patch[str | None]
    name: Patch[str | None]
    tenant_id: Internal[int] = 0


class TestPatchCommand:
    def test_partial_payload_tracks_fields_set(self) -> None:
        cmd, fields_set = AcmePatchUser.from_payload({"email": "a@b.com"})
        assert cmd.email == "a@b.com"
        assert cmd.name is None
        assert fields_set == frozenset({"email"})

    def test_explicit_none_vs_omitted(self) -> None:
        cmd, fields_set = AcmePatchUser.from_payload({"email": None, "name": "Alice"})
        assert cmd.email is None
        assert cmd.name == "Alice"
        assert "email" in fields_set
        assert "name" in fields_set

    def test_omitted_field_not_in_fields_set(self) -> None:
        cmd, fields_set = AcmePatchUser.from_payload({"name": "Alice"})
        assert cmd.email is None
        assert "email" not in fields_set
        assert "name" in fields_set

    def test_internal_field_default_not_in_fields_set(self) -> None:
        cmd, fields_set = AcmePatchUser.from_payload({"email": "a@b.com"})
        assert cmd.tenant_id == 0
        assert "tenant_id" not in fields_set

    def test_empty_payload_valid(self) -> None:
        cmd, fields_set = AcmePatchUser.from_payload({})
        assert cmd.email is None
        assert cmd.name is None
        assert cmd.tenant_id == 0
        assert fields_set == frozenset()
