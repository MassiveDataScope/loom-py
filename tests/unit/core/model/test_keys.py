"""Business keys: the canonical form, the derived id and the declarations that startup checks."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum, IntEnum, StrEnum
from typing import Any
from uuid import UUID

import msgspec
import pytest

from loom.core.config import ConfigError
from loom.core.errors import BoundaryValidationError, InvalidType
from loom.core.model import (
    BaseModel,
    BusinessKey,
    ColumnField,
    CreatedAtField,
    KeyHash,
    LoomStructMeta,
    UpdatedAtField,
    business_key_of,
    declared_unique,
    derived_key_of,
    verify_keys,
)
from loom.core.model.enums import ServerDefault
from loom.core.model.keys import canonical_form, is_canonical_type
from tests.helpers.keys import derived_id

NAMESPACE = UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
UUID5 = KeyHash.uuid5(NAMESPACE)
SHA256 = KeyHash.sha256()


class AcmeTier(StrEnum):
    GOLD = "gold"


class AcmeRank(IntEnum):
    FIRST = 1


class AcmeListing(BaseModel):
    __tablename__ = "acme_listings"
    __business_key__ = BusinessKey("portal", "external_id")

    id: str = ColumnField(primary_key=True, derived=UUID5)
    portal: str = ColumnField()
    external_id: str = ColumnField()
    title: str | None = None


class _ListingCommand(msgspec.Struct):
    portal: str
    external_id: str


class AcmeSlot(BaseModel):
    __tablename__ = "acme_slots"
    __business_key__ = BusinessKey("portal", "slot")

    id: int = ColumnField(primary_key=True, autoincrement=True)
    portal: str = ColumnField()
    slot: int = ColumnField()


def _model(**attributes: Any) -> type[BaseModel]:
    return LoomStructMeta("AcmeDraft", (BaseModel,), {"__tablename__": "acme_drafts", **attributes})


_ISO_PLUS_TWO = timezone(timedelta(hours=2))


class TestCanonicalForm:
    @pytest.mark.parametrize(
        ("value", "canonical"),
        [
            ("plain", "plain"),
            ("Café", "Café"),
            ("Mixed Case ", "Mixed Case "),
            (7, "7"),
            (-3, "-3"),
            (True, "true"),
            (False, "false"),
            (AcmeTier.GOLD, "gold"),
            (AcmeRank.FIRST, "1"),
            (Decimal("1.10"), "1.1"),
            (Decimal("1E+2"), "100"),
            (Decimal("-0.0"), "0"),
            (datetime(2026, 1, 2, 3, 4, 5, 6, tzinfo=_ISO_PLUS_TWO), "2026-01-02T01:04:05.000006Z"),
            (datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC), "2026-01-02T03:04:05.000000Z"),
            (date(2026, 1, 2), "2026-01-02"),
            (UUID("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
        ],
        ids=lambda value: type(value).__name__ + ":" + str(value),
    )
    def test_each_value_has_one_canonical_string(self, value: object, canonical: str) -> None:
        assert canonical_form(value) == canonical

    @pytest.mark.parametrize(
        ("value", "error"),
        [
            (datetime(2026, 1, 2), ValueError),
            (Decimal("NaN"), ValueError),
            (1.5, TypeError),
            (b"raw", TypeError),
            (None, TypeError),
        ],
        ids=["naive-datetime", "non-finite-decimal", "float", "bytes", "none"],
    )
    def test_a_value_without_a_canonical_form_is_refused(
        self, value: object, error: type[Exception]
    ) -> None:
        with pytest.raises(error):
            canonical_form(value)

    @pytest.mark.parametrize(
        ("python_type", "expected"),
        [
            (str, True),
            (int, True),
            (bool, True),
            (datetime, True),
            (AcmeTier, True),
            (float, False),
        ],
    )
    def test_the_field_types_with_a_canonical_form(self, python_type: type, expected: bool) -> None:
        assert is_canonical_type(python_type) is expected

    def test_an_enum_whose_values_have_no_canonical_form_is_not_a_key_type(self) -> None:
        coords = Enum("Coords", {"ORIGIN": (0, 0)})

        assert is_canonical_type(coords) is False


class TestGoldenVectors:
    """Fixed input to fixed output: a change here is a change of every stored id."""

    @pytest.mark.parametrize(
        ("values", "uuid5", "sha256"),
        [
            (
                {"portal": "idealista", "external_id": "123"},
                "f90f18f3-4e23-5dcc-b138-8fa5956aecbe",
                "10caa70de3729efba45ec72438146c4490e66b826ce5900312dd4e74bf870e21",
            ),
            (
                {"portal": "café", "external_id": "é"},
                "a1c45d5f-b1f4-543e-a676-101351d42de7",
                "1e81a1cf260bc4d501fbd7bd14dd0d1d298d39e9031cec85df1d13a1b1c4dba1",
            ),
            (
                {
                    "a": 1,
                    "b": True,
                    "amount": Decimal("1.10"),
                    "d": datetime(2026, 1, 2, 3, 4, 5, 6, tzinfo=_ISO_PLUS_TWO),
                    "e": date(2026, 1, 2),
                    "f": UUID("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"),
                },
                "ec0163ac-f96b-5319-901a-61e457c1f777",
                "5b75fd5ba87173c40bd6dfcb0c525293d49b7b8ea338c874e10c5b7d50d73e6b",
            ),
        ],
        ids=["ascii", "unicode", "every-type"],
    )
    def test_the_formulas_give_the_pinned_ids(
        self, values: dict[str, Any], uuid5: str, sha256: str
    ) -> None:
        assert derived_id(UUID5, **values) == uuid5
        assert derived_id(SHA256, **values) == sha256


class TestKeyFor:
    def test_a_missing_field_is_refused_naming_model_and_field(self) -> None:
        with pytest.raises(
            BoundaryValidationError, match="AcmeListing: .*'external_id' is missing"
        ):
            AcmeListing.key_for({"portal": "p"})

    def test_a_field_without_a_value_is_refused_naming_model_and_field(self) -> None:
        with pytest.raises(
            BoundaryValidationError, match="AcmeListing: .*'external_id' has no value"
        ):
            AcmeListing.key_for({"portal": "p", "external_id": None})

    def test_a_naive_datetime_is_refused_naming_the_field(self) -> None:
        with pytest.raises(BoundaryValidationError, match="'at'.*naive datetime"):
            derived_id(UUID5, at=datetime(2026, 1, 1))

    def test_the_namespace_is_part_of_the_formula(self) -> None:
        other = KeyHash.uuid5(UUID(int=1))

        assert derived_id(other, v="x") != derived_id(UUID5, v="x")

    def test_a_model_that_derives_no_key_refuses_the_call(self) -> None:
        with pytest.raises(InvalidType, match="derives no key"):
            AcmeSlot.key_for({"portal": "p", "slot": 1})

    @pytest.mark.parametrize(
        "row",
        [
            {"external_id": "123", "portal": "idealista", "title": "x", "extra": 1},
            AcmeListing(id="whatever", portal="idealista", external_id="123", title="y"),
            msgspec.convert({"portal": "idealista", "external_id": "123"}, _ListingCommand),
        ],
        ids=["mapping-with-extra-columns", "model-instance", "command"],
    )
    def test_any_row_shape_reads_only_the_business_key_in_its_declared_order(
        self, row: object
    ) -> None:
        assert AcmeListing.key_for(row) == "f90f18f3-4e23-5dcc-b138-8fa5956aecbe"

    def test_the_getters_are_built_once_per_model(self) -> None:
        assert derived_key_of(AcmeListing) is derived_key_of(AcmeListing)

    def test_a_model_cannot_have_a_field_namedderived_id(self) -> None:
        with pytest.raises(TypeError, match="AcmeDraft.key_for cannot be a field"):
            _model(__annotations__={"id": int, "key_for": str}, id=ColumnField(primary_key=True))


class TestBusinessKey:
    def test_it_needs_a_field(self) -> None:
        with pytest.raises(ValueError, match="at least one field"):
            BusinessKey()

    def test_it_refuses_a_repeated_field(self) -> None:
        with pytest.raises(ValueError, match="repeats a field"):
            BusinessKey("portal", "portal")


class TestBusinessKeyDeclaration:
    def test_a_model_without_one_has_none(self) -> None:
        assert business_key_of(_model(id=ColumnField(primary_key=True))) is None

    def test_an_unknown_field_is_refused_naming_the_model_and_the_field(self) -> None:
        model = _model(
            __annotations__={"id": int},
            id=ColumnField(primary_key=True),
            __business_key__=BusinessKey("ghost"),
        )

        with pytest.raises(ValueError, match="AcmeDraft: __business_key__ names unknown column"):
            business_key_of(model)

    def test_a_nullable_field_is_refused(self) -> None:
        model = _model(
            __annotations__={"id": int, "portal": "str | None"},
            id=ColumnField(primary_key=True),
            portal=ColumnField(),
            __business_key__=BusinessKey("portal"),
        )

        with pytest.raises(ValueError, match="'portal' is nullable"):
            business_key_of(model)

    def test_a_field_type_without_a_canonical_form_is_refused(self) -> None:
        model = _model(
            __annotations__={"id": int, "ratio": float},
            id=ColumnField(primary_key=True),
            ratio=ColumnField(),
            __business_key__=BusinessKey("ratio"),
        )

        with pytest.raises(ValueError, match="'ratio' is float, which has no canonical form"):
            business_key_of(model)

    def test_a_declaration_that_is_not_a_business_key_is_refused(self) -> None:
        model = _model(
            __annotations__={"id": int},
            id=ColumnField(primary_key=True),
            __business_key__=("portal",),
        )

        with pytest.raises(InvalidType, match="must be a BusinessKey"):
            business_key_of(model)

    def test_it_is_a_unique_constraint(self) -> None:
        assert declared_unique(AcmeSlot) == (("portal", "slot"),)

    def test_a_unique_declared_by_hand_is_not_repeated(self) -> None:
        model = _model(
            __annotations__={"id": int, "portal": str, "slot": int},
            id=ColumnField(primary_key=True),
            portal=ColumnField(),
            slot=ColumnField(),
            __business_key__=BusinessKey("portal", "slot"),
            __unique__=[("slot", "portal")],
        )

        assert declared_unique(model) == (("slot", "portal"),)


class TestDerivedDeclaration:
    def test_a_model_deriving_its_id_exposes_the_formula(self) -> None:
        derived = derived_key_of(AcmeListing)

        assert derived is not None
        assert (derived.field, derived.business_key.fields) == ("id", ("portal", "external_id"))

    def test_a_model_without_a_derived_field_derives_none(self) -> None:
        assert derived_key_of(AcmeSlot) is None

    def test_deriving_without_a_business_key_is_refused(self) -> None:
        model = _model(
            __annotations__={"id": str},
            id=ColumnField(primary_key=True, derived=SHA256),
        )

        with pytest.raises(ValueError, match="needs a __business_key__"):
            derived_key_of(model)

    def test_deriving_a_field_that_is_not_the_primary_key_is_refused(self) -> None:
        model = _model(
            __annotations__={"id": int, "code": str, "portal": str},
            id=ColumnField(primary_key=True),
            code=ColumnField(derived=SHA256),
            portal=ColumnField(),
            __business_key__=BusinessKey("portal"),
        )

        with pytest.raises(ValueError, match="'code' must be the model's single primary-key"):
            derived_key_of(model)

    def test_deriving_a_composite_primary_key_is_refused(self) -> None:
        model = _model(
            __annotations__={"tenant": str, "id": str, "portal": str},
            tenant=ColumnField(primary_key=True),
            id=ColumnField(primary_key=True, derived=SHA256),
            portal=ColumnField(),
            __business_key__=BusinessKey("portal"),
        )

        with pytest.raises(ValueError, match="'id' must be the model's single primary-key"):
            derived_key_of(model)

    def test_deriving_a_key_that_is_not_text_is_refused(self) -> None:
        model = _model(
            __annotations__={"id": int, "portal": str},
            id=ColumnField(primary_key=True, derived=SHA256),
            portal=ColumnField(),
            __business_key__=BusinessKey("portal"),
        )

        with pytest.raises(InvalidType, match="must be annotated str"):
            derived_key_of(model)

    def test_a_key_containing_its_own_derived_field_is_refused(self) -> None:
        model = _model(
            __annotations__={"id": str},
            id=ColumnField(primary_key=True, derived=SHA256),
            __business_key__=BusinessKey("id"),
        )

        with pytest.raises(ValueError, match="cannot be part of its own business key"):
            derived_key_of(model)

    def test_two_derived_fields_are_refused(self) -> None:
        model = _model(
            __annotations__={"id": str, "other": str, "portal": str},
            id=ColumnField(primary_key=True, derived=SHA256),
            other=ColumnField(derived=SHA256),
            portal=ColumnField(),
            __business_key__=BusinessKey("portal"),
        )

        with pytest.raises(ValueError, match="only one field derives"):
            derived_key_of(model)


class TestDerivedKey:
    def test_the_id_is_the_formula_over_the_business_key_in_its_order(self) -> None:
        derived = derived_key_of(AcmeListing)
        assert derived is not None

        assert derived.id_for({"external_id": "123", "portal": "idealista", "title": "x"}) == (
            "f90f18f3-4e23-5dcc-b138-8fa5956aecbe"
        )

    def test_a_supplied_id_that_matches_is_kept(self) -> None:
        derived = derived_key_of(AcmeListing)
        assert derived is not None
        row = {"portal": "idealista", "external_id": "123"}

        completed = derived.complete({**row, "id": "f90f18f3-4e23-5dcc-b138-8fa5956aecbe"})

        assert completed["id"] == "f90f18f3-4e23-5dcc-b138-8fa5956aecbe"

    def test_a_supplied_id_that_does_not_match_names_model_field_and_formula(self) -> None:
        derived = derived_key_of(AcmeListing)
        assert derived is not None

        with pytest.raises(
            BoundaryValidationError,
            match=r"AcmeListing\.id is derived from \(portal, external_id\) with uuid5",
        ):
            derived.complete({"portal": "idealista", "external_id": "123", "id": "other"})

    def test_updating_a_business_key_field_is_refused(self) -> None:
        derived = derived_key_of(AcmeListing)
        assert derived is not None

        with pytest.raises(BoundaryValidationError, match=r"\(portal\) cannot be updated"):
            derived.refuse_change(["title", "portal"])

    def test_updating_other_fields_is_allowed(self) -> None:
        derived = derived_key_of(AcmeListing)
        assert derived is not None

        derived.refuse_change(["title"])


class TestVerifyKeys:
    def test_it_reports_the_rows_whose_id_does_not_match_with_the_expected_id(self) -> None:
        good = {
            "id": "f90f18f3-4e23-5dcc-b138-8fa5956aecbe",
            "portal": "idealista",
            "external_id": "123",
        }
        bad = {"id": "stale", "portal": "idealista", "external_id": "123"}

        mismatches = verify_keys(AcmeListing, [good, bad])

        assert [(m.row, m.expected) for m in mismatches] == [(bad, good["id"])]

    def test_a_model_that_derives_no_key_has_nothing_to_verify(self) -> None:
        with pytest.raises(InvalidType, match="derives no key"):
            verify_keys(AcmeSlot, [])


class AcmeAudited(BaseModel):
    __tablename__ = "acme_audited"
    __business_key__ = BusinessKey("portal")

    id: str = ColumnField(primary_key=True, derived=SHA256)
    portal: str = ColumnField()
    title: str = ColumnField(default="")

    def __post_init__(self) -> None:
        if self.id is msgspec.UNSET:
            raise ValueError("the id was not derived before the application's check")
        if self.title == "bad":
            raise ValueError("bad title")


class AcmeLater(AcmeListing):
    __tablename__ = "acme_later"

    note: str = ColumnField(default="")


class AcmeObserved(BaseModel):
    __tablename__ = "acme_observed"
    __business_key__ = BusinessKey("sensor", "observed_at")

    id: str = ColumnField(primary_key=True, derived=SHA256)
    sensor: str = ColumnField()
    observed_at: datetime = ColumnField()


class TestModelDerivesItsId:
    def test_a_model_with_every_business_key_field_computes_its_id(self) -> None:
        listing = AcmeListing(portal="idealista", external_id="123")

        assert listing.id == "f90f18f3-4e23-5dcc-b138-8fa5956aecbe"

    def test_a_model_missing_a_business_key_field_leaves_the_id_unset(self) -> None:
        listing = AcmeListing(portal="idealista")

        assert listing.id is msgspec.UNSET

    def test_a_supplied_id_is_not_recomputed(self) -> None:
        listing = AcmeListing(id="stored", portal="idealista", external_id="123")

        assert listing.id == "stored"

    def test_decoding_a_row_without_an_id_computes_it(self) -> None:
        listing = msgspec.convert({"portal": "idealista", "externalId": "123"}, AcmeListing)

        assert listing.id == "f90f18f3-4e23-5dcc-b138-8fa5956aecbe"

    def test_decoding_a_row_with_an_id_keeps_it(self) -> None:
        row = {"id": "stored", "portal": "idealista", "externalId": "123"}

        assert msgspec.convert(row, AcmeListing).id == "stored"

    def test_the_model_is_frozen_once_built(self) -> None:
        listing = AcmeListing(portal="idealista", external_id="123")

        with pytest.raises(AttributeError):
            listing.portal = "fotocasa"  # type: ignore[misc]

    def test_the_application_post_init_runs_after_the_id_is_derived(self) -> None:
        assert AcmeAudited(portal="idealista").id != msgspec.UNSET

    def test_the_application_post_init_still_validates(self) -> None:
        with pytest.raises(ValueError, match="bad title"):
            AcmeAudited(portal="idealista", title="bad")

    def test_a_subclass_derives_its_id_too(self) -> None:
        assert AcmeLater(portal="idealista", external_id="123").id == (
            "f90f18f3-4e23-5dcc-b138-8fa5956aecbe"
        )

    def test_a_value_without_a_canonical_form_is_refused_at_construction(self) -> None:
        with pytest.raises(BoundaryValidationError, match="'observed_at'.*naive datetime"):
            AcmeObserved(sensor="s", observed_at=datetime(2026, 1, 1))

    def test_a_model_that_derives_nothing_computes_no_id(self) -> None:
        assert AcmeSlot(portal="p", slot=1).id is None


_NFC_JOSE = "Jos\u00e9"
_NFD_JOSE = "Jose\u0301"


class AcmePlainListing(BaseModel):
    __tablename__ = "acme_plain_listings"

    id: str = ColumnField(primary_key=True)
    portal: str = ColumnField()


class TestModelStoresTheTextItHashes:
    def test_construction_stores_a_decomposed_key_field_in_nfc(self) -> None:
        listing = AcmeListing(portal="idealista", external_id=_NFD_JOSE)

        assert listing.external_id == _NFC_JOSE
        assert listing.id == AcmeListing(portal="idealista", external_id=_NFC_JOSE).id

    def test_decoding_a_decomposed_row_stores_it_in_nfc(self) -> None:
        row = {"portal": "idealista", "externalId": _NFD_JOSE}

        assert msgspec.convert(row, AcmeListing).external_id == _NFC_JOSE

    def test_a_supplied_id_is_verified_against_the_normalized_key(self) -> None:
        derived = derived_key_of(AcmeListing)
        assert derived is not None
        supplied = AcmeListing.key_for({"portal": "idealista", "external_id": _NFC_JOSE})

        completed = derived.complete(
            {"id": supplied, "portal": "idealista", "external_id": _NFD_JOSE}
        )

        assert completed["external_id"] == _NFC_JOSE

    def test_a_normalized_value_is_left_as_the_same_object(self) -> None:
        text = "".join(["Jos", "\u00e9"])

        assert AcmeListing(portal="idealista", external_id=text).external_id is text

    def test_a_field_outside_the_business_key_is_left_as_given(self) -> None:
        listing = AcmeListing(portal="idealista", external_id="123", title=_NFD_JOSE)

        assert listing.title == _NFD_JOSE

    def test_a_model_without_a_derived_key_is_left_as_given(self) -> None:
        assert AcmePlainListing(id="1", portal=_NFD_JOSE).portal == _NFD_JOSE


def _derived_with(**fields: Any) -> type[BaseModel]:
    return _model(
        __annotations__={
            "id": str,
            **{name: annotation for name, (annotation, _) in fields.items()},
        },
        id=ColumnField(primary_key=True, derived=SHA256),
        __business_key__=BusinessKey(*fields),
        **{name: spec for name, (_, spec) in fields.items()},
    )


class TestServerSetKeyFields:
    @pytest.mark.parametrize(
        ("annotation", "spec"),
        [
            (datetime, CreatedAtField()),
            (datetime | None, CreatedAtField()),
            (datetime, UpdatedAtField()),
            (str, ColumnField(server_default=ServerDefault.UUID4)),
            (int, ColumnField(autoincrement=True)),
        ],
        ids=["created_at", "optional-created_at", "updated_at", "server-default", "autoincrement"],
    )
    def test_a_field_the_store_sets_after_the_insert_cannot_feed_the_key(
        self, annotation: type, spec: Any
    ) -> None:
        model = _derived_with(stamp=(annotation, spec))

        with pytest.raises(ConfigError, match=r"AcmeDraft\.stamp .*observed_at"):
            derived_key_of(model)

    def test_a_business_time_field_the_application_sets_is_accepted(self) -> None:
        assert derived_key_of(AcmeObserved) is not None

    def test_a_model_that_does_not_derive_may_key_on_such_a_field(self) -> None:
        model = _model(
            __annotations__={"id": int, "stamp": datetime},
            id=ColumnField(primary_key=True),
            stamp=CreatedAtField(),
            __business_key__=BusinessKey("stamp"),
        )

        assert business_key_of(model) is not None
