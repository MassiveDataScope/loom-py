"""Business keys, and the primary keys derived from them.

A business key names the fields that identify one entity at the model's grain.
A derived key is a primary key computed from those fields with a fixed
formula, so the same business key gives the same id in every store. This module
owns the formula and the canonical form of every value it hashes.
"""

from __future__ import annotations

import hashlib
import unicodedata
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum, StrEnum
from operator import attrgetter, itemgetter
from typing import Any
from uuid import UUID

import msgspec

from loom.core.errors import BoundaryValidationError, InvalidType, InvalidValue

CANONICAL_TYPES: tuple[type, ...] = (str, int, Decimal, date, UUID, Enum)
"""The classes a business-key field may be annotated with, ``bool`` and ``datetime`` included."""


def canonical_form(value: object) -> str:
    """Return the one string that stands for *value* inside a derived key.

    ``str`` is Unicode NFC, as given: no case folding or trimming. ``int`` is
    decimal and ``bool`` is ``true`` or ``false``. An ``Enum`` is its value.
    A ``Decimal`` is its normalized plain form, so ``1.10`` and ``1.1`` agree.
    A ``datetime`` is converted to UTC and written in ISO 8601 with
    microseconds and ``Z``. A ``date`` is ISO 8601 and a ``UUID`` is lowercase
    hyphenated.

    Raises:
        InvalidType: If the value's type has no canonical form.
        InvalidValue: If the value is a naive ``datetime`` or a non-finite ``Decimal``.
    """
    match value:
        case Enum():
            return canonical_form(value.value)
        case bool():
            return "true" if value else "false"
        case str():
            return _nfc(value)
        case int():
            return str(value)
        case Decimal():
            return _canonical_decimal(value)
        case datetime():
            return _canonical_datetime(value)
        case date():
            return value.isoformat()
        case UUID():
            return str(value)
        case _:
            raise InvalidType(f"{type(value).__name__} has no canonical form")


def _nfc(text: str) -> str:
    return text if unicodedata.is_normalized("NFC", text) else unicodedata.normalize("NFC", text)


def is_canonical_type(python_type: type) -> bool:
    """Whether values annotated *python_type* have a canonical form.

    An ``Enum`` qualifies when each of its members' values does.
    """
    if not issubclass(python_type, CANONICAL_TYPES):
        return False
    if issubclass(python_type, Enum):
        return all(is_canonical_type(type(member.value)) for member in python_type)
    return True


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise InvalidValue("a non-finite Decimal has no canonical form")
    return "0" if value == 0 else format(value.normalize(), "f")


def _canonical_datetime(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise InvalidValue("a naive datetime has no canonical form; give it a timezone")
    utc = value.astimezone(UTC).replace(tzinfo=None)
    return f"{utc.isoformat(timespec='microseconds')}Z"


class KeyAlgorithm(StrEnum):
    """The closed set of formulas that turn a business key into an id."""

    UUID5 = "uuid5"
    SHA256 = "sha256"


@dataclass(frozen=True, slots=True)
class KeyHash:
    """The formula that turns the canonical strings of a business key into an id.

    The strings, in order, are encoded as a JSON list and hashed: ``uuid5``
    returns the UUID text under *namespace*, ``sha256`` the hex digest. Build
    it with :meth:`uuid5` or :meth:`sha256`; applications read a derived id
    through ``Model.key_for``, not through the formula.
    """

    algorithm: KeyAlgorithm
    namespace: UUID | None = None

    @classmethod
    def uuid5(cls, namespace: UUID) -> KeyHash:
        """Derive a ``uuid5`` text under *namespace*; the namespace is part of the formula."""
        return cls(KeyAlgorithm.UUID5, namespace)

    @classmethod
    def sha256(cls) -> KeyHash:
        """Derive the SHA-256 hex digest."""
        return cls(KeyAlgorithm.SHA256)

    @property
    def label(self) -> str:
        """The formula as text, for error messages."""
        if self.namespace is None:
            return self.algorithm.value
        return f"{self.algorithm.value}({self.namespace})"

    def digest(self, canonical_values: Sequence[str]) -> str:
        """Return the id of the business key whose canonical strings are *canonical_values*."""
        encoded = msgspec.json.encode(list(canonical_values))
        if self.algorithm is KeyAlgorithm.UUID5 and self.namespace is not None:
            return str(uuid.uuid5(self.namespace, encoded.decode()))
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class BusinessKey:
    """The fields that identify one entity at the model's grain; unique over all of them.

    History rows include their time or version field, events their timestamp,
    multi-tenant tables their tenant. Declare it as ``__business_key__`` on the
    model. Startup refuses an unknown or nullable field.

    Raises:
        InvalidValue: If no field is named, or one is named twice.
    """

    fields: tuple[str, ...]

    def __init__(self, *fields: str) -> None:
        if not fields:
            raise InvalidValue("BusinessKey needs at least one field")
        if len(set(fields)) != len(fields):
            raise InvalidValue("BusinessKey repeats a field")
        object.__setattr__(self, "fields", fields)


@dataclass(frozen=True, slots=True)
class KeyMismatch:
    """A row whose id is not the one its business key derives."""

    row: Mapping[str, Any]
    expected: str


def _as_tuple(getter: Callable[[Any], Any], count: int) -> Callable[[Any], tuple[Any, ...]]:
    """Make *getter*, an ``attrgetter`` or ``itemgetter``, return a tuple for any *count*."""
    if count == 1:
        return lambda row: (getter(row),)
    return getter


@dataclass(frozen=True, slots=True)
class DerivedKey:
    """The primary-key field of a model and the formula that fills it from its business key.

    The getters that read the business-key fields are built once, here, so a
    call reads, canonicalises and hashes.
    """

    model: str
    field: str
    business_key: BusinessKey
    key_hash: KeyHash
    _from_attributes: Callable[[Any], tuple[Any, ...]] = dataclass_field(
        init=False, repr=False, compare=False
    )
    _from_items: Callable[[Any], tuple[Any, ...]] = dataclass_field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        names = self.business_key.fields
        object.__setattr__(self, "_from_attributes", _as_tuple(attrgetter(*names), len(names)))
        object.__setattr__(self, "_from_items", _as_tuple(itemgetter(*names), len(names)))

    def id_for(self, row: object) -> str:
        """Return the id *row* derives, reading only the business-key fields from it.

        *row* is a mapping or any object with the fields as attributes; its
        other fields are ignored.

        Raises:
            BoundaryValidationError: If a business-key field is missing, has
                no value or has no canonical form.
        """
        values = self._read(row)
        canonical = [
            self._canonical(name, value)
            for name, value in zip(self.business_key.fields, values, strict=True)
        ]
        return self.key_hash.digest(canonical)

    def normalize_text(self, model: msgspec.Struct) -> None:
        """Store the NFC form of each ``str`` business-key field of *model*, in place.

        The id hashes the NFC form, so the stored value must be that form for
        the business key to stay unique per id. A field already in NFC is not
        written, which works on a frozen model.
        """
        for name in self.business_key.fields:
            value = getattr(model, name, None)
            if isinstance(value, str) and _nfc(value) != value:
                msgspec.structs.force_setattr(model, name, _nfc(value))

    def has_every_field(self, row: object) -> bool:
        """Whether *row* holds a value for every business-key field."""
        try:
            values = self._read(row)
        except BoundaryValidationError:
            return False
        return all(value is not None and value is not msgspec.UNSET for value in values)

    def complete(self, values: dict[str, Any]) -> dict[str, Any]:
        """Return *values* with the derived id in the key field.

        Raises:
            BoundaryValidationError: If *values* carries an id other than the
                derived one, or a business-key field is missing.
        """
        values = self._with_normalized_text(values)
        derived = self.id_for(values)
        supplied = values.get(self.field)
        if supplied is not None and supplied is not msgspec.UNSET and str(supplied) != derived:
            fields = ", ".join(self.business_key.fields)
            raise BoundaryValidationError(
                f"{self.model}.{self.field} is derived from ({fields}) with "
                f"{self.key_hash.label}; the id {supplied!r} does not match {derived!r}",
                ((self.field, f"must be {derived}"),),
            )
        return {**values, self.field: derived}

    def _with_normalized_text(self, values: dict[str, Any]) -> dict[str, Any]:
        normalized = {
            name: _nfc(values[name])
            for name in self.business_key.fields
            if isinstance(values.get(name), str)
        }
        return {**values, **normalized}

    def refuse_change(self, written: Iterable[str]) -> None:
        """Refuse an update that writes a business-key field, which would orphan the id.

        Raises:
            BoundaryValidationError: Naming the model and the fields.
        """
        changed = tuple(name for name in written if name in self.business_key.fields)
        if changed:
            raise BoundaryValidationError(
                f"{self.model}.{self.field} is derived from its business key, so "
                f"({', '.join(changed)}) cannot be updated; create a new row instead",
                tuple((name, "is part of the derived key") for name in changed),
            )

    def verify(self, rows: Iterable[Mapping[str, Any]]) -> tuple[KeyMismatch, ...]:
        """Report the rows whose id is not the one their business key derives."""
        mismatches = []
        for row in rows:
            expected = self.id_for(row)
            if str(row.get(self.field)) != expected:
                mismatches.append(KeyMismatch(row, expected))
        return tuple(mismatches)

    def _read(self, row: object) -> tuple[Any, ...]:
        reader = self._from_items if isinstance(row, Mapping) else self._from_attributes
        try:
            return reader(row)
        except (KeyError, AttributeError):
            missing = next(name for name in self.business_key.fields if not _has(row, name))
            raise BoundaryValidationError(
                f"{self.model}: the business key field {missing!r} is missing",
                ((missing, "is required"),),
            ) from None

    def _canonical(self, name: str, value: object) -> str:
        if value is None or value is msgspec.UNSET:
            raise BoundaryValidationError(
                f"{self.model}: the business key field {name!r} has no value",
                ((name, "is required"),),
            )
        try:
            return canonical_form(value)
        except (TypeError, ValueError) as exc:
            raise BoundaryValidationError(
                f"{self.model}: the business key field {name!r}: {exc}", ((name, str(exc)),)
            ) from exc


def _has(row: object, name: str) -> bool:
    return name in row if isinstance(row, Mapping) else hasattr(row, name)
