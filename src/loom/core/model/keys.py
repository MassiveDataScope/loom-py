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
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum, StrEnum
from typing import Any
from uuid import UUID

import msgspec

from loom.core.errors import BoundaryValidationError

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
        TypeError: If the value's type has no canonical form.
        ValueError: If the value is a naive ``datetime`` or a non-finite ``Decimal``.
    """
    match value:
        case Enum():
            return canonical_form(value.value)
        case bool():
            return "true" if value else "false"
        case str():
            return unicodedata.normalize("NFC", value)
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
            raise TypeError(f"{type(value).__name__} has no canonical form")


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
        raise ValueError("a non-finite Decimal has no canonical form")
    return "0" if value == 0 else format(value.normalize(), "f")


def _canonical_datetime(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("a naive datetime has no canonical form; give it a timezone")
    utc = value.astimezone(UTC).replace(tzinfo=None)
    return f"{utc.isoformat(timespec='microseconds')}Z"


class KeyAlgorithm(StrEnum):
    """The closed set of formulas that turn a business key into an id."""

    UUID5 = "uuid5"
    SHA256 = "sha256"


@dataclass(frozen=True, slots=True)
class KeyHash:
    """The formula that derives a primary key from the canonical form of a business key.

    The canonical strings of the key's values, in order, are encoded as a JSON
    list and hashed: ``uuid5`` returns the UUID text under *namespace*,
    ``sha256`` the hex digest. Build it with :meth:`uuid5` or :meth:`sha256`.
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

    def key_for(self, values: Mapping[str, Any]) -> str:
        """Return the id of the business key whose values are *values*.

        The values are hashed in the order of the mapping, which is the order
        of the business key; a name is not part of the hash.

        Raises:
            BoundaryValidationError: If a value is ``None``, or has no
                canonical form.
        """
        canonical = [self._canonical(name, value) for name, value in values.items()]
        encoded = msgspec.json.encode(canonical)
        if self.algorithm is KeyAlgorithm.UUID5 and self.namespace is not None:
            return str(uuid.uuid5(self.namespace, encoded.decode()))
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _canonical(name: str, value: object) -> str:
        if value is None:
            raise BoundaryValidationError(
                f"business key field {name!r} has no value", ((name, "is required"),)
            )
        try:
            return canonical_form(value)
        except (TypeError, ValueError) as exc:
            raise BoundaryValidationError(
                f"business key field {name!r}: {exc}", ((name, str(exc)),)
            ) from exc


@dataclass(frozen=True, slots=True)
class BusinessKey:
    """The fields that identify one entity at the model's grain; unique over all of them.

    History rows include their time or version field, events their timestamp,
    multi-tenant tables their tenant. Declare it as ``__business_key__`` on the
    model. Startup refuses an unknown or nullable field.

    Raises:
        ValueError: If no field is named, or one is named twice.
    """

    fields: tuple[str, ...]

    def __init__(self, *fields: str) -> None:
        if not fields:
            raise ValueError("BusinessKey needs at least one field")
        if len(set(fields)) != len(fields):
            raise ValueError(f"BusinessKey repeats a field: {', '.join(fields)}")
        object.__setattr__(self, "fields", fields)


@dataclass(frozen=True, slots=True)
class KeyMismatch:
    """A row whose id is not the one its business key derives."""

    row: Mapping[str, Any]
    expected: str


@dataclass(frozen=True, slots=True)
class DerivedKey:
    """The primary-key field of a model and the formula that fills it from its business key."""

    model: str
    field: str
    business_key: BusinessKey
    key_hash: KeyHash

    def id_for(self, values: Mapping[str, Any]) -> str:
        """Return the id *values* derive, reading the business-key fields from it.

        Raises:
            BoundaryValidationError: If a business-key field is missing or has
                no canonical form.
        """
        return self.key_hash.key_for({name: values.get(name) for name in self.business_key.fields})

    def complete(self, values: dict[str, Any]) -> dict[str, Any]:
        """Return *values* with the derived id in the key field.

        Raises:
            BoundaryValidationError: If *values* carries an id other than the
                derived one, or a business-key field is missing.
        """
        derived = self.id_for(values)
        supplied = values.get(self.field)
        if supplied is not None and str(supplied) != derived:
            fields = ", ".join(self.business_key.fields)
            raise BoundaryValidationError(
                f"{self.model}.{self.field} is derived from ({fields}) with "
                f"{self.key_hash.label}; the id {supplied!r} does not match {derived!r}",
                ((self.field, f"must be {derived}"),),
            )
        return {**values, self.field: derived}

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
