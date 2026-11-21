# Business keys and derived keys

A **business key** is the set of fields that identify one entity at the grain of its model.
It is always unique over all its fields. The grain decides what belongs in it: a history or
SCD2 row includes its time or version field, an event includes its timestamp, a multi-tenant
table includes its tenant.

A **derived key** is a primary key computed from the business key with a fixed formula, so the
same business key gives the same id in every store and in every system that applies the formula.

## Which one to use

| Business key is... | Declare | The id is | Duplicates are stopped by |
|---|---|---|---|
| immutable, and writers may not know the id | `BusinessKey` + `ColumnField(derived=KeyHash...)` | a pure function of the key | the primary key |
| mutable, or the id must stay stable when it changes | `BusinessKey` + a random id | whatever you supply | a `UNIQUE` constraint (SQL stores) |

Derive only when the business key can never change: the id is the key's hash, so changing a
key field would orphan every reference to the row. loom refuses such an update for a model that
derives its id. If the key can change, keep a random id and let the `UNIQUE` constraint enforce it.

```python
import uuid

from loom.core.model import BaseModel, BusinessKey, ColumnField, KeyHash

LISTINGS_NS = uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")


class Listing(BaseModel):
    __tablename__ = "listings"
    __business_key__ = BusinessKey("portal", "external_id")

    id: str = ColumnField(primary_key=True, derived=KeyHash.uuid5(LISTINGS_NS))
    portal: str = ColumnField()
    external_id: str = ColumnField()
```

`KeyHash.uuid5(namespace)` and `KeyHash.sha256()` are the only formulas; the namespace is part
of the formula, so keep it constant forever. The model refuses at startup an unknown field, a
nullable field, an empty key, a field whose type has no canonical form, a derived field that is
not the single `str` primary key, and a field named `key_for`.

## Who computes the id

**The model.** A model that derives its id computes it when it is built (constructor, `msgspec`
decode or convert, `make()`), if the id is unset and the model holds every business-key field.
If a field is missing the id stays unset. A supplied id is never recomputed, so a row read from
the database carries its stored id and pays no hashing. The id field is declared `str`: a reader
always sees a `str`, because rows come from the store with their id; only a model built before
its key is complete holds `msgspec.UNSET`. An `__post_init__` of your own keeps working without
calling `super()`: the id is derived first.

```python
Listing(portal="idealista", external_id="123").id   # the derived id
Listing(portal="idealista").id                      # msgspec.UNSET
```

**The repository checks.** At `create` and `create_many` every repository (SQLAlchemy, Mongo,
DynamoDB and `InMemoryRepository`) completes an unset id, verifies that a supplied id equals
`Listing.key_for(row)`, and refuses a row still missing a business-key field, naming the model
and the field. A mismatch stores nothing. An update that writes a business-key field is refused,
even with an equal value.

Identity must be known at creation: every business-key field has a value before the row is
written. A field the store sets after the insert cannot feed the key, so startup refuses a
business-key field of a derived model that has `server_default`, `server_onupdate` or
`autoincrement` (`CreatedAtField()` and `UpdatedAtField()` included). Keep two times apart:
`created_at` is audit, set by loom or the store after the fact; `observed_at` is business time,
set by the application, and may be part of a business key.

`Listing.key_for(row)` returns the id for a model, a command, any object with those attributes,
or a mapping such as an ETL row with extra columns; only the business-key fields are read, in
the declared order.

A business-key field reassigned in memory after construction (possible with a model declared
`frozen=False`, or with `msgspec.structs.force_setattr`) leaves a stale id. The repository catches
it at create, and writes outside loom do not, so use `verify_keys` for those and prefer frozen
models, which are the default, when deriving.

## Canonical form

Each business-key value becomes one canonical string; the list of strings is encoded as JSON and
hashed. The owner of the rule is `loom.core.model.keys.canonical_form`.

| Type | Canonical form |
|---|---|
| `str` | as given, Unicode NFC. No case folding or trimming: normalise before writing if you need it |
| `int` | decimal |
| `bool` | `true` or `false` |
| `Enum`, `StrEnum` | its value, canonicalised by its type |
| `Decimal` | `format(value.normalize(), "f")`, so `1.10` and `1.1` agree; `0` for any zero; NaN and infinity are refused |
| `datetime` | timezone-aware only, converted to UTC, ISO 8601 with microseconds and `Z`; a naive datetime is refused |
| `date` | ISO 8601 |
| `UUID` | lowercase, hyphenated |

Any other type, `float` included, is refused at startup. `uuid5` hashes the JSON text under the
namespace; `sha256` returns the hex digest of its UTF-8 bytes.

## Uniqueness, store by store

- **SQLAlchemy**: a `UNIQUE` constraint over the business key, with or without a derived id.
  `create_many(on_conflict=IGNORE)` skips the rows whose business key exists.
- **Mongo**: loom does not manage indexes. Create a unique index over the business key fields
  yourself; a derived id is already unique through `_id`.
- **DynamoDB**: only the primary key is unique, so a `BusinessKey` without a derived key is
  refused at startup.

## Limits

Writes that bypass the repository (ETL `IntoTable`, streaming, raw SQL) do not derive keys yet.
Compute the id with `Model.key_for(row)` before writing, and check data with
`verify_keys(Listing, rows)`, which returns the rows whose id does not match, with the id they
should have.

## Changing a business key

Redefining a business key, its fields, their order, the formula or the namespace, changes every
derived id. It is an application migration: recompute the ids with `Model.key_for` (under the old
definition and under the new one), rewrite the references, then use `verify_keys` to prove the
result.

## Separate timestamps

`TimestampedModel` declares `created_at` and `updated_at`. A model that needs only one declares
it with `CreatedAtField()` or `UpdatedAtField()`. The SQL stores set them in the database; Mongo
and DynamoDB set them in the repository.
