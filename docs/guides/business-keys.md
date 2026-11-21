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

## Reading and writing

`Listing.key_for(row)` returns the id. `row` may be a model, a command, any object with those
attributes, or a mapping such as an ETL row with extra columns; only the business-key fields are
read, in the declared order.

Every repository (SQLAlchemy, Mongo, DynamoDB and `InMemoryRepository`) fills the id on `create`
and `create_many`. A command that carries an id must carry the derived one, or the create is
refused with an error naming the model, the field and the formula. An update that writes a
business-key field is refused.

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
