"""DynamoDB implementation of the repository contract.

DynamoDB is a key-value / document store, not a relational engine. This
repository implements faithfully only what DynamoDB supports natively through
the item primary key: ``get_by_id``, ``create``, ``create_many``, ``update`` and
``delete``. A lookup by another field, listing and counting need a full table
scan or a secondary index, so the class declares none of ``FieldReadable``,
``Listable`` or ``Countable``: the auto-CRUD gate mounts no list route over it
and the DI container binds no such capability.

A ``ServerDefault.NOW`` field left empty on create and a ``ServerOnUpdate.NOW``
field on every update receive the current UTC time, as in the Mongo repository.

Model-to-table mapping is intentionally simple: every model is stored in the
single table configured under ``stores.<name>.table``. The item primary
key attribute is the model's ``primary_key`` field, resolved via
:func:`loom.core.model.introspection.get_id_attribute`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import Executor
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, ClassVar, Generic, cast

import msgspec

from loom.core.concurrency import run_in_executor
from loom.core.config import ConfigError
from loom.core.connections.aws import aws_error_code
from loom.core.errors import Conflict, InternalError
from loom.core.model.convert import to_struct
from loom.core.model.introspection import (
    business_key_of,
    derived_key_of,
    get_column_fields,
    get_table_name,
)
from loom.core.stores.catalog import store_of
from loom.core.stores.repository import (
    BulkCreatable,
    Creatable,
    Deletable,
    IdT,
    OnConflict,
    OutputT,
    Readable,
    Updatable,
)
from loom.core.stores.repository.record import (
    ServerStamps,
    creation_values,
    refuse_key_change,
    single_id_attribute,
)
from loom.core.stores.uow.coordinator import Participant
from loom.core.stores.uow.scope import InvocationScope, current_scope

_CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailedException"
_CONDITION_FAILED_REASON = "ConditionalCheckFailed"
_NOTHING_WRITTEN = frozenset({_CONDITIONAL_CHECK_FAILED, "TransactionCanceledException"})
_TRANSACTION_LIMIT = 100


class DynamoDBRepository(
    Readable[OutputT],
    Creatable[OutputT],
    BulkCreatable[OutputT],
    Updatable[OutputT],
    Deletable[OutputT],
    Generic[OutputT, IdT],
):
    """Key-value repository backed by a single boto3 low-level ``dynamodb`` client.

    The low-level client (not the resource) is used because it is thread-safe:
    every operation runs on a worker thread, and a shared client
    can be called from multiple worker threads concurrently. Items cross the
    wire as low-level ``AttributeValue`` dicts (``{"S": ...}``, ``{"N": ...}``);
    :class:`boto3.dynamodb.types.TypeSerializer` /
    :class:`~boto3.dynamodb.types.TypeDeserializer` translate them to and from
    plain Python values, preserving the ``float``↔``Decimal`` round-trip
    DynamoDB requires.

    Args:
        client: A boto3 low-level ``dynamodb`` client (or a compatible object
            exposing ``get_item`` / ``put_item`` / ``delete_item`` /
            ``transact_write_items`` with the client-shaped ``TableName`` + ``AttributeValue`` API).
        table_name: Name of the DynamoDB table backing this repository.
        model: The Loom struct model this repository is bound to. Its
            ``primary_key`` field becomes the DynamoDB partition key.
        executor: Threads the blocking client calls run on; ``None`` uses the
            event loop's default executor.

    Raises:
        ConfigError: If the model's primary key is composite, or it declares a
            business key without deriving its primary key from it.
    """

    backend_name: ClassVar[str] = "dynamodb"

    def __init__(
        self, client: Any, table_name: str, model: type, *, executor: Executor | None = None
    ) -> None:
        from boto3.dynamodb.types import (  # type: ignore[import-untyped]
            TypeDeserializer,
            TypeSerializer,
        )
        from botocore.exceptions import ClientError  # type: ignore[import-untyped]

        refuse_business_key_without_hash(model)
        self._client = client
        self._executor = executor
        self._table_name = table_name
        self._model = model
        self._store = store_of(model)
        self._id_attr = single_id_attribute(
            model, why_single="the repository keys each item by one partition key"
        )
        self._column_fields = frozenset(get_column_fields(model).keys())
        self._stamps = ServerStamps.of(model)
        self._serializer = TypeSerializer()
        self._deserializer = TypeDeserializer()
        self._client_error = ClientError

    @property
    def model(self) -> type:
        """Loom model bound to the repository."""
        return self._model

    @property
    def entity_name(self) -> str:
        """Model table name, the cache namespace shared with every backend.

        Distinct from ``table_name``: several models may share one DynamoDB
        table, but each keeps its own cache namespace.
        """
        return get_table_name(self._model)

    async def get_by_id(self, obj_id: IdT, profile: str = "default") -> OutputT | None:
        """Fetch one entity by its partition key via ``GetItem``."""
        response = await run_in_executor(
            self._executor,
            self._client.get_item,
            TableName=self._table_name,
            Key=self._encode_key(obj_id),
        )
        item = response.get("Item")
        if item is None:
            return None
        return self._to_output(self._decode_item(item))

    async def create(self, data: msgspec.Struct) -> OutputT:
        """Persist a new item with a conditional ``PutItem`` (insert, not upsert).

        The ``attribute_not_exists`` condition rejects an overwrite of an
        existing item; a failed condition is surfaced as
        :class:`~loom.core.errors.Conflict`.
        """
        internal = self._creation_item(data)
        await self._put_new(internal)
        return self._to_output(internal)

    async def create_many(
        self, data: Sequence[msgspec.Struct], *, on_conflict: OnConflict = OnConflict.ERROR
    ) -> tuple[OutputT, ...]:
        """Persist new items, each conditioned on its key not existing yet.

        With ``ERROR`` the items are written with ``TransactWriteItems`` in
        chunks of 100: each chunk is atomic, the batch as a whole is not, so
        a conflict in a later chunk leaves the earlier chunks written. A key
        repeated in *data* raises before anything is written. With
        ``IGNORE`` every item is written with its own conditional
        ``PutItem`` and an item whose key exists is skipped. An empty *data*
        returns ``()`` without a call.

        Args:
            data: Command structs to persist.
            on_conflict: Whether an existing key raises (``ERROR``) or skips
                its item (``IGNORE``).

        Returns:
            The written output structs in input order: one per input with
            ``ERROR``, only the written items with ``IGNORE``.

        Raises:
            Conflict: With ``ERROR``, on a key that already exists or is
                repeated in *data*.
        """
        items = [self._creation_item(item) for item in data]
        if on_conflict is OnConflict.IGNORE:
            written = []
            for item in items:
                try:
                    await self._put_new(item)
                except Conflict:
                    continue
                written.append(item)
        else:
            self._refuse_repeated_keys(items)
            for start in range(0, len(items), _TRANSACTION_LIMIT):
                await self._put_all_new(items[start : start + _TRANSACTION_LIMIT])
            written = items
        return tuple(self._to_output(item) for item in written)

    async def update(self, obj_id: IdT, data: msgspec.Struct) -> OutputT | None:
        """Apply a partial update by read-merge-write, keyed on the partition key.

        The write carries an ``attribute_exists`` condition so a concurrent
        delete between the read and the write cannot resurrect the item. Returns
        ``None`` when no item exists for ``obj_id`` — whether detected by the
        initial read or by the conditional write losing the race.
        """
        self._check_write()
        response = await run_in_executor(
            self._executor,
            self._client.get_item,
            TableName=self._table_name,
            Key=self._encode_key(obj_id),
        )
        existing = response.get("Item")
        if existing is None:
            return None
        changes = self._to_internal(data)
        changes.pop(self._id_attr, None)
        refuse_key_change(self._model, changes)
        stamped = msgspec.to_builtins(self._stamps.with_update_stamps(changes, datetime.now(UTC)))
        merged = {**self._decode_item(existing), **stamped}
        try:
            await self._write(
                self._client.put_item,
                TableName=self._table_name,
                Item=self._encode_item(merged),
                ConditionExpression="attribute_exists(#pk)",
                ExpressionAttributeNames={"#pk": self._id_attr},
            )
        except self._client_error as exc:
            if aws_error_code(exc) == _CONDITIONAL_CHECK_FAILED:
                return None
            raise
        return self._to_output(merged)

    async def delete(self, obj_id: IdT) -> bool:
        """Delete an item by partition key, returning whether it existed."""
        try:
            await self._write(
                self._client.delete_item,
                TableName=self._table_name,
                Key=self._encode_key(obj_id),
                ConditionExpression="attribute_exists(#pk)",
                ExpressionAttributeNames={"#pk": self._id_attr},
            )
        except self._client_error as exc:
            if aws_error_code(exc) == _CONDITIONAL_CHECK_FAILED:
                return False
            raise
        return True

    def _new_item_put(self, item: dict[str, Any]) -> dict[str, Any]:
        """Return the ``PutItem`` arguments that write *item* only when its key is absent."""
        return {
            "TableName": self._table_name,
            "Item": self._encode_item(item),
            "ConditionExpression": "attribute_not_exists(#pk)",
            "ExpressionAttributeNames": {"#pk": self._id_attr},
        }

    async def _put_new(self, item: dict[str, Any]) -> None:
        """Write *item* with a ``PutItem`` conditioned on its key not existing.

        Raises:
            Conflict: If an item with the same key already exists.
        """
        try:
            await self._write(self._client.put_item, **self._new_item_put(item))
        except self._client_error as exc:
            if aws_error_code(exc) == _CONDITIONAL_CHECK_FAILED:
                raise self._conflict(item.get(self._id_attr)) from exc
            raise

    async def _put_all_new(self, items: list[dict[str, Any]]) -> None:
        """Write *items* in one transaction that is cancelled when any key exists.

        Raises:
            Conflict: Naming the first key that already exists.
        """
        try:
            await self._write(
                self._client.transact_write_items,
                TransactItems=[{"Put": self._new_item_put(item)} for item in items],
            )
        except self._client_error as exc:
            reasons = exc.response.get("CancellationReasons", [])
            existing = next(
                (
                    item
                    for item, reason in zip(items, reasons, strict=False)
                    if reason.get("Code") == _CONDITION_FAILED_REASON
                ),
                None,
            )
            if existing is not None:
                raise self._conflict(existing.get(self._id_attr)) from exc
            raise

    def _refuse_repeated_keys(self, items: list[dict[str, Any]]) -> None:
        """Raise on the first key *items* carry twice.

        Raises:
            Conflict: Naming the repeated key.
        """
        seen: set[Any] = set()
        for item in items:
            key = item.get(self._id_attr)
            if key in seen:
                raise self._conflict(key)
            seen.add(key)

    def _conflict(self, key: Any) -> Conflict:
        """Build the error for a key that already exists."""
        return Conflict(f"{self._model.__name__} with {self._id_attr}={key!r} already exists.")

    def _check_write(self) -> tuple[InvocationScope | None, Participant | None]:
        """Pass a write through the invocation scope's checkpoint, on the event loop."""
        scope = current_scope()
        if scope is None:
            return None, None
        return scope, scope.participant_for(self._store, self._client, write=True, record=False)

    async def _write(self, call: Callable[..., object], **kwargs: Any) -> None:
        """Run one client write, checked before the call and recorded once it may have landed.

        A failed condition or a cancelled transaction wrote nothing and is
        not recorded; a timeout or any other failure may have written, and is.
        """
        scope, participant = self._check_write()
        landed = True
        try:
            await run_in_executor(self._executor, call, **kwargs)
        except self._client_error as exc:
            landed = aws_error_code(exc) not in _NOTHING_WRITTEN
            raise
        finally:
            if landed and scope is not None and participant is not None:
                scope.record_write(participant)

    def _creation_item(self, data: msgspec.Struct) -> dict[str, Any]:
        """Return the item to store for *data*, keyed by internal field name.

        Every field that is not ``msgspec.UNSET`` is included, defaults too, and
        a ``ServerDefault.NOW`` field left empty holds the current UTC time.
        """
        values = self._stamps.with_create_stamps(
            creation_values(self._model, data), datetime.now(UTC)
        )
        return cast(dict[str, Any], msgspec.to_builtins(values))

    def _to_internal(self, data: msgspec.Struct) -> dict[str, Any]:
        """Serialize a struct to a dict keyed by internal (snake_case) field names."""
        builtins = msgspec.to_builtins(data)
        if not isinstance(builtins, dict):
            raise InternalError("Struct payload must serialize to a dict")
        encoded_to_internal: dict[str, str] = {
            field.encode_name: field.name for field in msgspec.structs.fields(type(data))
        }
        result: dict[str, Any] = {}
        for key, value in builtins.items():
            name = str(key)
            result[encoded_to_internal.get(name, name)] = value
        return result

    def _to_output(self, item: dict[str, Any]) -> OutputT:
        """Build the model output struct from a stored item, ignoring unknown attrs."""
        kwargs = {key: value for key, value in item.items() if key in self._column_fields}
        return cast(OutputT, to_struct(self._model, kwargs))

    def _encode_value(self, value: Any) -> dict[str, Any]:
        """Serialize a plain Python value to a low-level ``AttributeValue`` dict.

        Floats are converted to ``Decimal`` first because DynamoDB — and
        ``TypeSerializer`` — reject native floats.
        """
        return cast("dict[str, Any]", self._serializer.serialize(_floats_to_decimal(value)))

    def _decode_value(self, value: dict[str, Any]) -> Any:
        """Deserialize a low-level ``AttributeValue`` dict back to a Python value.

        DynamoDB numbers arrive as ``Decimal``; they are narrowed back to
        ``int``/``float`` to match what was originally stored.
        """
        return _decimals_to_number(self._deserializer.deserialize(value))

    def _encode_item(self, item: dict[str, Any]) -> dict[str, Any]:
        """Serialize a full item dict to low-level ``AttributeValue`` form."""
        return {key: self._encode_value(value) for key, value in item.items()}

    def _decode_item(self, item: dict[str, Any]) -> dict[str, Any]:
        """Deserialize a full low-level item dict back to plain Python values."""
        return {key: self._decode_value(value) for key, value in item.items()}

    def _encode_key(self, obj_id: Any) -> dict[str, Any]:
        """Build the low-level ``Key`` dict for the partition key.

        The key is converted with :func:`msgspec.to_builtins`, as ``create``
        stores it, so a key such as a ``UUID`` matches its stored form.
        """
        return {self._id_attr: self._encode_value(msgspec.to_builtins(obj_id))}


def refuse_business_key_without_hash(model: type) -> None:
    """Refuse a model whose business key DynamoDB could not keep unique.

    Only the primary key is unique in DynamoDB, so a ``BusinessKey`` needs a
    ``KeyHash`` on the primary key to be enforced.

    Raises:
        ConfigError: If the model declares a business key and derives no id from it.
    """
    if business_key_of(model) is not None and derived_key_of(model) is None:
        raise ConfigError(
            f"{model.__qualname__} declares a business key, but DynamoDB only enforces "
            "its primary key; derive the key with ColumnField(primary_key=True, "
            "derived=KeyHash...)"
        )


def _floats_to_decimal(value: Any) -> Any:
    """Recursively convert ``float`` to ``Decimal`` — DynamoDB rejects floats."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {key: _floats_to_decimal(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_floats_to_decimal(item) for item in value]
    return value


def _decimals_to_number(value: Any) -> Any:
    """Recursively convert DynamoDB ``Decimal`` back to ``int``/``float``."""
    if isinstance(value, Decimal):
        as_int = int(value)
        return as_int if as_int == value else float(value)
    if isinstance(value, dict):
        return {key: _decimals_to_number(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decimals_to_number(item) for item in value]
    return value
