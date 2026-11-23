import sys
import typing
from typing import TYPE_CHECKING, Any, ClassVar

import msgspec
from msgspec import UNSET, UnsetType

from loom.core.errors import InvalidType
from loom.core.model.field import ColumnFieldSpec
from loom.core.model.introspection import derived_key_of
from loom.core.model.projection import Projection
from loom.core.model.relation import Relation
from loom.core.model.struct import LoomStruct

if sys.version_info >= (3, 14):
    import annotationlib

if TYPE_CHECKING:

    class _StructMeta(type):
        pass
else:
    _StructMeta = type(msgspec.Struct)


_KEY_FOR = "key_for"
_DERIVES_KEY = "__derives_key__"


def _set_derived_id(model: Any) -> None:
    """Normalise the business-key text of *model*, then fill its derived id when it is unset."""
    derived = derived_key_of(type(model))
    if derived is None:
        return
    derived.normalize_text(model)
    if getattr(model, derived.field) is not UNSET:
        return
    if derived.has_every_field(model):
        msgspec.structs.force_setattr(model, derived.field, derived.id_for(model))


def _deriving_post_init(application_post_init: Any) -> Any:
    """Build the ``__post_init__`` that derives the id, then runs the application's own."""

    def __post_init__(self: Any) -> None:
        _set_derived_id(self)
        if application_post_init is not None:
            application_post_init(self)

    return __post_init__


def _inherited_post_init(bases: tuple[type, ...]) -> Any:
    return next((base.__post_init__ for base in bases if hasattr(base, "__post_init__")), None)


def _resolve_annotation(annotation: Any, namespace: dict[str, Any]) -> Any:
    """Resolve a stringified annotation to its actual type."""
    if not isinstance(annotation, str):
        return annotation
    holder = type("_AnnotationHolder", (), {"__annotations__": {"annotation": annotation}})
    eval_ns = {"typing": typing, **namespace}
    hints = typing.get_type_hints(holder, globalns=eval_ns, localns=eval_ns, include_extras=True)
    return hints["annotation"]


def _take_class_body_annotations(namespace: dict[str, Any]) -> dict[str, Any]:
    if "__annotations__" in namespace:
        annotations: dict[str, Any] = namespace["__annotations__"]
        return annotations
    if sys.version_info >= (3, 14):
        annotate = annotationlib.get_annotate_from_class_namespace(namespace)
        if annotate is not None:
            namespace.pop("__annotate__", None)
            namespace.pop("__annotate_func__", None)
            return annotationlib.call_annotate_function(annotate, annotationlib.Format.VALUE)
    return {}


def _build_eval_namespace(namespace: dict[str, Any]) -> dict[str, Any]:
    """Build annotation evaluation namespace from module globals + class locals."""
    eval_ns: dict[str, Any] = {}
    module = namespace.get("__module__")
    if module:
        mod = sys.modules.get(module)
        if mod:
            eval_ns.update(vars(mod))
    eval_ns.update(namespace)
    return eval_ns


def _resolve_column_default(spec: ColumnFieldSpec) -> Any:
    """Resolve runtime default assigned to a model field in class namespace."""
    if spec.field.default is not msgspec.UNSET:
        return spec.field.default
    if spec.field.primary_key and spec.field.autoincrement:
        return None
    return UNSET


def _mark_optional_unset(
    attr_name: str,
    annotations: dict[str, Any],
    eval_ns: dict[str, Any],
) -> None:
    """Widen annotation to include ``UnsetType`` for lazy-loaded fields."""
    if attr_name not in annotations:
        return
    resolved = _resolve_annotation(annotations[attr_name], eval_ns)
    annotations[attr_name] = resolved | UnsetType


class LoomStructMeta(_StructMeta):
    """Metaclass that intercepts ``Relation`` and ``Projection`` assignments
    before ``StructMeta`` processes the class body.

    Each intercepted attribute is replaced with ``UNSET`` as its default,
    and the type annotation is widened to ``T | UnsetType`` so that
    ``omit_defaults=True`` strips unloaded fields from serialisation output.
    """

    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> Any:
        kwargs.setdefault("frozen", True)
        kwargs.setdefault("kw_only", True)
        kwargs.setdefault("omit_defaults", True)
        kwargs.setdefault("rename", "camel")

        columns: dict[str, ColumnFieldSpec] = {}
        relations: dict[str, Relation] = {}
        projections: dict[str, Projection] = {}
        annotations = _take_class_body_annotations(namespace)
        if _KEY_FOR in annotations:
            raise InvalidType(
                f"{name}.{_KEY_FOR} cannot be a field: it is the method that derives the key"
            )

        eval_ns = _build_eval_namespace(namespace)

        defaults: dict[str, Any] = {}
        for attr_name, value in namespace.items():
            if isinstance(value, ColumnFieldSpec):
                columns[attr_name] = value
                defaults[attr_name] = _resolve_column_default(value)
                continue

            if isinstance(value, Relation):
                relations[attr_name] = value
            elif isinstance(value, Projection):
                projections[attr_name] = value
            else:
                continue

            defaults[attr_name] = UNSET
            _mark_optional_unset(attr_name, annotations, eval_ns)

        derives_here = any(spec.field.derived is not None for spec in columns.values())
        derives_inherited = any(getattr(base, _DERIVES_KEY, False) for base in bases)
        own_post_init = namespace.get("__post_init__")
        if derives_here or (derives_inherited and own_post_init is not None):
            namespace["__post_init__"] = _deriving_post_init(
                own_post_init or _inherited_post_init(bases)
            )

        namespace.update(defaults)
        namespace["__annotations__"] = annotations
        struct_cls: Any = super().__new__(cls, name, bases, namespace, **kwargs)
        struct_cls.__loom_columns__ = columns
        struct_cls.__loom_relations__ = relations
        struct_cls.__loom_projections__ = projections
        setattr(struct_cls, _DERIVES_KEY, derives_here or derives_inherited)
        return struct_cls


class BaseModel(LoomStruct, metaclass=LoomStructMeta):  # type: ignore[metaclass]
    """Base for all loom domain models.

    Subclasses must declare ``__tablename__`` and provide typed attributes.
    Column metadata is optional via ``ColumnField(...)``. ``__store__`` names
    the store holding the model, ``default`` when left out
    (:func:`loom.core.stores.catalog.store_of`).
    """

    __tablename__: ClassVar[str]
    __store__: ClassVar[str]

    @classmethod
    def key_for(cls, row: object) -> str:
        """Return the id the model derives from the business key of *row*.

        *row* is a model instance, any object with the business-key fields as
        attributes (a command, a ``Struct``) or a mapping (an ETL row); the
        model reads only its business-key fields, in the declared order, and
        ignores the rest.

        Raises:
            InvalidType: If the model derives no key.
            BoundaryValidationError: If a business-key field is missing, has
                no value or has no canonical form.
        """
        derived = derived_key_of(cls)
        if derived is None:
            raise InvalidType(f"{cls.__name__} derives no key")
        return derived.id_for(row)
