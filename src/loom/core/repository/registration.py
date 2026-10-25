from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast, get_origin

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.model import BaseModel, LoomStruct
from loom.core.repository.abc.repo_for import (
    BulkCreatable,
    Countable,
    Creatable,
    Deletable,
    Listable,
    Readable,
    Updatable,
)
from loom.core.repository.registry import (
    DefaultRepositoryBuilder,
    RepositoryBuildContext,
    RepositoryRegistration,
    RepositoryToken,
    get_repository_registration,
)

_UNBUILT = object()

_STANDARD_PROTOCOLS: tuple[Any, ...] = (
    Readable,
    Creatable,
    BulkCreatable,
    Updatable,
    Deletable,
    Listable,
    Countable,
)


class RepositoryDecorator(Protocol):
    """Binding key for the hook applied to every repository this module builds.

    A single slot: registering a second decorator replaces the first, it
    does not compose.  The provider looks it up when it runs, so the order
    in which the decorator and the repository module reach the container
    does not matter.
    """

    def __call__(self, repository: Any) -> Any: ...


def is_standard_capability(annotation: object) -> bool:
    """Return whether *annotation* is a parametrised standard capability.

    Args:
        annotation: A type annotation such as ``Listable[Order]``.

    Returns:
        ``True`` for ``Protocol[Model]`` whose origin is one of the standard
        ``-able`` capability protocols; ``False`` for anything else, including
        the bare protocol and ``RepoFor[Model]``.
    """
    return get_origin(annotation) in _STANDARD_PROTOCOLS


def capabilities_of(repository_type: type) -> tuple[type, ...]:
    """Return the standard capability protocols *repository_type* implements.

    The single capability source: the auto-CRUD gate mounts the operations
    these protocols cover, and the registration module binds the matching DI
    keys.

    Args:
        repository_type: The concrete repository class to inspect.

    Returns:
        The protocols found in the class MRO, in the stable order of the
        standard capability list, which the auto-CRUD gate relies on.
    """
    mro = set(repository_type.__mro__)
    return tuple(proto for proto in _STANDARD_PROTOCOLS if proto in mro)


def _capability_keys(repository_type: type, model: type[LoomStruct]) -> tuple[Any, ...]:
    """Return the ``Protocol[model]`` DI keys for the capabilities of *repository_type*."""
    return tuple(cast(Any, proto)[model] for proto in capabilities_of(repository_type))


def is_direct_protocol(cls: type) -> bool:
    """True when *cls* explicitly declares ``Protocol`` as a base.

    Uses the Python internal ``_is_protocol`` marker written on the class's
    own ``__dict__`` — not inherited — so concrete implementations of a
    Protocol are not mistaken for protocol declarations.
    """
    return bool(cls.__dict__.get("_is_protocol"))


def _detect_capability_keys(
    repository_type: type[Any],
    model: type[LoomStruct],
) -> tuple[Any, ...]:
    """Return the DI keys this repository should be registered under.

    Detects two categories:

    * **Standard -able generics** — ``Readable[Model]``, ``Creatable[Model]``,
      etc. — for every standard capability protocol found in the repository's
      MRO.
    * **Custom Protocol bases** — any class in ``repository_type.__orig_bases__``
      that is itself a Protocol (i.e. it carries ``_is_protocol = True`` in its
      own ``__dict__``) and is not one of the standard capabilities.

    Args:
        repository_type: The concrete repository class to inspect.
        model: The Loom struct model the repository is bound to.

    Returns:
        Tuple of DI keys in stable order, with no duplicates.
    """
    keys: list[Any] = list(_capability_keys(repository_type, model))

    for base in getattr(repository_type, "__orig_bases__", ()):
        origin: Any = getattr(base, "__origin__", base)
        if (
            isinstance(origin, type)
            and is_direct_protocol(origin)
            and origin not in _STANDARD_PROTOCOLS
        ):
            keys.append(origin)

    return tuple(dict.fromkeys(keys))


def _default_capability_keys(
    model: type[LoomStruct],
    default_repository_type: type | None,
) -> tuple[Any, ...]:
    """Return the capability keys of a default-built repository for *model*.

    Called only for ``BaseModel`` types that have no explicit registration.
    Without a default repository type (a backend serving no repositories)
    no capability is bound.
    """
    if default_repository_type is None:
        return ()
    return _capability_keys(default_repository_type, model)


@dataclass(frozen=True)
class _RepositoryBindingSpec:
    registration: RepositoryRegistration | None
    capability_keys: tuple[Any, ...]
    binding_key: object


def _build_repository_specs(
    *,
    models: Sequence[type[BaseModel]],
    explicit_models: Sequence[type[LoomStruct]],
    default_repository_type: type | None,
) -> dict[type[LoomStruct], _RepositoryBindingSpec]:
    repository_specs: dict[type[LoomStruct], _RepositoryBindingSpec] = {}

    for model in set(explicit_models) | set(models):
        registration = get_repository_registration(model)
        if registration is not None:
            repository_specs[registration.model] = _registered_repository_spec(registration)

    for model in models:
        if model not in repository_specs:
            repository_specs[model] = _default_repository_spec(model, default_repository_type)

    return repository_specs


def _registered_repository_spec(
    registration: RepositoryRegistration,
) -> _RepositoryBindingSpec:
    capability_keys = _detect_capability_keys(registration.repository_type, registration.model)
    binding_key = capability_keys[0] if capability_keys else registration.repository_type
    return _RepositoryBindingSpec(
        registration=registration,
        capability_keys=capability_keys,
        binding_key=binding_key,
    )


def _default_repository_spec(
    model: type[LoomStruct],
    default_repository_type: type | None,
) -> _RepositoryBindingSpec:
    return _RepositoryBindingSpec(
        registration=None,
        capability_keys=_default_capability_keys(model, default_repository_type),
        binding_key=RepositoryToken(model),
    )


def _build_repository_provider(
    *,
    container: LoomContainer,
    model: type[LoomStruct],
    registration: RepositoryRegistration | None,
    build_registered_repository: Callable[[RepositoryBuildContext, RepositoryRegistration], Any],
    default_builder: Callable[[RepositoryBuildContext], Any] | None,
) -> Callable[[], Any]:
    """Return the provider shared by every DI key of *model* this module registers.

    The first call builds the repository, applies the registered
    :class:`RepositoryDecorator` if any, and keeps the result, so each key
    this module registers resolves one instance and the decorator runs once
    per model per container.  Keys registered beforehand are left untouched,
    so a pre-registered primary key makes ``resolve_repo`` and the capability
    keys resolve different objects.
    """
    context = RepositoryBuildContext(model=model, container=container)
    memo: Any = _UNBUILT

    def _build() -> Any:
        if registration is not None:
            return build_registered_repository(context, registration)
        if default_builder is not None:
            return default_builder(context)
        if not issubclass(model, BaseModel):
            raise RuntimeError(
                f"No default repository can be built for non-persistible type {model.__qualname__}"
            )
        builder = container.resolve(DefaultRepositoryBuilder)
        return builder(context)

    def _decorate(repository: Any) -> Any:
        if not container.is_registered(RepositoryDecorator):
            return repository
        decorator = container.resolve(RepositoryDecorator)
        return decorator(repository)

    def _provider() -> Any:
        nonlocal memo
        if memo is _UNBUILT:
            memo = _decorate(_build())
        return memo

    return _provider


def _register_repository_binding(
    *,
    container: LoomContainer,
    model: type[LoomStruct],
    capability_keys: tuple[Any, ...],
    binding_key: object,
    provider: Callable[[], Any],
) -> None:
    if not container.is_registered(binding_key):
        container.register(binding_key, provider, scope=Scope.APPLICATION)
    container.register_repo(model, binding_key)
    for key in capability_keys:
        if not container.is_registered(key):
            container.register(key, provider, scope=Scope.APPLICATION)


def build_repository_registration_module(
    *,
    models: Sequence[type[BaseModel]],
    explicit_models: Sequence[type[LoomStruct]] = (),
    build_registered_repository: Callable[[RepositoryBuildContext, RepositoryRegistration], Any],
    default_repository_type: type | None = None,
    default_builder: Callable[[RepositoryBuildContext], Any] | None = None,
) -> Callable[[LoomContainer], None]:
    """Build a container module that registers main repositories.

    Explicit registrations declared with ``repository_for(...)`` are preferred.
    Persistible ``BaseModel`` types without an explicit registration use
    ``default_builder`` when one is passed, and the container's
    ``DefaultRepositoryBuilder`` when it is not. The passed builder is not
    installed in the container.

    Capability DI keys (standard ``-able`` generics and custom Protocol bases)
    are auto-detected and registered alongside the primary binding key so that
    use cases can declare fine-grained capability dependencies without casts.
    Default-built repositories bind the capabilities of
    ``default_repository_type``; ``None`` binds none.
    """
    repository_specs = _build_repository_specs(
        models=models,
        explicit_models=explicit_models,
        default_repository_type=default_repository_type,
    )

    def register(container: LoomContainer) -> None:
        for model, spec in repository_specs.items():
            provider = _build_repository_provider(
                container=container,
                model=model,
                registration=spec.registration,
                build_registered_repository=build_registered_repository,
                default_builder=default_builder,
            )
            _register_repository_binding(
                container=container,
                model=model,
                capability_keys=spec.capability_keys,
                binding_key=spec.binding_key,
                provider=provider,
            )

    return register
