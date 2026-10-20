from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar, cast

T = TypeVar("T", bound=type[object])
F = TypeVar("F", bound=Callable[..., object])


def cached(cls: T) -> T:
    """Declarative marker for repositories that support cache wrapping."""
    cls.__cache_policy__ = True  # type: ignore[attr-defined]
    return cls


def declares_cache_policy(cls: type) -> bool:
    """Return whether *cls* was marked with :func:`cached`."""
    return bool(getattr(cls, "__cache_policy__", False))


def cache_query(
    *,
    scope: str = "list",
    ttl_key: str | None = None,
) -> Callable[[F], F]:
    """Declarative marker for custom repository read methods.

    Annotate the return type.  The wrapper derives a codec from it and applies
    it to the cached read and to the fresh one alike, so a hit and a miss
    return the same type; the supported grammar is a :class:`msgspec.Struct`,
    a scalar, or a ``list``, ``tuple`` or optional of those.  A missing return
    annotation or one outside the grammar — a mapping, a generic container,
    ``Any``, a forward reference the defining module cannot resolve — makes
    wrapping the repository raise ``RuntimeError``, naming the method.

    The declared type is what the caller gets, on the fresh call as on the
    cached one: a method annotated ``-> Stats`` that returns a subclass of
    ``Stats`` hands back a narrowed ``Stats``, and the fields the subclass
    added are dropped.  Declare the type you mean to return.

    A method that returns ``None`` is not cached: the backend cannot tell a
    stored ``None`` from a miss, so the read runs again next time.  A cached
    payload that no longer fits the declared type — an older deployment wrote
    it, and the type has since gained a field — is treated as a miss and
    overwritten, not raised to the caller.

    Treat the returned value as immutable.  Concurrent callers that miss
    together are served the *same object* by the coalesced load, while a
    caller served from the cache gets a freshly decoded one, so mutating a
    result makes the two paths disagree.  Return a struct, or a fresh copy.

    ``scope="entity"`` requires the model's primary key as the first
    positional argument (a keyword argument does not count); the wrapper
    raises ``TypeError`` otherwise, before touching the cache backend.  When
    the primary-key type resolves to a plain class the argument must be of
    that exact type — a ``datetime`` for a ``date`` key or a ``bool`` for an
    ``int`` key is rejected — while a key declared ``int | None`` resolves
    no class and gets no call-time validation.  A read keyed by any other
    field is a list-scoped read.

    Args:
        scope: ``"entity"`` for a single-entity read, ``"list"`` otherwise;
            decides which tags invalidate the entry and which TTL applies.
        ttl_key: Entity name whose TTL override applies, when the method
            caches something other than its own entity.

    Returns:
        The decorator that marks the method.
    """

    def decorator(func: F) -> F:
        func.__cache_query__ = {"scope": scope, "ttl_key": ttl_key}  # type: ignore[attr-defined]
        return func

    return decorator


@dataclass(frozen=True, slots=True)
class _CallPolicy:
    """What a ``@cache_call`` coroutine declares about its own caching.

    Attributes:
        ttl_key: Key whose ``ttl:`` override applies, or ``None`` for the
            configured default TTL.
        unless: Predicate over the result; truthy means store nothing.
        version: Bumped by the caller to invalidate every existing entry of
            this function, which nothing else can invalidate.
    """

    ttl_key: str | None
    unless: Callable[[Any], bool] | None
    version: int


def cache_call(
    *,
    ttl_key: str | None = None,
    unless: Callable[[Any], bool] | None = None,
    version: int = 1,
) -> Callable[[F], F]:
    """Mark a coroutine whose result a bound ``CachedCalls`` may store.

    The decorator only declares: it writes the policy on the function and
    returns the very same object, so the module imports with no configuration
    and the coroutine stays importable and unit-testable on its own. The
    composition root binds it later.

    The coroutine must be a **pure function of its arguments**: it may not read
    ambient identity — a contextvar tenant, a caller's credential — and may not
    hold a caller-scoped session. The key sees only the arguments, and the load
    is detached into its own task, so a coroutine that reads ambient state
    serves one caller's answer to another.

    A cached call is a TTL cache with no invalidation: unlike a repository read
    it carries no dependency tags, because loom cannot know what a coroutine
    depends on. It expires, or the caller bumps *version*.

    Args:
        ttl_key: Key whose ``ttl:`` override applies. It shares the namespace
            with entity TTLs, so a key equal to an entity name deliberately
            shares that entity's override.
        unless: Predicate over the result; truthy means the result is returned
            and nothing is stored. An empty answer from a rate-limited service
            is the case it exists for.
        version: Bump to invalidate every entry this function already wrote.

    Returns:
        The decorator that marks the coroutine.

    Raises:
        TypeError: The decorated object is not a coroutine function. A cached
            call is awaited once and its single result is stored, which a
            plain function, a generator and an async generator cannot honour.
    """

    def decorator(func: F) -> F:
        if not inspect.iscoroutinefunction(func):
            raise TypeError(
                f"@cache_call requires a coroutine function; {func.__qualname__} is not one"
            )
        func.__cache_call__ = _CallPolicy(ttl_key, unless, version)  # type: ignore[attr-defined]
        # ``iscoroutinefunction`` narrows the callable and erases ``F``.
        return cast(F, func)

    return decorator


def declares_cache_call(func: object) -> _CallPolicy | None:
    """Return the policy *func* was marked with by :func:`cache_call`.

    Args:
        func: Any callable, marked or not.

    Returns:
        The declared policy, or ``None`` when *func* carries none.
    """
    policy = getattr(func, "__cache_call__", None)
    return policy if isinstance(policy, _CallPolicy) else None
