"""Where a grant applies: a path of segments that covers every path below it."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

_SEPARATOR = "/"


@dataclass(frozen=True, slots=True, order=True, init=False)
class Scope:
    """A node in the product's resource hierarchy.

    loom gives the segments no meaning: the product decides what each level
    is.  A scope covers itself and everything below it, so a grant on
    ``Scope.of("acme")`` reaches ``Scope.of("acme", "sales", "orders")`` but
    never ``Scope.of("globex")`` nor the root.

    ``str(scope)`` and :meth:`parse` are inverse and stable, so the text form
    is safe to store.

    Attributes:
        path: Segments from the root down; empty for the root.

    Example::

        workspace = Scope.of("acme")
        table = workspace.child("sales", "orders")
        assert workspace.covers(table)
        assert Scope.parse(str(table)) == table
    """

    path: tuple[str, ...]

    def __init__(self, path: Iterable[str]) -> None:
        """Create the scope at *path*.

        Args:
            path: Segments from the root down; empty for the root.

        Raises:
            TypeError: When *path* is a single string or holds a non-string.
            ValueError: When a segment is empty or contains ``/``.
        """
        if isinstance(path, str):
            raise TypeError(
                "A scope path is a sequence of segments, not a string; use Scope.parse()."
            )
        segments = tuple(path)
        for segment in segments:
            if not isinstance(segment, str):
                raise TypeError(f"A scope segment must be a string: {segment!r}.")
            if not segment or _SEPARATOR in segment:
                raise ValueError(
                    f"A scope segment must be non-empty and free of {_SEPARATOR!r}: {segment!r}."
                )
        object.__setattr__(self, "path", segments)

    @classmethod
    def root(cls) -> Scope:
        """Return the scope that covers every other scope.

        Returns:
            The root scope.
        """
        return cls(())

    @classmethod
    def of(cls, *segments: str) -> Scope:
        """Build the scope at *segments* below the root.

        Args:
            *segments: One or more path segments, outermost first.

        Returns:
            The scope at that path.

        Raises:
            ValueError: When no segment is given (use :meth:`root`) or a
                segment is empty or contains ``/``.
        """
        if not segments:
            raise ValueError(
                "Scope.of() needs at least one segment; use Scope.root() for the root."
            )
        return cls(segments)

    @classmethod
    def parse(cls, text: str) -> Scope:
        """Read a scope back from its ``str()`` form.

        Args:
            text: ``/`` for the root, or ``/a/b``.

        Returns:
            The scope *text* renders.

        Raises:
            ValueError: When *text* does not start with ``/``, ends with one
                (other than the root) or holds an empty segment.
        """
        if not text.startswith(_SEPARATOR):
            raise ValueError(f"A scope text starts with {_SEPARATOR!r}: {text!r}.")
        if text == _SEPARATOR:
            return cls.root()
        return cls(text[1:].split(_SEPARATOR))

    def child(self, *segments: str) -> Scope:
        """Return the scope at *segments* below this one.

        Args:
            *segments: Path segments to append, outermost first.

        Returns:
            The nested scope.

        Raises:
            ValueError: When a segment is empty or contains ``/``.
        """
        return Scope((*self.path, *segments))

    def covers(self, other: Scope) -> bool:
        """Report whether a grant on this scope reaches *other*.

        Args:
            other: The scope being accessed.

        Returns:
            ``True`` when *other* is this scope or lies below it.
        """
        return other.path[: len(self.path)] == self.path

    def __str__(self) -> str:
        """Render the scope as ``/`` or ``/a/b``; :meth:`parse` reads it back."""
        return _SEPARATOR + _SEPARATOR.join(self.path)
