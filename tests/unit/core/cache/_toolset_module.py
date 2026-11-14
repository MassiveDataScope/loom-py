"""Toolset classes as an artifact's own module writes them, for AC12.

They live in a real module, imported normally, because
``from __future__ import annotations`` is what the whole test turns on: every
annotation below is a string only *this* module's globals resolve, while a
wrapper built inside ``loom.core.cache.calls`` carries that module's globals
instead. A class defined inside a test function, or one built by ``exec``,
would not reproduce that.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

import msgspec
from pydantic import Field

from loom.core.cache import cache_call

if TYPE_CHECKING:
    from typing import TypeAlias

    Cursor: TypeAlias = str
    """Visible to a type checker and to nothing else, so a runtime resolution
    of ``resume``'s hints raises ``NameError`` — AC12's unresolvable case."""


class AcmeToolsetDoc(msgspec.Struct):
    """Result type named by annotation, resolvable only from this module."""

    title: str


class SearchTools:
    """Toolset an artifact would publish through a ``kind: python`` factory."""

    @cache_call(ttl_key="web_search")
    async def search(
        self,
        query: str,
        limit: Annotated[int, Field(description="Never more than this many documents.")] = 10,
    ) -> list[AcmeToolsetDoc]:
        """Search the corpus.

        Args:
            query: What to look for.
            limit: How many documents to return.

        Returns:
            The matching documents.
        """
        return [AcmeToolsetDoc(title=f"{query}#{index}") for index in range(limit)]

    async def describe(self, title: str) -> AcmeToolsetDoc:
        """Build a document from a title, uncached because nothing declares it.

        Args:
            title: The title to wrap.

        Returns:
            The document.
        """
        return AcmeToolsetDoc(title=title)


class CursorTools:
    """Toolset whose second method no runtime resolution can type."""

    @cache_call()
    async def latest(self, limit: int = 5) -> list[AcmeToolsetDoc]:
        """Return the most recent documents.

        Args:
            limit: How many documents to return.

        Returns:
            The most recent documents.
        """
        return [AcmeToolsetDoc(title=str(index)) for index in range(limit)]

    @cache_call()
    async def resume(self, cursor: Cursor) -> list[AcmeToolsetDoc]:
        """Continue a search from a cursor whose type only a type checker sees.

        Args:
            cursor: Opaque position in a previous result set.

        Returns:
            The next documents.
        """
        return [AcmeToolsetDoc(title=cursor)]
