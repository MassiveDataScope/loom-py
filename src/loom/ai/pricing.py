"""Model prices a deployment declares under ``ai.prices``, keyed by model id."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Final

from loom.core.model import LoomFrozenStruct

__all__ = ["ModelPrice"]

_TOKENS_PER_UNIT: Final = Decimal(1_000_000)


class ModelPrice(LoomFrozenStruct, frozen=True, kw_only=True):
    """What one model costs, in USD per million tokens.

    A configured price takes precedence over any price the engine knows for
    the same model, both for ``policies.max_usd`` and for ``AgentUsage.cost``.

    Attributes:
        input: Rate of an uncached input token.
        output: Rate of an output token.
        cache_read: Rate of an input token read from the provider's cache;
            ``None`` bills it at ``input``.
        cache_write: Rate of an input token written to the provider's cache;
            ``None`` bills it at ``input``.
        source: Where the rates were taken from, for the audit trail.
        as_of: When the rates were read from ``source``.

    Raises:
        ValueError: When a rate is negative; a ``msgspec`` decode reports it
            as a ``ValidationError``.
    """

    input: Decimal
    output: Decimal
    cache_read: Decimal | None = None
    cache_write: Decimal | None = None
    source: str | None = None
    as_of: date | None = None

    def __post_init__(self) -> None:
        rates = (self.input, self.output, self.cache_read, self.cache_write)
        if any(rate is not None and rate < 0 for rate in rates):
            raise ValueError("a model price cannot be negative")

    def cost(
        self,
        *,
        input_tokens: int,
        output_tokens: int,
        cache_read_tokens: int = 0,
        cache_write_tokens: int = 0,
    ) -> Decimal:
        """Return the USD cost of one usage.

        Args:
            input_tokens: Every input token, cached ones included.
            output_tokens: Output tokens.
            cache_read_tokens: Input tokens read from the provider's cache.
            cache_write_tokens: Input tokens written to the provider's cache.

        Returns:
            The cost, unrounded.
        """
        uncached = input_tokens - cache_read_tokens - cache_write_tokens
        cache_read = self.input if self.cache_read is None else self.cache_read
        cache_write = self.input if self.cache_write is None else self.cache_write
        total = (
            uncached * self.input
            + output_tokens * self.output
            + cache_read_tokens * cache_read
            + cache_write_tokens * cache_write
        )
        return total / _TOKENS_PER_UNIT
