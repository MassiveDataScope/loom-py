"""What a Lambda entry point refuses to compose, so nothing it cannot serve disappears silently."""

from __future__ import annotations

from loom.core.config.errors import ConfigError


class LambdaUnservedConfigError(ConfigError):
    """Raised when the configuration declares something a Lambda adapter does not serve.

    Raised by the recipe, before anything opens.

    Attributes:
        key: Where the unserved declaration lives: a configuration key, or
            ``manifest.AGENTS`` for the agents a manifest module declares.
    """

    def __init__(self, key: str, message: str) -> None:
        super().__init__(f"{key}: {message}")
        self.key = key
