"""Deploy check for a password written inside a configuration URL.

The check reads the document as written and resolves nothing. A placeholder
stays text, so it is not judged. The finding names the key and never the value.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from loom.core.checks import Finding, deploy_check
from loom.core.model.redaction import mask_dsn

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

EMBEDDED_PASSWORD_CHECK: Final = "loom.config.W001"
"""Id of the check warning about a password embedded in a configuration URL."""

_PLACEHOLDER: Final = "${"


@deploy_check(EMBEDDED_PASSWORD_CHECK)
def _embedded_password(config: ConfigContext) -> Finding | None:
    paths = _password_urls(_document(config), "")
    if not paths:
        return None
    named = ", ".join(paths)
    return Finding(
        message=f"{named} embeds a password in a URL.",
        hint="move the password to a password field and leave it out of the URL.",
    )


def _document(config: ConfigContext) -> object:
    """Return the loaded document unresolved.

    ``raw`` reads one key. This check has to see every key, still without
    calling a resolver.
    """
    from omegaconf import OmegaConf

    return OmegaConf.to_container(config._config, resolve=False)


def _password_urls(node: object, path: str) -> list[str]:
    if isinstance(node, str):
        if _PLACEHOLDER in node or not path:
            return []
        if mask_dsn(node) != node:
            return [path]
        return []
    if isinstance(node, dict):
        found: list[str] = []
        for key, item in node.items():
            child = f"{path}.{key}" if path else str(key)
            found.extend(_password_urls(item, child))
        return found
    if isinstance(node, list):
        found = []
        for index, item in enumerate(node):
            found.extend(_password_urls(item, f"{path}[{index}]"))
        return found
    return []
