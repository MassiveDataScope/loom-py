"""Config value declaration for ETL steps.

:class:`FromConfig` names a key of the config the runner was built from.  The
executor resolves it when the step runs and passes the value to ``execute()``
as a keyword argument named like the class attribute.  It is not a frame
source: no reader is involved and it never becomes a
:class:`~loom.etl.declarative.source.SourceSpec`.
"""

from __future__ import annotations

from loom.core.errors import InvalidValue


class FromConfig:
    """Declare a config value injected into ``execute()``.

    The key is a dot-separated path into the runner's config (the YAML given
    to :meth:`~loom.etl.ETLRunner.from_yaml`), so ``${oc.env:...}``
    interpolations and resolvers such as ``${secrets:...}`` apply.  The value
    is resolved when the step runs, never at import or declaration:
    ``${oc.env:...}`` is read on every resolution, while a resolver's result
    is cached by the config for the lifetime of the runner.  Loom stores the
    value on no plan and writes it to no log or event; a failure names the
    key and the expected type only.

    Args:
        key: Dot-separated config path, e.g. ``"respondio.api_token"``.
        value_type: Type the value is converted to with ``msgspec``: a scalar
            such as ``str`` or ``int``, a ``Literal["a", "b"]``, or a
            ``msgspec.Struct`` for a whole section.  Defaults to ``str``.
            A null or absent key is always reported as not set.

    Raises:
        InvalidValue: When *key* is empty or has an empty segment.

    Example::

        class FetchMessages(ETLStep[DailyParams]):
            api_token = FromConfig("respondio.api_token")
            respondio = FromConfig("respondio", RespondioSettings)
            target = IntoTemp("messages")

            def execute(
                self,
                params: DailyParams,
                *,
                api_token: str,
                respondio: RespondioSettings,
            ) -> pl.LazyFrame: ...
    """

    __slots__ = ("_key", "_value_type")

    def __init__(self, key: str, value_type: object = str) -> None:
        if not key or any(not part for part in key.split(".")):
            raise InvalidValue("FromConfig key must be a dot-separated path")
        self._key = key
        self._value_type = value_type

    @property
    def key(self) -> str:
        """Dot-separated config path."""
        return self._key

    @property
    def value_type(self) -> object:
        """Type the resolved value is converted to."""
        return self._value_type

    def __repr__(self) -> str:
        return f"FromConfig({self._key!r}, {type_label(self._value_type)})"


def type_label(value_type: object) -> str:
    """Return a readable name for *value_type*, without any value.

    Args:
        value_type: A class or a typing construct such as ``Literal[...]``.

    Returns:
        The class name, or the typing construct's ``repr``.
    """
    return value_type.__name__ if isinstance(value_type, type) else repr(value_type)
