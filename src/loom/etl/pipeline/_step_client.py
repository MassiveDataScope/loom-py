"""Client-mode ETL step base type.

:class:`ClientStep` is a specialisation of :class:`~loom.etl.ETLStep` for steps
that execute engine commands directly via the native client — DDL statements,
maintenance operations, partition management, and similar side-effect-only work.

Unlike a regular step, a ``ClientStep``:

* declares **no sources** — there are no DataFrame frames to read.
* declares **no frame output** — its target is always :class:`~loom.etl.IntoClient`,
  which signals the executor to skip the normal read-execute-write path.
* receives the engine client as a ``client`` keyword argument in ``execute()``.

The concrete client type depends on the backend configured in the runner:

* ClickHouse runner → ``clickhouse_connect.driver.Client`` (native HTTP client)

To use ``ClientStep``, wire the runner with a
:class:`~loom.etl.ports.contracts.ClientCommandExecutor` implementation
(e.g. :class:`~loom.etl.io.ClickHouseClientExecutor`)::

    from loom.etl import ETLRunner
    from loom.etl.io import ClickHouseClientExecutor

    runner = ETLRunner(
        reader=...,
        writer=...,
        client_executor=ClickHouseClientExecutor(url="clickhouse://user:pass@host:8123/db"),
    )

Example step::

    from loom.etl import ClientStep, ETLParams

    class DailyParams(ETLParams):
        run_date: date

    class OptimizeOrders(ClientStep[DailyParams]):
        def execute(self, params: DailyParams, *, client: Any) -> None:
            client.command("OPTIMIZE TABLE orders FINAL")
"""

from __future__ import annotations

import typing
from typing import Any, ClassVar, Generic, TypeVar, cast

from loom.core.errors import InvalidType
from loom.etl.declarative.target import IntoFile, IntoHistory, IntoTable, IntoTemp
from loom.etl.declarative.target._client import IntoClient
from loom.etl.pipeline._step import ETLStep

ParamsT = TypeVar("ParamsT")


class ClientStep(ETLStep[ParamsT], Generic[ParamsT]):
    """Base class for engine-client steps with no DataFrame input or output.

    Subclass and implement :meth:`execute`.  The executor injects the engine
    client via the ``client`` keyword argument — declare it explicitly in your
    overriding signature.

    The ``target`` class variable is pre-set to :class:`~loom.etl.IntoClient`
    and must not be overridden in subclasses.

    Args:
        ParamsT: Typed params struct for this step, matching the enclosing
            :class:`~loom.etl.ETLProcess` / :class:`~loom.etl.ETLPipeline`.

    Raises:
        InvalidType: At class-definition time if a subclass overrides ``target``
            with anything other than an :class:`~loom.etl.IntoClient` instance.

    Example::

        class RebuildAggregation(ClientStep[DailyParams]):
            def execute(self, params: DailyParams, *, client: Any) -> None:
                client.command(
                    f"INSERT INTO aggregation_daily "
                    f"SELECT * FROM staging_daily WHERE dt = '{params.run_date}'"
                )
    """

    target: ClassVar[IntoTable | IntoHistory | IntoFile | IntoTemp | IntoClient | None] = (
        IntoClient()
    )

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        params_type = _extract_client_step_params(cls)
        if params_type is not None:
            cls._params_type = params_type
        if "target" in cls.__dict__ and not isinstance(cls.__dict__["target"], IntoClient):
            raise InvalidType(
                f"{cls.__qualname__}: 'target' must remain IntoClient() on ClientStep "
                "subclasses — ClientStep produces no DataFrame output. "
                "Remove the 'target' override or use ETLStep instead."
            )

    def execute(self, params: Any, **kwargs: Any) -> None:
        """Execute engine commands via the injected client.

        Must be overridden.  Declare ``*, client: <ClientType>`` explicitly:

        Args:
            params: Typed params instance for this run.
            **kwargs: The executor injects ``client=<engine_client>`` here at
                runtime.  Declare it as ``*, client: <ClientType>`` in your
                implementation signature.

        Returns:
            None — client steps produce no DataFrame output.

        Raises:
            NotImplementedError: When the subclass has not implemented this method.
        """
        raise NotImplementedError(f"{type(self).__qualname__} must implement execute()")


def _extract_client_step_params(cls: type) -> type | None:
    """Extract the ParamsT type argument from ClientStep[ParamsT] in orig_bases."""
    for base in getattr(cls, "__orig_bases__", ()):
        origin = getattr(base, "__origin__", None)
        if origin is ClientStep:
            args = typing.get_args(base)
            if args:
                return cast(type, args[0])
    return None
