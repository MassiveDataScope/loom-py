"""Client-mode target sentinel — no DataFrame is written.

:class:`IntoClient` is the target declaration for :class:`~loom.etl.ClientStep`.
It signals to the executor that this step has no frame output: instead, the
step's ``execute()`` receives the engine client directly via a keyword argument.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ClientSpec:
    """Compiled sentinel for a client-mode step.

    Produced by :meth:`IntoClient._to_spec`. Consumed by
    :class:`~loom.etl.executor.ETLExecutor` to route execution to
    :class:`~loom.etl.ports.contracts.ClientCommandExecutor` instead of
    the normal read → execute → write path.

    Never constructed directly in user code.
    """

    @property
    def kind(self) -> str:
        """Target kind identifier."""
        return "client"


class IntoClient:
    """Declare that a step produces no DataFrame output.

    Use as the ``target`` class variable on a :class:`~loom.etl.ClientStep`.
    The executor will inject the engine client (e.g. the ClickHouse native
    client) as a ``client`` keyword argument to ``execute()``.

    This is the default ``target`` on :class:`~loom.etl.ClientStep`; you
    rarely need to set it explicitly.

    Example::

        class OptimizeOrders(ClientStep[MyParams]):
            def execute(self, params: MyParams, *, client: Any) -> None:
                client.command("OPTIMIZE TABLE orders FINAL")
    """

    __slots__ = ()

    def _to_spec(self) -> ClientSpec:
        """Compile to a frozen sentinel spec for the executor."""
        return ClientSpec()
