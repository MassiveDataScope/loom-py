from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session

from loom.core.logger import get_logger
from loom.core.repository.sqlalchemy.session_settings import (
    SessionSettings,
    install_session_settings,
)
from loom.core.tracing import get_trace_id


class SessionManager:
    """Async SQLAlchemy session manager with pooling support."""

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any],
        *,
        inject_trace_id: bool = True,
        session_settings: SessionSettings | None = None,
        **engine_kwargs: object,
    ) -> SessionManager:
        """Build a session manager from a resolved SQLAlchemy config mapping.

        Args:
            config: Resolved config mapping containing a ``url`` entry and
                optional pool tuning keys.
            inject_trace_id: When ``True``, prefixes SQL statements with the
                active trace id when available.
            session_settings: Optional provider of transaction-local Postgres
                settings; see :meth:`SessionManager.__init__` for the contract.
            **engine_kwargs: Additional keyword arguments forwarded to the
                async engine constructor.

        Returns:
            A configured :class:`SessionManager`.

        Raises:
            ValueError: If ``url`` is missing or empty, or if ``session_settings``
                is given and ``url`` is not a Postgres URL.
        """
        url = config.get("url")
        if not url:
            raise ValueError("SQLAlchemy sink config requires a 'url'.")
        return cls(
            str(url),
            echo=bool(config.get("echo", False)),
            pool_pre_ping=bool(config.get("pool_pre_ping", True)),
            pool_size=_optional_int(config.get("pool_size"), 10),
            max_overflow=_optional_int(config.get("max_overflow"), 20),
            pool_timeout=_optional_int(config.get("pool_timeout"), 30),
            pool_recycle=_optional_int(config.get("pool_recycle"), 1800),
            connect_args=dict(config.get("connect_args") or {}),
            inject_trace_id=inject_trace_id,
            session_settings=session_settings,
            **engine_kwargs,
        )

    def __init__(
        self,
        url: str,
        *,
        echo: bool = False,
        pool_pre_ping: bool = True,
        pool_size: int | None = 10,
        max_overflow: int | None = 20,
        pool_timeout: int | None = 30,
        pool_recycle: int | None = 1800,
        connect_args: dict[str, object] | None = None,
        inject_trace_id: bool = True,
        session_settings: SessionSettings | None = None,
        **engine_kwargs: object,
    ) -> None:
        """Create a session manager backed by an async SQLAlchemy engine.

        Args:
            url: Database connection URL (e.g. ``"postgresql+asyncpg://..."``).
            echo: If ``True``, log all generated SQL statements.
            pool_pre_ping: Test connections before checkout to detect stale ones.
            pool_size: Number of permanent connections in the pool.
            max_overflow: Maximum additional connections beyond ``pool_size``.
            pool_timeout: Seconds to wait before raising on pool exhaustion.
            pool_recycle: Seconds after which a connection is recycled.
            connect_args: Extra keyword arguments passed to the DBAPI ``connect()`` call.
            inject_trace_id: When ``True``, prefixes every SQL statement with a
                ``/* trace_id=<id> */`` comment when a trace identifier is active
                in the current async context.  Visible in database slow-query logs
                and ``pg_stat_activity``.  Defaults to ``True``.
            session_settings: When given, a callable that returns the settings for
                the transaction about to start, or ``None`` to apply none. Each
                outer transaction then begins with one ``SELECT`` that calls
                ``set_config(key, value, true)`` once per entry, with keys and
                values bound as parameters. The settings are local to the
                transaction, so the connection returns to the pool clean. Postgres
                only, and incompatible with ``isolation_level="AUTOCOMMIT"``, which
                would discard the settings after each statement; when ``None``,
                the manager behaves exactly as it does without this option.
            **engine_kwargs: Additional keyword arguments forwarded to ``create_async_engine``.

        Raises:
            ValueError: If ``session_settings`` is given and ``url`` is not a
                Postgres URL, or the engine is configured with
                ``isolation_level="AUTOCOMMIT"``.
        """
        if session_settings is not None:
            backend = make_url(url).get_backend_name()
            if backend != "postgresql":
                raise ValueError(
                    f"session_settings requires a postgresql URL, got backend {backend!r}"
                )
            if _autocommit(engine_kwargs):
                raise ValueError(
                    "session_settings is incompatible with isolation_level='AUTOCOMMIT'"
                )
        engine_config: dict[str, object] = {
            "echo": echo,
            "pool_pre_ping": pool_pre_ping,
            **engine_kwargs,
        }
        if pool_size is not None:
            engine_config["pool_size"] = pool_size
        if max_overflow is not None:
            engine_config["max_overflow"] = max_overflow
        if pool_timeout is not None:
            engine_config["pool_timeout"] = pool_timeout
        if pool_recycle is not None:
            engine_config["pool_recycle"] = pool_recycle
        if connect_args is not None:
            engine_config["connect_args"] = connect_args

        self._log = get_logger(__name__).bind(module="session_manager")
        self._engine = create_async_engine(url, **engine_config)
        self._has_session_settings = session_settings is not None
        factory_kwargs: dict[str, Any] = {}
        if session_settings is not None:
            session_class = type("SettingsSession", (Session,), {})
            install_session_settings(session_class, session_settings)
            factory_kwargs["sync_session_class"] = session_class
        self._session_factory = async_sessionmaker(
            bind=self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
            **factory_kwargs,
        )
        if inject_trace_id:
            _register_trace_id_listener(self._engine)
        self._log.info(
            "SessionManagerInitialized",
            backend=self._engine.url.get_backend_name(),
            driver=self._engine.url.get_driver_name(),
            inject_trace_id=inject_trace_id,
            session_settings=session_settings is not None,
        )

    @property
    def has_session_settings(self) -> bool:
        """Whether every transaction of this manager starts with the product's settings."""
        return self._has_session_settings

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Yield a scoped async session that is automatically closed on exit.

        Yields:
            An ``AsyncSession`` bound to the managed engine.
        """
        self._log.debug("SessionScopeOpened")
        session = self._session_factory()
        try:
            yield session
        finally:
            await session.close()
            self._log.debug("SessionScopeClosed")

    async def dispose(self) -> None:
        """Dispose of the engine and release all pooled connections."""
        await self._engine.dispose()
        self._log.info("SessionManagerDisposed")

    @property
    def engine(self) -> AsyncEngine:
        """The underlying async SQLAlchemy engine."""
        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """The configured async session factory bound to the engine."""
        return self._session_factory


def _register_trace_id_listener(async_engine: AsyncEngine) -> None:
    """Register a ``before_cursor_execute`` listener that injects trace comments.

    The listener is attached to the underlying sync engine so it fires for
    every SQL statement executed through the async engine.  When a
    trace-id is active in the current async context, the statement is
    prefixed with ``/* trace_id=<id> */``.

    Args:
        async_engine: The :class:`~sqlalchemy.ext.asyncio.AsyncEngine` whose
            sync engine will receive the listener.
    """

    @event.listens_for(async_engine.sync_engine, "before_cursor_execute", retval=True)
    def _inject(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> tuple[str, Any]:
        tid = get_trace_id()
        if tid:
            statement = f"/* trace_id={tid} */ " + statement
        return statement, parameters


def _autocommit(engine_kwargs: Mapping[str, object]) -> bool:
    execution_options = engine_kwargs.get("execution_options")
    levels = (
        engine_kwargs.get("isolation_level"),
        execution_options.get("isolation_level")
        if isinstance(execution_options, Mapping)
        else None,
    )
    return any(isinstance(level, str) and level.upper() == "AUTOCOMMIT" for level in levels)


def _optional_int(value: Any, default: int) -> int:
    if value is None:
        return default
    return int(value)
