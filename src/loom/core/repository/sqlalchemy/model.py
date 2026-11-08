from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from loom.core.identity import current_identity
from loom.core.repository.sqlalchemy.profile_loader import ProfileLoader


class Base(DeclarativeBase):
    """Base SQLAlchemy declarative class."""


class TimestampMixin:
    """Created/updated timestamps for all persistent entities."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        server_onupdate=func.now(),
    )


class IdentityMixin:
    """Integer primary key for entities with standard identity."""

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )


def current_actor() -> str | None:
    """The subject of the caller in context, or ``None`` when anonymous."""
    identity = current_identity()
    return identity.subject if identity.is_authenticated else None


class AuditActorMixin:
    """Who created and who last changed the row, filled from the caller.

    ``created_by`` is set on insert and ``updated_by`` on every update from
    the identity in context; a value the code sets itself is kept. The audit
    log (``Audited``) records every change; these columns are the row's own
    summary of it.
    """

    created_by: Mapped[str | None] = mapped_column(
        String(255), nullable=True, default=current_actor
    )
    updated_by: Mapped[str | None] = mapped_column(
        String(255), nullable=True, default=current_actor, onupdate=current_actor
    )


class BaseModel(Base, IdentityMixin, TimestampMixin):
    """Default project model base (identity + timestamps)."""

    __abstract__ = True

    @classmethod
    def get_profile_options(cls, profile: str) -> list[Any]:
        return ProfileLoader.get_options(cls, profile)


class AuditableModel(Base, IdentityMixin, TimestampMixin, AuditActorMixin):
    """Optional model base that also tracks actor fields."""

    __abstract__ = True

    @classmethod
    def get_profile_options(cls, profile: str) -> list[Any]:
        return ProfileLoader.get_options(cls, profile)
