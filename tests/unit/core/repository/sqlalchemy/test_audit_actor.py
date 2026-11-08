"""``AuditableModel.created_by`` and ``updated_by`` come from the caller (FR-014)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import String, create_engine, select
from sqlalchemy.orm import Mapped, Session, mapped_column

from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.repository.sqlalchemy.model import AuditableModel


class Ticket(AuditableModel):
    __tablename__ = "tickets_actor"
    title: Mapped[str] = mapped_column(String(50))


@pytest.fixture
def session() -> Iterator[Session]:
    engine = create_engine("sqlite://")
    Ticket.metadata.create_all(engine, tables=[Ticket.__table__])  # type: ignore[list-item]
    with Session(engine) as session:
        yield session


def _as(subject: str) -> object:
    return set_identity(Identity(subject=subject))


def test_insert_records_the_caller_and_update_records_who_changed_it(session: Session) -> None:
    token = _as("ana")
    try:
        session.add(Ticket(title="a"))
        session.commit()
    finally:
        reset_identity(token)  # type: ignore[arg-type]
    token = _as("bob")
    try:
        ticket = session.scalars(select(Ticket)).one()
        ticket.title = "b"
        session.commit()
    finally:
        reset_identity(token)  # type: ignore[arg-type]

    assert (ticket.created_by, ticket.updated_by) == ("ana", "bob")


def test_an_anonymous_caller_leaves_the_actor_empty(session: Session) -> None:
    session.add(Ticket(title="a"))
    session.commit()

    assert session.scalars(select(Ticket)).one().created_by is None


def test_a_value_the_code_sets_is_kept(session: Session) -> None:
    token = _as("ana")
    try:
        session.add(Ticket(title="a", created_by="import"))
        session.commit()
    finally:
        reset_identity(token)  # type: ignore[arg-type]

    assert session.scalars(select(Ticket)).one().created_by == "import"
