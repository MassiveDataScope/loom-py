"""Optional product hook for audit actor visibility."""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeAlias, runtime_checkable

if TYPE_CHECKING:
    from sqlalchemy import Select

    from loom.core.identity.identity import Identity
    from loom.core.persistence.stores import StoreKey


class _Actors(Enum):
    ALL_SUBJECTS = "ALL_SUBJECTS"
    OWN = "OWN"

    def __repr__(self) -> str:
        return self.value


ALL_SUBJECTS = _Actors.ALL_SUBJECTS
OWN = _Actors.OWN

AuditActors: TypeAlias = "frozenset[str] | Select[Any] | Literal[_Actors.ALL_SUBJECTS, _Actors.OWN]"


@runtime_checkable
class AuditSubjects(Protocol):
    """Choose the actors whose audit rows a caller may see."""

    async def audit_subjects(self, identity: Identity, store: StoreKey) -> AuditActors: ...


class OwnSubjectsOnly:
    """Default hook when a product provides no actor selection."""

    async def audit_subjects(self, identity: Identity, store: StoreKey) -> AuditActors:
        return OWN
