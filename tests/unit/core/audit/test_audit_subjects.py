"""The optional product hook that chooses visible audit actors."""

import asyncio
import copy
import pickle
from types import SimpleNamespace

import pytest

from loom.core.audit import ALL_SUBJECTS, OWN, AuditActors, AuditSubjects
from loom.core.audit.subjects import OwnSubjectsOnly
from loom.core.authz import AuthzProduct
from loom.core.identity.identity import Identity
from loom.core.persistence.stores import StoreKey


def test_audit_subjects_is_optional_for_an_authz_product() -> None:
    product = SimpleNamespace(catalog=object(), delegate=object(), elevations={})

    assert isinstance(product, AuthzProduct)
    assert not isinstance(product, AuditSubjects)


def test_audit_subjects_accepts_an_async_product_hook() -> None:
    class Product:
        async def audit_subjects(self, identity: Identity, store: StoreKey) -> AuditActors:
            return frozenset({identity.subject, str(store)})

    product = Product()
    assert isinstance(product, AuditSubjects)
    assert asyncio.run(
        product.audit_subjects(Identity(subject="alice"), StoreKey("ledger"))
    ) == frozenset({"alice", "ledger"})


def test_actor_sentinels_have_stable_public_identity_and_repr() -> None:
    from loom.core import audit

    assert audit.ALL_SUBJECTS is ALL_SUBJECTS
    assert audit.OWN is OWN
    assert repr(ALL_SUBJECTS) == "ALL_SUBJECTS"
    assert repr(OWN) == "OWN"


def test_default_hook_returns_own_without_becoming_public_api() -> None:
    from loom.core import audit

    hook = OwnSubjectsOnly()
    assert isinstance(hook, AuditSubjects)
    assert asyncio.run(hook.audit_subjects(Identity(subject="alice"), StoreKey("ledger"))) is OWN
    assert "OwnSubjectsOnly" not in audit.__all__
    assert not hasattr(audit, "OwnSubjectsOnly")


@pytest.mark.parametrize("sentinel", [ALL_SUBJECTS, OWN])
def test_sentinels_survive_copy_and_pickle(sentinel: object) -> None:
    assert copy.copy(sentinel) is sentinel
    assert copy.deepcopy(sentinel) is sentinel
    assert pickle.loads(pickle.dumps(sentinel)) is sentinel
