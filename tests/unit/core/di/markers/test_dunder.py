"""Dunder probes by copy, pickle, repr and introspection never meet ``MarkerNotResolved``."""

from __future__ import annotations

import copy
import inspect
import pickle
from typing import Any

import pytest

from loom.core.execution import MarkerNotResolved
from loom.core.use_case.markers import Agent, Caller, Mcp

MARKERS = [Mcp("acme-crm", include=["find_*"]), Agent("acme-triage"), Caller()]


class AcmeHolder:
    def __init__(self, crm: Any = Mcp("acme-crm", include=["find_*"])) -> None:
        self.crm = crm


class TestMarkerDunder:
    @pytest.mark.parametrize("marker", MARKERS, ids=repr)
    def test_copies_and_pickles_keep_the_marker(self, marker: Any) -> None:
        for twin in (copy.copy(marker), copy.deepcopy(marker), pickle.loads(pickle.dumps(marker))):
            assert type(twin) is type(marker)
            assert repr(twin) == repr(marker)

    @pytest.mark.parametrize("marker", MARKERS, ids=repr)
    def test_a_dunder_probe_reads_as_absent(self, marker: Any) -> None:
        assert not hasattr(marker, "__wrapped__")
        with pytest.raises(AttributeError) as caught:
            marker.__wrapped__  # noqa: B018 - attribute access is the behaviour under test
        assert type(caught.value) is AttributeError

    @pytest.mark.parametrize("marker", MARKERS, ids=repr)
    def test_a_non_dunder_probe_reads_as_absent_too(self, marker: Any) -> None:
        assert getattr(marker, "_repr_html_", None) is None
        with pytest.raises(MarkerNotResolved):
            marker._repr_html_  # noqa: B018 - attribute access is the behaviour under test

    def test_an_object_holding_a_marker_deep_copies_and_its_class_has_a_signature(self) -> None:
        holder = AcmeHolder()

        assert copy.deepcopy(holder).crm.server == "acme-crm"
        crm = inspect.signature(AcmeHolder).parameters["crm"]
        assert repr(crm.default) == "Mcp('acme-crm', include=['find_*'])"
