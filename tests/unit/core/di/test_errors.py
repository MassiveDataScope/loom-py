"""``ResolutionError`` keeps its public home, ``loom.core.di.container``, by name and in pickles."""

from __future__ import annotations

import pickle

from loom.core.di import ResolutionError as Exported
from loom.core.di.container import ResolutionError
from loom.core.di.graph import DependencyGraphError


class TestDependencyErrors:
    def test_its_qualified_name_is_its_public_home(self) -> None:
        assert f"{ResolutionError.__module__}.{ResolutionError.__qualname__}" == (
            "loom.core.di.container.ResolutionError"
        )
        assert Exported is ResolutionError

    def test_it_survives_a_pickle_round_trip(self) -> None:
        restored = pickle.loads(
            pickle.dumps(ResolutionError("No binding registered for: AcmePort"))
        )

        assert type(restored) is ResolutionError
        assert str(restored) == "No binding registered for: AcmePort"

    def test_a_graph_refusal_pickles_as_itself(self) -> None:
        restored = pickle.loads(pickle.dumps(DependencyGraphError("AcmeRoot -> a: Acme: cycle")))

        assert type(restored) is DependencyGraphError
