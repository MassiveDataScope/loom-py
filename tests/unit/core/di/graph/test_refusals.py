"""Refusals 1-6 of the analysis, and the warning that stands in for refusal 6 off a root."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.errors import DependencyUnresolvable
from loom.core.di.graph import (
    LENIENT_REFUSALS,
    REFUSALS,
    ClassNode,
    DependencyGraphError,
    KeyNode,
    refuse_unmapped_repository,
)
from loom.core.di.scope import Scope
from loom.core.model import LoomStruct
from loom.core.repository.abc import RepoFor
from loom.core.repository.dependency import classify_key
from tests.unit.core.di.doubles import AcmeTakesInput
from tests.unit.core.di.graph import acme
from tests.unit.core.di.graph.acme import AcmeCaller
from tests.unit.core.di.graph.doubles import AcmeClock, AcmeLedger, AcmeRates, AcmeReport


class AcmeAuditor:
    def __init__(self, caller: object = AcmeCaller()) -> None:
        self.caller = caller


class AcmeAuditorAudit:
    def __init__(self, auditor: AcmeAuditor) -> None:
        self.auditor = auditor


class AcmeTakesNames:
    def __init__(self, names: tuple[str, ...]) -> None:
        self.names = names


class AcmeNamesLedger:
    def __init__(self, names: tuple[str, ...]) -> None:
        self.names = names


class AcmeUnannotated:
    def __init__(self, names) -> None:  # type: ignore[no-untyped-def]
        self.names = names


class AcmeNamesReport:
    def __init__(self, ledger: AcmeNamesLedger) -> None:
        self.ledger = ledger


class AcmeTrade(LoomStruct):
    pass


class AcmeSettlement:
    def __init__(self, trades: RepoFor[AcmeTrade]) -> None:
        self.trades = trades


class TestGraphRefusals:
    def test_refusal_1_an_unmapped_repository_of_a_node_the_rule_names(self) -> None:
        graph = LoomContainer(classify_key=classify_key).graph
        root = ClassNode(AcmeSettlement)
        rules = (*REFUSALS, refuse_unmapped_repository({root}))

        with pytest.raises(
            DependencyGraphError,
            match="AcmeSettlement: trades: no repository mapping is registered for AcmeTrade",
        ):
            graph.analyse([root], rules)

    def test_an_unmapped_repository_of_a_node_the_rule_does_not_name_is_left_alone(self) -> None:
        graph = LoomContainer(classify_key=classify_key).graph
        rules = (*REFUSALS, refuse_unmapped_repository(set()))

        analysis = graph.analyse([ClassNode(AcmeSettlement)], rules)

        assert graph.edges(ClassNode(AcmeSettlement)) == ()
        assert analysis.reachable == {ClassNode(AcmeSettlement)}

    def test_refusal_1_a_reached_key_with_no_binding(self) -> None:
        with pytest.raises(DependencyGraphError, match="AcmeClock: no binding is registered"):
            acme.analysed(acme.container(), AcmeLedger)

    def test_refusal_2_a_cycle(self) -> None:
        built = acme.container()
        built.register(AcmeLedger, AcmeRates)
        built.register(AcmeRates, AcmeRates)

        with pytest.raises(DependencyGraphError, match="dependency cycle"):
            acme.analysed(built, AcmeReport)

    def test_refusal_3_a_singleton_holding_a_per_execution_binding(self) -> None:
        built = acme.container()
        built.register(AcmeLedger, AcmeLedger, scope=Scope.APPLICATION)
        built.register(AcmeClock, AcmeClock, scope=Scope.REQUEST)

        with pytest.raises(DependencyGraphError, match="cannot hold a per-execution one"):
            acme.analysed(built, AcmeReport)

    def test_refusal_4_a_singleton_holding_a_kind_of_one_execution(self) -> None:
        built = acme.container()
        built.register(AcmeAuditor, AcmeAuditor, scope=Scope.APPLICATION)

        with pytest.raises(DependencyGraphError, match="cannot hold a 'acme-caller' value"):
            acme.analysed(built, AcmeAuditorAudit)

    def test_a_per_execution_binding_may_hold_a_kind_of_one_execution(self) -> None:
        built = acme.container()
        built.register(AcmeAuditor, AcmeAuditor, scope=Scope.REQUEST)

        assert acme.analysed(built, AcmeAuditorAudit).warnings == ()

    def test_refusal_5_a_refused_declaration_in_a_constructor(self) -> None:
        with pytest.raises(
            DependencyGraphError, match=r"payload: AcmeInput\(\) belongs to execute"
        ):
            acme.analysed(acme.container(), AcmeTakesInput)

    def test_refusal_6_an_unresolvable_required_parameter_of_a_root(self) -> None:
        with pytest.raises(
            DependencyGraphError, match=r"names: tuple\[str, \.\.\.\] names nothing"
        ):
            acme.analysed(acme.container(), AcmeTakesNames)

    def test_an_unannotated_root_parameter_is_named_as_such(self) -> None:
        with pytest.raises(DependencyGraphError, match="AcmeUnannotated: names has no annotation"):
            acme.analysed(acme.container(), AcmeUnannotated)

    def test_refusal_6_an_unresolvable_parameter_of_a_reached_binding(self) -> None:
        built = acme.container()
        built.register(AcmeNamesLedger, AcmeNamesLedger)

        with pytest.raises(DependencyGraphError) as refused:
            acme.analysed(built, AcmeNamesReport)

        assert str(refused.value) == (
            "AcmeNamesReport -> ledger: AcmeNamesLedger: "
            "names: tuple[str, ...] names nothing the container can serve"
        )

    def test_under_the_lenient_rules_an_unresolvable_parameter_off_a_root_is_a_warning(
        self,
    ) -> None:
        built = acme.container()
        built.register(AcmeNamesLedger, AcmeNamesLedger)

        analysis = built.graph.analyse([ClassNode(AcmeNamesReport)], LENIENT_REFUSALS)

        (warning,) = analysis.warnings
        assert isinstance(warning, DependencyUnresolvable)
        assert str(warning).startswith("AcmeNamesLedger -> names: tuple[str, ...]")

    def test_the_lenient_rules_leave_a_missing_binding_alone(self) -> None:
        graph = acme.container().graph

        analysis = graph.analyse([ClassNode(AcmeLedger)], LENIENT_REFUSALS)

        assert KeyNode(AcmeClock) in analysis.reachable
