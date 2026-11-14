from __future__ import annotations

from loom.core.expr import EqExpr, RootRef, evaluate_expr
from loom.core.model import LoomFrozenStruct


class _Payload(LoomFrozenStruct, frozen=True):
    country: str
    amount: int
    tags: tuple[str, ...]


class _Message(LoomFrozenStruct, frozen=True):
    payload: _Payload
    meta: dict[str, bytes]


class TestExpressions:
    def test_path_ref_builds_attribute_and_item_paths(self) -> None:
        msg = RootRef("message")

        assert msg.payload.country.path == ("payload", "country")
        assert msg.meta["risk"].path == ("meta", "risk")

    def test_comparison_builds_expression_node(self) -> None:
        msg = RootRef("message")
        expr = msg.payload.country == "ES"

        assert isinstance(expr, EqExpr)
        assert expr.right == "ES"

    def test_evaluate_expr_resolves_composed_predicates(self) -> None:
        msg = RootRef("message")
        message = _Message(
            payload=_Payload(country="ES", amount=1500, tags=("paid", "priority")),
            meta={"risk": b"high"},
        )
        expr = (
            (msg.payload.country == "ES")
            & msg.payload.amount.between(1000, 2000)
            & msg.payload.tags.isin({("paid", "priority")})
            & (msg.meta["risk"] == b"high")
        )

        assert evaluate_expr(expr, {"message": message}) is True

    def test_evaluate_expr_supports_negation_and_or(self) -> None:
        msg = RootRef("message")
        message = _Message(
            payload=_Payload(country="FR", amount=20, tags=("paid",)),
            meta={},
        )
        expr = ~((msg.payload.country == "ES") | (msg.payload.amount > 1000))

        assert evaluate_expr(expr, {"message": message}) is True
