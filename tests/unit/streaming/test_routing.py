from __future__ import annotations

import pytest

from loom.core.expr import ExprNode, PathRef
from loom.core.model import LoomFrozenStruct
from loom.streaming import Message, MessageMeta, Process, RecordStep, Route, Router, msg
from loom.streaming.nodes._protocols import Predicate, Selector
from loom.streaming.nodes._router import evaluate_predicate, select_value


class AcmeRoutingOrder(LoomFrozenStruct, frozen=True):
    country: str
    amount: int
    event_type: str
    tags: tuple[str, ...] = ()


class AcmeRoutingValidatedOrder(LoomFrozenStruct, frozen=True):
    order_id: str = "ok"


class _CreatedRouteStep(RecordStep[AcmeRoutingOrder, AcmeRoutingValidatedOrder]):
    def execute(
        self, message: Message[AcmeRoutingOrder], **kwargs: object
    ) -> AcmeRoutingValidatedOrder:
        return AcmeRoutingValidatedOrder(order_id=f"created:{message.payload.event_type}")


class _ManualReviewStep(RecordStep[AcmeRoutingOrder, AcmeRoutingValidatedOrder]):
    def execute(
        self, message: Message[AcmeRoutingOrder], **kwargs: object
    ) -> AcmeRoutingValidatedOrder:
        return AcmeRoutingValidatedOrder(order_id=f"manual:{message.payload.event_type}")


class _DefaultRouteStep(RecordStep[AcmeRoutingOrder, AcmeRoutingValidatedOrder]):
    def execute(
        self, message: Message[AcmeRoutingOrder], **kwargs: object
    ) -> AcmeRoutingValidatedOrder:
        return AcmeRoutingValidatedOrder(order_id=f"default:{message.payload.event_type}")


class _CountrySelector:
    def select(self, message: Message[AcmeRoutingOrder]) -> object:
        return message.payload.country


class _HighRiskPredicate:
    def matches(self, message: Message[AcmeRoutingOrder]) -> bool:
        return message.payload.amount > 1000 and message.meta.headers.get("risk") == b"high"


def _message(
    *,
    country: str = "ES",
    amount: int = 1500,
    event_type: str = "created",
    message_type: str | None = None,
    headers: dict[str, bytes] | None = None,
) -> Message[AcmeRoutingOrder]:
    return Message(
        payload=AcmeRoutingOrder(country=country, amount=amount, event_type=event_type),
        meta=MessageMeta(
            message_id="msg-1",
            message_type=message_type,
            headers=headers or {},
        ),
    )


class TestMsgExpression:
    @pytest.mark.parametrize(
        ("expr", "message", "expected"),
        [
            (msg.payload.country, _message(country="FR"), "FR"),
            (msg.meta.message_type, _message(message_type="order.created"), "order.created"),
            (_CountrySelector(), _message(country="FR"), "FR"),
        ],
    )
    def test_msg_expression_selects_values(
        self,
        expr: PathRef | Selector[AcmeRoutingOrder],
        message: Message[AcmeRoutingOrder],
        expected: object,
    ) -> None:
        assert select_value(expr, message) == expected

    @pytest.mark.parametrize(
        ("predicate", "message", "expected"),
        [
            (
                (
                    (msg.payload.country == "ES")
                    & (msg.payload.amount >= 1000)
                    & msg.payload.amount.between(1000, 2000)
                    & (msg.meta.headers["risk"] == b"high")
                ),
                _message(headers={"risk": b"high"}),
                True,
            ),
            (
                (msg.payload.country.isin({"ES", "FR"})) & ~(msg.payload.amount > 100),
                _message(country="FR", amount=20),
                True,
            ),
            (_HighRiskPredicate(), _message(amount=10, headers={"risk": b"low"}), False),
            (_HighRiskPredicate(), _message(headers={"risk": b"high"}), True),
        ],
    )
    def test_msg_expression_evaluates_predicates(
        self,
        predicate: ExprNode | Predicate[AcmeRoutingOrder],
        message: Message[AcmeRoutingOrder],
        expected: bool,
    ) -> None:
        assert evaluate_predicate(predicate, message) is expected


class TestRouter:
    def test_router_by_declares_keyed_routes(self) -> None:
        created = Process[AcmeRoutingOrder, AcmeRoutingValidatedOrder](_CreatedRouteStep())
        cancelled = Process[AcmeRoutingOrder, AcmeRoutingValidatedOrder](_ManualReviewStep())
        default = Process[AcmeRoutingOrder, AcmeRoutingValidatedOrder](_DefaultRouteStep())

        router = Router.by(
            msg.payload.event_type,
            routes={"created": created, "cancelled": cancelled},
            default=default,
        )

        assert router.selector == msg.payload.event_type
        assert router.routes == {"created": created, "cancelled": cancelled}
        assert router.default is default

    def test_router_when_declares_ordered_predicate_routes(self) -> None:
        manual = Process[AcmeRoutingOrder, AcmeRoutingValidatedOrder](_ManualReviewStep())
        normal = Process[AcmeRoutingOrder, AcmeRoutingValidatedOrder](_CreatedRouteStep())
        route = Route(when=msg.payload.amount > 1000, process=manual)

        router = Router.when((route,), default=normal)

        assert router.predicate_routes == (route,)
        assert router.default is normal
        assert evaluate_predicate(route.when, _message()) is True

    def test_router_rejects_empty_declarations(self) -> None:
        with pytest.raises(ValueError, match="keyed route"):
            Router.by(msg.payload.event_type, routes={})

        with pytest.raises(ValueError, match="selector or at least one predicate"):
            Router.when(())
