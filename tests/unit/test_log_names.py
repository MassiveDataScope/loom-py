"""A log event or key is checked when its class is declared, in production too.

The declarations are process-wide: a name a test declares stays under its own ``acme.*`` value.
"""

from __future__ import annotations

import pytest

from loom.core.log_names import LogEvent, LogKey, ValueKind


class TestSubKey:
    def test_a_bag_makes_the_key_of_a_name_inside_it(self) -> None:
        class Sample(LogKey):
            BAG = "acme.subkey.bag", ValueKind.OBJECT

        assert Sample.BAG.of("tenant") == "acme.subkey.bag.tenant"

    def test_a_key_that_carries_a_value_makes_no_key_inside_it(self) -> None:
        class Sample(LogKey):
            VALUE = "acme.subkey.value"

        with pytest.raises(ValueError, match="not a bag"):
            Sample.VALUE.of("tenant")


class TestKeyDeclaration:
    @pytest.mark.parametrize(
        ("first", "second"),
        [
            pytest.param(("acme.clash.leaf",), ("acme.clash.leaf.child",), id="key-under-a-value"),
            pytest.param(("acme.clash.tree.child",), ("acme.clash.tree",), id="value-over-keys"),
            pytest.param(
                ("acme.clash.count", ValueKind.NUMBER), ("acme.clash.count",), id="second-kind"
            ),
            pytest.param(
                ("acme.clash.bag", ValueKind.OBJECT), ("acme.clash.bag.name",), id="key-in-a-bag"
            ),
        ],
    )
    def test_a_key_with_a_second_meaning_is_refused(
        self, first: tuple[object, ...], second: tuple[object, ...]
    ) -> None:
        LogKey("First", {"KEY": first})

        with pytest.raises(ValueError, match="second meaning"):
            LogKey("Second", {"KEY": second})

    def test_a_key_declared_again_with_its_kind_is_the_same_key(self) -> None:
        LogKey("First", {"KEY": ("acme.again.key", ValueKind.ARRAY)})

        again = LogKey("Second", {"KEY": ("acme.again.key", ValueKind.ARRAY)})

        assert again.KEY.kind is ValueKind.ARRAY

    @pytest.mark.parametrize(
        "value",
        ["Acme.form.key", "acme..key", "acme.key_", "acme.key__name", "acme.key-name", ".acme"],
        ids=["capital", "empty-segment", "trailing-underscore", "double-underscore", "dash", "dot"],
    )
    def test_a_key_out_of_form_is_refused(self, value: str) -> None:
        with pytest.raises(ValueError, match="snake_case"):
            LogKey("Sample", {"KEY": value})

    @pytest.mark.parametrize(
        ("value", "module"),
        [
            pytest.param("loom.acme.key", __name__, id="loom-key-outside-loom"),
            pytest.param("loom.acme.key", "loomx.ext", id="loom-key-in-a-look-alike-package"),
            pytest.param("acme.key", "loom.acme.event", id="loom-area-key-outside-loom-prefix"),
        ],
    )
    def test_a_key_on_the_wrong_side_of_loom_is_refused(self, value: str, module: str) -> None:
        with pytest.raises(ValueError, match="reserved to loom"):
            LogKey("Sample", {"KEY": value}, module=module)


class TestEventDeclaration:
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("AcmeThingDone", id="camel"),
            pytest.param("acme_thing_done", id="no-area"),
            pytest.param("acme.done", id="no-fact"),
            pytest.param("Acme.thing_done", id="capital"),
            pytest.param("acme.thing__done", id="double-underscore"),
            pytest.param("acme.thing_done_", id="trailing-underscore"),
            pytest.param("acme._thing_done", id="leading-underscore"),
            pytest.param("[BOOT] x", id="sentence"),
        ],
    )
    def test_an_event_out_of_form_is_refused(self, value: str) -> None:
        with pytest.raises(ValueError, match="<area>.<subject>_<fact>"):
            LogEvent("Sample", {"EVENT": value})

    def test_an_event_another_class_declared_is_refused(self) -> None:
        LogEvent("First", {"EVENT": "acme.thing_shared"})

        with pytest.raises(ValueError, match="already declared by First"):
            LogEvent("Second", {"EVENT": "acme.thing_shared"})

    def test_an_event_twice_in_one_class_is_refused(self) -> None:
        with pytest.raises(ValueError, match="already declared by Aliased"):
            LogEvent("Aliased", {"ONE": "acme.thing_aliased", "TWO": "acme.thing_aliased"})

    def test_a_class_declared_again_keeps_its_values(self) -> None:
        LogEvent("Again", {"EVENT": "acme.thing_again"})

        assert LogEvent("Again", {"EVENT": "acme.thing_again"}).EVENT == "acme.thing_again"
