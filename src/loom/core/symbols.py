"""Resolve a ``module:symbol`` reference, offline.

Neutral on purpose: the reference form is the convention every declarative
surface of the framework uses to name application code, so the reader belongs
where any pillar can reach it without depending on another one.
"""

from __future__ import annotations

from importlib import import_module

from loom.core.errors import InvalidValue


def import_symbol(ref: str) -> object:
    """Import the symbol a ``module:symbol`` reference points at.

    Args:
        ref: Reference in ``module:symbol`` form.

    Returns:
        The imported symbol.

    Raises:
        ImportError: If the module does not import.
        AttributeError: If the module lacks the symbol.
        InvalidValue: If the reference is not in ``module:symbol`` form.
    """
    module_name, _, symbol_name = ref.partition(":")
    if not module_name or not symbol_name:
        raise InvalidValue("a reference is not in module:symbol form")
    module = import_module(module_name)
    return getattr(module, symbol_name)
