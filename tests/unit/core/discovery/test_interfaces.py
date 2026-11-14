from __future__ import annotations

import sys
import types

import pytest

from loom.core.discovery.interfaces import InterfacesDiscoveryEngine


class TestInterfaceDiscovery:
    def test_interfaces_discovery_rejects_empty_module_list_naming_manifest_agents(self) -> None:
        """The 'no module paths' message points at manifest mode and AGENTS."""
        engine = InterfacesDiscoveryEngine([])
        with pytest.raises(ValueError, match=r"discovery\.mode: manifest") as excinfo:
            engine.discover()

        assert "AGENTS" in str(excinfo.value)

    def test_interfaces_discovery_rejects_module_without_interfaces_naming_manifest_agents(
        self,
    ) -> None:
        """The 'no RestInterface' message points at manifest mode and AGENTS."""
        module_name = "tests.unit.core.discovery._interfaces_without_rest_interface"
        module = types.ModuleType(module_name)
        sys.modules[module_name] = module

        try:
            engine = InterfacesDiscoveryEngine([module_name], warn_recommended=False)
            with pytest.raises(ValueError, match=r"discovery\.mode: manifest") as excinfo:
                engine.discover()
        finally:
            sys.modules.pop(module_name, None)

        assert "No RestInterface subclasses discovered" in str(excinfo.value)
        assert "AGENTS" in str(excinfo.value)
