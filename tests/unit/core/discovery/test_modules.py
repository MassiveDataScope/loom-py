from __future__ import annotations

import pytest

from loom.core.discovery.modules import ModulesDiscoveryEngine


class TestModuleDiscovery:
    def test_modules_discovery_rejects_empty_module_list_naming_manifest_agents(self) -> None:
        """The 'no module paths' message points at manifest mode and AGENTS."""
        engine = ModulesDiscoveryEngine([])
        with pytest.raises(ValueError, match=r"discovery\.mode: manifest") as excinfo:
            engine.discover()

        assert "AGENTS" in str(excinfo.value)
