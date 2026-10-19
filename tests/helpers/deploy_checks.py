"""An application check module written to disk, for the runners' deploy-check tests.

Each call writes a module registering one error check under an area unique
to the test directory, so the global registry never holds two checks with
one id. The check fails only when the configuration sets its trigger key,
so it stays inert for every other test in the process.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

_NON_IDENTIFIER = re.compile(r"\W", re.ASCII)
_SOURCE = """\
from loom.core.checks import Finding, deploy_check


@deploy_check("{check_id}")
def _triggered(config):
    if not config.has("{trigger}"):
        return None
    return Finding(message="{trigger} is set", hint="unset {trigger}")
"""


@dataclass(frozen=True)
class FailingCheckModule:
    """A module registering an error check that fails when *trigger* is set.

    Attributes:
        module: Importable name, once *directory* is on ``sys.path``.
        check_id: Id of the registered check.
        trigger: Configuration key whose presence makes the check fail.
        directory: Directory holding the module.
    """

    module: str
    check_id: str
    trigger: str
    directory: Path

    def checks_section(self) -> dict[str, list[str]]:
        """Return the ``checks`` section listing the module."""
        return {"modules": [self.module]}


def write_failing_check_module(tmp_path: Path) -> FailingCheckModule:
    """Write the module into *tmp_path* and describe it."""
    area = f"apptest_{_NON_IDENTIFIER.sub('_', tmp_path.name)}".lower()
    module = f"{area}_checks"
    check = FailingCheckModule(
        module=module,
        check_id=f"{area}.E001",
        trigger=f"{area}.unsafe",
        directory=tmp_path,
    )
    (tmp_path / f"{module}.py").write_text(
        _SOURCE.format(check_id=check.check_id, trigger=check.trigger), encoding="utf-8"
    )
    return check
