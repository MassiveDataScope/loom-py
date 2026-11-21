"""The ``loom`` command line, installed with the ``cli`` extra."""

from __future__ import annotations

import sys

from loom.core.logger.install import install_entry_point

CLI_DEPENDENCIES = frozenset({"typer", "yaml"})
INSTALL_HINT = 'the loom command needs the cli extra: pip install "loom-kernel[cli]"'


def main() -> None:
    """Entry point of the ``loom`` script; without the ``cli`` extra it prints how to install it.

    It is a runtime entry point: loom's record factory and process hooks go in
    before any configuration is read, and the previous hooks come back when
    the command ends.
    """
    with install_entry_point():
        try:
            from loom.cli.app import app
        except ModuleNotFoundError as exc:
            if exc.name not in CLI_DEPENDENCIES:
                raise
            print(INSTALL_HINT, file=sys.stderr)
            raise SystemExit(1) from exc
        app()
