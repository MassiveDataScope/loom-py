"""The ``loom`` command line, installed with the ``cli`` extra."""

from __future__ import annotations

import sys

CLI_DEPENDENCIES = frozenset({"typer", "yaml"})
INSTALL_HINT = 'the loom command needs the cli extra: pip install "loom-kernel[cli]"'


def main() -> None:
    """Entry point of the ``loom`` script; without the ``cli`` extra it prints how to install it."""
    try:
        from loom.cli.app import app
    except ModuleNotFoundError as exc:
        if exc.name not in CLI_DEPENDENCIES:
            raise
        print(INSTALL_HINT, file=sys.stderr)
        raise SystemExit(1) from exc
    app()
