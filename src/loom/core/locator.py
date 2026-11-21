"""Put an application's code directory on ``sys.path``.

``add_code_path`` is what the command line, the schema tools and the
application factories share to make the code named by ``app.code_path``
importable.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Final

DEFAULT_CODE_PATH: Final = "src"
"""``app.code_path`` when the configuration names none."""


def add_code_path(code_path: str | os.PathLike[str], config_dir: Path) -> Path:
    """Put an application's code directory first on ``sys.path``, once.

    Args:
        code_path: ``app.code_path``; relative to *config_dir* unless absolute.
        config_dir: Directory of the application's first configuration file.

    Returns:
        The resolved directory. It is added only when it exists, so a
        configuration naming no local code leaves ``sys.path`` alone.
    """
    candidate = Path(code_path)
    if not candidate.is_absolute():
        candidate = config_dir / candidate
    resolved = candidate.resolve()
    if resolved.is_dir() and str(resolved) not in sys.path:
        sys.path.insert(0, str(resolved))
    return resolved
