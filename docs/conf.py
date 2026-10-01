"""Sphinx configuration for loom-kernel documentation."""

from __future__ import annotations

import importlib
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

project = "loom-kernel"
author = "the-reacher-data"
copyright = f"{datetime.now().year}, {author}"
release = os.getenv("READTHEDOCS_VERSION", "latest")

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_design",
]

templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    "_inventories",
    "Thumbs.db",
    ".DS_Store",
    "guides",
    "architecture",
    "examples-repo",
]

html_theme = "furo"
html_static_path = ["_static"]
html_title = "loom-kernel docs"
html_css_files = [
    "custom.css",
]

autosummary_generate = True
autosummary_generate_overwrite = True
autosummary_imported_members = False
autodoc_typehints = "description"
autodoc_typehints_format = "short"
autodoc_member_order = "bysource"
autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
}
autodoc_preserve_defaults = True
napoleon_google_docstring = True
napoleon_numpy_docstring = False

# Generate ids for headings up to level 3 so Markdown pages can link to a
# specific section of another page ('other.md#some-heading'). Without this,
# MyST emits no local ids and every such cross-reference is a build warning.
myst_heading_anchors = 3


# loom.core.sql re-exports: canonical docs live in loom.core.sql.abc
# and loom.core.sql.config, both listed in reference/api/core.rst.
_SQL_PACKAGE = "loom.core.sql"
_SQL_REEXPORTED_NAMES = (
    "RoleNotAllowedError",
    "RoleRequiredError",
    "RolesNotBoundError",
    "SqlColumn",
    "SqlConfig",
    "SqlConnectionConfig",
    "SqlEndpointConfig",
    "SqlExecutionError",
    "SqlExecutionOptions",
    "SqlExecutor",
    "SqlQueryResult",
    "UnknownConnectionError",
)

# loom.ai re-exports: canonical docs live in the modules that DEFINE each
# symbol (loom.ai.abc, loom.ai.config, loom.ai.errors, loom.ai.inference,
# loom.ai.runtime), listed in reference/api/ai.rst. The 'loom.ai' facade and
# 'loom.ai.compiler' re-export them for import convenience; documenting them
# twice makes every cross-reference ambiguous. Same treatment as loom.core.sql.
_AI_PACKAGE = "loom.ai"


def _reexported_names(package: str) -> tuple[str, ...]:
    """Return the names a façade package re-exports from its submodules.

    Derived from ``__all__`` rather than listed by hand: the hand-written list
    silently went stale the moment the pillar published seven more names, and
    the strict build only said so in CI. A façade re-exports everything it
    declares, so the declaration is the list.

    Args:
        package: Importable façade package, e.g. ``"loom.ai"``.

    Returns:
        Every name in the package's ``__all__``, or an empty tuple when the
        package cannot be imported in this environment.
    """
    try:
        module = importlib.import_module(package)
    except ImportError:
        return ()
    return tuple(getattr(module, "__all__", ()))


_AI_REEXPORTED_NAMES = _reexported_names(_AI_PACKAGE)


_AI_COMPILER_PACKAGE = "loom.ai.compiler"
_AI_COMPILER_REEXPORTED_NAMES = (
    "AgentCompilationError",
    "AgentCompilationIssue",
    "AgentErrorCode",
)

_DUPLICATED_REEXPORTS = {
    ("loom.core.errors", "RuleViolation"),
    ("loom.core.errors", "RuleViolations"),
    ("loom.core.use_case", "RuleViolation"),
    ("loom.core.use_case", "RuleViolations"),
    ("loom.etl.lineage", "ETLObservabilityConfig"),
    ("loom.testing", "CompilationError"),
    ("loom.rest.model", "PaginationMode"),
    *((_SQL_PACKAGE, symbol) for symbol in _SQL_REEXPORTED_NAMES),
    *((_AI_PACKAGE, symbol) for symbol in _AI_REEXPORTED_NAMES),
    *((_AI_COMPILER_PACKAGE, symbol) for symbol in _AI_COMPILER_REEXPORTED_NAMES),
}


def _skip_duplicate_reexports(
    app: Any,
    what: str,
    name: str,
    obj: Any,
    skip: bool,
    options: Any,
) -> bool:
    """Skip known re-exported symbols that duplicate canonical API objects."""
    del what, obj, options
    current_module = app.env.temp_data.get("autodoc:module")
    if (current_module, name) in _DUPLICATED_REEXPORTS:
        return True
    return skip


source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

intersphinx_mapping = {
    "python": (
        "https://docs.python.org/3",
        (None, "_inventories/python-objects.inv"),
    ),
}

suppress_warnings = ["ref.python"]

# Optional dependencies are mocked to keep docs builds lightweight and stable.
autodoc_mock_imports = [
    "aiocache",
    "celery",
    "deltalake",
    "fastapi",
    "fsspec",
    "kombu",
    "omegaconf",
    "polars",
    "pyarrow",
    "prometheus_client",
    "pydantic",
    "pyspark",
    "redis",
    "sqlalchemy",
    "starlette",
    "uvicorn",
]


def setup(app: Any) -> None:
    """Register Sphinx hooks."""
    app.connect("autodoc-skip-member", _skip_duplicate_reexports)
