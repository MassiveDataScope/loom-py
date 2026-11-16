"""SQLAlchemy query compiler for QuerySpec.

Translates :class:`~loom.core.stores.repository.query.QuerySpec` objects into
SQLAlchemy constructs (WHERE clauses, ORDER BY, cursor predicates).

Public surface:

- :class:`QuerySpecCompiler` — main orchestrator.
- :exc:`FilterPathError` — unknown field path.
- :exc:`UnsafeFilterError` — filter denied by repository policy.
"""

from loom.core.repository.sqlalchemy.query_compiler.compiler import QuerySpecCompiler
from loom.core.repository.sqlalchemy.query_compiler.errors import (
    FilterPathError,
    UnsafeFilterError,
)

__all__ = ["FilterPathError", "QuerySpecCompiler", "UnsafeFilterError"]
