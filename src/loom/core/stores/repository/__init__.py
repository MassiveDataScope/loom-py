"""Repository ports: capabilities, queries, cursors, registration and records."""

from loom.core.stores.repository import checks as checks
from loom.core.stores.repository.capabilities import (
    BulkCreatable,
    Countable,
    Creatable,
    Deletable,
    FieldReadable,
    Listable,
    OnConflict,
    Readable,
    RepoFor,
    Updatable,
)
from loom.core.stores.repository.catalog import RepositoryCatalog
from loom.core.stores.repository.cursor import (
    CURSOR_SIGNING_CHECK,
    Cursor,
    CursorCodec,
    CursorConfig,
    CursorKeySource,
    register_cursor_codec,
    require_cursor_sort,
)
from loom.core.stores.repository.mutation import MutationEvent
from loom.core.stores.repository.query import (
    CursorResult,
    FilterGroup,
    FilterOp,
    FilterParams,
    FilterSpec,
    PageParams,
    PageResult,
    PaginationMode,
    QuerySpec,
    SortSpec,
    build_page_result,
)
from loom.core.stores.repository.query_compiler import QueryCompiler
from loom.core.stores.repository.registration import (
    RepositoryDecorator,
    build_repository_registration_module,
)
from loom.core.stores.repository.registry import (
    DefaultRepositoryBuilder,
    RepositoryBuildContext,
    RepositoryBuilder,
    RepositoryRegistration,
    RepositoryToken,
    get_repository_registration,
    repository_for,
)
from loom.core.stores.repository.repository import (
    CreateT,
    IdT,
    OutputT,
    Repository,
    RepositoryRead,
    RepositoryWrite,
    UpdateT,
)
from loom.core.stores.repository.session_scope import (
    SupportsCacheScope,
    SupportsCallerScopedSession,
)

__all__ = [
    "MutationEvent",
    "RepositoryDecorator",
    "build_repository_registration_module",
    "DefaultRepositoryBuilder",
    "RepositoryBuildContext",
    "RepositoryBuilder",
    "RepositoryCatalog",
    "RepositoryRegistration",
    "RepositoryToken",
    "get_repository_registration",
    "repository_for",
    "CURSOR_SIGNING_CHECK",
    "CreateT",
    "Cursor",
    "CursorCodec",
    "CursorConfig",
    "CursorKeySource",
    "CursorResult",
    "FilterGroup",
    "FilterOp",
    "FilterParams",
    "FilterSpec",
    "IdT",
    "OutputT",
    "PageParams",
    "PageResult",
    "PaginationMode",
    "QueryCompiler",
    "QuerySpec",
    "BulkCreatable",
    "Countable",
    "Creatable",
    "Deletable",
    "FieldReadable",
    "Listable",
    "OnConflict",
    "Readable",
    "RepoFor",
    "Updatable",
    "Repository",
    "RepositoryRead",
    "RepositoryWrite",
    "SortSpec",
    "SupportsCacheScope",
    "SupportsCallerScopedSession",
    "UpdateT",
    "build_page_result",
    "register_cursor_codec",
    "require_cursor_sort",
]
