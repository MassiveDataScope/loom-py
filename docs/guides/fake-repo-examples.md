# Fake repo examples

This project contains a documented example in `docs/examples-repo/index.md`.

Recommended reading order:

1. Product commands and models.
2. Use cases with `Rule` and `Compute`.
3. Custom repository contract and SQLAlchemy implementation.
4. Interface declaration and route wiring.
5. Integration tests that validate end-to-end behavior using these examples.

These examples are the reference implementation for DSL usage and expected behavior.

Useful files:

- `tests/integration/fake_repo/product/repository_contract.py`
- `tests/integration/fake_repo/product/repository.py`
- `tests/integration/fake_repo/src/app/product/use_cases.py`
- `tests/integration/core/use_case/test_use_case_crud_integration.py`
- `tests/integration/core/use_case/test_custom_repository_integration.py`

Custom repository pattern:

- Declare the custom contract as `RepoFor[Model]` plus only the extra methods.
- Import `repository_for` from `loom.core.stores.repository`, not from the SQLAlchemy package.
- Register the SQLAlchemy implementation with `@repository_for(Model)` and make the
  repository class inherit the capability `Protocol` directly.
- Keep CRUD-only use cases on `self.main_repo`; they automatically receive the custom implementation.
- Reserve constructor-injected repository contracts for advanced cases where a use case or job needs a secondary repository dependency; the primary public example should stay on `main_repo`.
- `repository_for(...)` also works for non-persistible logical types:
  - use `LoomStruct` for neutral internal structs
  - use `Response` for API-facing structs that should serialize in `camelCase`
- The SQLAlchemy bootstrap module now registers `DefaultRepositoryBuilder` with
  `SQLAlchemyDefaultRepositoryBuilder` as the current fallback for `BaseModel`.
- That fallback keeps `SessionManager` inside the SQLAlchemy adapter, so SQLAlchemy
  construction details do not leak into the core repository contract.
- To replace the project-wide backend, register another `DefaultRepositoryBuilder`
  implementation in the container; `UseCase[Model, Result]` will continue to
  resolve `self.main_repo` through that new base repository.
- Explicit `repository_for(...)` bindings still take precedence for both
  persistible and non-persistible types.
- When a custom repository needs more constructor dependencies than the default
  class path supports, use `repository_for(..., builder=...)` and resolve those
  dependencies from `RepositoryBuildContext.container`.
