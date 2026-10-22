# Clean architecture rules

Project invariants:

1. Domain/Application layers must not import infrastructure.
2. Repositories are interface-driven.
3. Runtime side effects are explicit and observable.
4. Public APIs are typed and documented.

This contract is enforced through module boundaries and strict typing checks.

## Optional dependencies: islands

A dependency loom does not require lives behind an extra, and the code that
uses it follows one pattern, whatever the pillar:

1. **One optional dependency, one island.** An island is a module that
   imports the vendor SDK at its top, with an ordinary `import`, and nothing
   else in loom imports that SDK. The island builds what the SDK serves
   (`loom.ai.engines.pydantic_ai.providers.bedrock`, `loom.core.sql.clickhouse`,
   `loom.streaming.bytewax`).
2. **The selector loads the island, not the SDK.** A registry keyed by
   provider, engine or backend name loads the island by module path once it
   knows the name — `loom.core.plugins.optional.import_optional` for a module
   of loom, `loom.core.plugins.entrypoints` for a plugin distribution — and
   translates `MissingExtraError` into its own coded failure naming the extra
   (`PROVIDER_NOT_INSTALLED` in the AI layer).
3. **Nothing else.** No `import` inside a function to reach a vendor SDK, no
   module-level `try`/`except ImportError` with a `None` sentinel checked
   later, no `importlib.import_module("vendor")` on the request path, no
   attribute lookup on a vendor module.

The island needs no `try`/`except` of its own: its `ImportError` is what
`import_optional` translates. Islands today: the pydantic-ai providers and
extras (`loom.ai.engines.pydantic_ai.providers`, `.extras`), the client
builders of `loom.core.connections.clients` (boto3, pymongo,
clickhouse-connect; see [ADR 0006](adr/0006-hexagonal-layers.md)), and the whole-package islands
that predate the rule (`loom.core.sql.clickhouse`, `loom.streaming.bytewax`,
`loom.etl.testing`). A dependency listed under `dependencies` is not
optional and gets no island: import it at the top like any other. A test that stands in for an absent or stubbed SDK
blocks the SDK **and** evicts the island, so the next load re-imports it;
`tests/helpers/extras.py` does both. A module that reads an SDK the application
itself may or may not have loaded — pydantic in `loom.core.model` — gates on
`sys.modules` instead, on purpose: loom never loads what the application did
not. Code that predates this rule is migrated when it is next touched.
