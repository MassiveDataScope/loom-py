# Architecture overview

`loom-kernel` follows clean architecture boundaries:

- **Domain/Application core**: use cases, rules, compute steps, repository contracts.
- **Infrastructure adapters**: SQLAlchemy repositories, cache decorators, transport integrations.
- **Entry points**: REST/FastAPI runtimes, bootstrap, runtime execution.

Key goal: business behavior stays independent from framework and persistence details.
