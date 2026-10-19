# ADR 0005: One environment selector and deploy checks

- Status: Accepted
- Branch: `hotfix/central-environment`
- Applies to: the release after loom-kernel 2.22.0

## Context

Before this decision loom had two notions of "environment", each with its own
values and default, and neither connected to the configuration files:

- `loom.core.logger.config.Environment` (`dev`, `prod`). Unknown values mapped
  to `dev`, an empty value meant `dev`, and `LoggerConfig.environment`,
  `LogConfig.environment` and `configure_logging_from_values(environment=...)`
  carried it. The renderer and colours followed it: `prod` rendered JSON,
  anything else coloured console lines.
- `loom.prefect.FlowCtx.environment: str = "prod"`, set from the flow's `env`
  parameter and read by nothing.

`create_app(*config_paths)` already merged several YAML files, and callers
listed `config/base.yaml`, `config/production.yaml` by hand. Nothing checked
that a deployed configuration was safe: an API with no authentication and its
OpenAPI schema published started in production exactly as it did on a laptop.

## Decision

Follow the pattern of Django, Spring and Rails: **secure defaults with explicit
escape hatches.** Insecure options stay available for development and tests;
they are never the silent default, and in a deployed environment they warn or
fail at startup.

### One selector

`loom.core.environment` is the only module that reads the selector.

- `Environment(StrEnum)`: `production`, `staging`, `development`, `test`.
  `is_deployed` is true for production and staging.
- `resolve_environment(explicit=None)`: the explicit argument, else `LOOM_ENV`,
  else `production`. An unknown value, an empty `LOOM_ENV` included, raises
  `ConfigError` naming the valid values. It never falls back.
- The application factories (`create_app` for REST and Celery,
  `bootstrap_worker`) resolve it once and pass it on: to the logger, to the
  deploy checks, and into the container (`create_kernel(environment=...)`
  registers it at `APPLICATION` scope). No module holds it in a global.
- `tests/unit/test_environment_containment.py` fails when any other module
  names `LOOM_ENV`, `ENVIRONMENT` or a similar variable;
  `tests/unit/test_domain_containment.py` fails when any other core module
  names an environment (`prod`, `production`, `staging`, `dev`,
  `development`).

Precedents: Rails' `RAILS_ENV` and Spring's active profile select the
configuration; ASP.NET Core treats an unset `ASPNETCORE_ENVIRONMENT` as
`Production`; Django and Flask ship with debug off unless a setting turns it
on.

### The default is production

A forgotten variable in a container fails safe: the application runs with the
production defaults and the deploy checks. A forgotten variable on a laptop
fails loudly: the deploy checks run there too, and their error says to set
`LOOM_ENV=development`. The opposite default would let a container missing one
variable serve as a development build without a word.

### Configuration layering

`create_app(config_dir=...)`, the Celery `create_app(config_dir=...)` and
`bootstrap_worker(config_dir=...)` load `base.yaml`, then
`<environment>.yaml` when it exists. Explicit file lists keep working: they
are the explicit form, not a legacy one. A factory takes exactly one form.

Precedence, lowest first: defaults in code; `base.yaml` or the first explicit
file; the overlay or each later explicit file; within any file, its own values
over its `includes`.

`create_kernel` loads no configuration (it receives a config object), so it
takes only `environment=`.

### Deploy checks

`loom.core.checks` is one module, after Django's system check framework and
`manage.py check --deploy`:

- `@deploy_check("<area>.<E|W><nnn>")` registers a function of the loaded
  `ConfigContext` returning a `Finding(message, hint)` or `None`. The runner
  reports a `CheckResult(id, message, hint)` whose level is the letter of the
  id: `E` fails startup, `W` logs a warning.
- Ids are stable. loom's areas start with `loom.` (`loom.rest.E001`); an
  application registers under its own (`billing.gateway.E001`). A duplicate or
  malformed id is refused at registration.
- `enforce_deploy_checks(config, environment, skip=...)` runs only when the
  environment is deployed. `skip` takes ids or areas the caller knows not to
  apply: a worker skips `loom.rest`, and `create_app` skips `loom.rest.E001`
  when an `authenticator` is passed in code, since the check reads the
  configuration only.
- `checks.silenced: [ids]` in the configuration silences by id, like Django's
  `SILENCED_SYSTEM_CHECKS`. The list is visible in the file of the environment
  that accepts the exception, and every deployed startup logs a warning naming
  the silenced ids.
- A check registers when its module is imported. loom's live in packages the
  factories import anyway: `loom.core.logger` and `loom.rest`.

There is no plugin framework: no entry points, tags or dependency ordering.

### Initial checks

Only settings loom already had:

| Id | Finding |
|---|---|
| `loom.rest.E001` | `app.rest.auth.jwt` is not configured: the API authenticates nobody. |
| `loom.rest.W001` | `app.rest.docs_url`, `redoc_url` or `openapi_url` is not `null`: the schema is published. |
| `loom.logger.W001` | `observability.log.config` asks for the `console` renderer. |

The checks read configuration keys and resolve no placeholder, so they run in
CI without the application's secrets.

### CLI

`loom check --deploy [--env X] <config>` runs every check without starting
the application, whatever the environment, prints each finding with its id
and hint, lists the silenced ids, and exits `1` when an error remains.
`<config>` is a directory (layered by `--env`, which defaults to `LOOM_ENV`)
or explicit files. It loads loom's own checks, the REST ones only when
FastAPI is installed.

### Migration, no shims

loom has no external legacy, so the duplicates are removed outright:

- the logger's `Environment`, `LoggerConfig.environment` and
  `LogConfig.environment` are gone; `configure_logging(config, environment=...)`
  and `configure_logging_from_values(environment=...)` take the central
  `Environment`, production by default;
- `ObservabilityRuntime.from_config(config, environment=...)` hands it to the
  logger;
- `FlowCtx.environment` is gone.

## Coordination

`hotfix/strict-signed-cursors` adds `persistence.cursors.signing` (`signed` by
default, `ephemeral`, `unsigned`). Its check is not part of this decision.
After this branch merges, that branch registers `loom.cursors.E001` with
`@deploy_check` in a module its package already imports, and adds the package
to `_CHECK_PACKAGES` in `loom.cli.check` so `loom check --deploy` sees it.

## Consequences

- An application that sets no `LOOM_ENV` now starts as production: JSON logs,
  and the deploy checks. One without authentication, or that passes no
  `authenticator`, fails at startup until it configures one, sets
  `LOOM_ENV=development`, or silences `loom.rest.E001`.
- The logger's default renderer changed from console to JSON for any process
  that does not pass its environment. The ETL runner, the streaming runner and
  the Prefect maintenance flow call `ObservabilityRuntime.from_config` without
  one, so they log JSON unless `observability.log.config.renderer` says
  otherwise.
- The Prefect flow parameter `env` and the `environments.<env>` blocks of a
  flow YAML stay as they are: they name deployment targets (work pools), with
  free-form keys and `"prod"` as the flow default. Unifying them with
  `Environment` would change the Prefect deploy API and is left open.
