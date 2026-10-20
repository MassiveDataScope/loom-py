# ADR 0005: One environment selector, layered configuration and deploy checks

- Status: Accepted
- Branch: `hotfix/central-environment`
- Applies to: the release after loom-kernel 2.22.0

## Context

Before this decision loom had three notions of "environment", each with its
own values and default, and none connected to the configuration files:

- `loom.core.logger.config.Environment` (`dev`, `prod`). Unknown values mapped
  to `dev`, an empty value meant `dev`, and `LoggerConfig.environment`,
  `LogConfig.environment` and `configure_logging_from_values(environment=...)`
  carried it. The renderer and colours followed it: `prod` rendered JSON,
  anything else coloured console lines.
- `loom.prefect.FlowCtx.environment: str = "prod"`, set from the flow's `env`
  parameter and read by nothing.
- The Prefect flow parameter `env` (default `"prod"`) and the free-form keys
  of a flow YAML's `environments` block, which picked the work pool.

`create_app(*config_paths)` already merged several YAML files, and callers
listed `config/base.yaml`, `config/production.yaml` by hand. No variable could
override a key. Nothing checked that a deployed configuration was safe: an API
with no authentication and its OpenAPI schema published started in production
exactly as it did on a laptop. A malformed `observability` section silently
fell back to defaults, and an unknown logger key was ignored.

## Decision

Follow the pattern of Django, Spring and Rails: **secure defaults with explicit
escape hatches**, and **one place that tells the code which environment it is
in**. Insecure options stay available for development and tests; they are
never the silent default, and in a deployed environment they warn or fail at
startup.

### One selector

`loom.core.environment` is the only module that names or reads loom's process
variables: the selector, the `LOOM__*` overrides and every other `LOOM_*`
variable (`LOOM_CONFIG`, `LOOM_STORAGE_CONFIG_PATH`, `LOOM_ETL_CONFIG`,
`LOOM_SPOOL_DIR`, the Spark test ones), which the rest of loom reads through
`loom_variable` and sets for a block through `scoped_loom_variable`.

- `Environment(StrEnum)`: `production`, `staging`, `development`, `test`.
  `is_deployed` is true for production and staging.
- `resolve_environment(explicit=None)`: the explicit argument, else `LOOM_ENV`,
  else `production`. An unknown value, an empty `LOOM_ENV` included, raises
  `ConfigError` naming the valid values. It never falls back.
  `parse_environment(raw, source=...)` applies the same rules to a value that
  names an environment without selecting the running one.
- Every entry point resolves it once and passes it on: to the logger, to the
  deploy checks, and into the container (`bootstrap_app` registers it at
  `APPLICATION` scope with the configuration, before the modules run). The
  entry points are REST and Celery `create_app`, `bootstrap_worker`,
  `ETLRunner`, `StreamingRunner` and the Prefect flows. The public factories
  (`create_kernel`, `bootstrap_app`, `ObservabilityRuntime.from_config`,
  `configure_logging*`) take `environment=None` and resolve it the same way.
  No module holds it in a global.
- Prefect: the flow parameter `env` and the keys of `environments` are
  `Environment` values; an unknown key or value fails. A run resolves the
  process environment (`LOOM_ENV`, else production) and `env` only confirms
  it (`confirm_environment`): a run whose `env` differs fails before anything
  runs, so a run parameter cannot turn a production worker into a
  development one. `deploy_flow` and `discover_and_deploy_etls` resolve `env`
  like the factories, pick `environments.<env>`, and record the environment
  as the deployment's `env` parameter, so a run on a worker of another
  environment fails loudly.
- `tests/unit/test_environment_containment.py` fails when any other module
  names `LOOM_ENV`, `ENVIRONMENT`, a similar variable, a `LOOM_*` variable or
  the `LOOM__` prefix as a string literal; when, outside the environment
  module, any access to `os.environ`, `environ`, `os.getenv` or `getenv` is
  anything but a read of one variable named by a string literal that does not
  start with `LOOM_` (closed world: a dynamic key, iteration, `.copy()`,
  `dict(os.environ)`, a write or membership on a name fails, except in two
  allowlisted dynamic readers with their reason); and when src, the docs or
  the README name a `LOOM_*` variable loom does not own. Each failure class
  of the closed-world rule has one self-test, and one fixture shows the
  allowed shape;
  `tests/unit/test_domain_containment.py` fails when any other module of loom
  names an environment (`prod`, `production`, `staging`, `dev`,
  `development`), with one exemption keyed by file and literal for a literal
  that is not one.

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

### Configuration sources

`LOOM_ENV` is the only required variable. Precedence, lowest first:

1. defaults in code;
2. `base.yaml`, or the first explicit file;
3. `<LOOM_ENV>.yaml`, or each later explicit file;
4. `LOOM__SECTION__KEY` variables.

Within any file, its own values win over its `includes`.

- `create_app(config_dir=...)`, the Celery `create_app(config_dir=...)` and
  `bootstrap_worker(config_dir=...)` load `base.yaml`, then
  `<environment>.yaml` when it exists. Explicit file lists keep working: they
  are the explicit form, not a legacy one. A factory takes exactly one form.
  `ETLRunner.from_yaml`, `StreamingRunner.from_yaml`, the Prefect storage
  configuration and the migration locator take a file or a directory, which
  `config_source_paths` layers the same way.
- Migrations: Alembic has no application entry point, so the locator still
  needs `LOOM_CONFIG` or an explicit path; either may name the same config
  directory, layered by `LOOM_ENV` like the application. `app.code_path` is
  put on `sys.path` by one function, `loom.core.locator.add_code_path`, for
  `create_app`, the CLI and the locator alike: relative to the first file's
  directory, once, and only when the directory exists.
- `loom.core.config.overrides.load_application_config` is the one function
  every application load goes through: REST and Celery `create_app`,
  `bootstrap_worker`, `loom check --deploy`, `ETLRunner.from_yaml` (and so
  the Prefect flows), `StreamingRunner.from_yaml`, the migration locator and
  `load_kafka_settings`. A per-flow Prefect YAML defines a flow, not the
  process's application configuration, and is read without overrides.
  `ConfigContext.from_yaml` is the raw loader and applies none.
  `create_kernel` loads no configuration, so it takes only `environment=`.
- `LOOM__*` variables are built on OmegaConf: they become a dotlist
  (`OmegaConf.from_dotlist`) merged as the last layer. A double underscore
  separates levels, as in ASP.NET Core and pydantic-settings. Each level
  is letters, digits and underscores, and matches the declared key
  case-insensitively, walking the configuration as written so no placeholder
  is resolved; an undeclared level is added in lower case. The first level
  must name a section the configuration declares or `ConfigKey` lists;
  anything else is a startup error naming the variable. Two variables for the
  same key, or for a key and one of its parents, are refused naming both, and
  the merge follows the resolved keys. Deeper typos are refused by the section
  structs that forbid unknown fields, which are those with a single reader:
  `app.rest` and below, the `observability` sub-sections, `sql`, `celery`,
  `jobs`, every `storage` struct, `kafka`, the bytewax runtime, `dynamodb`
  and the flow config. `app`, `database`, `persistence` and the top of
  `observability` are
  decoded in part by several readers and accept keys another reader owns;
  `ai` keeps its own validation.
- An override sets one leaf value. A value that is a mapping, or a list
  holding one at any depth, is refused, and so is a target the files declare
  that way (`app.rest.disable_routes`), whatever the new value: an override
  cannot replace a structure. Lists of scalars are leaves.
- In production and staging an override may set only an allowlisted key.
  A denylist of reviewed keys kept missing paths (`prometheus.enabled`, the
  REST interfaces, `code_path`); an allowlist names the keys opened instead
  of the keys forgotten. loom ships `loom.core.config.overrides.OVERRIDABLE`,
  below, and an application extends it only in its reviewed files, under
  `config.overridable`: a literal list of dotted keys, `*` standing for one
  level but never the first. An entry inside `checks` or `config` is a
  startup error naming the entry: both are read as written, so the files
  could not route them to the environment any other way. Most other keys
  (`app.rest.auth.jwt.audience`, say) the files could already set from
  `${oc.env:...}`, so listing them is the same reviewed choice; a deploy
  check still judges the merged value. The entries are
  checked at every load, in every environment and with or without a
  variable, so a bad entry fails in development first. `config` can never be
  overridden, in any environment. Keys match case-insensitively and exactly;
  an entry covers no sibling or child. In development and test any leaf may
  be overridden. The `checks` section is also read as written, and a
  placeholder anywhere in it is a `ConfigError`.
- An allowlisted key also changes every `${...}` placeholder in the files
  that references it: overriding `shared.timeout` changes each key set to
  `${shared.timeout}`. loom does not trace those references; the files are
  reviewed for them, as for `config.overridable` itself. A key the files set
  from `${oc.env:...}` is controlled by the process environment because the
  files chose so.

| Key | Why it is safe to change per deploy |
|---|---|
| `observability.log.config.level` | Verbosity only; what is logged does not change, how much does. |
| `observability.log.config.named_levels.*` | The same, per stdlib logger (a noisy library). |
| `celery.worker_concurrency` | Capacity of a worker; tasks and their routing do not change. |
| `celery.worker_max_tasks_per_child` | Process recycling; a resource limit. |
| `celery.worker_prefetch_multiplier` | Queue fairness against throughput; a capacity knob. |
| `celery.runtime.shutdown_timeout_ms` | How long shutdown waits for in-flight work. |
| `streaming.runtime.workers_per_process` | Capacity of a streaming process. |
| `streaming.runtime.epoch_interval_ms` | Snapshot cadence; a throughput and recovery trade-off. |
| `streaming.runtime.force_shutdown_timeout_ms` | How long shutdown waits for in-flight work. |
| `sql.connections.*.pool_size` | Connection pool capacity. |
| `sql.connections.*.executor_threads` | Executor capacity. |
| `sql.connections.*.connect_timeout` | Network timeout. |
| `sql.connections.*.send_receive_timeout` | Network timeout. |

The SQL timeouts, `pool_size` and `executor_threads` must be above zero; a
`0` fails when the section decodes. The Celery and streaming worker counts
and `worker_prefetch_multiplier` are not constrained.

Left off on purpose: anything that changes where data goes or who may reach
it (broker and result URLs, the OTel endpoint, CORS origins, documentation
and metrics paths, authentication, `checks`), the structure of the
application (discovery, interfaces, routes, `code_path`) and
`sql.connections.*.max_execution_time`, a security limit on every query.

The allowlist guards against careless per-deploy overrides, not against an
attacker. Whoever controls the process environment also controls `LOOM_ENV`,
`LOOM_CONFIG`, `LOOM_STORAGE_CONFIG_PATH`, `LOOM_ETL_CONFIG` and
`PYTHONPATH`, and so which files load and which code runs; the allowlist is
no boundary against that access.

- Values are literals: each value is parsed first and any string in it
  holding `${` is refused, so a quoted, `\x24`-escaped or nested placeholder
  cannot slip through; a variable reaching inside an interpolated key is
  refused too. Values parse as a YAML 1.1 dotlist (`5`, `false`, `no`,
  `[a, b]`, empty is null, ` #` starts a comment) and the msgspec structs
  validate them; a list is replaced whole. Any parse failure, a YAML tag that
  fails to construct included, becomes a `ConfigError` naming the variable;
  this module never logs or raises with an override's value.
- One WARNING line names the keys that came from variables, never their
  values. WARNING because it is written before the logger is configured.
- What belongs in `LOOM__*`: per-deployment tuning, the allowlist above and
  whatever the application adds to it. What does not: secrets and
  application structure.
- Secrets: on ECS, the task definition's `secrets` (`valueFrom` with a
  Secrets Manager or SSM ARN) inject them as variables, and the YAML reads
  them with `${oc.env:...}`. On other platforms `${secrets:...}` and
  `${ssm:...}` resolve them at load. Do not pass secrets through `LOOM__*`:
  the override line names every key, and a `${...}` value is refused. loom
  builds no literal-secret check; the docs recommend gitleaks in pre-commit
  and CI.

Example on ECS (one container definition of the task definition):

```json
{
  "environment": [
    {"name": "LOOM_ENV", "value": "production"},
    {"name": "LOOM__OBSERVABILITY__LOG__CONFIG__LEVEL", "value": "INFO"},
    {"name": "LOOM__CELERY__WORKER_CONCURRENCY", "value": "8"}
  ],
  "secrets": [
    {
      "name": "DATABASE_URL",
      "valueFrom": "arn:aws:secretsmanager:eu-west-1:123456789012:secret:store/database-url"
    }
  ]
}
```

```yaml
# config/base.yaml
database:
  url: ${oc.env:DATABASE_URL}
```

Stale keys fail where the section struct forbids unknown fields (above), and
an `observability` section that fails validation stops startup instead of
falling back to defaults.

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
  environment is deployed, on the final merged configuration; otherwise it
  logs one WARNING that the checks were skipped, naming the environment and
  `LOOM_ENV`. `skip` takes ids
  or areas a process knows not to apply: processes serving no HTTP (the
  worker, the ETL and streaming runners, the maintenance flow) skip
  `REST_CHECKS` (`loom.rest`). Runners run them before building any backend
  or agent runner.
- `checks.silenced: [ids]` silences by id, like Django's
  `SILENCED_SYSTEM_CHECKS`. The list is visible in the file of the environment
  that accepts the exception, and every deployed startup logs a warning naming
  the silenced ids.
- The `loom.core.checks` logger, a stdlib logger, sets its own WARNING level,
  and the handlers `configure_logging` attaches pass every record, so a
  parent at `CRITICAL`
  (the top-level level or `named_levels.loom`) cannot hide a check's warning
  or the silenced ids, in any environment; a named level on
  `loom.core.checks` itself is the explicit way to quiet them. A level on the
  logger rather than running the checks before logging is configured,
  because every entry point then writes them through the configured handlers
  whatever order it builds things in.
- `checks.modules: [dotted.module]` names the modules registering the
  application's own checks. Every run imports them first, so they run at
  startup and in CI alike. `create_app` and the CLI run the checks once
  `app.code_path` is importable; the worker and runners import from their own
  path, as for discovery. An unknown key under `checks` or a module failing
  at import is a `ConfigError`.
- loom's own checks live in packages the entry points import anyway:
  `loom.core.logger` and `loom.rest`.

There is no plugin framework: no entry points, tags or dependency ordering.

### Initial checks

Only settings loom already had, plus the one declaration that lets the
configuration name a mechanism supplied in code:

| Id | Finding |
|---|---|
| `loom.rest.E001` | Neither a literal `app.rest.auth.jwt` mapping nor the literal `app.rest.auth.custom: true` is set: the API authenticates nobody. A placeholder `jwt` fails, since it may resolve to null. |
| `loom.rest.W001` | `app.rest.docs_url`, `redoc_url` or `openapi_url` is not `null`: the schema is published. |
| `loom.logger.W001` | `observability.log.config` asks for the `console` renderer. |

`app.rest.auth.custom: true` declares that `create_app` receives an
`authenticator`. `create_app` refuses an `authenticator` without it, `custom:
true` without an `authenticator`, and `custom: true` beside `jwt`. The CLI and
`create_app` therefore read the same answer, and nobody has to silence
`loom.rest.E001` to pass CI. `create_app` registers the in-process job service
when the `celery` section is absent (with a warning) or the Celery extra is
not installed; a `celery` section that fails validation stops startup.

The checks read keys as written (`ConfigContext.raw`) and resolve no
placeholder, so they run in CI without the application's secrets; a value
given by a placeholder is judged as written.

### CLI

`loom check --deploy [--env X] [--skip AREA]... <config>` runs every check
without starting the application, whatever the environment, prints each
finding with its id and hint, lists the silenced ids, and exits `1` when an
error remains. `<config>` is a directory (layered by `--env`, which defaults
to `LOOM_ENV`) or explicit files; it loads with loom's default resolvers, and
the `LOOM__*` variables of the shell apply (those of a deployed task do not,
which is why security settings belong in the files). A configuration does
not say which process it is for (a REST app may declare `celery`, a worker
may share `app.rest`), so the CLI does not guess: `--skip loom.rest` checks a
worker, ETL or streaming configuration as those processes do at startup,
and the command prints the areas it skipped.
`app.code_path` joins the import path as `create_app` adds it, so
`checks.modules` import. It loads loom's own checks, the REST ones only when
FastAPI is installed.

### Migration, no shims

loom has no external legacy, so the duplicates are removed outright:

- the logger's `Environment`, `LoggerConfig.environment` and
  `LogConfig.environment` are gone; `configure_logging(config, environment=...)`
  and `configure_logging_from_values(environment=...)` take the central
  `Environment`, resolved when `None`;
- `ObservabilityRuntime.from_config(config, environment=...)` hands it to the
  logger;
- `FlowCtx.environment` is gone, and the Prefect `"prod"` defaults with it.

## Coordination

`hotfix/strict-signed-cursors` adds `persistence.cursors.signing` (`signed` by
default, `ephemeral`, `unsigned`). Its check is not part of this decision.
After this branch merges, that branch registers `loom.cursors.E001` with
`@deploy_check` in a module its package already imports, and adds the package
to `_load_check_packages` in `loom.cli.check` so `loom check --deploy` sees
it. `loom.cursors.E001` applies only when a repository backend is
configured. The development fixture in `tests/unit/rest/test_deploy_checks.py`
that builds a REST app from its own YAML already sets
`persistence.cursors.signing: ephemeral`. The tests that will break are the
ones that start an application with a repository backend in a deployed
environment and expect it to start:

- `tests/unit/rest/test_deploy_checks.py`:
  `test_an_authenticator_declared_as_custom_starts_in_production`,
  `test_a_warning_is_logged_and_the_app_starts` and
  `test_a_silenced_error_lets_the_app_start_and_is_logged` (their projects
  declare a SQLite `database`);
- `tests/integration/celery_bootstrap/test_bootstrap_worker.py`:
  `test_bootstrap_worker_uses_top_level_observability_section`, which starts
  a staging worker with a `database`.

The deployed tests that expect a `DeployCheckError` or a `ConfigError` keep
passing, the error then listing the cursor finding too, and the
`loom check --deploy` tests declare no repository backend.

The four need the cursor setting in their configurations, or the id under
`checks.silenced`. The guide's overlay examples need the same setting.

When #371 rebases on this branch it must also:

- rename the cursor key example variables, which carry the `LOOM_` prefix
  in `docs/rest/persistence-backends.md` and
  `src/loom/core/repository/abc/cursor.py`, to `CURSOR_KEY_...`. `LOOM_`
  names belong to `loom.core.environment`, and the containment test fails on
  a `LOOM_*` name loom does not own;
- route the `os.environ` default of the cursor key sources (`environ=` in
  `abc/cursor.py`) through `loom.core.environment`, or add that file to the
  containment test's dynamic readers with its reason: the closed-world rule
  refuses a bare `os.environ` outside the environment module;
- keep `persistence.cursors.*` off the override allowlist. In production and
  staging `LOOM__PERSISTENCE__CURSORS__SIGNING=unsigned` then fails at
  startup as a key off the allowlist, before any deploy check runs;
- make its cursor configuration struct forbid unknown fields if
  `persistence.cursors` has a single reader, like the other single-reader
  sections.

## Consequences

- An application that sets no `LOOM_ENV` starts as production: JSON logs and
  the deploy checks. One without authentication fails at startup until it
  configures it, sets `LOOM_ENV=development`, or silences `loom.rest.E001`.
- `create_app(authenticator=...)` requires `app.rest.auth.custom: true`.
- Flow YAML `environments` keys must be environment names; `prod`, `dev` and
  `local` must be renamed.
- An unknown key under a single-reader section, a malformed `observability`
  section, or a `LOOM__` variable that names no known section, sets a
  mapping, replaces a declared structure, names `config`, names a key off
  the allowlist in production or staging, or holds `${`
  stops startup, as does a `config.overridable` entry that is malformed,
  starts with `*`, or lies inside `checks` or `config`.
- The SQL timeouts, `pool_size` and `executor_threads`, the Celery
  `runtime.shutdown_timeout_ms` and the streaming runtime's
  `epoch_interval_ms` and `force_shutdown_timeout_ms` must be above zero.
- The handlers `configure_logging` attaches pass every record, so a named
  level below the top-level level now takes effect for stdlib loggers;
  structlog loggers, loom's own included, still drop events below the
  top-level level.
- A consumer's test suite must set `LOOM_ENV=test`, or every application it
  builds starts as production and runs the deploy checks.
- The migration locator resolves `LOOM_ENV`, applies the `LOOM__*` variables
  and puts `app.code_path` on `sys.path` (when the directory exists), as the
  application does.
- A Prefect worker must run with `LOOM_ENV` set to the environment of its
  deployments; a run whose `env` differs fails.
