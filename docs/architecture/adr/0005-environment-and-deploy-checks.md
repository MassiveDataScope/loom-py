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

`loom.core.environment` is the only module that reads the process environment
for loom: the selector and the `LOOM__*` overrides.

- `Environment(StrEnum)`: `production`, `staging`, `development`, `test`.
  `is_deployed` is true for production and staging.
- `resolve_environment(explicit=None)`: the explicit argument, else `LOOM_ENV`,
  else `production`. An unknown value, an empty `LOOM_ENV` included, raises
  `ConfigError` naming the valid values. It never falls back.
  `parse_environment(raw, source=...)` applies the same rules to a value that
  names an environment without selecting the running one.
- Every entry point resolves it once and passes it on: to the logger, to the
  deploy checks, and into the container (`create_kernel(environment=...)`
  registers it at `APPLICATION` scope). The entry points are REST and Celery
  `create_app`, `bootstrap_worker`, `ETLRunner`, `StreamingRunner` and the
  Prefect flows. No module holds it in a global.
- Prefect: the flow parameter `env` and the keys of `environments` are
  `Environment` values; an unknown key or value fails. A run resolves `env`
  (default `None`: `LOOM_ENV`, else production) and hands it to the runner.
  `deploy_flow` and `discover_and_deploy_etls` resolve `env` the same way,
  pick `environments.<env>`, and record the environment as the deployment's
  `env` parameter, so its runs resolve to it.
- `tests/unit/test_environment_containment.py` fails when any other module
  names `LOOM_ENV`, `ENVIRONMENT`, a similar variable or the `LOOM__` prefix;
  `tests/unit/test_domain_containment.py` fails when any other module of loom
  names an environment (`prod`, `production`, `staging`, `dev`,
  `development`), with one listed exemption for a literal that is not one.

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
- `loom.core.config.overrides.load_application_config` is the one function
  every entry point loads through: REST and Celery `create_app`,
  `bootstrap_worker`, `loom check --deploy`, `ETLRunner.from_yaml` (and so
  the Prefect flows), `StreamingRunner.from_yaml` and the migration locator.
  `create_kernel` loads no configuration, so it takes only `environment=`.
- `LOOM__*` variables are built on OmegaConf: they become a dotlist
  (`OmegaConf.from_dotlist`) merged as the last layer. A double underscore
  separates levels, as in ASP.NET Core and pydantic-settings. Each level
  matches the declared key case-insensitively; an undeclared level is added
  in lower case. The first level must name a section the configuration
  declares or loom knows (`ConfigKey`, `checks`); anything else is a startup
  error naming the variable. Deeper typos are refused by the section structs,
  which forbid unknown fields. Values parse as a dotlist (`5`, `false`,
  `[a, b]`, `{a: 1}`) and the msgspec structs validate them; a list is
  replaced whole.
- One WARNING line names the keys that came from variables, never their
  values. WARNING because it is written before the logger is configured.
- What belongs in `LOOM__*`: per-deployment tuning such as log level, pool
  sizes and timeouts, the OTel endpoint, CORS origins and the public URL.
  What does not: secrets and application structure (discovery, routes, the
  authentication mechanism).
- Secrets: on ECS, the task definition's `secrets` (`valueFrom` with a
  Secrets Manager or SSM ARN) inject them as variables, and the YAML reads
  them with `${oc.env:...}`. On other platforms `${secrets:...}` and
  `${ssm:...}` resolve them at load. Secrets never go through `LOOM__*`, so
  they never reach the override line. loom builds no literal-secret check;
  the docs recommend gitleaks in pre-commit and CI.

Example on ECS:

```json
{
  "environment": [
    {"name": "LOOM_ENV", "value": "production"},
    {"name": "LOOM__OBSERVABILITY__LOG__CONFIG__LEVEL", "value": "INFO"},
    {"name": "LOOM__APP__REST__CORS__ALLOW_ORIGINS", "value": "[https://shop.example]"}
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

Stale keys fail: `LoggerConfig` and its handler configs forbid unknown
fields, and an `observability` section that fails validation stops startup
instead of falling back to defaults.

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
  environment is deployed, on the final merged configuration. `skip` takes ids
  or areas a process knows not to apply: processes serving no HTTP (the
  worker, the ETL and streaming runners, the maintenance flow) skip
  `REST_CHECKS` (`loom.rest`).
- `checks.silenced: [ids]` silences by id, like Django's
  `SILENCED_SYSTEM_CHECKS`. The list is visible in the file of the environment
  that accepts the exception, and every deployed startup logs a warning naming
  the silenced ids.
- `checks.modules: [dotted.module]` names the modules registering the
  application's own checks. Every run imports them first, so they run at
  startup and in CI alike. `create_app` runs the checks once `app.code_path` is
  importable. An unknown key under `checks` or a module that cannot be
  imported is a `ConfigError`.
- loom's own checks live in packages the entry points import anyway:
  `loom.core.logger` and `loom.rest`.

There is no plugin framework: no entry points, tags or dependency ordering.

### Initial checks

Only settings loom already had, plus the one declaration that lets the
configuration name a mechanism supplied in code:

| Id | Finding |
|---|---|
| `loom.rest.E001` | Neither `app.rest.auth.jwt` nor `app.rest.auth.custom: true` is set: the API authenticates nobody. |
| `loom.rest.W001` | `app.rest.docs_url`, `redoc_url` or `openapi_url` is not `null`: the schema is published. |
| `loom.logger.W001` | `observability.log.config` asks for the `console` renderer. |

`app.rest.auth.custom: true` declares that `create_app` receives an
`authenticator`. `create_app` refuses an `authenticator` without it, `custom:
true` without an `authenticator`, and `custom: true` beside `jwt`. The CLI and
`create_app` therefore read the same answer, and nobody has to silence
`loom.rest.E001` to pass CI.

The checks read configuration keys and resolve no placeholder, so they run in
CI without the application's secrets.

### CLI

`loom check --deploy [--env X] <config>` runs every check without starting
the application, whatever the environment, prints each finding with its id
and hint, lists the silenced ids, and exits `1` when an error remains.
`<config>` is a directory (layered by `--env`, which defaults to `LOOM_ENV`)
or explicit files; the `LOOM__*` variables apply. `app.code_path` joins the
import path as `create_app` adds it, so `checks.modules` import. It loads
loom's own checks, the REST ones only when FastAPI is installed.

### Migration, no shims

loom has no external legacy, so the duplicates are removed outright:

- the logger's `Environment`, `LoggerConfig.environment` and
  `LogConfig.environment` are gone; `configure_logging(config, environment=...)`
  and `configure_logging_from_values(environment=...)` take the central
  `Environment`, production by default;
- `ObservabilityRuntime.from_config(config, environment=...)` hands it to the
  logger;
- `FlowCtx.environment` is gone, and the Prefect `"prod"` defaults with it.

## Coordination

`hotfix/strict-signed-cursors` adds `persistence.cursors.signing` (`signed` by
default, `ephemeral`, `unsigned`). Its check is not part of this decision.
After this branch merges, that branch registers `loom.cursors.E001` with
`@deploy_check` in a module its package already imports, and adds the package
to `_CHECK_PACKAGES` in `loom.cli.check` so `loom check --deploy` sees it.
Because the checks run on the merged configuration,
`LOOM__PERSISTENCE__CURSORS__SIGNING=unsigned` in production will fail
startup once that check exists.

## Consequences

- An application that sets no `LOOM_ENV` starts as production: JSON logs and
  the deploy checks. One without authentication fails at startup until it
  configures it, sets `LOOM_ENV=development`, or silences `loom.rest.E001`.
- `create_app(authenticator=...)` requires `app.rest.auth.custom: true`.
- Flow YAML `environments` keys must be environment names; `prod`, `dev` and
  `local` must be renamed.
- An unknown logger key, a malformed `observability` section, or a `LOOM__`
  variable naming no known section stops startup.
