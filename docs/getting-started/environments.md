# Environments, configuration and deploy checks

A loom application runs in one of four environments: `production`, `staging`,
`development` or `test`. One selector picks it, and the environment then picks
the configuration overlay, the logger defaults and whether the deploy checks
run. The pattern is the one Django, Spring and Rails use: secure defaults live
in code, configuration files override them, and nothing guesses the
environment on its own.

`LOOM_ENV` is the only variable loom requires.

## Selecting the environment

The environment comes from, in order:

1. the `environment=` argument of the entry point: REST and Celery
   `create_app`, `bootstrap_worker`, `ETLRunner`, `StreamingRunner` (a
   Prefect flow's `env` parameter is not a selector: it only confirms the
   worker's `LOOM_ENV`, see below);
2. the `LOOM_ENV` variable;
3. `production`, when neither is given.

An unknown value, an empty `LOOM_ENV` included, stops startup with an error
listing the valid ones. It never falls back to another environment.

Production is the default so that a container started without the variable
runs with the secure defaults. Locally, the same default makes a forgotten
variable visible: the deploy checks run, and an error among them stops startup
with a message saying to set `LOOM_ENV=development`.

The application factories resolve the environment once and register it in the
container, so a use case that needs it injects it:

```python
from loom.core.environment import Environment


class DescribeDeployment(UseCase[Deployment, str]):
    def __init__(self, environment: Environment) -> None:
        super().__init__()
        self._environment = environment
```

Only `loom.core.environment` names or reads `LOOM_ENV`, the `LOOM__*`
variables or any other `LOOM_*` variable; an architecture test enforces it.

### Prefect flows

The `env` parameter of every loom flow and the keys of a flow YAML's
`environments` block are environments too: `production`, `staging`,
`development` or `test`. Any other key or value fails. `deploy_flow` and
`discover_and_deploy_etls` resolve `env` like the factories (default:
`LOOM_ENV`, else production), pick `environments.<env>`, and record the
environment as the deployment's `env` parameter.

A run takes its environment from the worker (`LOOM_ENV`, else production);
`env` only confirms it. A run whose `env` differs fails before anything runs,
so a deployment run on a worker of another environment fails loudly, and a
custom run cannot turn a production worker into a development one. Set
`LOOM_ENV` on each worker to the environment of its deployments.

## Configuration sources

From lowest to highest precedence:

1. defaults in code (the secure ones);
2. `base.yaml`, or the first explicit file;
3. `<LOOM_ENV>.yaml`, or each later explicit file in order;
4. the `LOOM__SECTION__KEY` variables.

Within any file, its own values win over the files it `includes`. Every entry
point loads its configuration this way (the REST and Celery factories, the
worker, `ETLRunner`, `StreamingRunner`, the Prefect flows, the migration
locator and `loom check --deploy`), and the deploy checks run on the final,
merged result.

### Files per environment

Keep one directory with a base and one overlay per environment:

```text
config/
  base.yaml          # everything shared
  development.yaml   # local overrides
  test.yaml          # test-suite overrides
  production.yaml    # deployed overrides
.env                 # LOOM_ENV=development, for local runs (see below)
```

```python
from loom.rest.fastapi.auto import create_app

app = create_app(config_dir="config")
```

`create_app(config_dir=...)` loads `base.yaml`, then `<environment>.yaml` when
it exists. An environment without an overlay runs on the base alone; a
directory without `base.yaml` is a startup error. The Celery
`create_app(config_dir=...)` and `bootstrap_worker(config_dir=...)` do the
same, and so do `ETLRunner.from_yaml`, `StreamingRunner.from_yaml`, the
storage configuration of a Prefect flow run (`LOOM_STORAGE_CONFIG_PATH`) and
the migration locator (`LOOM_CONFIG`) when given a directory instead of a
file.

loom does not read `.env`: your runner does, for example
`uv run --env-file .env ...` or `env_file:` in docker compose.

Explicit files remain the other form, for layouts that do not fit a
directory:

```python
app = create_app("config/base.yaml", "config/eu.yaml", environment="production")
```

A factory takes one form or the other, never both.

An example layout:

```yaml
# config/base.yaml
app:
  name: my_store
  code_path: src
  discovery:
    mode: modules
    modules:
      include: [app.product.model, app.product.interface]
database:
  url: ${oc.env:DATABASE_URL}
```

```yaml
# config/development.yaml
database:
  url: sqlite+aiosqlite:///store.db
persistence:
  cursors:
    signing: ephemeral   # a random key per process; see the persistence guide
```

```yaml
# config/test.yaml
database:
  url: sqlite+aiosqlite:///
persistence:
  cursors:
    signing: unsigned
```

```yaml
# config/production.yaml
app:
  rest:
    docs_url: null
    redoc_url: null
    openapi_url: null
    auth:
      jwt:
        secret_path: /run/secrets/jwt
        algorithms: [HS256]
        audience: my-store
persistence:
  cursors:
    current_key_id: "2026-10"
    keys:
      "2026-10": {path: /run/secrets/cursor-2026-10}
```

```bash
# .env
LOOM_ENV=development
```

The logger section refuses a key it does not declare, a removed one included
(`observability.log.config.environment`), and an `observability` section that
fails validation stops startup: a stale setting fails loudly instead of being
silently ignored.

### `LOOM__*` overrides

A configuration key can be overridden from a variable named `LOOM__` plus
its path, with a double underscore between levels, as in ASP.NET Core and
pydantic-settings. In development and test any leaf can; in production and
staging only an allowlisted key can (below):

```bash
LOOM__OBSERVABILITY__LOG__CONFIG__LEVEL=DEBUG     # observability.log.config.level
LOOM__SQL__CONNECTIONS__ANALYTICS__POOL_SIZE=20   # sql.connections.analytics.pool_size
LOOM__DATABASE__POOL__SIZE=1                      # database.pool.size
LOOM__CELERY__WORKER_CONCURRENCY=8                # celery.worker_concurrency
```

- A level is letters, digits and underscores; a `.` or `[` in a level stops
  startup. Each level matches the key the configuration declares at that
  place, whatever its case. A level the configuration does not declare is
  added in lower case; the section types that refuse unknown keys
  (`app.rest`, the `observability` sub-sections, `sql`, `celery`, `jobs`,
  `storage`, `kafka`, the bytewax runtime, `dynamodb`) then refuse a typo.
  The keys of `app`, `database`, `persistence` and the top of
  `observability`, which several readers share, are not checked.
- The first level must name a section the configuration declares or loom
  knows. Anything else (`LOOM__APPP__NAME`) stops startup with an error naming
  the variable. Two variables for the same key, or for a key and one of its
  parents, stop startup naming both.
- An override sets one leaf value. A value that is or holds a mapping
  (`{a: 1}`, `[1, {a: 2}]`) is refused, and so is a target the files declare
  as a mapping or a list holding mappings, whether the new value is null, a
  scalar or a list: an override cannot replace a structure. A list of
  scalars is a leaf.
- In production and staging an override may set only an allowlisted key:
  the operational keys loom ships (log levels, Celery and streaming worker
  counts and timeouts, SQL and database pool sizes, driver and DynamoDB
  timeouts; the full list and
  the reason for each are in ADR 0005), plus the keys your reviewed files
  list:

  ```yaml
  # config/base.yaml
  config:
    overridable:
      - app.rest.version
      - sql.connections.*.default_limit   # '*' stands for one level
  ```

  `config` is a reserved top-level section: loom reads it, so do not put
  your own keys under it.

  Any other key stops startup naming the variable; that includes the
  authentication settings, the deploy checks, the documentation and metrics
  paths and the per-query time limit (`max_execution_time`). `config` itself
  can never be overridden. An entry cannot start with `*`, and an entry
  that can match a key inside `checks`, `config` or `persistence.cursors`
  (`persistence.*.signing` included) stops startup naming the entry; the entries
  are checked at every load, in every environment. Any other key you list
  is a reviewed choice, much like setting it from `${oc.env:...}` in the
  files; the deploy checks still judge the merged value.
- An allowlisted key also changes every `${...}` placeholder in the files
  that references it: overriding `shared.timeout` changes each key set to
  `${shared.timeout}`. loom does not trace those references, so review them
  with the list. A key your files set from `${oc.env:...}` is controlled by
  the process environment because the files chose so.
- Values are literals. The value is parsed first and any string in it
  holding `${` is refused, however it is written (quoted, escaped as `\x24`,
  inside a list), so an override cannot reach another variable or a secret.
  A variable reaching inside a key whose value is an interpolation is
  refused too.
- Values parse as an OmegaConf dotlist (YAML 1.1), then the section types
  validate them:
  `5` is an integer, `false` a boolean and `[a, b]` a list. A list is
  replaced whole; its items cannot be set one by one. YAML 1.1 rules apply:
  `yes`, `no`, `on` and `off` are booleans, `1.10` becomes `1.1`, `0755` is
  octal, an empty value is null, ` #` starts a comment and `a: b` is a
  mapping, and so refused. A value that does not parse, a YAML tag that fails
  to construct included, stops startup naming the variable, never the
  value. Quote a value to keep it a string: with `app.rest.version` listed
  under `config.overridable`, `LOOM__APP__REST__VERSION='"1.10"'` sets the
  API version published in the OpenAPI document to the string `1.10`.
- At startup one WARNING line names the keys that came from variables. Their
  values are never logged.
- A log level on a parent cannot hide the deploy checks: the
  `loom.core.checks` logger sets its own WARNING level, so
  `LOOM__OBSERVABILITY__LOG__CONFIG__LEVEL=CRITICAL` still shows a check's
  warning and the silenced ids. To quiet them on purpose, name the logger:
  `named_levels: {loom.core.checks: ERROR}`.

What belongs in `LOOM__*`: operational tuning that changes per deployment
without changing the application: loom's allowlist, and what you add to it
for your own operational keys.

What does not: secrets, and the structure of the application (discovery,
routes, authentication mechanism), which belongs in the reviewed files. Do
not pass secrets through `LOOM__*`: the override line names every key it
sets.

The allowlist guards against careless per-deploy overrides, not against an
attacker. Whoever controls the process environment also controls `LOOM_ENV`,
`LOOM_CONFIG`, `LOOM_STORAGE_CONFIG_PATH`, `LOOM_ETL_CONFIG` and
`PYTHONPATH`, and so which files load and which code runs; protect the
environment itself (the task definition, its IAM permissions) for that.

The overrides apply to an application's configuration only. A per-flow
Prefect YAML defines a flow, not the process's configuration, and is read
without them.

### Secrets

On ECS, put secrets in the task definition's `secrets`. ECS reads them from
Secrets Manager or SSM Parameter Store with the task's execution role and
injects them as variables, and the YAML references them with
`${oc.env:...}`:

```json
{
  "containerDefinitions": [
    {
      "name": "api",
      "environment": [
        {"name": "LOOM_ENV", "value": "production"},
        {"name": "LOOM__OBSERVABILITY__LOG__CONFIG__LEVEL", "value": "INFO"},
        {"name": "LOOM__CELERY__WORKER_CONCURRENCY", "value": "8"}
      ],
      "secrets": [
        {
          "name": "DATABASE_URL",
          "valueFrom": "arn:aws:secretsmanager:eu-west-1:123456789012:secret:store/database-url"
        },
        {
          "name": "PAYMENTS_API_KEY",
          "valueFrom": "arn:aws:ssm:eu-west-1:123456789012:parameter/store/payments-api-key"
        },
        {
          "name": "CURSOR_KEY_2026_10",
          "valueFrom": "arn:aws:secretsmanager:eu-west-1:123456789012:secret:store/cursor-2026-10"
        }
      ]
    }
  ]
}
```

```yaml
# config/base.yaml
database:
  url: ${oc.env:DATABASE_URL}
payments:
  api_key: ${oc.env:PAYMENTS_API_KEY}
persistence:
  cursors:
    current_key_id: "2026-10"
    keys:
      "2026-10": {env: CURSOR_KEY_2026_10}   # the variable is named, never interpolated
```

On other platforms, the `${secrets:...}` and `${ssm:...}` resolvers read
Secrets Manager and SSM at load time.

loom has no check for secrets written into the configuration. Scan the
repository with [gitleaks](https://github.com/gitleaks/gitleaks), in
pre-commit and in CI:

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/gitleaks/gitleaks
    rev: vX.Y.Z   # pin the latest gitleaks release
    hooks:
      - id: gitleaks
```

```yaml
# .github/workflows/ci.yml (a step)
- uses: gitleaks/gitleaks-action@v2
  env:
    GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

## Logger defaults

The logger's renderer and colours follow the environment unless
`observability.log.config` sets them:

| Environment | Renderer | Colours |
|---|---|---|
| `production`, `staging` | `json` | off |
| `development`, `test` | `console` | on |

## Deploy checks

In `production` and `staging`, every entry point runs the deploy checks on
its final configuration before the application or the runner is built, in
the spirit of Django's
`manage.py check --deploy`: REST and Celery `create_app`, `bootstrap_worker`,
`ETLRunner` and `StreamingRunner` built from a configuration, and the Prefect
flows through them. Each check has a stable id `<area>.<E|W><nnn>`: an `E`
check fails startup, a `W` check logs a warning.

| Id | What it finds |
|---|---|
| `loom.rest.E001` | The API authenticates no caller: neither a literal `app.rest.auth.jwt` mapping nor the literal `app.rest.auth.custom: true` is set. A `jwt` given by a placeholder fails, since it may resolve to null. |
| `loom.rest.W001` | The API publishes its documentation or schema: one of `app.rest.docs_url`, `redoc_url`, `openapi_url` is not `null`. |
| `loom.logger.W001` | `observability.log.config` asks for the `console` renderer, top-level or on a handler. |
| `loom.cursors.E001` | A `database` or `persistence.mongo` section is set and pagination cursors are not signed with a configured key: `persistence.cursors.signing` is set to anything but the literal `signed`, or `current_key_id` names no entry of `keys`. |

Processes that serve no HTTP (the worker and the runners) skip the
`loom.rest` checks. A runner built from a whole configuration (a file, a
directory, or the mapping `StreamingRunner.from_dict` takes) runs them;
`ETLRunner.from_dict` and `from_spark`, which receive only the storage and
observability sections, run none.

### Authentication supplied in code

The configuration always names the authentication mechanism. For the built-in
JWT one, configure `app.rest.auth.jwt`. For an authenticator passed in code,
declare it:

```yaml
app:
  rest:
    auth:
      custom: true
```

```python
app = create_app(config_dir="config", authenticator=MyMtlsAuthenticator())
```

`create_app` refuses an `authenticator` without `custom: true`, `custom: true`
without an `authenticator`, and `custom: true` beside `jwt`. `loom check
--deploy` therefore reads the same answer from the same file.

### Accepting a finding on purpose

List the id under `checks.silenced`, in the file of the environment that
accepts it:

```yaml
# config/production.yaml
checks:
  silenced:
    - loom.rest.E001   # the gateway in front authenticates every request
```

A silenced check does not run, and every startup in a deployed environment
logs a warning naming the silenced ids, so the exception stays visible.

### Checks of your own

An application registers its own checks under its own area:

```python
# src/store/checks.py
from loom.core.checks import Finding, deploy_check
from loom.core.config import ConfigContext


@deploy_check("billing.gateway.E001")
def _no_sandbox_gateway(config: ConfigContext) -> Finding | None:
    if config.section_or_default("billing.gateway", str, "") != "sandbox":
        return None
    return Finding(
        message="billing.gateway is the sandbox in a deployed environment.",
        hint="set billing.gateway to the live gateway.",
    )
```

and lists the module in the configuration:

```yaml
checks:
  modules:
    - store.checks
```

Every run of the checks imports `checks.modules` first, at startup and in
`loom check --deploy` alike. `create_app` and the CLI put `app.code_path` on
the import path first; the worker and the runners import from the process's
own path, as they do their discovery modules. An unknown key under `checks`,
or a module that cannot be imported, is a configuration error.

### Running the checks in CI

`loom check --deploy` (in the `cli` extra) runs the checks without starting
the application, whatever the environment, and exits `1` when an error
remains:

```bash
loom check --deploy --env production config/
loom check --deploy config/base.yaml config/production.yaml
```

`--env` picks the overlay of a directory and defaults to `LOOM_ENV`. The
`LOOM__*` variables of the shell apply as at startup; those of the deployed
task do not, so keep security settings in the files. The command cannot tell
which process a configuration is for, so it runs every check: pass
`--skip loom.rest` (repeatable, any id or area) for a worker, ETL or
streaming configuration, as those processes skip the REST checks at startup.
The configuration loads with loom's default resolvers, and the checks read
keys as written, so a `${secrets:...}` value no check reads needs no access. `app.code_path` (`src`
by default, relative to the first file) joins the import path as `create_app`
adds it, so `checks.modules` import here as well. The checks read
configuration keys only, so no secret needs to be reachable. The command
loads loom's own checks, the REST ones only when the `rest` extra is
installed.

## Tests

loom's own suite runs with `LOOM_ENV=test`. Do the same in yours, so the
applications your tests build skip the deploy checks and log to the console:

```python
# conftest.py
import pytest


@pytest.fixture(scope="session", autouse=True)
def _test_environment():
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("LOOM_ENV", "test")
        yield
```

A test about the production path passes `environment="production"` to the
entry point.
