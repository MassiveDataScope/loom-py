# Environments and deploy checks

A loom application runs in one of four environments: `production`, `staging`,
`development` or `test`. One selector picks it, and the environment then picks
the configuration overlay, the logger defaults and whether the deploy checks
run. The pattern is the one Django, Spring and Rails use: secure defaults live
in code, configuration files override them, and nothing guesses the
environment on its own.

## Selecting the environment

The environment comes from, in order:

1. the `environment=` argument of `create_app` (REST or Celery) or
   `bootstrap_worker`;
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

No other loom module reads `LOOM_ENV`; an architecture test enforces it.

## Configuration per environment

Keep one directory with a base and one overlay per environment:

```text
config/
  base.yaml          # everything shared
  development.yaml   # local overrides
  test.yaml          # test-suite overrides
  production.yaml    # deployed overrides
.env                 # LOOM_ENV=development, for local runs
```

```python
from loom.rest.fastapi.auto import create_app

app = create_app(config_dir="config")
```

`create_app(config_dir=...)` loads `base.yaml`, then `<environment>.yaml` when
it exists. An environment without an overlay runs on the base alone; a
directory without `base.yaml` is a startup error. The Celery
`create_app(config_dir=...)` and `bootstrap_worker(config_dir=...)` do the
same.

Explicit files remain the other form, for layouts that do not fit a
directory:

```python
app = create_app("config/base.yaml", "config/eu.yaml", environment="production")
```

A factory takes one form or the other, never both.

### Precedence

From lowest to highest:

1. defaults in code (the secure ones);
2. `base.yaml`, or the first explicit file;
3. the overlay of the environment, or each later explicit file in order;
4. within any file, its own values over the files it `includes`.

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
```

```yaml
# config/test.yaml
database:
  url: sqlite+aiosqlite:///
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
```

```bash
# .env
LOOM_ENV=development
```

## Logger defaults

The logger's renderer and colours follow the environment unless
`observability.log.config` sets them:

| Environment | Renderer | Colours |
|---|---|---|
| `production`, `staging` | `json` | off |
| `development`, `test` | `console` | on |

## Deploy checks

In `production` and `staging`, `create_app` and `bootstrap_worker` run the
deploy checks before building anything, in the spirit of Django's
`manage.py check --deploy`. Each check has a stable id
`<area>.<E|W><nnn>`: an `E` check fails startup, a `W` check logs a warning.

| Id | What it finds |
|---|---|
| `loom.rest.E001` | The API authenticates no caller: `app.rest.auth.jwt` is not configured. An `authenticator` passed to `create_app` satisfies it. |
| `loom.rest.W001` | The API publishes its documentation or schema: one of `app.rest.docs_url`, `redoc_url`, `openapi_url` is not `null`. |
| `loom.logger.W001` | `observability.log.config` asks for the `console` renderer, top-level or on a handler. |

A worker serves no HTTP, so `bootstrap_worker` skips the `loom.rest` checks.

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

The check registers when its module is imported; import it from the module
that builds the application.

### Running the checks in CI

`loom check --deploy` (in the `cli` extra) runs the checks without starting
the application, whatever the environment, and exits `1` when an error
remains:

```bash
loom check --deploy --env production config/
loom check --deploy config/base.yaml config/production.yaml
```

`--env` picks the overlay of a directory and defaults to `LOOM_ENV`. The
checks read configuration keys only, so no secret needs to be reachable. The
command loads loom's own checks; the REST ones only when the `rest` extra is
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
factory.
