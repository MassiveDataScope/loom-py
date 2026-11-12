# From FastAPI to Lambda in 5 minutes

The same application runs on FastAPI or on AWS Lambda behind API Gateway
(HTTP API, payload 2.0). Use cases, repositories, models and store
configuration stay as they are. You change one configuration key, or two lines
of code.

## 1. Install the extra

```bash
pip install "loom-kernel[lambda]"
```

## 2. Switch the adapter

Write one entry module and let the configuration pick the adapter:

```python
# src/entry.py
from loom.composition import create_app

app = create_app("config/app.yaml")
```

```yaml
app:
  name: orders
  entry: fastapi
```

To run it on Lambda, set `entry: lambda`, or keep the YAML and set
`LOOM__APP__ENTRY=lambda` (and `LOOM_ENV`) on the function: the override
is allowed in every environment. The code does not change. Uvicorn serves
`entry:app` and Lambda runs `entry.app` as its handler.

If you prefer to pass the adapter in code, the change is two lines, the import
and the call:

```python
from loom.composition import create_app
from loom.serverless.aws.lambda_ import LambdaAdapter

app = create_app("config/app.yaml", adapter=LambdaAdapter())
```

An explicit `adapter=` wins over `app.entry`. Keep the call at module level:
Lambda runs it once during Init, opens the application there and keeps it
open across warm invocations. If the application fails to open, Init fails.

Routes, authentication, the error envelope, status codes and headers are the
same on both adapters. A contract suite runs the same requests against both
and requires identical replies. Some features have no Lambda equivalent:

| On FastAPI | On Lambda |
|---|---|
| Health, OpenAPI and docs routes | Not served. With no `exclude_paths`, no route is public, so a route at `/health` needs a token |
| `observability.prometheus.enabled` | Refused at startup |
| SQL endpoints (`app.rest.sql`) | Refused at startup |
| Agents (an `ai` section, manifest `AGENTS`) | Hosted for the use cases: only the agents an `Agent()` marker names are built, and no agent HTTP or A2A endpoint is served. See [Agents and MCP on Lambda](#agents-and-mcp-on-lambda) |
| MCP servers over `transport: stdio` | Refused at startup |
| CORS (`app.rest.cors`) | Applied by the application, as on FastAPI. Do not configure it in API Gateway as well |

Placeholders such as `${secrets:...}` and `${ssm:...}` resolve once, during
Init, and the values stay in the container's memory.

Several functions can share one configuration, each with its own `trigger`:
an HTTP API, a worker for loom jobs, or a function run by EventBridge, S3,
SNS or a schedule. See [Composition and lifecycle](composition.md) for
`lambda.functions` and `LOOM__APP__ENTRY=<function>`.

### Agents and MCP on Lambda

An `ai` section works on every trigger: `http`, `jobs`, `eventbridge`,
`s3`, `sns` and `schedule`. The agent
runtime and the session of every MCP server the function reaches open once,
during Init, and stay open across warm invocations. A Lambda function serves no
agent endpoint, so:

- it builds only the agents named by the `Agent()` markers of its use cases,
  and opens only their servers and those of its `Mcp()` markers;
- `ai.endpoints.<name>.enabled` and `ai.a2a.expose` are ignored, with one
  warning each at Init;
- an MCP server over `transport: stdio` is refused at startup: a frozen
  function keeps no subprocess.

Two limits apply:

- **Init has about 10 seconds**, and `ai.startup_timeout_ms` defaults to
  `10000`. Set it lower on Lambda, so a slow MCP server fails Init by name
  before Lambda stops Init, and the rest of Init still has time.
- **API Gateway allows about 29 seconds per request.** A long agent run belongs
  on a `jobs` trigger.

An MCP call interrupted mid-flight fails with `MCP_CALL_INTERRUPTED` and is
never repeated. An `http` request answers `500` `internal_error` with a
`trace_id`, and the code appears in the log only. A `jobs` record is reported
in `batchItemFailures`, so SQS delivers it again. How sessions survive
a freeze, and what each case costs, is in [MCP on AWS
Lambda](../ai/mcp.md#mcp-on-aws-lambda).

## 3. Test it without AWS

`LambdaTestClient` from `loom.testing` turns requests into API Gateway events
and calls the real handler in process; see
[Testing Lambda functions](../rest/testing.md#testing-lambda-functions).
It is an `httpx.Client`, like FastAPI's `TestClient`, so the same requests
and assertions work against both.

## 4. Check and describe the deployment

Run both commands in CI, against the configuration of the target environment:

```bash
loom check --deploy config/ --env production
loom deploy manifest config/ --env production
```

`loom check --deploy` runs every deploy check without starting the
application and exits `1` when an error remains. `loom deploy manifest`
prints a JSON description of each function: its handler, its trigger and what
it reaches; it needs `lambda.handler`, here `entry.app`. Packaging and
infrastructure stay in CI/CD: your deployment tool reads the manifest, as
[Deploy manifest](deploy-manifest.md) describes. loom ships no infrastructure
code.
