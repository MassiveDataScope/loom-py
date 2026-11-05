# Deploy manifest

loom does not ship infrastructure code: no SAM, CDK or Terraform.
`loom deploy manifest` prints a JSON document that describes each function in
`lambda.functions`: the handler it runs, what triggers it and what it reaches.
A deployment tool, such as a CDK construct, reads the document and creates
the infrastructure.

```bash
loom deploy manifest config/ --env production
loom deploy manifest config/ --env production --function worker
```

The command loads the configuration the same way `loom check --deploy` does:
a config directory (`base.yaml` plus the overlay of `--env`) or explicit
files, `LOOM__` overrides included. It runs the same deploy checks, and
`--skip` works the same way. When a check fails, the command prints the
findings to stderr, prints no manifest and exits `1`. On success, stdout holds
only the manifest.

## What the configuration declares

```yaml
lambda:
  handler: entry.handler
  functions:
    public_api:
      http: [OrdersInterface]
    worker:
      sqs: true
      queue: jobs
      batch_size: 5
sqs:
  queues:
    jobs: https://sqs.eu-west-1.amazonaws.com/123456789012/shop-jobs
```

- `lambda.handler` is the Lambda handler that every function runs (the module
  that calls `create_app`, in `module.attribute` form). Every function runs
  the same handler and picks its own function through `LOOM__APP__ENTRY`.
- `lambda.functions.<name>.queue` names the queue an sqs function consumes.
  It must be a key of `sqs.queues`.

The manifest refuses a configuration that does not declare these keys.

## Schema (`manifest_version: 1`)

```json
{
  "manifest_version": 1,
  "environment": "production",
  "functions": {
    "public_api": {
      "handler": "entry.handler",
      "variables": {"LOOM_ENV": "production", "LOOM__APP__ENTRY": "public_api"},
      "event": {
        "type": "http",
        "routes": [{"method": "GET", "path": "/orders/{order_id}"}]
      },
      "auth": {"jwt": {"secret_path": "${secrets:/shop/jwt.path}"}},
      "secrets": ["/shop/jwt"],
      "ssm_parameters": ["/shop/db-url"],
      "stores": {"default": "sqlalchemy"},
      "queues": {"jobs": "https://sqs.eu-west-1.amazonaws.com/123456789012/shop-jobs"},
      "code_path": "src",
      "modules": ["shop.api"]
    },
    "worker": {
      "event": {
        "type": "sqs",
        "queue": "jobs",
        "url": "https://sqs.eu-west-1.amazonaws.com/123456789012/shop-jobs",
        "batch_size": 5,
        "batching_window_s": null,
        "max_concurrency": null,
        "report_batch_item_failures": true
      },
      "auth": null
    }
  }
}
```

The `worker` entry above is abbreviated. Every function has every field.

| Field | Source |
|---|---|
| `handler` | `lambda.handler` |
| `environment` | The environment the manifest was built for (`--env` or `LOOM_ENV`), at the top level. |
| `variables` | The environment variables the deployment sets: the function's name and the environment. |
| `event.routes` (http) | Each route of the function's interfaces, prefix included, as startup discovers them. Python interfaces come from `app.discovery` and YAML ones from `app.rest.interfaces`. `app.rest.disable_routes` is applied. |
| `event` (sqs) | `queue`, its URL from `sqs.queues`, and the event source mapping settings of the function. `report_batch_item_failures` is `true` unless the function sets it to `false`. |
| `auth` | The function's `auth`, or else `app.rest.auth`, as written. `null` for an sqs function. |
| `secrets`, `ssm_parameters` | The names in every `${secrets:...}` and `${ssm:...}` placeholder of the configuration, except those in the sections of other functions. A dotted JSON tail is dropped, so `/shop/jwt.path` lists the secret `/shop/jwt`. `%VAR%` tokens stay as written; surrounding spaces and quotes are dropped, as the resolver receives the key. |
| `stores` | The backend of each store in `stores`, except `backend: none`. |
| `queues` | `sqs.queues` as written: the queues the application may send jobs to. |
| `code_path` | `app.code_path` (`src` by default), relative to the configuration. |
| `modules` | The modules the function imports at startup: those `app.discovery` names for the function's adapter, `checks.modules`, and the module of a `module:Class` `app.job_service`. |

The manifest never contains the value of a secret or a parameter. It reads
the configuration as written and calls no resolver. A placeholder that
nests another one inside a `secrets` or `ssm` key cannot be named, so the
command refuses it. The keys and lists come out sorted, so the same
configuration always produces the same bytes and CI can diff the output.

A change that a consumer must handle increases `manifest_version`. A new
field does not.

## How a deployment tool uses it

A deployment tool builds one Lambda for each entry, for instance with a CDK
construct that takes one manifest entry:

- the code package (from `code_path` and `modules`) and the handler;
- the environment variables;
- an API Gateway route for each http route, or an SQS event source mapping
  with the declared settings and `ReportBatchItemFailures`;
- an IAM role that reads only the listed secrets and parameters, reaches the
  listed stores and sends to the listed queues.

The CI/CD integration and the policy checks (cfn-guard, Checkov) belong to
the deployment tool and stay out of loom.

## Limits

- Today every function imports all of `app.discovery`, so `modules` is the
  same for every function of an adapter. It narrows once each function
  declares its own modules.
- `stores` and `queues` list what the configuration declares, not what each
  function uses.
- Routes are listed before the CRUD routes that the store's backend cannot
  serve are dropped.
- A secret or key that code reads, such as a `JwtIssuerConfig` built with
  `private_key_ref`, does not appear in the configuration, so the manifest
  cannot list it. Add it to the role in the deployment.
