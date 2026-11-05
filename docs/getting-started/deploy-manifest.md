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
`--skip` works the same way. Their findings go to stderr. When a check fails,
the command prints no manifest and exits `1`. stdout holds only the manifest.

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

The manifest refuses a configuration that does not declare these keys, a
handler or queue URL given by a placeholder, and a function whose key is not a
Python identifier.

## Schema (`manifest_version: 1`)

```json
{
  "manifest_version": 1,
  "environment": "production",
  "handler": "entry.handler",
  "code_path": "src",
  "queues": {"jobs": "https://sqs.eu-west-1.amazonaws.com/123456789012/shop-jobs"},
  "functions": {
    "public_api": {
      "event": {
        "type": "http",
        "routes": [{"method": "GET", "path": "/orders/{order_id}"}],
        "auth": {"jwt": {"secret_path": "${secrets:/shop/jwt.path}"}}
      },
      "secrets": ["/shop/jwt"],
      "ssm_parameters": ["/shop/db-url"]
    },
    "worker": {
      "event": {
        "type": "sqs",
        "queue": "jobs",
        "batch_size": 5,
        "batching_window_s": null,
        "max_concurrency": null,
        "report_batch_item_failures": true
      },
      "secrets": ["/shop/jwt"],
      "ssm_parameters": ["/shop/db-url"]
    }
  }
}
```

| Field | Source |
|---|---|
| `environment` | The environment the manifest was built for (`--env` or `LOOM_ENV`). |
| `handler` | `lambda.handler`. |
| `code_path` | `app.code_path` (`src` by default), relative to the configuration. |
| `queues` | `sqs.queues` as written: the queues the application may send jobs to. |
| `event.routes` (http) | Each route of the function's interfaces, prefix included, as startup composes them: Python interfaces from `app.discovery`, YAML ones from `app.rest.interfaces`, `app.rest.disable_routes` applied and the CRUD routes the store's backend cannot serve dropped. A `disable_routes` entry that matches no route is refused, as at startup. |
| `event.auth` (http) | The function's `auth`, or else `app.rest.auth`, as written. |
| `event` (sqs) | `queue` and the event source mapping settings of the function. `report_batch_item_failures` is `true` unless the function sets it to `false`. The discovery mode must be one a worker scans. |
| `secrets`, `ssm_parameters` | The secret and parameter names the function resolves: those of every `${secrets:...}` and `${ssm:...}` placeholder of the configuration, except the other functions' sections. A dotted JSON tail is dropped, so `/shop/jwt.path` lists the secret `/shop/jwt`. `%VAR%` tokens stay as written. An sqs function lists those of `app.rest.auth` too: a worker resolves the whole `app` section. |

The deployment sets `LOOM_ENV` to `environment` and `LOOM__APP__ENTRY` to
the function's key.

The manifest never contains the value of a secret, a parameter or a
variable: it reads the configuration as written and calls no resolver.
Surrounding spaces and one pair of quotes are dropped from a key, as the
resolver receives it. A placeholder nested inside a `secrets` or `ssm` key
cannot be named, so the command refuses it, and so it refuses a name holding
a `*` or `?` wildcard.
The keys and lists come out sorted, so the same configuration always produces
the same bytes and CI can diff the output.

A change that a consumer must handle increases `manifest_version`. A new
field does not.

## How a deployment tool uses it

A deployment tool builds one Lambda for each entry, for instance with a CDK
construct that takes one manifest entry:

- the code package (from `code_path`) and the handler;
- the environment variables `LOOM_ENV` and `LOOM__APP__ENTRY`;
- an API Gateway route for each http route, or an SQS event source mapping
  with the declared settings and `ReportBatchItemFailures`;
- an IAM role that reads only the listed secrets and parameters and sends to
  the listed queues.

The CI/CD integration and the policy checks (cfn-guard, Checkov) belong to
the deployment tool and stay out of loom.

## Limits

- `queues` lists what the configuration declares, not what each function uses.
- Only `secrets` and `ssm` placeholders are listed. The references of a
  custom resolver that the application passes to `create_app` in code are
  not; add them to the role in the deployment.
- A secret or key that code reads, such as a `JwtIssuerConfig` built with
  `private_key_ref`, does not appear in the configuration, so the manifest
  cannot list it. Add it to the role in the deployment.
