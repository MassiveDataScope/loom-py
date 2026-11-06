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
      trigger: {source: http, interfaces: [OrdersInterface]}
    worker:
      trigger: {source: jobs, queue: {name: jobs, batch_size: 5}}
    invoices_in:
      trigger:
        source: eventbridge
        job: ImportInvoice
        identity: {subject: svc:invoices, roles: [invoice_importer]}
    alerts:
      trigger: {source: sns, queue: {name: alerts}, job: ImportInvoice}
sqs:
  queues:
    jobs: https://sqs.eu-west-1.amazonaws.com/123456789012/shop-jobs
    alerts: https://sqs.eu-west-1.amazonaws.com/123456789012/shop-alerts
```

- `lambda.handler` is the Lambda handler that every function runs (the module
  that calls `create_app`, in `module.attribute` form). Every function runs
  the same handler and picks its own function through `LOOM__APP__ENTRY`.
- `lambda.functions.<name>.trigger.queue.name` names the queue a `jobs`
  function, or an event trigger with a `queue:` block, consumes. It must be a
  key of `sqs.queues`, and no two functions may consume the same queue.

The manifest refuses a configuration that does not declare these keys, a
handler or queue URL given by a placeholder, a function whose key is not a
Python identifier, and an event trigger whose job startup would refuse.

## Schema (`manifest_version: 2`)

```json
{
  "manifest_version": 2,
  "environment": "production",
  "handler": "entry.handler",
  "code_path": "src",
  "queues": {
    "alerts": "https://sqs.eu-west-1.amazonaws.com/123456789012/shop-alerts",
    "jobs": "https://sqs.eu-west-1.amazonaws.com/123456789012/shop-jobs"
  },
  "functions": {
    "alerts": {
      "source": "sns",
      "via": "sqs",
      "event": {
        "queue": "alerts",
        "batch_size": null,
        "batching_window_s": null,
        "max_concurrency": null,
        "report_batch_item_failures": true,
        "dead_letter_required": true,
        "job": "ImportInvoice",
        "identity": null,
        "raw_message_delivery": false
      },
      "secrets": ["/shop/jwt"],
      "ssm_parameters": ["/shop/db-url"]
    },
    "invoices_in": {
      "source": "eventbridge",
      "via": null,
      "event": {
        "job": "ImportInvoice",
        "identity": {"subject": "svc:invoices", "roles": ["invoice_importer"], "attributes": {}},
        "on_failure": {"maximum_retry_attempts": 2, "maximum_event_age_s": 21600}
      },
      "secrets": ["/shop/jwt"],
      "ssm_parameters": ["/shop/db-url"]
    },
    "public_api": {
      "source": "http",
      "via": null,
      "event": {
        "routes": [{"method": "GET", "path": "/orders/{order_id}"}],
        "auth": {"jwt": {"secret_path": "${secrets:/shop/jwt.path}"}}
      },
      "secrets": ["/shop/jwt"],
      "ssm_parameters": ["/shop/db-url"]
    },
    "worker": {
      "source": "jobs",
      "via": null,
      "event": {
        "queue": "jobs",
        "batch_size": 5,
        "batching_window_s": null,
        "max_concurrency": null,
        "report_batch_item_failures": true,
        "dead_letter_required": true
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
| `source`, `via` | The trigger's `source`, and `sqs` for an event trigger with a `queue:` block, else `null`. |
| `event.routes` (http) | Each route of the function's interfaces, prefix included, as startup composes them: Python interfaces from `app.discovery`, YAML ones from `app.rest.interfaces`, `app.rest.disable_routes` applied and the CRUD routes the store's backend cannot serve dropped. A `disable_routes` entry that matches no route is refused, as at startup. |
| `event.auth` (http) | The trigger's `auth`, or else `app.rest.auth`, as written. |
| `event` (jobs, queued) | `queue` (the block's `name`) and the event source mapping settings of the trigger's `queue:` block. `report_batch_item_failures` is `true` unless the trigger sets it to `false`. `dead_letter_required: true`: the queue needs a dead-letter queue, where messages that keep failing end. The discovery mode must be one a worker scans. An event trigger adds its `job` and `identity`; an sns one `raw_message_delivery: false`, which its subscription must keep. |
| `event` (direct) | The `job`, the `identity` it runs as (`null`: anonymous) and `on_failure`: the asynchronous invocation's `maximum_retry_attempts` and `maximum_event_age_s`, defaults applied. Its presence means the deployment must configure an on-failure destination, where a failed event goes. |
| `secrets`, `ssm_parameters` | The secret and parameter names the function resolves: those of every `${secrets:...}` and `${ssm:...}` placeholder of the configuration, except the other functions' sections. A dotted JSON tail is dropped, so `/shop/jwt.path` lists the secret `/shop/jwt`. `%VAR%` tokens stay as written. A function running jobs lists those of `app.rest.auth` too: it resolves the whole `app` section. |

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

A change that a consumer must handle increases `manifest_version`: a field
removed or renamed, or a value that changes meaning. A new field does not.
Version 2 replaced the event's `type` (`http`, `sqs`) with each function's
`source` and `via`, and added the event triggers.

## How a deployment tool uses it

A deployment tool builds one Lambda for each entry, for instance with a CDK
construct that takes one manifest entry:

- the code package (from `code_path`) and the handler;
- the environment variables `LOOM_ENV` and `LOOM__APP__ENTRY`;
- an API Gateway route for each http route; an SQS event source mapping with
  the declared settings and `ReportBatchItemFailures` for a `jobs` or
  queued function; for a direct trigger, the asynchronous invocation's
  retries, maximum event age and on-failure destination;
- the rule, bucket notification, topic subscription or schedule that sends a
  trigger's events, which loom does not declare;
- who may invoke the function and write to its queue: whoever can runs the
  declared job as the declared identity, so restrict `lambda:InvokeFunction`
  and the queue policy, and pin an identity-bearing function in code with
  `LambdaAdapter(function=...)`;
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
