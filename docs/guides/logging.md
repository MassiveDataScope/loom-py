# Logging

Loom logs with the standard library. There is no loom logger to import: every
module, yours and loom's, calls `logging.getLogger(__name__)`. What loom adds is
a closed vocabulary, one filter and a choice of output format, so that every
record says **which service emitted it** and lets you **follow one `trace_id`**
across services.

## Writing

In your application a plain string is enough. Every record gets the service
identity, the `trace_id`, the use case or job and the code location from loom's
filter, whatever logger wrote it:

```python
import logging

log = logging.getLogger(__name__)

log.warning("scraper.page_failed", extra={"url.full": url, "http.response.status_code": 503})
```

Put values in `extra`, not in the message (`f"failed {url}"`): a field can be
filtered and grouped by a collector, a value inside the message cannot.

A `LogEvent` class is optional for applications. It pays off when the same event
is written in several places (no typo can split it) or when you want a catalogue
of your events. loom's own code always uses `LogEvent` members and `LogField`
keys; the tests enforce both on `loom.*` loggers only:

```python
import logging

from loom.core.logger import LogEvent, LogField

log = logging.getLogger(__name__)


class InvoiceLogEvent(LogEvent, prefix="billing"):
    ISSUED = "billing.invoice_issued"


log.info(InvoiceLogEvent.ISSUED, extra={LogField.JOB: "SendInvoice"})
```

- In loom's code, the message is the **event**: a `LogEvent` member named
  `<area>.<subject>_<fact>`, lowercase. It never holds a value: no f-string, no
  `%` format. Values go in the fields, where a collector can filter on them.
- An event class declares the prefix of its area once, as a class keyword, and every
  member value starts with it and a dot: `class JobLogEvent(LogEvent, prefix="job")`
  holds `job.delivery_abandoned`. The prefix is a public name: moving the package that
  owns the events does not rename them. Loom keeps the classes of a package in its
  `log_events.py` (`<Area>LogEvent`); the modules that sit directly in `loom.core` share
  `core`. Architecture tests check that each class lives in a `log_events.py`, that
  each value matches `^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$` after its prefix, and that no
  value repeats across loom. An application is free to follow the same layout.
- loom's field keys are `LogField` members: OpenTelemetry semantic conventions where one
  exists (`service.name`, `code.function.name`, `exception.type`), `loom.*`
  otherwise. The OpenTelemetry names follow **semantic conventions specification 1.45.0**, the
  release that renamed `messaging.system` to `messaging.system.name` (value `aws.sqs` for
  SQS) and `messaging.kafka.*` to `kafka.*` (`kafka.offset`). loom follows the
  specification, not the version of the `opentelemetry-semantic-conventions` Python
  package, which may be older. The GenAI keys (`gen_ai.*`) follow the separate GenAI
  conventions. Event names and field keys are part of the public contract of
  a release.
- A field value is a scalar (`str`, `int`, `float`, `bool`, `None`) or a short list of
  scalars. The formatters write any other object as its type name, never its `repr`; a
  `Secret` is masked; a value longer than 2048 characters is cut with a marker. Keep
  secrets and personal data out of fields anyway.
- What an emitter puts in a field: a count plus bounded scalars, never a list of
  dicts. A loop over items writes one record with `loom.count` and, for failures,
  `loom.failed_count` and `error.type`, not one entry per item with its detail.
- The user is not an operational field. `user.id` and `user.hash` appear only on
  security events, written as `observability.log.user_identity` says (see "Who a
  security event concerns"); roles, claims, client addresses and tokens are never logged.
- Exceptions: the message of a driver or of application code often carries row
  values, so the `json` and `text` formats write **`exception.type` only** below
  ERROR. From ERROR up they write `exception.type`, `exception.message` and
  `exception.stacktrace`. A call opts into the message below ERROR, never by
  default, with `extra={LogField.EXCEPTION_MESSAGE_SAFE: True}`, for an exception
  whose message you know holds no data. With the `lambda` format loom installs no
  formatter: the runtime renders the exception, including its message and stack
  trace, and the filter cannot redact it. The filter does add `exception.type`
  there, so the field exists in every format.
- Reserved keys: `timestamp`, `level`, `message`, `logger` and `exception.*` belong
  to the record. An `extra` key with one of those names does not overwrite it:
  the `json` and `text` formats write it as `loom.extra.<key>`.
- Levels: ERROR needs a human, WARNING is degraded but handled, INFO is lifecycle
  and business facts, DEBUG is diagnosis. One record per fact: batch the logs of a
  loop.

Loom objects (use cases, jobs, steps) have no logger attribute. Application code
logs the Python way, one `getLogger(__name__)` per module, so each module keeps its
own level control. Per-class loggers are optional, not the rule:
`logging.getLogger(f"{__name__}.{Cls.__name__}")`. Loom objects are identified by
the context fields below, not by the logger name.

## What every record carries

One `logging.Filter`, attached to the **handler**, stamps every record that reaches
it, whoever emitted it (loom, your code, a library):

| Field | Value |
|---|---|
| `service.name`, `service.version` | `app.name`, `app.version` of the configuration |
| `deployment.environment.name` | the resolved environment |
| `loom.runtime` | `fastapi`, `celery`, `lambda` or `cli` |
| `trace_id` | the active trace id (HTTP header, job envelope, flow) |
| `loom.use_case` | the use case being executed |
| `loom.job`, `loom.task_id`, `loom.attempt` | the job message being run |
| `code.function.name`, `code.line.number`, `code.file.path` | where the record was emitted |

A field you pass as `extra` is kept as you gave it. Following one `trace_id` shows
the lines of loom and of your application together.

Lifecycle records (`observability.lifecycle_started`, `_ended`, `_failed`) also carry the
`meta` of the span as `loom.meta.<key>`, but each scope has a closed list of the keys it may
write (`LIFECYCLE_META` in `loom.core.observability.observer.log`); any other key is
dropped, so a span cannot put an open-ended value in the log. A failed record also
carries `error_type`. The subject and the roles of the caller are not on the list.

Add fields of your own to a block with `log_context`; they follow the current thread
or asyncio task and never reach a concurrent request:

```python
from loom.core.logger import LogField, log_context

with log_context(invoice_id=invoice_id):
    log.info(InvoiceEvent.ISSUED)  # carries invoice_id

with log_context({LogField.JOB: "SendInvoice"}):  # a dotted key goes in a mapping
    ...
```

## Choosing the format

```yaml
observability:
  log:
    format: json      # lambda | json | text; omit to follow the environment
    level: INFO
    named_levels:
      botocore: WARNING
```

| Format | Output | Default when |
|---|---|---|
| `lambda` | Nothing installed: the Lambda runtime renders the JSON and sets the level | `AWS_LAMBDA_FUNCTION_NAME` is set |
| `json` | One JSON object per line on stdout; fields at the top level; `exception.type`, `exception.message`, `exception.stacktrace` | a deployed environment (`production`, `staging`) |
| `text` | A readable line with `key=value` fields; line breaks in values are escaped | any other environment |

The settings are validated at startup and by `loom check` as written
(`loom.logger.E001` for an unregistered format, `loom.logger.E002` for an unknown
level, `loom.logger.E003` for an unknown `user_identity`, `loom.logger.E004` for
`hash` without a `user_hash_key`, and `loom.logger.W001`, below; a value that holds a
placeholder is not judged).

An extra or an application adds a format by registering a callable from the
settings to the handler (or `None`) under the `loom.logging.formats` entry-point
group.

### Who a security event concerns

Operational events carry no identity: follow them by `trace_id`. A security event
(a denied access, a rejected token, an identity switch) says whom it concerns in the form
the deployment chooses:

```yaml
observability:
  log:
    user_identity: hash                 # hash | id | none (default)
    user_hash_key: ${oc.env:LOG_USER_HASH_KEY}
```

| Mode | Fields written |
|---|---|
| `none` | no user field |
| `id` | `user.id`: the subject as it is, for subjects that are already opaque ids |
| `hash` | `user.hash`: HMAC-SHA256 of the subject under `user_hash_key`, in hex; the key is a `Secret` and is required |

`UserIdentityPolicy.fields_for(subject)` (`loom.core.logger.user_identity`) is the one
place that answers; the code that emits a security event asks it and spreads the result
into `extra`. It is built once from this section and reached through
`Observability.user_identity`; an `Observability` built without configuration
(`Observability.noop()`) writes no user. Applications that want a user on operational
records add it with `log_context`, under their own responsibility.

`loom check` warns with `loom.logger.W001` when the mode is `none` and the environment
is production, as the configuration was loaded for: `--env production`, or `LOOM_ENV` when the flag is absent (unset meaning production). It cannot know statically whether
the application emits security events, so it warns whenever production writes no user;
silence it in `checks.silenced` when none is wanted.

### AWS Lambda

Set the function's log format to JSON (`LoggingConfig: {LogFormat: JSON,
ApplicationLogLevel: INFO}`). The runtime then renders every `logging` record as
JSON, with each `extra` key at the top level, and sets the level from the function
configuration: loom installs no handler and puts only its filter on the runtime's
handler. Loom never ships logs over the network; the destination is a deployment
choice (CloudWatch Logs, or a log destination delivered to S3, which costs less and
needs no CloudWatch Insights).

## ETL: logging never triggers a computation

A log field is filled with what the run already produced, never with something it
computes for the log: no `count()`, `collect()`, `first()`, `height` on a lazy
frame, `distinct()` or schema inference. Row counts come from the write's own
metrics or from a frame already materialised for the write; an unknown count is
left out, not computed. Log one record per step and per write, not per partition or
batch.
