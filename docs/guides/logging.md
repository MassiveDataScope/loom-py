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


class InvoiceEvent(LogEvent):
    ISSUED = "billing.invoice_issued"


log.info(InvoiceEvent.ISSUED, extra={LogField.JOB: "SendInvoice"})
```

- In loom's code, the message is the **event**: a `LogEvent` member named
  `<area>.<subject>_<fact>`, lowercase. It never holds a value: no f-string, no
  `%` format. Values go in the fields, where a collector can filter on them.
- loom's field keys are `LogField` members: OpenTelemetry semantic conventions where one
  exists (`service.name`, `code.function.name`, `exception.type`), `loom.*`
  otherwise. Event names and field keys are part of the public contract of a
  release.
- A field value is a scalar (`str`, `int`, `float`, `bool`, `None`) or a short list
  of them. The formatters write any other object as its type name, never its
  `repr`; a `Secret` is masked; a value longer than 2048 characters is cut with a
  marker. Keep secrets and personal data out of fields anyway.
- Exceptions: the context filter, on every handler loom owns, writes what a
  record says about its exception: `exception.type`, `loom.error.code` for a loom
  error, and from ERROR up `exception.stacktrace` (the standard traceback, its last
  16 384 characters). The message is never a field of its own. Pass `exc_info` once
  per failure, where your code stops the exception, so one failure is one record
  and one stack. With the `lambda` format loom installs no formatter, so the
  runtime renders the record; the filter's fields are on it all the same.
- Reserved keys: `timestamp`, `level`, `message` and `logger` belong to the record;
  the `json` and `text` formats write an `extra` of one of those names as
  `loom.extra.<key>`. The exception fields belong to the filter, which drops an
  `extra` of their name.
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
| `json` | One JSON object per line on stdout; fields at the top level; `exception.type`, `loom.error.code`, `exception.stacktrace` | a deployed environment (`production`, `staging`) |
| `text` | A readable line with `key=value` fields; line breaks in values are escaped | any other environment |

The settings are validated at startup and by `loom check` as written
(`loom.logger.E001` for an unregistered format, `loom.logger.E002` for an unknown
level; a value that holds a placeholder is not judged).

An extra or an application adds a format by registering a callable from the
settings to the handler (or `None`) under the `loom.logging.formats` entry-point
group.

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
