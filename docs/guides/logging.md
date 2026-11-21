# Logging

Loom logs with the standard library: every module, yours and loom's, calls
`logging.getLogger(__name__)`. Loom adds what makes each record say **which
service emitted it**, lets you **follow one `trace_id`** across services, and
keeps values of your business out of every output.

## A record is (event, level, fields)

```python
import logging

from loom.core.logger import LogEvent, LogField, LogKey

log = logging.getLogger(__name__)


class BillingLogEvent(LogEvent):
    INVOICE_ISSUED = "billing.invoice_issued"


class BillingLogField(LogKey):
    INVOICE_ID = "billing.invoice_id"


log.info(BillingLogEvent.INVOICE_ISSUED, extra={BillingLogField.INVOICE_ID: invoice.id})
```

- **The message is the event:** a dotted lowercase name, declared once by the
  owner of the fact, never a sentence with values in it.
- **Values go in `extra`,** under declared keys, as scalars or short lists of
  scalars. A cross-cutting key is a `LogField` member; a key one package writes
  is declared beside its events, in its `log_events.py`. A count is
  `LogField.COUNT`: the event says what was counted.
- **An exception travels as `exc_info=`,** never as `str(exc)` in a field.
- A field that costs work to build is built only behind
  `log.isEnabledFor(logging.DEBUG)`.

`loom.testing.log_calls.log_call_offences(paths)` checks these rules on your
code, as loom's tests check them on loom's.

## What loom does to every record

When a record is **created**, before any handler sees it, loom's record factory:

- stamps `service.name`, `service.version`, `deployment.environment.name`,
  `loom.runtime`, `trace_id` (the OpenTelemetry trace id when a span records),
  `span_id`, the `log_context` fields (`loom.use_case`, `loom.job`,
  `loom.task_id`, `loom.attempt`, yours) and `code.function.name`,
  `code.line.number`;
- turns the exception into `exception.type` (fully qualified),
  `loom.error.code` for a `LoomError` and, from ERROR up, `exception.stacktrace`:
  frames and type names of every link of the chain, **no message, no note, no
  member of an exception group**. Then it drops `exc_info` and `stack_info`, so
  no handler, formatter, queue or runtime can print the message.

When a **handler** writes it, `LogFilter` renders each value: a `Secret` is
masked, an object that is not a scalar or a list of at most 20 scalars is its
type name (never its `repr`), a string is cut at 2048 characters. Loom puts the
filter on every root handler at start-up and on the Lambda runtime's handler,
and warns once about a root handler added later without it. The factory and the
filter are installed even with `observability.log.enabled: false`, which only
turns lifecycle records off.

No exception message reaches any output loom emits: logs, span status and
events, the Lambda error report.

## One fact, one record

- A scope (use case, job, step, node) writes `observability.lifecycle_started`
  and `observability.lifecycle_ended` with `loom.scope`, `loom.name`,
  `loom.status`, `loom.duration_ms` and, for a failure, `exception.type` and
  `loom.error.code`. Every use case runs in a `use_case` scope, whatever runtime
  ran it: REST, a job, the CLI or Lambda.
- The stack of one exception is written once, by the first record that reports
  it at ERROR (the innermost scope). Outer scopes write the type only, at WARNING.
- An expected failure (`LoomError.expected`: a not-found, a rule violation, a
  denial, a validation error, a job signal) ends its scope at INFO, with no stack.
- A handler logs only its own decision: the job runner `job.delivery_retried`
  or `job.delivery_abandoned`; the Lambda boundary only a failure no scope wrote.

## Formats

```yaml
observability:
  log:
    format: json      # lambda | json | text; omit to follow the environment
    level: INFO
    named_levels:
      my_library: DEBUG
```

| Format | Output | Default when |
|---|---|---|
| `lambda` | The runtime's handler and level. With `AWS_LAMBDA_LOG_FORMAT=JSON` the runtime writes each field at the top level; with the default Text format loom gives its handler loom's JSON layout, so no field is lost | `AWS_LAMBDA_FUNCTION_NAME` is set |
| `json` | One JSON object per line on stdout, fields first, then `timestamp`, `level`, `message`, `logger` | `production`, `staging` |
| `text` | One readable line with `key=value` fields; line breaks escaped, the stack indented below | any other environment |

A package adds a format under the `loom.logging.formats` entry-point group.

## Uncaught exceptions and Lambda

`sys.excepthook`, `threading.excepthook` and the asyncio exception handler of a
running application write one ERROR record through loom. On Lambda, an
exception that leaves the handler, a trigger or the init is logged once, then
replaced by a `LambdaInvocationError` naming only its type and code: the
runtime's `errorMessage` carries no value. Query `exception.type` and
`loom.error.code` in the logged record.

## Third-party libraries

Loom cannot clean the text a library writes in its own messages. It sets floors:
`sqlalchemy.engine`, `botocore`, `urllib3`, `httpx` and `uvicorn.access` at
WARNING, `celery.app.trace` at ERROR; `loom.logger.W001` warns when
`named_levels` lowers one.

- **uvicorn:** run it with `uvicorn.run(app, log_config=None)`, so its loggers
  propagate to loom's handlers instead of uvicorn's own.
- **Celery:** loom connects `setup_logging`, so Celery leaves the root logger alone.
- **Prefect:** Prefect's own run loggers write through Prefect's handlers; loom
  stamps their records but cannot clean the messages Prefect builds.

## ETL: logging never computes

A log field is filled with what the run already produced: no `count()`,
`collect()` or schema inference for a log. One record per step and per write.

## Reference

Every event name and field key loom declares is listed in
`docs/reference/log-contract.txt`. Names are stable from 12.0: the list may only
grow.
