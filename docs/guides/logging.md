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

## Loom's log is the source of truth

A failure's details live in one place, loom's log: its type, its code, the
fields it declares and its frames-only stack, written once. Everything else
gets the exception's **public form** or a **reference** to that record.

- **An exception's message is never a payload.** HTTP error bodies, SSE error
  events, a2a task status, Kafka error envelopes and DLQ headers, the `loom`
  command's output and Prefect states and notifications carry
  `public_form(exc)`: a `LoomError`'s code and declared fields, any other
  exception's fully qualified type. A validation error answered as `4xx`
  (422 included) keeps its text, which is about the data the client sent: a
  custom validator must not put a secret in its message.
- **A `LoomError` declares safe fields** with `log_fields()`, a mapping of
  `LogKey` members to scalars, empty by default. They go into the record and
  the public form through value rendering, so a `Secret` stays masked.

  ```python
  class InvoiceLocked(Conflict):
      def __init__(self, invoice_id: int) -> None:
          super().__init__(f"invoice {invoice_id} is locked")
          self.invoice_id = invoice_id

      def log_fields(self) -> Mapping[LogKey, Scalar]:
          return {BillingLogField.INVOICE_ID: self.invoice_id}
  ```

  Its `409` body says `conflict billing.invoice_id=7`; the message stays in
  the process for the developer.
- **A handoff hands a reference.** Where a failure leaves the process to a
  runtime, a broker or a result store, loom raises a `FailureReference`
  instead: type, code, `trace_id`, `expected` and `retryable`, never the
  original. The failure is logged first when no record wrote its stack. The
  handoffs are the Lambda Init and every invocation, the ASGI lifespan
  (startup and shutdown), the Celery result, retry, failure callback and
  worker init, and a flow built by loom's Prefect factories. A test that sees
  the reference reads the failure from its record:

  ```python
  from loom.testing.failures import logged_failure

  with pytest.raises(FailureReference) as raised:
      worker.run(InvoiceJob)
  record = logged_failure(caplog.records, raised.value)
  assert record.__dict__["exception.type"] == "billing.errors.InvoiceLocked"
  ```

  Celery stores the reference: `AsyncResult.get()` and an eager task raise it,
  and loom's task base decides retries itself (`autoretry_for` is not
  supported). A failure callback receives `exc_type`, the original's fully
  qualified type, and `exc_msg`, the reference's text.
- `loom.testing.log_calls.exception_text_offences(paths)` finds every place
  your code formats an exception's text (`str`, `repr`, an f-string, `%`,
  `.format()` of an except-bound or exception-typed name or its `.args`, or
  `traceback` text). It cannot see text built through a helper.

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

A runtime entry point, not `configure_logging`, installs the process hooks:
Lambda Init, the ASGI lifespan, Celery worker init and the `loom` command. So
a test runner keeps its own. `sys.excepthook`, `threading.excepthook` and
`sys.unraisablehook` write one record through loom, and warnings are captured
as `py.warnings` records that carry their category only. `Application.running`
sets the asyncio exception handler, which covers uvicorn's loop and every loop
loom hosts. Lambda Init and the `loom` command install the record factory
before they read any configuration.

On Lambda, an exception that leaves the Init or an invocation is logged once,
then replaced by a reference whose class is named after the original type, so
`errorType` still names it for Step Functions; `errorMessage` is the
reference's text: type, code and `trace_id`. Query `exception.type` and
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

## Outside loom

Loom's rules cover what loom emits and what passes through `logging`. These
stay outside, each with its own limit:

- OpenTelemetry auto-instrumentations, the Lambda OpenTelemetry layer and Sentry
  write their own spans and events, with the messages they choose.
- Text a library builds itself and logs as a plain string: the floors above
  limit how much of it is written.
- Celery's own failure log of a task's arguments, and text a library prints
  straight to stderr.
- A failure in your imports before loom loads.
- A Prefect flow you decorate with `@flow` yourself: only flows built by loom's
  factories hand Prefect a reference.
- A marked exception raised again writes only its type: frames it gained after
  its first report are not written.

## ETL: logging never computes

A log field is filled with what the run already produced: no `count()`,
`collect()` or schema inference for a log. One record per step and per write.

## Reference

Every event name and field key loom declares is listed in
`docs/reference/log-contract.txt`. Names are stable from 12.0: the list may only
grow.
