# Logging

Loom logs with the standard library. There is no loom logger to import: every
module, yours and loom's, calls `logging.getLogger(__name__)`. What loom adds is
named events and fields and a choice of output format, so that every
record says **which service emitted it** and lets you **follow one `trace_id`**
across services.

## Writing

In your application a plain string is enough. Every record gets the service
identity, the `trace_id`, the use case or job and the code location from loom's
formats, whatever logger wrote it:

```python
import logging

log = logging.getLogger(__name__)

log.warning("scraper.page_failed", extra={"url.full": url, "http.response.status_code": 503})
```

Put values in `extra`, not in the message (`f"failed {url}"`): a field can be
filtered and grouped by a collector, a value inside the message cannot.

A `LogEvent` class is optional for applications. It pays off when the same event
is written in several places (no typo can split it) or when you want a catalogue
of your events. loom's own code always logs a `LogEvent` member with `LogKey` fields; the tests
enforce it on `loom.*` loggers only (see
[What loom logs and what is yours](#what-loom-logs-and-what-is-yours)):

```python
import logging

from loom.core.log_names import LogEvent, LogField

log = logging.getLogger(__name__)


class InvoiceEvent(LogEvent):
    ISSUED = "billing.invoice_issued"


log.info(InvoiceEvent.ISSUED, extra={LogField.JOB: "SendInvoice"})
```

- In loom's code, the message is the **event**: a `LogEvent` member named
  `<area>.<subject>_<fact>`, lowercase, and loom's are `loom.<area>.<subject>_<fact>`:
  an application's events must not start with `loom.`. It never holds a value: no f-string, no
  `%` format. Values go in the fields, where a collector can filter on them.
  The form is checked when the class is declared: a member out of form, or a value
  another class or member already declared, raises `ValueError` at import, so an
  application event never reuses one of loom's. Naming the fact as a past
  participle (`invoice_issued`, `delivery_abandoned`) is this guide's convention.
- loom's field keys are `LogKey` members (`LogField` for the ones loom's formats write): OpenTelemetry semantic conventions where one
  exists (`service.name`, `code.function.name`, `exception.type`, `error.type`), `loom.*`
  otherwise. Event names and field keys are part of the public contract of a
  release.
- A field value is a scalar (`str`, `int`, `float`, `bool`, `None`) or a short list
  of them. The formatters write any other object as its type name, never its
  `repr`; a `Secret` is masked; a value longer than 2048 characters is cut with a
  marker. Keep secrets and personal data out of fields anyway.
- Exceptions: each of loom's formats, from the `exc_info` of the record, writes what
  it says about its exception: `exception.type`, `loom.error.code` for a loom
  error, and from ERROR up `exception.stacktrace` (the standard traceback, its last
  16 384 characters). The message is never a field of its own. Pass `exc_info` once
  per failure, where your code stops the exception, so one failure is one record
  and one stack. A WARNING with `exc_info` writes the type and the code but no stack:
  a handled degradation does not need a human to read a traceback. A `stack_info=True`
  stack is written at any level, after the traceback. With the `lambda` format
  the runtime's handler writes loom's JSON line too, so no field is lost; the formatter
  sets `_frame_type` on the record, which the runtime's handler (`awslambdaric`) reads to
  frame the line with the record's level.
- Reserved keys: `timestamp`, `level`, `message` and `logger` belong to the record;
  the `json` and `text` formats write an `extra` of one of those names as
  `loom.extra.<key>`. The exception fields belong to loom's formats, which replace an
  `extra` of their name and never write an `extra` stack.
- Levels: ERROR needs a human, WARNING is degraded but handled, INFO is lifecycle
  and business facts, DEBUG is diagnosis. One record per fact: batch the logs of a
  loop.

Loom objects (use cases, jobs, steps) have no logger attribute. Application code
logs the Python way, one `getLogger(__name__)` per module, so each module keeps its
own level control. Per-class loggers are optional, not the rule:
`logging.getLogger(f"{__name__}.{Cls.__name__}")`. Loom objects are identified by
the context fields below, not by the logger name.

## What every record carries

Each of loom's outputs (`json`, `text`, `lambda` and the OpenTelemetry export) derives
these fields itself when it writes a record, in the thread that emitted it, whoever
emitted it (loom, your code, a library). Loom writes no context onto the record: another
handler sees the record as it was logged.

| Field | Value |
|---|---|
| `service.name`, `service.version` | `app.name`, `app.version` of the configuration |
| `deployment.environment.name` | the resolved environment |
| `loom.runtime` | `fastapi`, `celery`, `lambda` or `cli` |
| `trace_id`, `span_id` | inside a recording OpenTelemetry span, the span's own trace id and span id, so the record correlates with its trace even when the span continues a caller's `traceparent`; an `extra` of either name is overridden there; outside a span, `trace_id` is loom's id (HTTP header, job envelope, flow) and there is no `span_id` |
| `loom.use_case.name` | the use case being executed |
| `loom.job.name`, `loom.task_id`, `loom.attempt` | the job message being run |
| `code.function.name`, `code.line.number`, `code.file.path` | where the record was emitted |

A field you pass as `extra` wins over these, rendered like any other. Following one `trace_id` shows
the lines of loom and of your application together.

Add fields of your own to a block with `log_context`; they follow the current thread
or asyncio task and never reach a concurrent request:

```python
from loom.core.log_names import LogField
from loom.core.logger import log_context

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
    level: INFO       # DEBUG | INFO | WARNING | ERROR | CRITICAL, in capitals
    named_levels:
      botocore: INFO  # over loom's defaults for chatty libraries
    subject_key: ${LOG_SUBJECT_KEY}  # keys user.hash on security events; omit to write no caller
```

Libraries that write a record per request or query start at `WARNING`: `asyncio`,
`botocore`, `boto3`, `s3transfer`, `urllib3`, `httpx`, `httpcore` and
`sqlalchemy.engine` (`QUIET_LIBRARIES` in `loom.core.logger.settings`): configuring
logging sets these loggers' levels, so loom changes them even if your code set them
before. A `named_levels` entry overrides one only under its exact logger name:
`botocore: INFO` leaves `boto3` at `WARNING`.

`subject_key` is a `Secret`: no log, repr or dump shows it. Use a random value of
at least 32 bytes, the same in every service whose records you correlate, and keep it
out of the configuration file through a placeholder. Changing it changes every
`user.hash` from then on.

| Format | Output | Default when |
|---|---|---|
| `lambda` | The Lambda runtime's handler, given loom's JSON line under both log formats (`Text` and `JSON`); loom sets the level | `AWS_LAMBDA_FUNCTION_NAME` is set |
| `json` | One JSON object per line on stdout; fields at the top level; `exception.type`, `loom.error.code`, `exception.stacktrace` | a deployed environment (`production`, `staging`) |
| `text` | A readable line with a UTC timestamp and logfmt `key=value` fields; a value holding a space, `=` or `"` is quoted, line breaks are escaped | any other environment |

The settings are validated at startup and by `loom check` as written
(`loom.logger.E001` for a format other than `lambda`, `json` or `text`, `loom.logger.E002` naming every
unknown level at once; a value that holds a placeholder is not judged).

### AWS Lambda

Inside Lambda the `lambda` format gives the runtime's handler loom's JSON line,
under the function's `Text` and `JSON` log formats alike, so every field, mask and
failure field survives; under `JSON` the line carries the runtime's JSON frame.
The root level is `observability.log.level`, as anywhere else. Loom never ships logs over the network; the destination is a deployment
choice (CloudWatch Logs, or a log destination delivered to S3, which costs less and
needs no CloudWatch Insights).

### Prefect

`PrefectObserver` writes loom's lifecycle records on its `loom.*` logger, inside a
flow run or not. A flow loom builds ships the records of the `loom` logger to Prefect UI through
Prefect's own API log handler (batching, size limit and `PREFECT_LOGGING_TO_API_ENABLED`
are Prefect's), each as its event followed by its fields in logfmt, masked as loom's
formats mask them, and its stack. Your own loggers and other libraries' do not reach
the UI through loom; use Prefect's own mechanisms for them, such as
`PREFECT_LOGGING_EXTRA_LOGGERS`, which is also how a flow of your own ships `loom`'s.

## What loom logs and what is yours

Loom guarantees **its own records**, the ones written by a `loom.*` logger. The
test suite fails on any such record, DEBUG included, that breaks these rules; nothing else is checked.

| Loom guarantees | In every `loom.*` record |
|---|---|
| Constant events | The message is a `LogEvent` member, never a sentence with values and never `%` arguments. Each member is `<area>.<subject>_<fact>` and declared by one class only, checked at declaration. |
| Declared keys | Every field key is a `LogKey` member and carries the `ValueKind` its member declares: text unless it says number, boolean or list. An `OBJECT` key is a bag whose names are yours, read through `.of(name)`, which raises `ValueError` on any other key. A key is dotted lowercase `snake_case` and never names both a value and the prefix of other keys; declaring one out of form or one that would raises `ValueError`. Keys under `loom.` are loom's: declaring one outside the `loom` package raises `ValueError`, so an application's keys use their own prefix. The contract reads each key from the record: a declared key passed as plain text counts as undeclared. |
| No exception text | Loom's fields never carry the text of an exception or the data in it: a record about a caught exception passes it as `exc_info`, and loom's formats write `exception.type` (the fully qualified class name, a builtin bare, as OpenTelemetry names it), `loom.error.code` and, from ERROR up, the standard Python stack. That stack includes the exception messages, foreign ones too: driver and library text is yours (set `stores.<name>.hide_parameters: true` if bound parameters must not appear). Other handlers on the root logger receive `exc_info` as usual. The contract fails any loom record with `exc_info` whose fields hold `str(exc)`. `exception.type` comes from `exc_info` alone, and means that the record carries an exception: a loom record whose fields hold it breaks the contract. A failure with no exception to carry, loom's token code (which must not keep its frames) and the lifecycle observer (which only holds the summary), writes `error.type`, with `loom.error.code`, through `ErrorSummary.fields()`. |
| No user values | Loom's own records, DEBUG included, carry the shape of the data they handle, never its values: a cache key holds the entity id as its digest, a ClickHouse query as its table, columns and `loom.etl.predicates` count, and a Delta write predicate is not logged. |
| Who, only on security events, as a keyed pseudonym | Following the OWASP Logging Cheat Sheet, security events say who acted without writing the subject: `user.hash` is the HMAC-SHA256 of the caller's subject under `observability.log.subject_key`, in hex, cut to 32 characters. They are a token issued (`loom.rest.token_issued`), an authorization denied (`loom.rest.authorization_denied`, `loom.stores.role_authorization_denied`) and a caller switched (`loom.identity.caller_switched`, with the previous caller in `loom.identity.previous_user.hash`). To find a known user's records, compute the HMAC of their subject with the same key and search for it; without the key a hash cannot be reversed or matched to a guess. Without a key no caller is written at all, never the subject or an unkeyed hash, and startup logs `loom.logging.subject_hash_disabled` once. Operational records and spans never carry end-user identity, run parameters or other user values. RLS writes are also recorded in the `audit_log` table. |
| One render | Each of loom's outputs renders its own copy of the values: a `Secret` is masked, an object is its type name, a long value is cut. `json`, `lambda`, `text` and OpenTelemetry export write the same thing, and loom writes no context onto the record nor changes the values on it. |
| Prefixes for open bags | A job's `meta` is written under `loom.lifecycle.meta.<key>`, and the format's own layout keys (`timestamp`, `level`, `message`, `logger`) under `loom.extra.<key>`. An `extra` key with a loom field name replaces loom's value: that is your choice. |

| Yours | What it means |
|---|---|
| Your own loggers | Any logger outside `loom.*` is yours: loom does not check its messages, `%` arguments or keys. |
| The values you pass | What you put in `meta`, in the attributes of a use case or job, in `log_context` and in the names and ids you choose reaches the log as you gave it. Do not put secrets there; wrap a sensitive string in `Secret` to have it masked. |
| Your exceptions | Loom writes the stack of an exception it logs, and a stack holds the text of the exception, yours included. Keep secrets out of exception messages. The text of a SQLAlchemy error includes its bound parameters; set `stores.<name>.hide_parameters: true` to leave them out. Postgres' own `DETAIL:` text from the driver is not affected. |
| Your handlers | Loom's context lives in its formats, not on the record: a handler you add yourself without loom's formatter sees the record as it was logged, with no service identity, trace id or `log_context` field, and any handler sees the raw `extra` values, before or after loom's. A `JsonFormatter` used alone on a handler of yours still writes the code origin, the running context, the trace ids and the failure's type and stack, but no service identity. |

Declaring the events and keys of your own code is optional, and worth it when
several places write the same event:

```python
import logging

from loom.core.log_names import LogEvent, LogKey, ValueKind

log = logging.getLogger(__name__)


class InvoiceEvent(LogEvent):
    ISSUED = "billing.invoice_issued"


class InvoiceKey(LogKey):
    NUMBER = "billing.invoice.number"
    TOTAL = "billing.invoice.total", ValueKind.NUMBER


log.info(InvoiceEvent.ISSUED, extra={InvoiceKey.NUMBER: "INV-2041", InvoiceKey.TOTAL: 129.9})
```

### The names are a contract

From release 12.0 the events and keys loom declares are public and only grow:
`docs/reference/log-contract.txt` lists each as `event <name>` or `key <name> <kind>`, generated
from the declarations; the module that declares a name is not published, so a declaration may move
between modules. A test fails when a listed name is no longer declared (a removal or a rename breaks
collectors and alerts built on it) and when a declared name is not listed. After adding a name, run
`uv run poe log-contract` under Python < 3.13 with every extra installed (bytewax needs it) and commit the file.

## ETL: logging never triggers a computation

A log field is filled with what the run already produced, never with something it
computes for the log: no `count()`, `collect()`, `first()`, `height` on a lazy
frame, `distinct()` or schema inference. Row counts come from the write's own
metrics or from a frame already materialised for the write; an unknown count is
left out, not computed. Log one record per step and per write, not per partition or
batch.
