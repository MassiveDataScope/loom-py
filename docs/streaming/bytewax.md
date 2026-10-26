# Bytewax Runtime

`loom.streaming.bytewax` is the production adapter that turns a declarative
`StreamFlow` into a real Bytewax dataflow.

For a runnable end-to-end reference implementation, see:
[dummy-loom-streaming](https://github.com/the-reacher-data/dummy-loom-streaming).

## What it does

- compiles the flow
- resolves config bindings
- wires Kafka sources and sinks
- applies `With` / `WithAsync`
- runs observability hooks

## Public entrypoint

```python
from loom.streaming.bytewax import StreamingRunner
```

## When to use it

Use the Bytewax adapter when you want:

- topic-to-topic streaming
- typed message envelopes
- branch-aware fan-out
- `CollectBatch` before `WithAsync`
- runtime observability for batch and node lifecycle events

## Flow contract

The clean authoring contract is:

```python
from loom.streaming import CollectBatch, IntoTopic, Process, WithAsync

process = Process(
    CollectBatch(max_records=50, timeout_ms=2000),
    WithAsync(
        process=Process(
            # per-message async step
            ...
            IntoTopic(...),
        ),
        max_concurrency=50,
    ),
)
```

`CollectBatch` belongs before `WithAsync` when you want batch aggregation as an
input shape. If you want to wait for a batch outside the async step, keep that
logic in the parent flow, not inside the task itself.

## Writing your own destination

`IntoTopic` and `IntoTable` are the destinations loom ships. A destination it
does not ship is a **frozen dataclass** satisfying `IntoSink`: no base class, no
registration, no loom import beyond the payload type. The compiler recognises it
by structure and resolves its configuration from `streaming.sinks.<name>`.

```python
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, ClassVar

@dataclass(frozen=True)
class IntoJsonl:
    payload: type[IncidentEvent]
    name: str = "incidents"          # the key under streaming.sinks
    router_branch_safe: ClassVar[bool] = True

    def build_partition(
        self,
        config: Any,                 # your own resolved section
        worker_index: int,
        worker_count: int,
        bridge: Any = None,
        session_manager: Any = None,
    ) -> "JsonlPartition":
        return JsonlPartition(config["path"])


class JsonlPartition:
    def __init__(self, path: str) -> None:
        self._path, self._buffer = path, []

    def write_batch(self, items: Sequence[IncidentEvent]) -> int:
        self._buffer.extend(items)   # once per Bytewax epoch
        return 0                     # buffered, nothing durable yet

    def close(self) -> int:
        flush(self._path, self._buffer)   # must be idempotent
        flushed, self._buffer = len(self._buffer), []
        return flushed               # how many buffered events are now durable
```

`Process(IntoJsonl(payload=IncidentEvent))` type-checks under `mypy --strict`:
the three attributes are read-only in the protocol, so a frozen dataclass, a
msgspec struct or a class variable all satisfy them, and `config` and
`session_manager` are opaque, so your sink narrows them to whatever its backend
resolves. `write_batch` runs per epoch, per worker; `close` may be called even
when `write_batch` never was. Both return how many of the oldest events not yet
reported durable they made durable, `0` when they only buffered: under
at-least-once delivery loom completes the source offsets of exactly those
events, so a crash before the flush consumes them again.

## Booting from YAML (`resolvers=`)

`StreamingRunner.from_yaml(flow, path)` loads the config and builds the runner;
`run(config_path=...)` does the same load on an existing runner. Both register
loom's built-in AWS resolvers, `secrets` (Secrets Manager) and `ssm` (SSM
Parameter Store), by default, so a flow booted through the factory reads a
secret with no code beyond the factory call:

```yaml
# config/streaming.yaml
kafka:
  consumer:
    brokers:
      - ${ssm:/prod/incidents/kafka-broker}
    group_id: incidents-service
    topics: ["incidents.in"]
    delivery: at_most_once
    security:
      protocol: SASL_SSL
      sasl_mechanism: SCRAM-SHA-512
      sasl_username: incidents
      sasl_password: "${secrets:/prod/incidents/kafka-password}"
```

```python
from loom.streaming.bytewax import StreamingRunner

runner = StreamingRunner.from_yaml(incident_flow, "config/streaming.yaml")
runner.run()
```

The built-in resolvers use boto3's default region and credential chain, and
create their client only when a placeholder resolves: a YAML with no
`${secrets:...}` or `${ssm:...}` never touches AWS and boots without boto3.
When a placeholder does resolve and boto3 is missing, the error names the extra
to install, `loom-kernel[config-ssm]`.

`resolvers=` adds your own prefixes or overrides a built-in by name, on
`from_yaml` and on `run(config_path=...)`. Any object with a `name` and a
`resolve(key) -> object` works:

```python
class VaultResolver:
    name = "vault"

    def resolve(self, key: str) -> str:
        return read_vault_secret(key)

runner = StreamingRunner.from_yaml(
    incident_flow, "config/streaming.yaml", resolvers=[VaultResolver()]
)
```

A resolver you pass with the same name as a built-in wins; a built-in default
never replaces a resolver already registered earlier in the process, so
calling the factory more than once is safe. `load_config` registers no
defaults, and resolvers passed to it explicitly replace an earlier registration
of the same name.

## Runtime notes

- `WithAsync` executes one message per task, concurrently.
- `CollectBatch` emits observable batch events.
- Errors are routed through the explicit error wiring.
- `msg` and `payload` are public expression roots for routing predicates.
