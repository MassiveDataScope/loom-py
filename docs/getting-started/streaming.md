# Streaming Quickstart

`loom.streaming` lets you declare typed streaming flows with explicit sources,
steps, batching, branching, and transport wiring.

The companion demo repository for this subsystem is
[dummy-loom-streaming](https://github.com/the-reacher-data/dummy-loom-streaming).

## Install

Choose the streaming runtime and Kafka transport:

```bash
pip install "loom-kernel[streaming,kafka]"
```

The `streaming` extra installs Bytewax only on Python < 3.13, because Bytewax
publishes no wheel for Python 3.13 or later yet. A project that runs streaming
flows must pin `requires-python = ">=3.11,<3.13"` in its own `pyproject.toml`; on
Python 3.13 or later the extra resolves without Bytewax, and importing
`loom.streaming.bytewax` raises an `ImportError` that names the extra and the
versions it needs.

The Mongo change-stream source (`FromMongoCDC`) needs `pymongo`, which ships
with the `mongo` extra:

```bash
pip install "loom-kernel[streaming,kafka,mongo]"
```

A source under `mongo.source` or `mongo.sources.<name>` takes the MongoDB
connection keys every loom context shares (`url`, `database`, `username`,
`password`, `connect_timeout`, `read_timeout`, `server_selection_timeout`, all
in seconds, and `pool.size`/`pool.timeout`). A ClickHouse sink's
`database.<name>` takes the ClickHouse keys (`url`, `username`, `password`,
`connect_timeout`, `read_timeout`, `pool.size`). See
ADR 0006 (`docs/architecture/adr/0006-hexagonal-layers.md`).

Upgrade note: `uri` is now `url`, `max_pool_size` is now `pool.size` and
`server_selection_timeout_ms` is now `server_selection_timeout`, in seconds;
a sink database's `send_receive_timeout` is now `read_timeout` and `pool_size`
is now `pool.size`. An old key stops the flow's compilation naming the new one.

## Minimal flow

```python
from loom.streaming import (
    CollectBatch,
    FromTopic,
    IntoTopic,
    Process,
    StreamFlow,
    WithAsync,
    payload,
)

from app.streaming.tasks import FetchRequestTask
from app.streaming.types import ScrapeRequest, ScrapeResponse


async_scrape_flow = StreamFlow(
    name="async_smoke_flow",
    source=FromTopic[ScrapeRequest](
        name="scrape.requests",
        payload=ScrapeRequest,
    ),
    process=Process(
        CollectBatch(max_records=50, timeout_ms=2000),
        WithAsync(
            process=Process(
                FetchRequestTask,
                IntoTopic[ScrapeResponse](
                    name="scrape.responses",
                    payload=ScrapeResponse,
                ),
            ),
            max_concurrency=50,
        ),
    ),
)
```

## Run it

Use the Bytewax runner to compile the flow, resolve YAML bindings, and execute
the graph:

```python
from loom.streaming.bytewax import StreamingRunner

runner = StreamingRunner.from_yaml(async_scrape_flow, "config/streaming.yaml")
runner.run()
```

## Public imports

Prefer the public surface:

```python
from loom.streaming import (
    Broadcast,
    CollectBatch,
    ContextFactory,
    Fork,
    FromTopic,
    IntoTopic,
    Message,
    Process,
    RecordStep,
    Router,
    StreamFlow,
    With,
    WithAsync,
    msg,
    payload,
)
```

Use `loom.streaming.nodes`, `loom.streaming.kafka`, and
`loom.streaming.bytewax` only when you need a narrower or runtime-specific
surface.

For a runnable end-to-end example, see the companion repository:
[dummy-loom-streaming](https://github.com/the-reacher-data/dummy-loom-streaming).
