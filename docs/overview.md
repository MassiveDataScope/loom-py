# Project Overview

`loom-kernel` is a framework kernel for building backend systems with a common
set of typed building blocks:

- **Core** for models, commands, use cases, routing, logging, and shared
  infrastructure contracts.
- **REST** for HTTP APIs, auto-CRUD, jobs, and FastAPI-based delivery.
- **ETL** for declarative batch pipelines with backend-specific runners.
- **Streaming** for typed topic-oriented flows with Kafka transport and a
  Bytewax runtime adapter.

The three public subsystems solve different problems, but they share the same
design rules:

- author with typed Python objects
- resolve configuration from YAML
- keep runtime concerns behind adapters
- prefer explicit contracts over reflection-heavy magic

The companion demo repository for the streaming subsystem is
[dummy-loom-streaming](https://github.com/the-reacher-data/dummy-loom-streaming).

## At a glance

| Area | What it is | Main runtime |
|------|------------|--------------|
| Core | Shared kernel for models, commands, use cases, routing, logging, config, and engine primitives. | Internal kernel services |
| REST | Request/response application layer with typed use cases, auto-CRUD, jobs, and callbacks. | FastAPI + Celery |
| ETL | Declarative batch pipelines over tables and files, with backend-specific execution. | Polars + Spark |
| Streaming | Typed topic-oriented flows for microservices and event-driven processing. | Kafka + Bytewax |

## REST

REST is the request/response part of the kernel.

Use it when you want:

- typed use cases with declarative dependencies
- model-driven auto-CRUD
- background jobs and callbacks
- FastAPI delivery with minimal ceremony

The public entrypoint is `loom.rest`, and the quickstart is focused on models,
interfaces, and use-case wiring.

REST also covers asynchronous/background work through Celery jobs and callbacks.

## ETL

ETL is the batch-processing part of the kernel.

Use it when you want:

- typed ETL parameters
- declarative pipeline/process/step graphs
- backend-specific execution on Polars or Spark
- file and table targets with explicit write modes

The public entrypoint is `loom.etl`, and the quickstart is focused on
`ETLStep`, `ETLProcess`, and `ETLPipeline`.

The two supported execution paths are:

- Polars for local and lake-style batch processing
- Spark for distributed batch processing

The model is table-centric: sources, targets, schemas, and write modes are the
core abstractions.

## Streaming

Streaming is the topic-oriented part of the kernel.

Use it when you want:

- typed `StreamFlow` declarations
- Kafka topics as sources and sinks
- synchronous and asynchronous steps
- fan-out, routing, and batch collection
- execution through the Bytewax adapter

The public entrypoint is `loom.streaming`, with transport support in
`loom.streaming.kafka` and runtime support in `loom.streaming.bytewax`.

Streaming is the right fit for microservices, fan-out, branching, and
message-driven workflows.

The design goal is consistent authoring across domains:

- declare behavior in Python types
- resolve configuration from YAML
- keep runtime adapters separate from authoring APIs
- preserve strong typing and explicit contracts

## How the parts fit together

| Part | Authoring surface | Runtime surface |
|------|-------------------|-----------------|
| Core | `loom.core.*` | shared kernel services |
| REST | `loom.rest.*` | FastAPI runtime |
| ETL | `loom.etl.*` | Polars / Spark runners |
| Streaming | `loom.streaming.*` | Kafka / Bytewax adapters |

## Streaming in practice

Streaming is split into three public layers:

1. `loom.streaming` for flow declarations.
2. `loom.streaming.kafka` for transport contracts and codecs.
3. `loom.streaming.bytewax` for the execution adapter.

The public flow authoring API uses `StreamFlow`, `Process`, `FromTopic`,
`IntoTopic`, `CollectBatch`, `With`, `WithAsync`, `Fork`, `Broadcast`, and
`Router`.

## Documentation structure

- [REST Quickstart](getting-started/rest)
- [ETL Quickstart](getting-started/etl)
- [Streaming Quickstart](getting-started/streaming)
- [Streaming Bytewax runtime](streaming/bytewax)
