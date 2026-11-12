# ADR 0011: Use-case entry points, ports and adapters, no transport inheritance

- Status: Accepted. Phase 1 (agents and MCP on Lambda) is built; phase 2
  (the `sqs` source and result sinks) is planned.
- Branch: `feat/010-usecase-entrypoints`
- Applies to: the release that ships phase 1, and phase 2 after it
- Specification: `.specify/specs/010-usecase-entrypoints/`
- ADR 0010 is reserved for spec 009.

## Context

A use case could be served over REST, on FastAPI or on a Lambda `http`
function. It could not be served by a message trigger (SQS, EventBridge, the
jobs worker) without a parallel `Job` class, it could not host agents or MCP
on Lambda, and a message trigger had no declared place to send its result.

Applications worked around this with inheritance: `SqsXUseCase(XUseCase)`
overrides `execute` to persist its result. That repeats the compiled
signature, puts the LLM call inside the write transaction, and puts the
transport in the use case's name. `Job` already carries its transport as
class variables (queue, retries, countdown), so the pattern had a precedent.

The Lambda adapter also refused the `ai` section and manifest agents, with no
written reason. The refusal rested on the belief that loom's MCP sessions
could not survive a frozen process.

## Decision

1. **The use case returns its result and does not know how it was called.**
   The entry point decides how a request arrives and what happens to the
   result. A use case is required only to return a serialisable result and to
   raise domain failures that do not depend on the transport.
2. **No transport inheritance.** No use case is subclassed per transport, and
   no transport setting is a class variable of a use case. Queue, timeout,
   identity, key, result and failure handling are fields of the entry point's
   configuration (`lambda.functions.<f>.trigger`). The planned `sqs` trigger,
   which runs a use case, is a struct of its own, not a subclass of
   `EventTrigger`, whose `job` is required.
3. **Ports and adapters for entry points.** The planned
   `loom.core.entrypoint` package holds the entry plan, the result sinks and
   idempotency. It imports no SDK and names no adapter. Sink kinds reach it as
   an injected mapping: core kinds (`use_case`, `repository`, `callback`) live
   in core, the AWS kind (`sqs`) in `loom.sinks`, which assembles both with the
   configuration union. The Lambda adapter passes them in. Core holds no AWS
   struct.
4. **Every entry point composes through the same root** (ADR 0007). A Lambda
   function hosts the `ai` section through the composition root, as FastAPI
   does. The recipe flag `agent_endpoints` tells the root whether the entry
   point serves agents over HTTP. When it does not (Lambda), only the agents
   an `Agent()` marker names are compiled, and only their servers and the
   `Mcp()` servers are opened at Init.
5. **MCP sessions live for the process on every entry point.** Lambda uses
   the sessions FastAPI uses, not one session per invocation. One session per
   server is shared by markers, agent runs and python tools, and it renews
   after a failure. A call is sent again only when the server proved it never
   ran (a "Session not found" answer); any other failure is raised as
   `MCP_CALL_INTERRUPTED` and never repeated. After an idle wall-clock gap of
   more than 5 seconds, the session's HTTP pool replaces its connections
   before the next request, which keeps the first call after a Lambda thaw off
   a dead connection without renewing the session.
6. **What Lambda cannot serve is refused or reported, never dropped
   silently.** `transport: stdio` is refused at startup, from the
   configuration as written and again as resolved. Agent endpoint keys are
   warned about and ignored.

## Reviews

The plan went through an architecture review, three rounds of a proposal
critic and owner decisions recorded in the specification. They replaced
per-invocation MCP sessions with process sessions and the existing renewal;
required that an MCP error the client makes itself never be read as a server
answer; made an interrupted call one code on every path; and moved the
deployed latency measurement (A3) out of the release gate.

## Consequences

- An agent run whose MCP call is interrupted now fails the run instead of
  handing the model a refusal. This is deliberate: a non-idempotent tool is
  never run twice by loom. It applies on FastAPI too.
- A queue trigger redelivers a job whose call was interrupted, so a tool may
  run again on redelivery. Idempotent delivery belongs to phase 2.
- Behind an HTTP proxy, the stock MCP transport is kept, so there is no
  idle-gap eviction. A stale connection fails one call, and the next renews.
- Agent endpoints on Lambda are future work.
- The client-made error code set is pinned to mcp 2.3.0. An upgrade of
  fastmcp, mcp, pydantic-ai or httpx2 reruns the classification and transport
  tests.
- See [MCP Deployment](../../ai/mcp.md#mcp-on-aws-lambda) for the operational
  detail.
