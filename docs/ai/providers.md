# Model Providers

An artifact never names a model. It names a **role** — `default`, `reasoning`,
`cheap` — and the deployment binds that role to a concrete provider and model in
the `ai.models` section. Switching vendor is a configuration edit; the artifact
does not change, and neither does its granted capabilities.

```yaml
ai:
  models:
    default:                       # the role an artifact gets when it declares none
      provider: openai
      model: gpt-4o-mini
      credentials_ref: OPENAI_API_KEY
```

A role an artifact declares but the deployment does not bind fails **start-up**
with `MODEL_ROLE_UNBOUND`. It is never deferred to the first request.

## The matrix

| `provider` | Extra | Required settings | `credentials_ref` means | Covers |
|---|---|---|---|---|
| `bedrock` | `ai-bedrock` | `region` | an **AWS profile name**; omit it to use the standard boto3 chain (environment, role, instance profile) | AWS Bedrock, all its hosted model families |
| `openai` | `ai-openai` | — | the **name of the environment variable** holding the API key; omit it and the SDK reads its own default | OpenAI's own API |
| `anthropic` | `ai-anthropic` | — | the **name of the environment variable** holding the API key; omit it and the SDK reads its own default | Anthropic's own API |
| `gateway` | `ai-openai` | `endpoint` | the **name of the environment variable** holding the API key the endpoint expects | **any OpenAI-compatible endpoint** — see below |
| `typesafe` | `ai-typesafe` | — | the **name of the environment variable** holding the API key; omit it and the SDK reads `TYPESAFE_API_KEY` | TypeSafe's Jev — a decision model, see below |

Which **provider-run tools** each binding admits — web search, web fetch, code
execution — is decided by the model class of the installed engine, not by this
table; see [`native` capabilities](artifacts.md#native--tools-the-model-provider-runs).

An unknown `provider` fails start-up with `PROVIDER_UNKNOWN`, naming the ones
this release binds. A provider whose SDK is not installed fails with
`PROVIDER_NOT_INSTALLED`, **naming the extra to install**. A missing required
setting fails with `PROVIDER_SETTING_MISSING`, naming the setting. None of the
three is ever discovered by a user at request time.

## `gateway` is a protocol, not a vendor

This is the part worth reading twice.

`gateway` is **not** "some other vendor we also support". It is the
OpenAI-compatible chat-completions **protocol**, and it is a single provider
entry because the protocol — not the company behind the endpoint — is what loom
has to speak. Point `endpoint` at anything that implements it:

```yaml
ai:
  models:
    default:                                          # OpenRouter
      provider: gateway
      endpoint: https://openrouter.ai/api/v1
      model: anthropic/claude-sonnet-4
      credentials_ref: OPENROUTER_API_KEY

    local:                                            # Ollama, on the same host
      provider: gateway
      endpoint: http://localhost:11434/v1
      model: llama3.1:8b

    self_hosted:                                      # vLLM, in-cluster
      provider: gateway
      endpoint: https://vllm.internal.example.com/v1
      model: meta-llama/Llama-3.1-70B-Instruct
      credentials_ref: VLLM_API_KEY
```

OpenRouter, Ollama, vLLM, LiteLLM, Together, Groq, an in-house inference proxy —
all the same entry, because all of them answer the same protocol. Adding a
vendor that speaks OpenAI-compatible requires **no loom release**: it is a
config edit. That is the whole reason the dispatch is keyed on protocol.

`endpoint` is required for `gateway` (`PROVIDER_SETTING_MISSING` otherwise),
because a compatible provider with no address is not a provider.

```{note}
Use the dedicated `openai` provider for OpenAI itself. `gateway` exists for
*other* endpoints speaking the same protocol; the two share an
implementation, and the distinct name is what makes a config review able to
tell "we call OpenAI" from "we call something OpenAI-shaped".
```

## Several roles at once

Roles let one application mix vendors by *purpose* rather than by agent:

```yaml
ai:
  models:
    default:
      provider: openai
      model: gpt-4o-mini
      credentials_ref: OPENAI_API_KEY
    reasoning:
      provider: bedrock
      model: anthropic.claude-sonnet-4-20250514-v1:0
      region: eu-west-1
    cheap:
      provider: gateway
      endpoint: https://openrouter.ai/api/v1
      model: meta-llama/llama-3.1-8b-instruct
      credentials_ref: OPENROUTER_API_KEY
```

An artifact then picks one with `model_role: reasoning`. Moving that agent to a
cheaper model later is a one-line config change with no artifact edit and no
redeploy of the agent definition.

## `typesafe` fills an output, it never writes text

Jev, TypeSafe's model, is not a language model: given a state, it answers
typed questions about it with a calibrated probability each, and pydantic-ai
turns the agent's `output_type` into those questions. Bind it to a role whose
output is a strict class of `bool`, `Literal`/`Enum`, bounded `float` or
`IntEnum` fields; a `str` field, a text answer, a file in the prompt or a
tool taking arguments is refused by pydantic-ai before any request.

```yaml
ai:
  models:
    verdict:
      provider: typesafe
      model: jev-latest
      credentials_ref: TYPESAFE_API_KEY
```

pydantic-ai fills a TypeSafe output through tool mode only, so this is the one
binding whose `output_mode` loom can check before the first request: pinning
`native` fails **start-up** with `OUTPUT_MODE_UNSUPPORTED`, naming the mode
the provider serves. `endpoint`, when set, replaces the API's base URL.
`streaming` changes nothing: Jev returns each answer whole.

What makes the answer worth having — the probability behind each field and
the margin by which it cleared its threshold — comes back on
`AgentAnswer.provider_details`, verbatim as pydantic-ai reports it
(`confidence`, `probabilities`, `scores`, `requests`), so a use case can act
on a firm verdict and escalate a doubtful one. In process only; see
[what the caller receives](artifacts.md#what-the-caller-receives).

## Credentials never live in the artifact — or in the config

`credentials_ref` is a **reference**, never the secret itself: an AWS profile
name for `bedrock`, and for the API-key providers the **name of an environment
variable** loom reads at start-up. An unset variable fails start-up with
`PROVIDER_SETTING_MISSING` naming it, instead of a 401 on the first call; omit
`credentials_ref` and each SDK reads its own default variable
(`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `TYPESAFE_API_KEY`).

The value is validated fail-closed at start-up — a value shaped like literal
secret material (`AKIA…`, `sk-…`, `ghp_…`, a URL with credentials in its
userinfo, anything containing whitespace, quotes or braces) is **rejected**, and
the rejected value is deliberately absent from the error message so the error
cannot leak the secret it just refused.

The binding is carried into the compiled plan as an `InferenceTarget`, which
redacts itself:

```pycon
>>> print(target)
InferenceTarget(provider='bedrock', model='anthropic.claude-sonnet-4-...', region='eu-west-1', endpoint=None, output_mode=None, streaming=True, credentials_ref=<redacted>, options=<redacted>)
```

and **refuses** to be serialised at all when it carries a secret reference —
`msgspec.json.encode` raises rather than emitting it. A plan that reaches a wire
encoder with a credential aboard is a bug worth surfacing, not smoothing over.
The concrete leak path this closes is an unredacted `repr` in a start-up
traceback.

## Vendor-specific settings

Anything a vendor supports that loom has no opinion about goes in `options`,
which is handed to the engine as its own model-settings vocabulary. Loom
introduces no second settings dialect:

```yaml
ai:
  models:
    reasoning:
      provider: anthropic
      model: claude-sonnet-4-20250514
      credentials_ref: ANTHROPIC_API_KEY
      options:
        temperature: 0.2
        max_tokens: 4096
```

`options` is confined to the deployment configuration and, like
`credentials_ref`, never reaches the artifact and never survives serialisation.

## Pinning the structured-output mode

Every agent answers with a structured object, and the engine has more than one
way of asking a model for it. Left alone, the engine picks the mode per
provider and model. When that choice is wrong for a particular model — it
accepts tool calls but not a native JSON-schema response, or the reverse — the
binding pins it with `output_mode`:

| `output_mode` | The engine asks for the answer as | When to set it |
|---|---|---|
| *absent* | whatever the engine resolves for that provider and model | the default; leave it unless a run fails at the provider |
| `tool` | a tool call whose arguments carry the object | the model supports tool calling and rejects the native mode |
| `native` | the provider's own structured-output response | the model supports structured output and misbehaves with the tool mode |

```yaml
ai:
  models:
    reasoning:
      provider: bedrock
      model: <model id>
      region: eu-west-1
      output_mode: tool
```

The mode is per binding and per deployment: the artifact never sees it, and
loom does not infer it per provider. Two consequences follow from that:

- An unknown value fails **start-up** with `OUTPUT_MODE_UNKNOWN`, naming the
  role and the two values loom offers. `prompted` is deliberately not one of
  them: the engine strips markdown fences before validating a prompted answer,
  while loom decodes the raw text part, so a fenced answer would pass the
  engine and fail loom.
- A mode the model rejects at **request time** is a provider refusal, and it
  surfaces as `PROVIDER_UNAVAILABLE`. The provider's detail stays server-side,
  in the logs; the caller sees the code. Loom cannot check a mode against a
  model before the first request, so a wrong pin is found there, not at
  start-up. The one exception is `typesafe`, whose models pydantic-ai serves
  by tool alone, so a `native` pin on it fails start-up with
  `OUTPUT_MODE_UNSUPPORTED`.

The `tool` mode and thinking work together. Loom sends no `tool_choice` of its
own, so whether the request forces the output tool is the engine's choice.
For an Anthropic model with adaptive thinking (`options: {thinking: true}` on
a model that supports it), pydantic-ai 2.52 and later send `tool_choice:
auto`, and the model reasons before it calls the tool. Earlier releases force
the tool, and the model then answers without thinking; install
`pydantic-ai-slim` 2.52 or later when a binding configures thinking.

## Asking for the answer whole, not as a stream

Loom streams every run, so it can supervise it as it happens; the provider
is asked to stream too. Some models stream badly: on Bedrock, one family
delivers a short tool call with its opening characters missing, one answer
in two, while the same answer asked without streaming arrives whole -- the
provider parses it before sending it. A binding can ask for that instead:

```yaml
ai:
  models:
    reasoning:
      provider: bedrock
      model: <model id>
      streaming: false
```

What changes for that role, and only for it: each model request is answered in
one piece. A run loses nothing else — the answer is validated and retried the
same way, the usage is the same, and a `run_stream` caller still receives what
the model said and every tool call and result, in order; the text simply
arrives as one delta per text part instead of many. The policies
(`run_timeout_ms`, `max_iterations`, `max_requests`) apply as before.

It is the choice pydantic-ai offers between `run` and `run_stream`, made per
model binding rather than per call: the artifact never has to know which model
serves it, and a role can be moved back by deleting the line.

## What does not happen

**No fallback routing.** An exhausted or failing provider fails the run. It is
never silently re-routed to another model — a request that quietly answers from
a different vendor than the one configured is an unauditable answer, and cost
and data-residency commitments are made per binding.

**No model client per request.** One model object is built per plan at start-up
and reused by every run, because the provider client owns the connection pool.
Rebuilding it per request would pay a new TLS handshake for every prompt.
