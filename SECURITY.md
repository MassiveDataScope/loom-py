# Security

## Dependency audit

Every pull request runs `pip-audit` over the locked runtime closure: the core
and every extra, as exported from `uv.lock` without the `dev` and `mcp-tests`
groups. Any known vulnerability fails the PR. The excluded groups are covered
by Dependabot alerts only.

## Accepted advisories

| Advisory | Package | Why loom is not affected | Exit condition |
|---|---|---|---|
| GHSA-3589-pxp7-3gj8 | `pydantic-ai-harness` < 0.59 | The flaw is in `CodeMode`. loom binds only `Skills` (`src/loom/ai/engines/pydantic_ai/extras/skills.py`) and never constructs `CodeMode`. | Raise the `ai-harness` floor to the fixed release and drop `--ignore-vuln GHSA-3589-pxp7-3gj8` from the `dependency-audit` job (T129). |
