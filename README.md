# Symphony Rust

Rust reference implementation of Symphony for Ubuntu 24, modeled on the repository root [`SPEC.md`](../SPEC.md) and the Elixir reference implementation in [`../elixir`](../elixir).

## Run

```bash
cd rust
cargo run -- \
  --i-understand-that-this-will-be-running-without-the-usual-guardrails \
  --port 4000 \
  /path/to/WORKFLOW.md
```

Press `q` to exit the TUI.

## GitHub Issues Workflow Example

```md
---
tracker:
  kind: github
  owner: openai
  repo: symphony
  api_key: $GITHUB_TOKEN
  active_states:
    - open
  terminal_states:
    - closed
workspace:
  root: ~/code/symphony-workspaces
agent:
  max_concurrent_agents: 5
codex:
  command: codex app-server
---

You are working on GitHub issue {{ issue.identifier }}.

Title: {{ issue.title }}

Description:
{{ issue.description }}
```
