---
tracker:
  kind: github
  owner: adyshimony
  repo: rust-symphony
  api_key: $GITHUB_TOKEN
  active_states:
    - open
  terminal_states:
    - closed
  labels:
    - symphony

polling:
  interval_ms: 15000

workspace:
  root: /tmp/symphony-workspaces/rust-symphony

hooks:
  after_create: |
    git clone --depth 1 git@github.com:adyshimony/rust-symphony.git .
  timeout_ms: 120000

agent:
  max_concurrent_agents: 1
  max_turns: 12
  max_retry_backoff_ms: 300000
  max_autonomous_turns_before_review: 5
  max_runtime_minutes_before_review: 25

codex:
  command: codex app-server
  turn_timeout_ms: 3600000
  read_timeout_ms: 5000
  stall_timeout_ms: 300000
  approval_policy:
    reject:
      sandbox_approval: true
      rules: true
      mcp_elicitations: true
  thread_sandbox: workspace-write
  turn_sandbox_policy:
    type: workspaceWrite
    writableRoots:
      - /tmp/symphony-workspaces/rust-symphony

server:
  port: 4000
---

You are implementing GitHub issue {{ issue.identifier }} in the `rust-symphony` repository.

Issue title: {{ issue.title }}

Issue description:
{{ issue.description }}

Execution rules:
- Work only inside the provided workspace.
- Do not access files outside the workspace unless the task explicitly requires it.
- Implement only the current issue. Do not start follow-up issues unless explicitly required by this issue.
- Preserve existing architecture and naming unless the issue explicitly asks for a refactor.
- Prefer minimal, targeted changes that keep the runtime understandable.
- Add or update tests for behavior you change.
- Run `cargo test` before finishing.
- Update README only if the issue changes user-visible behavior or operator-facing workflow.
- Do not mutate the tracker, do not close issues, and do not edit labels or assignees.
- Leave a clear handoff in the final result: what changed, what was tested, and any remaining risks.
