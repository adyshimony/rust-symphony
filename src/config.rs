use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use serde_yaml::Value as YamlValue;

use crate::model::{CodexRuntimeSettings, WorkflowDefinition, WorkspaceHooks};

fn default_linear_endpoint() -> String {
    "https://api.linear.app/graphql".to_string()
}

fn default_github_endpoint() -> String {
    "https://api.github.com".to_string()
}

fn default_active_states() -> Vec<String> {
    vec!["Todo".to_string(), "In Progress".to_string()]
}

fn default_terminal_states() -> Vec<String> {
    vec![
        "Closed".to_string(),
        "Cancelled".to_string(),
        "Canceled".to_string(),
        "Duplicate".to_string(),
        "Done".to_string(),
    ]
}

fn default_poll_interval_ms() -> u64 {
    30_000
}

fn default_workspace_root() -> String {
    std::env::temp_dir()
        .join("symphony_workspaces")
        .to_string_lossy()
        .into_owned()
}

fn default_hook_timeout_ms() -> u64 {
    60_000
}

fn default_max_concurrent_agents() -> usize {
    10
}

fn default_max_turns() -> u32 {
    20
}

fn default_max_retry_backoff_ms() -> u64 {
    300_000
}

fn default_discovery_turn_required() -> bool {
    true
}

fn default_max_autonomous_turns_before_review() -> u32 {
    5
}

fn default_max_runtime_minutes_before_review() -> u64 {
    20
}

fn default_max_changed_files_before_review() -> usize {
    12
}

fn default_max_diff_lines_before_review() -> usize {
    800
}

fn default_max_idle_minutes_before_review() -> u64 {
    10
}

fn default_max_tokens_before_review() -> u64 {
    200_000
}

fn default_codex_command() -> String {
    "codex app-server".to_string()
}

fn default_turn_timeout_ms() -> u64 {
    3_600_000
}

fn default_read_timeout_ms() -> u64 {
    5_000
}

fn default_stall_timeout_ms() -> u64 {
    300_000
}

fn default_thread_sandbox() -> String {
    "workspace-write".to_string()
}

fn default_observability_enabled() -> bool {
    true
}

fn default_observability_refresh_ms() -> u64 {
    1_000
}

fn default_observability_render_interval_ms() -> u64 {
    16
}

fn default_server_host() -> String {
    "127.0.0.1".to_string()
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RawWorkflowConfig {
    #[serde(default)]
    tracker: RawTrackerConfig,
    #[serde(default)]
    polling: RawPollingConfig,
    #[serde(default)]
    workspace: RawWorkspaceConfig,
    #[serde(default)]
    agent: RawAgentConfig,
    #[serde(default)]
    codex: RawCodexConfig,
    #[serde(default)]
    hooks: RawHooksConfig,
    #[serde(default)]
    observability: RawObservabilityConfig,
    #[serde(default)]
    server: RawServerConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RawTrackerConfig {
    kind: Option<String>,
    endpoint: Option<String>,
    api_key: Option<String>,
    project_slug: Option<String>,
    assignee: Option<String>,
    active_states: Option<Vec<String>>,
    terminal_states: Option<Vec<String>>,
    owner: Option<String>,
    repo: Option<String>,
    #[serde(default)]
    labels: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawPollingConfig {
    #[serde(default = "default_poll_interval_ms")]
    interval_ms: u64,
}

impl Default for RawPollingConfig {
    fn default() -> Self {
        Self {
            interval_ms: default_poll_interval_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawWorkspaceConfig {
    #[serde(default = "default_workspace_root")]
    root: String,
}

impl Default for RawWorkspaceConfig {
    fn default() -> Self {
        Self {
            root: default_workspace_root(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawAgentConfig {
    #[serde(default = "default_max_concurrent_agents")]
    max_concurrent_agents: usize,
    #[serde(default = "default_max_turns")]
    max_turns: u32,
    #[serde(default = "default_max_retry_backoff_ms")]
    max_retry_backoff_ms: u64,
    #[serde(default = "default_discovery_turn_required")]
    discovery_turn_required: bool,
    #[serde(default = "default_max_autonomous_turns_before_review")]
    max_autonomous_turns_before_review: u32,
    #[serde(default = "default_max_runtime_minutes_before_review")]
    max_runtime_minutes_before_review: u64,
    #[serde(default = "default_max_changed_files_before_review")]
    max_changed_files_before_review: usize,
    #[serde(default = "default_max_diff_lines_before_review")]
    max_diff_lines_before_review: usize,
    #[serde(default = "default_max_idle_minutes_before_review")]
    max_idle_minutes_before_review: u64,
    #[serde(default = "default_max_tokens_before_review")]
    max_tokens_before_review: u64,
    #[serde(default)]
    max_concurrent_agents_by_state: HashMap<String, usize>,
}

impl Default for RawAgentConfig {
    fn default() -> Self {
        Self {
            max_concurrent_agents: default_max_concurrent_agents(),
            max_turns: default_max_turns(),
            max_retry_backoff_ms: default_max_retry_backoff_ms(),
            discovery_turn_required: default_discovery_turn_required(),
            max_autonomous_turns_before_review: default_max_autonomous_turns_before_review(),
            max_runtime_minutes_before_review: default_max_runtime_minutes_before_review(),
            max_changed_files_before_review: default_max_changed_files_before_review(),
            max_diff_lines_before_review: default_max_diff_lines_before_review(),
            max_idle_minutes_before_review: default_max_idle_minutes_before_review(),
            max_tokens_before_review: default_max_tokens_before_review(),
            max_concurrent_agents_by_state: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawCodexConfig {
    #[serde(default = "default_codex_command")]
    command: String,
    #[serde(default = "default_turn_timeout_ms")]
    turn_timeout_ms: u64,
    #[serde(default = "default_read_timeout_ms")]
    read_timeout_ms: u64,
    #[serde(default = "default_stall_timeout_ms")]
    stall_timeout_ms: u64,
    #[serde(default)]
    approval_policy: Option<JsonValue>,
    #[serde(default = "default_thread_sandbox")]
    thread_sandbox: String,
    #[serde(default)]
    turn_sandbox_policy: Option<JsonValue>,
}

impl Default for RawCodexConfig {
    fn default() -> Self {
        Self {
            command: default_codex_command(),
            turn_timeout_ms: default_turn_timeout_ms(),
            read_timeout_ms: default_read_timeout_ms(),
            stall_timeout_ms: default_stall_timeout_ms(),
            approval_policy: None,
            thread_sandbox: default_thread_sandbox(),
            turn_sandbox_policy: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawHooksConfig {
    after_create: Option<String>,
    before_run: Option<String>,
    after_run: Option<String>,
    before_remove: Option<String>,
    #[serde(default = "default_hook_timeout_ms")]
    timeout_ms: u64,
}

impl Default for RawHooksConfig {
    fn default() -> Self {
        Self {
            after_create: None,
            before_run: None,
            after_run: None,
            before_remove: None,
            timeout_ms: default_hook_timeout_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawObservabilityConfig {
    #[serde(default = "default_observability_enabled")]
    dashboard_enabled: bool,
    #[serde(default = "default_observability_refresh_ms")]
    refresh_ms: u64,
    #[serde(default = "default_observability_render_interval_ms")]
    render_interval_ms: u64,
}

impl Default for RawObservabilityConfig {
    fn default() -> Self {
        Self {
            dashboard_enabled: default_observability_enabled(),
            refresh_ms: default_observability_refresh_ms(),
            render_interval_ms: default_observability_render_interval_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawServerConfig {
    port: Option<u16>,
    #[serde(default = "default_server_host")]
    host: String,
}

impl Default for RawServerConfig {
    fn default() -> Self {
        Self {
            port: None,
            host: default_server_host(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub workflow_path: PathBuf,
    pub tracker_kind: Option<String>,
    pub linear_endpoint: String,
    pub linear_api_token: Option<String>,
    pub linear_project_slug: Option<String>,
    pub linear_assignee: Option<String>,
    pub linear_active_states: Vec<String>,
    pub linear_terminal_states: Vec<String>,
    pub github_endpoint: String,
    pub github_api_token: Option<String>,
    pub github_owner: Option<String>,
    pub github_repo: Option<String>,
    pub github_assignee: Option<String>,
    pub github_labels: Vec<String>,
    pub poll_interval_ms: u64,
    pub workspace_root: PathBuf,
    pub workspace_hooks: WorkspaceHooks,
    pub max_concurrent_agents: usize,
    pub max_retry_backoff_ms: u64,
    pub max_turns: u32,
    pub discovery_turn_required: bool,
    pub max_autonomous_turns_before_review: u32,
    pub max_runtime_minutes_before_review: u64,
    pub max_changed_files_before_review: usize,
    pub max_diff_lines_before_review: usize,
    pub max_idle_minutes_before_review: u64,
    pub max_tokens_before_review: u64,
    pub max_concurrent_agents_by_state: HashMap<String, usize>,
    pub codex_command: String,
    pub codex_turn_timeout_ms: u64,
    pub codex_read_timeout_ms: u64,
    pub codex_stall_timeout_ms: u64,
    pub codex_runtime: CodexRuntimeSettings,
    pub observability_enabled: bool,
    pub observability_refresh_ms: u64,
    pub observability_render_interval_ms: u64,
    pub server_port: Option<u16>,
    pub server_host: String,
    pub repo_memory_path: PathBuf,
    pub prompt_template: String,
}

impl ServiceConfig {
    pub fn from_workflow(workflow_path: PathBuf, workflow: WorkflowDefinition) -> Result<Self> {
        let raw = parse_raw(&workflow.config)?;
        let tracker_kind = raw.tracker.kind.clone();
        let workspace_root = resolve_path_value(&raw.workspace.root)?;
        let linear_api_token =
            resolve_secret_value(raw.tracker.api_key.clone(), &["LINEAR_API_KEY"]);
        let linear_assignee =
            resolve_secret_value(raw.tracker.assignee.clone(), &["LINEAR_ASSIGNEE"]);
        let github_api_token =
            resolve_secret_value(raw.tracker.api_key.clone(), &["GITHUB_TOKEN", "GH_TOKEN"]);
        let github_assignee =
            resolve_secret_value(raw.tracker.assignee.clone(), &["GITHUB_ASSIGNEE"]);
        let linear_endpoint = raw
            .tracker
            .endpoint
            .clone()
            .unwrap_or_else(default_linear_endpoint);
        let github_endpoint = raw
            .tracker
            .endpoint
            .clone()
            .unwrap_or_else(default_github_endpoint);
        let active_states = match tracker_kind.as_deref() {
            Some("github") => raw
                .tracker
                .active_states
                .clone()
                .unwrap_or_else(|| vec!["open".to_string()]),
            _ => raw
                .tracker
                .active_states
                .clone()
                .unwrap_or_else(default_active_states),
        };
        let terminal_states = match tracker_kind.as_deref() {
            Some("github") => raw
                .tracker
                .terminal_states
                .clone()
                .unwrap_or_else(|| vec!["closed".to_string()]),
            _ => raw
                .tracker
                .terminal_states
                .clone()
                .unwrap_or_else(default_terminal_states),
        };

        let approval_policy = raw.codex.approval_policy.unwrap_or_else(|| {
            json!({
                "reject": {
                    "sandbox_approval": true,
                    "rules": true,
                    "mcp_elicitations": true
                }
            })
        });
        let turn_sandbox_policy = raw.codex.turn_sandbox_policy.unwrap_or_else(|| {
            json!({
                "type": "workspaceWrite",
                "writableRoots": [workspace_root.to_string_lossy()]
            })
        });
        let repo_memory_path = workspace_root.join(".symphony").join("repo-memory.md");

        Ok(Self {
            workflow_path,
            tracker_kind,
            linear_endpoint,
            linear_api_token,
            linear_project_slug: raw.tracker.project_slug.clone(),
            linear_assignee,
            linear_active_states: active_states,
            linear_terminal_states: terminal_states,
            github_endpoint,
            github_api_token,
            github_owner: raw.tracker.owner.clone(),
            github_repo: raw.tracker.repo.clone(),
            github_assignee,
            github_labels: raw.tracker.labels.clone(),
            poll_interval_ms: raw.polling.interval_ms,
            workspace_root,
            workspace_hooks: WorkspaceHooks {
                after_create: raw.hooks.after_create.clone(),
                before_run: raw.hooks.before_run.clone(),
                after_run: raw.hooks.after_run.clone(),
                before_remove: raw.hooks.before_remove.clone(),
                timeout_ms: raw.hooks.timeout_ms,
            },
            max_concurrent_agents: raw.agent.max_concurrent_agents,
            max_retry_backoff_ms: raw.agent.max_retry_backoff_ms,
            max_turns: raw.agent.max_turns,
            discovery_turn_required: raw.agent.discovery_turn_required,
            max_autonomous_turns_before_review: raw.agent.max_autonomous_turns_before_review,
            max_runtime_minutes_before_review: raw.agent.max_runtime_minutes_before_review,
            max_changed_files_before_review: raw.agent.max_changed_files_before_review,
            max_diff_lines_before_review: raw.agent.max_diff_lines_before_review,
            max_idle_minutes_before_review: raw.agent.max_idle_minutes_before_review,
            max_tokens_before_review: raw.agent.max_tokens_before_review,
            max_concurrent_agents_by_state: raw.agent.max_concurrent_agents_by_state.clone(),
            codex_command: raw.codex.command.clone(),
            codex_turn_timeout_ms: raw.codex.turn_timeout_ms,
            codex_read_timeout_ms: raw.codex.read_timeout_ms,
            codex_stall_timeout_ms: raw.codex.stall_timeout_ms,
            codex_runtime: CodexRuntimeSettings {
                approval_policy,
                thread_sandbox: raw.codex.thread_sandbox.clone(),
                turn_sandbox_policy,
            },
            observability_enabled: raw.observability.dashboard_enabled,
            observability_refresh_ms: raw.observability.refresh_ms,
            observability_render_interval_ms: raw.observability.render_interval_ms,
            server_port: raw.server.port,
            server_host: raw.server.host.clone(),
            repo_memory_path,
            prompt_template: if workflow.prompt_template.trim().is_empty() {
                default_prompt_template()
            } else {
                workflow.prompt_template
            },
        })
    }

    pub fn validate(&self) -> Result<()> {
        match self.tracker_kind.as_deref() {
            Some("linear") | Some("memory") => {}
            Some("github") => {
                if self.github_owner.as_deref().unwrap_or("").trim().is_empty() {
                    return Err(anyhow!("missing tracker.owner in WORKFLOW.md for github"));
                }
                if self.github_repo.as_deref().unwrap_or("").trim().is_empty() {
                    return Err(anyhow!("missing tracker.repo in WORKFLOW.md for github"));
                }
            }
            Some(other) => return Err(anyhow!("unsupported tracker kind: {other}")),
            None => return Err(anyhow!("missing tracker kind in WORKFLOW.md")),
        }
        if self.codex_command.trim().is_empty() && self.tracker_kind.as_deref() != Some("memory") {
            return Err(anyhow!("codex command missing in WORKFLOW.md"));
        }
        if self.linear_active_states.is_empty() {
            return Err(anyhow!("tracker.active_states must not be empty"));
        }
        Ok(())
    }

    pub fn state_limit(&self, state: &str) -> usize {
        self.max_concurrent_agents_by_state
            .get(state)
            .copied()
            .unwrap_or(self.max_concurrent_agents)
    }

    pub fn demo(workflow_path: PathBuf, server_port: Option<u16>) -> Self {
        let workspace_root = std::env::temp_dir().join("symphony_demo_workspaces");
        Self {
            workflow_path,
            tracker_kind: Some("memory".to_string()),
            linear_endpoint: default_linear_endpoint(),
            linear_api_token: None,
            linear_project_slug: Some("demo".to_string()),
            linear_assignee: None,
            linear_active_states: default_active_states(),
            linear_terminal_states: default_terminal_states(),
            github_endpoint: default_github_endpoint(),
            github_api_token: None,
            github_owner: None,
            github_repo: None,
            github_assignee: None,
            github_labels: Vec::new(),
            poll_interval_ms: default_poll_interval_ms(),
            workspace_root: workspace_root.clone(),
            workspace_hooks: WorkspaceHooks {
                after_create: None,
                before_run: None,
                after_run: None,
                before_remove: None,
                timeout_ms: default_hook_timeout_ms(),
            },
            max_concurrent_agents: default_max_concurrent_agents(),
            max_retry_backoff_ms: default_max_retry_backoff_ms(),
            max_turns: default_max_turns(),
            discovery_turn_required: default_discovery_turn_required(),
            max_autonomous_turns_before_review: default_max_autonomous_turns_before_review(),
            max_runtime_minutes_before_review: default_max_runtime_minutes_before_review(),
            max_changed_files_before_review: default_max_changed_files_before_review(),
            max_diff_lines_before_review: default_max_diff_lines_before_review(),
            max_idle_minutes_before_review: default_max_idle_minutes_before_review(),
            max_tokens_before_review: default_max_tokens_before_review(),
            max_concurrent_agents_by_state: HashMap::new(),
            codex_command: default_codex_command(),
            codex_turn_timeout_ms: default_turn_timeout_ms(),
            codex_read_timeout_ms: default_read_timeout_ms(),
            codex_stall_timeout_ms: default_stall_timeout_ms(),
            codex_runtime: CodexRuntimeSettings {
                approval_policy: json!({
                    "reject": {
                        "sandbox_approval": true,
                        "rules": true,
                        "mcp_elicitations": true
                    }
                }),
                thread_sandbox: default_thread_sandbox(),
                turn_sandbox_policy: json!({
                    "type": "workspaceWrite",
                    "writableRoots": [workspace_root.to_string_lossy()]
                }),
            },
            observability_enabled: true,
            observability_refresh_ms: default_observability_refresh_ms(),
            observability_render_interval_ms: default_observability_render_interval_ms(),
            server_port,
            server_host: default_server_host(),
            repo_memory_path: workspace_root.join(".symphony").join("repo-memory.md"),
            prompt_template: default_prompt_template(),
        }
    }
}

fn parse_raw(config: &YamlValue) -> Result<RawWorkflowConfig> {
    serde_yaml::from_value(config.clone()).context("failed to decode workflow config")
}

fn resolve_secret_value(value: Option<String>, env_keys: &[&str]) -> Option<String> {
    match value.as_deref() {
        None => resolve_envs(env_keys),
        Some(value) if value.trim().is_empty() => resolve_envs(env_keys),
        Some(value) if value.trim().starts_with('$') => {
            let explicit = value.trim().trim_start_matches('$');
            env::var(explicit).ok().or_else(|| resolve_envs(env_keys))
        }
        Some(value) => Some(value.to_string()),
    }
}

fn resolve_envs(env_keys: &[&str]) -> Option<String> {
    env_keys.iter().find_map(|key| env::var(key).ok())
}

fn resolve_path_value(value: &str) -> Result<PathBuf> {
    let expanded = if let Some(stripped) = value.strip_prefix("~/") {
        let home = env::var("HOME").context("HOME is not set")?;
        Path::new(&home).join(stripped)
    } else if let Some(env_key) = value.strip_prefix('$') {
        PathBuf::from(
            env::var(env_key).with_context(|| format!("missing env path variable {env_key}"))?,
        )
    } else {
        PathBuf::from(value)
    };
    Ok(expanded)
}

fn default_prompt_template() -> String {
    r#"You are working on a Linear issue.

Identifier: {{ issue.identifier }}
Title: {{ issue.title }}

Body:
{% if issue.description %}
{{ issue.description }}
{% else %}
No description provided.
{% endif %}"#
        .to_string()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::model::WorkflowDefinition;

    use super::ServiceConfig;

    #[test]
    fn config_uses_defaults() {
        let workflow = WorkflowDefinition {
            config: serde_yaml::from_str(
                r#"
tracker:
  kind: linear
  project_slug: demo
"#,
            )
            .unwrap(),
            prompt_template: String::new(),
        };
        let config = ServiceConfig::from_workflow(PathBuf::from("WORKFLOW.md"), workflow).unwrap();
        assert_eq!(config.max_turns, 20);
        assert_eq!(config.codex_command, "codex app-server");
    }
}
