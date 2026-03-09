use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IssueBlocker {
    pub id: Option<String>,
    pub identifier: Option<String>,
    pub state: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Issue {
    pub id: String,
    pub identifier: String,
    pub title: String,
    pub description: Option<String>,
    pub priority: Option<i64>,
    pub state: String,
    pub branch_name: Option<String>,
    pub url: Option<String>,
    pub labels: Vec<String>,
    pub blocked_by: Vec<IssueBlocker>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub assignee_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkflowDefinition {
    pub config: serde_yaml::Value,
    pub prompt_template: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CodexRuntimeSettings {
    pub approval_policy: Value,
    pub thread_sandbox: String,
    pub turn_sandbox_policy: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkspaceHooks {
    pub after_create: Option<String>,
    pub before_run: Option<String>,
    pub after_run: Option<String>,
    pub before_remove: Option<String>,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceState {
    pub path: String,
    pub workspace_key: String,
    pub created_now: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    Starting,
    Discovering,
    Editing,
    Testing,
    Waiting,
    Retrying,
    Paused,
    Held,
    NeedsReview,
    Stalled,
    Completed,
}

impl Default for RunPhase {
    fn default() -> Self {
        Self::Starting
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DiffStats {
    pub changed_files: u32,
    pub added_lines: u64,
    pub removed_lines: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RunTelemetry {
    pub phase: RunPhase,
    pub phase_detail: Option<String>,
    pub review_reason: Option<String>,
    pub last_command: Option<String>,
    pub last_file_touched: Option<String>,
    pub diff: DiffStats,
    pub stdout_log_path: Option<String>,
    pub stderr_log_path: Option<String>,
    pub progress_report_path: Option<String>,
    pub discovery_report_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSession {
    pub session_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub codex_app_server_pid: Option<String>,
    pub last_codex_event: Option<String>,
    pub last_codex_timestamp: Option<DateTime<Utc>>,
    pub last_codex_message: Option<String>,
    pub codex_input_tokens: u64,
    pub codex_output_tokens: u64,
    pub codex_total_tokens: u64,
    pub turn_count: u64,
}

impl Default for LiveSession {
    fn default() -> Self {
        Self {
            session_id: None,
            thread_id: None,
            turn_id: None,
            codex_app_server_pid: None,
            last_codex_event: None,
            last_codex_timestamp: None,
            last_codex_message: None,
            codex_input_tokens: 0,
            codex_output_tokens: 0,
            codex_total_tokens: 0,
            turn_count: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryEntry {
    pub issue_id: String,
    pub identifier: String,
    pub state: String,
    pub issue: Issue,
    pub attempt: u32,
    pub workspace_path: String,
    pub due_at: DateTime<Utc>,
    pub error: Option<String>,
    pub telemetry: RunTelemetry,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockedEntry {
    pub issue_id: String,
    pub identifier: String,
    pub state: String,
    pub issue: Issue,
    pub attempt: Option<u32>,
    pub workspace_path: String,
    pub blocked_at: DateTime<Utc>,
    pub session: LiveSession,
    pub telemetry: RunTelemetry,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CodexTotals {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub seconds_running: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PollingState {
    pub checking: bool,
    pub next_poll_in_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningEntry {
    pub issue_id: String,
    pub identifier: String,
    pub state: String,
    pub issue: Issue,
    pub attempt: Option<u32>,
    pub workspace_path: String,
    pub started_at: DateTime<Utc>,
    pub session: LiveSession,
    pub telemetry: RunTelemetry,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Snapshot {
    pub running: Vec<RunningEntry>,
    pub retrying: Vec<RetryEntry>,
    pub paused: Vec<BlockedEntry>,
    pub held: Vec<BlockedEntry>,
    pub needs_review: Vec<BlockedEntry>,
    pub codex_totals: CodexTotals,
    pub rate_limits: Option<Value>,
    pub polling: PollingState,
}

#[derive(Debug, Clone)]
pub struct RefreshResult {
    pub queued: bool,
    pub coalesced: bool,
    pub requested_at: DateTime<Utc>,
    pub operations: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct WorkerEvent {
    pub issue_id: String,
    pub update: WorkerUpdate,
}

#[derive(Debug, Clone)]
pub enum WorkerUpdate {
    Started {
        issue: Issue,
        attempt: Option<u32>,
        workspace_path: String,
        started_at: DateTime<Utc>,
        telemetry: RunTelemetry,
    },
    CodexMessage {
        event: String,
        timestamp: DateTime<Utc>,
        session_id: Option<String>,
        thread_id: Option<String>,
        turn_id: Option<String>,
        codex_app_server_pid: Option<String>,
        message: Option<String>,
        usage: Option<TokenUsage>,
        rate_limits: Option<Value>,
        phase: Option<RunPhase>,
        phase_detail: Option<String>,
        last_command: Option<String>,
        last_file_touched: Option<String>,
    },
    Telemetry {
        phase: Option<RunPhase>,
        phase_detail: Option<String>,
        review_reason: Option<String>,
        last_command: Option<String>,
        last_file_touched: Option<String>,
        diff: Option<DiffStats>,
    },
    NeedsReview {
        issue: Issue,
        attempt: Option<u32>,
        reason: String,
    },
    Finished {
        issue_id: String,
        success: bool,
        continuation: bool,
        issue: Issue,
        attempt: Option<u32>,
        error: Option<String>,
    },
}

#[derive(Debug, Clone, Default)]
pub struct WorkflowStamp {
    pub modified_epoch_secs: i64,
    pub size_bytes: u64,
    pub content_sha256: String,
}

#[derive(Debug, Clone)]
pub struct DueRetry {
    pub issue_id: String,
    pub attempt: u32,
    pub due_at: DateTime<Utc>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ControlActionStatus {
    Accepted,
    NotFound,
    Conflict,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlActionResult {
    pub status: ControlActionStatus,
    pub issue_identifier: String,
    pub message: String,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeState {
    pub running: HashMap<String, RunningEntry>,
    pub claimed: HashMap<String, String>,
    pub retrying: BTreeMap<String, RetryEntry>,
    pub paused: BTreeMap<String, BlockedEntry>,
    pub held: BTreeMap<String, BlockedEntry>,
    pub needs_review: BTreeMap<String, BlockedEntry>,
    pub completed_totals: CodexTotals,
    pub rate_limits: Option<Value>,
    pub polling: PollingState,
}

pub fn duration_to_seconds(duration: Duration) -> f64 {
    duration.as_secs_f64()
}
