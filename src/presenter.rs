use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::config::ServiceConfig;
use crate::model::{RefreshResult, RunningEntry, Snapshot};

#[derive(Debug, Clone, Serialize)]
pub struct StatePayload {
    pub generated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub counts: Option<CountsPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub running: Option<Vec<RunningEntryPayload>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retrying: Option<Vec<RetryEntryPayload>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codex_totals: Option<crate::model::CodexTotals>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limits: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorPayload>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CountsPayload {
    pub running: usize,
    pub retrying: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunningEntryPayload {
    pub issue_id: String,
    pub issue_identifier: String,
    pub state: String,
    pub session_id: Option<String>,
    pub turn_count: u64,
    pub last_event: Option<String>,
    pub last_message: Option<String>,
    pub started_at: Option<String>,
    pub last_event_at: Option<String>,
    pub tokens: TokensPayload,
}

#[derive(Debug, Clone, Serialize)]
pub struct TokensPayload {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetryEntryPayload {
    pub issue_id: String,
    pub issue_identifier: String,
    pub attempt: u32,
    pub due_at: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorPayload {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct IssuePayload {
    pub issue_identifier: String,
    pub issue_id: String,
    pub status: String,
    pub workspace: WorkspacePayload,
    pub attempts: AttemptsPayload,
    pub running: Option<RunningIssuePayload>,
    pub retry: Option<RetryIssuePayload>,
    pub logs: LogsPayload,
    pub recent_events: Vec<RecentEventPayload>,
    pub last_error: Option<String>,
    pub tracked: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkspacePayload {
    pub path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttemptsPayload {
    pub restart_count: u32,
    pub current_retry_attempt: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunningIssuePayload {
    pub session_id: Option<String>,
    pub turn_count: u64,
    pub state: String,
    pub started_at: Option<String>,
    pub last_event: Option<String>,
    pub last_message: Option<String>,
    pub last_event_at: Option<String>,
    pub tokens: TokensPayload,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetryIssuePayload {
    pub attempt: u32,
    pub due_at: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogsPayload {
    pub codex_session_logs: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecentEventPayload {
    pub at: Option<String>,
    pub event: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RefreshPayload {
    pub queued: bool,
    pub coalesced: bool,
    pub requested_at: String,
    pub operations: Vec<String>,
}

pub fn state_payload(snapshot: Option<&Snapshot>) -> StatePayload {
    let generated_at = iso(Utc::now());
    match snapshot {
        Some(snapshot) => StatePayload {
            generated_at,
            counts: Some(CountsPayload {
                running: snapshot.running.len(),
                retrying: snapshot.retrying.len(),
            }),
            running: Some(snapshot.running.iter().map(running_entry_payload).collect()),
            retrying: Some(
                snapshot
                    .retrying
                    .iter()
                    .map(|entry| RetryEntryPayload {
                        issue_id: entry.issue_id.clone(),
                        issue_identifier: entry.identifier.clone(),
                        attempt: entry.attempt,
                        due_at: Some(iso(entry.due_at)),
                        error: entry.error.clone(),
                    })
                    .collect(),
            ),
            codex_totals: Some(snapshot.codex_totals.clone()),
            rate_limits: snapshot.rate_limits.clone(),
            error: None,
        },
        None => StatePayload {
            generated_at,
            counts: None,
            running: None,
            retrying: None,
            codex_totals: None,
            rate_limits: None,
            error: Some(ErrorPayload {
                code: "snapshot_unavailable".to_string(),
                message: "Snapshot unavailable".to_string(),
            }),
        },
    }
}

pub fn issue_payload(
    issue_identifier: &str,
    config: &ServiceConfig,
    snapshot: &Snapshot,
) -> Option<IssuePayload> {
    let running = snapshot
        .running
        .iter()
        .find(|entry| entry.identifier == issue_identifier);
    let retry = snapshot
        .retrying
        .iter()
        .find(|entry| entry.identifier == issue_identifier);
    if running.is_none() && retry.is_none() {
        return None;
    }
    let issue_id = running
        .map(|entry| entry.issue_id.clone())
        .or_else(|| retry.map(|entry| entry.issue_id.clone()))
        .unwrap_or_default();
    Some(IssuePayload {
        issue_identifier: issue_identifier.to_string(),
        issue_id,
        status: if retry.is_some() && running.is_none() {
            "retrying".to_string()
        } else {
            "running".to_string()
        },
        workspace: WorkspacePayload {
            path: config
                .workspace_root
                .join(issue_identifier)
                .to_string_lossy()
                .into_owned(),
        },
        attempts: AttemptsPayload {
            restart_count: retry.map(|entry| entry.attempt.saturating_sub(1)).unwrap_or(0),
            current_retry_attempt: retry.map(|entry| entry.attempt).unwrap_or(0),
        },
        running: running.map(|entry| RunningIssuePayload {
            session_id: entry.session.session_id.clone(),
            turn_count: entry.session.turn_count,
            state: entry.state.clone(),
            started_at: Some(iso(entry.started_at)),
            last_event: entry.session.last_codex_event.clone(),
            last_message: entry.session.last_codex_message.clone(),
            last_event_at: entry.session.last_codex_timestamp.map(iso),
            tokens: TokensPayload {
                input_tokens: entry.session.codex_input_tokens,
                output_tokens: entry.session.codex_output_tokens,
                total_tokens: entry.session.codex_total_tokens,
            },
        }),
        retry: retry.map(|entry| RetryIssuePayload {
            attempt: entry.attempt,
            due_at: Some(iso(entry.due_at)),
            error: entry.error.clone(),
        }),
        logs: LogsPayload {
            codex_session_logs: Vec::new(),
        },
        recent_events: running
            .and_then(|entry| {
                entry.session.last_codex_timestamp.map(|at| RecentEventPayload {
                    at: Some(iso(at)),
                    event: entry.session.last_codex_event.clone(),
                    message: entry.session.last_codex_message.clone(),
                })
            })
            .into_iter()
            .collect(),
        last_error: retry.and_then(|entry| entry.error.clone()),
        tracked: Default::default(),
    })
}

pub fn refresh_payload(refresh: RefreshResult) -> RefreshPayload {
    RefreshPayload {
        queued: refresh.queued,
        coalesced: refresh.coalesced,
        requested_at: iso(refresh.requested_at),
        operations: refresh.operations,
    }
}

fn running_entry_payload(entry: &RunningEntry) -> RunningEntryPayload {
    RunningEntryPayload {
        issue_id: entry.issue_id.clone(),
        issue_identifier: entry.identifier.clone(),
        state: entry.state.clone(),
        session_id: entry.session.session_id.clone(),
        turn_count: entry.session.turn_count,
        last_event: entry.session.last_codex_event.clone(),
        last_message: entry.session.last_codex_message.clone(),
        started_at: Some(iso(entry.started_at)),
        last_event_at: entry.session.last_codex_timestamp.map(iso),
        tokens: TokensPayload {
            input_tokens: entry.session.codex_input_tokens,
            output_tokens: entry.session.codex_output_tokens,
            total_tokens: entry.session.codex_total_tokens,
        },
    }
}

pub fn iso<T: Into<DateTime<Utc>>>(value: T) -> String {
    value.into().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
