use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::config::ServiceConfig;
use crate::model::{
    ControlActionResult, ControlActionStatus, RefreshResult, RunningEntry, Snapshot,
};

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
    pub paused: Option<Vec<BlockedEntryPayload>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub held: Option<Vec<BlockedEntryPayload>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub needs_review: Option<Vec<BlockedEntryPayload>>,
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
    pub paused: usize,
    pub held: usize,
    pub needs_review: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunningEntryPayload {
    pub issue_id: String,
    pub issue_identifier: String,
    pub issue_title: String,
    pub state: String,
    pub workspace_path: String,
    pub session_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub codex_app_server_pid: Option<String>,
    pub turn_count: u64,
    pub phase: String,
    pub phase_detail: Option<String>,
    pub review_reason: Option<String>,
    pub last_event: Option<String>,
    pub last_message: Option<String>,
    pub last_command: Option<String>,
    pub last_file_touched: Option<String>,
    pub started_at: Option<String>,
    pub last_event_at: Option<String>,
    pub stdout_log_path: Option<String>,
    pub stderr_log_path: Option<String>,
    pub progress_report_path: Option<String>,
    pub discovery_report_path: Option<String>,
    pub diff: DiffPayload,
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
    pub issue_title: String,
    pub state: String,
    pub workspace_path: String,
    pub attempt: u32,
    pub phase: String,
    pub phase_detail: Option<String>,
    pub due_at: Option<String>,
    pub error: Option<String>,
    pub stdout_log_path: Option<String>,
    pub stderr_log_path: Option<String>,
    pub diff: DiffPayload,
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockedEntryPayload {
    pub issue_id: String,
    pub issue_identifier: String,
    pub issue_title: String,
    pub state: String,
    pub workspace_path: String,
    pub blocked_at: Option<String>,
    pub attempt: Option<u32>,
    pub phase: String,
    pub phase_detail: Option<String>,
    pub review_reason: Option<String>,
    pub last_event: Option<String>,
    pub last_message: Option<String>,
    pub last_command: Option<String>,
    pub last_file_touched: Option<String>,
    pub stdout_log_path: Option<String>,
    pub stderr_log_path: Option<String>,
    pub progress_report_path: Option<String>,
    pub discovery_report_path: Option<String>,
    pub diff: DiffPayload,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiffPayload {
    pub changed_files: u32,
    pub added_lines: u64,
    pub removed_lines: u64,
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
    pub paused: Option<BlockedIssuePayload>,
    pub held: Option<BlockedIssuePayload>,
    pub needs_review: Option<BlockedIssuePayload>,
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
    pub phase: String,
    pub phase_detail: Option<String>,
    pub started_at: Option<String>,
    pub last_event: Option<String>,
    pub last_message: Option<String>,
    pub last_event_at: Option<String>,
    pub last_command: Option<String>,
    pub last_file_touched: Option<String>,
    pub diff: DiffPayload,
    pub tokens: TokensPayload,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetryIssuePayload {
    pub attempt: u32,
    pub due_at: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockedIssuePayload {
    pub attempt: Option<u32>,
    pub state: String,
    pub phase: String,
    pub phase_detail: Option<String>,
    pub review_reason: Option<String>,
    pub blocked_at: Option<String>,
    pub last_event: Option<String>,
    pub last_message: Option<String>,
    pub last_command: Option<String>,
    pub last_file_touched: Option<String>,
    pub diff: DiffPayload,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogsPayload {
    pub stdout_log_path: Option<String>,
    pub stderr_log_path: Option<String>,
    pub progress_report_path: Option<String>,
    pub discovery_report_path: Option<String>,
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

#[derive(Debug, Clone, Serialize)]
pub struct ControlPayload {
    pub issue_identifier: String,
    pub status: String,
    pub message: String,
}

pub fn state_payload(snapshot: Option<&Snapshot>) -> StatePayload {
    let generated_at = iso(Utc::now());
    match snapshot {
        Some(snapshot) => StatePayload {
            generated_at,
            counts: Some(CountsPayload {
                running: snapshot.running.len(),
                retrying: snapshot.retrying.len(),
                paused: snapshot.paused.len(),
                held: snapshot.held.len(),
                needs_review: snapshot.needs_review.len(),
            }),
            running: Some(snapshot.running.iter().map(running_entry_payload).collect()),
            retrying: Some(
                snapshot
                    .retrying
                    .iter()
                    .map(|entry| RetryEntryPayload {
                        issue_id: entry.issue_id.clone(),
                        issue_identifier: entry.identifier.clone(),
                        issue_title: entry.issue.title.clone(),
                        state: entry.state.clone(),
                        workspace_path: entry.workspace_path.clone(),
                        attempt: entry.attempt,
                        phase: phase_name(&entry.telemetry.phase),
                        phase_detail: entry.telemetry.phase_detail.clone(),
                        due_at: Some(iso(entry.due_at)),
                        error: entry.error.clone(),
                        stdout_log_path: entry.telemetry.stdout_log_path.clone(),
                        stderr_log_path: entry.telemetry.stderr_log_path.clone(),
                        diff: diff_payload(&entry.telemetry.diff),
                    })
                    .collect(),
            ),
            paused: Some(snapshot.paused.iter().map(blocked_entry_payload).collect()),
            held: Some(snapshot.held.iter().map(blocked_entry_payload).collect()),
            needs_review: Some(
                snapshot
                    .needs_review
                    .iter()
                    .map(blocked_entry_payload)
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
            paused: None,
            held: None,
            needs_review: None,
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
    let paused = snapshot
        .paused
        .iter()
        .find(|entry| entry.identifier == issue_identifier);
    let held = snapshot
        .held
        .iter()
        .find(|entry| entry.identifier == issue_identifier);
    let needs_review = snapshot
        .needs_review
        .iter()
        .find(|entry| entry.identifier == issue_identifier);
    if running.is_none()
        && retry.is_none()
        && paused.is_none()
        && held.is_none()
        && needs_review.is_none()
    {
        return None;
    }
    let issue_id = running
        .map(|entry| entry.issue_id.clone())
        .or_else(|| retry.map(|entry| entry.issue_id.clone()))
        .or_else(|| paused.map(|entry| entry.issue_id.clone()))
        .or_else(|| held.map(|entry| entry.issue_id.clone()))
        .or_else(|| needs_review.map(|entry| entry.issue_id.clone()))
        .unwrap_or_default();
    Some(IssuePayload {
        issue_identifier: issue_identifier.to_string(),
        issue_id,
        status: if needs_review.is_some() {
            "needs_review".to_string()
        } else if paused.is_some() {
            "paused".to_string()
        } else if held.is_some() {
            "held".to_string()
        } else if retry.is_some() && running.is_none() {
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
            restart_count: retry
                .map(|entry| entry.attempt.saturating_sub(1))
                .unwrap_or(0),
            current_retry_attempt: retry.map(|entry| entry.attempt).unwrap_or(0),
        },
        running: running.map(|entry| RunningIssuePayload {
            session_id: entry.session.session_id.clone(),
            turn_count: entry.session.turn_count,
            state: entry.state.clone(),
            phase: phase_name(&entry.telemetry.phase),
            phase_detail: entry.telemetry.phase_detail.clone(),
            started_at: Some(iso(entry.started_at)),
            last_event: entry.session.last_codex_event.clone(),
            last_message: entry.session.last_codex_message.clone(),
            last_event_at: entry.session.last_codex_timestamp.map(iso),
            last_command: entry.telemetry.last_command.clone(),
            last_file_touched: entry.telemetry.last_file_touched.clone(),
            diff: diff_payload(&entry.telemetry.diff),
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
        paused: paused.map(|entry| BlockedIssuePayload {
            attempt: entry.attempt,
            state: entry.state.clone(),
            phase: phase_name(&entry.telemetry.phase),
            phase_detail: entry.telemetry.phase_detail.clone(),
            review_reason: entry.telemetry.review_reason.clone(),
            blocked_at: Some(iso(entry.blocked_at)),
            last_event: entry.session.last_codex_event.clone(),
            last_message: entry.session.last_codex_message.clone(),
            last_command: entry.telemetry.last_command.clone(),
            last_file_touched: entry.telemetry.last_file_touched.clone(),
            diff: diff_payload(&entry.telemetry.diff),
        }),
        held: held.map(|entry| BlockedIssuePayload {
            attempt: entry.attempt,
            state: entry.state.clone(),
            phase: phase_name(&entry.telemetry.phase),
            phase_detail: entry.telemetry.phase_detail.clone(),
            review_reason: entry.telemetry.review_reason.clone(),
            blocked_at: Some(iso(entry.blocked_at)),
            last_event: entry.session.last_codex_event.clone(),
            last_message: entry.session.last_codex_message.clone(),
            last_command: entry.telemetry.last_command.clone(),
            last_file_touched: entry.telemetry.last_file_touched.clone(),
            diff: diff_payload(&entry.telemetry.diff),
        }),
        needs_review: needs_review.map(|entry| BlockedIssuePayload {
            attempt: entry.attempt,
            state: entry.state.clone(),
            phase: phase_name(&entry.telemetry.phase),
            phase_detail: entry.telemetry.phase_detail.clone(),
            review_reason: entry.telemetry.review_reason.clone(),
            blocked_at: Some(iso(entry.blocked_at)),
            last_event: entry.session.last_codex_event.clone(),
            last_message: entry.session.last_codex_message.clone(),
            last_command: entry.telemetry.last_command.clone(),
            last_file_touched: entry.telemetry.last_file_touched.clone(),
            diff: diff_payload(&entry.telemetry.diff),
        }),
        logs: LogsPayload {
            stdout_log_path: running
                .map(|entry| entry.telemetry.stdout_log_path.clone())
                .or_else(|| retry.map(|entry| entry.telemetry.stdout_log_path.clone()))
                .or_else(|| paused.map(|entry| entry.telemetry.stdout_log_path.clone()))
                .or_else(|| held.map(|entry| entry.telemetry.stdout_log_path.clone()))
                .or_else(|| needs_review.map(|entry| entry.telemetry.stdout_log_path.clone()))
                .flatten(),
            stderr_log_path: running
                .map(|entry| entry.telemetry.stderr_log_path.clone())
                .or_else(|| retry.map(|entry| entry.telemetry.stderr_log_path.clone()))
                .or_else(|| paused.map(|entry| entry.telemetry.stderr_log_path.clone()))
                .or_else(|| held.map(|entry| entry.telemetry.stderr_log_path.clone()))
                .or_else(|| needs_review.map(|entry| entry.telemetry.stderr_log_path.clone()))
                .flatten(),
            progress_report_path: running
                .map(|entry| entry.telemetry.progress_report_path.clone())
                .or_else(|| retry.map(|entry| entry.telemetry.progress_report_path.clone()))
                .or_else(|| paused.map(|entry| entry.telemetry.progress_report_path.clone()))
                .or_else(|| held.map(|entry| entry.telemetry.progress_report_path.clone()))
                .or_else(|| needs_review.map(|entry| entry.telemetry.progress_report_path.clone()))
                .flatten(),
            discovery_report_path: running
                .map(|entry| entry.telemetry.discovery_report_path.clone())
                .or_else(|| retry.map(|entry| entry.telemetry.discovery_report_path.clone()))
                .or_else(|| paused.map(|entry| entry.telemetry.discovery_report_path.clone()))
                .or_else(|| held.map(|entry| entry.telemetry.discovery_report_path.clone()))
                .or_else(|| needs_review.map(|entry| entry.telemetry.discovery_report_path.clone()))
                .flatten(),
        },
        recent_events: running
            .and_then(|entry| {
                entry
                    .session
                    .last_codex_timestamp
                    .map(|at| RecentEventPayload {
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

pub fn control_payload(result: ControlActionResult) -> ControlPayload {
    let status = match result.status {
        ControlActionStatus::Accepted => "accepted",
        ControlActionStatus::NotFound => "not_found",
        ControlActionStatus::Conflict => "conflict",
    };
    ControlPayload {
        issue_identifier: result.issue_identifier,
        status: status.to_string(),
        message: result.message,
    }
}

fn running_entry_payload(entry: &RunningEntry) -> RunningEntryPayload {
    RunningEntryPayload {
        issue_id: entry.issue_id.clone(),
        issue_identifier: entry.identifier.clone(),
        issue_title: entry.issue.title.clone(),
        state: entry.state.clone(),
        workspace_path: entry.workspace_path.clone(),
        session_id: entry.session.session_id.clone(),
        thread_id: entry.session.thread_id.clone(),
        turn_id: entry.session.turn_id.clone(),
        codex_app_server_pid: entry.session.codex_app_server_pid.clone(),
        turn_count: entry.session.turn_count,
        phase: phase_name(&entry.telemetry.phase),
        phase_detail: entry.telemetry.phase_detail.clone(),
        review_reason: entry.telemetry.review_reason.clone(),
        last_event: entry.session.last_codex_event.clone(),
        last_message: entry.session.last_codex_message.clone(),
        last_command: entry.telemetry.last_command.clone(),
        last_file_touched: entry.telemetry.last_file_touched.clone(),
        started_at: Some(iso(entry.started_at)),
        last_event_at: entry.session.last_codex_timestamp.map(iso),
        stdout_log_path: entry.telemetry.stdout_log_path.clone(),
        stderr_log_path: entry.telemetry.stderr_log_path.clone(),
        progress_report_path: entry.telemetry.progress_report_path.clone(),
        discovery_report_path: entry.telemetry.discovery_report_path.clone(),
        diff: diff_payload(&entry.telemetry.diff),
        tokens: TokensPayload {
            input_tokens: entry.session.codex_input_tokens,
            output_tokens: entry.session.codex_output_tokens,
            total_tokens: entry.session.codex_total_tokens,
        },
    }
}

fn blocked_entry_payload(entry: &crate::model::BlockedEntry) -> BlockedEntryPayload {
    BlockedEntryPayload {
        issue_id: entry.issue_id.clone(),
        issue_identifier: entry.identifier.clone(),
        issue_title: entry.issue.title.clone(),
        state: entry.state.clone(),
        workspace_path: entry.workspace_path.clone(),
        blocked_at: Some(iso(entry.blocked_at)),
        attempt: entry.attempt,
        phase: phase_name(&entry.telemetry.phase),
        phase_detail: entry.telemetry.phase_detail.clone(),
        review_reason: entry.telemetry.review_reason.clone(),
        last_event: entry.session.last_codex_event.clone(),
        last_message: entry.session.last_codex_message.clone(),
        last_command: entry.telemetry.last_command.clone(),
        last_file_touched: entry.telemetry.last_file_touched.clone(),
        stdout_log_path: entry.telemetry.stdout_log_path.clone(),
        stderr_log_path: entry.telemetry.stderr_log_path.clone(),
        progress_report_path: entry.telemetry.progress_report_path.clone(),
        discovery_report_path: entry.telemetry.discovery_report_path.clone(),
        diff: diff_payload(&entry.telemetry.diff),
    }
}

fn phase_name(phase: &crate::model::RunPhase) -> String {
    serde_json::to_value(phase)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

fn diff_payload(diff: &crate::model::DiffStats) -> DiffPayload {
    DiffPayload {
        changed_files: diff.changed_files,
        added_lines: diff.added_lines,
        removed_lines: diff.removed_lines,
    }
}

pub fn iso<T: Into<DateTime<Utc>>>(value: T) -> String {
    value
        .into()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        BlockedEntry, CodexTotals, Issue, LiveSession, RetryEntry, RunPhase, RunTelemetry,
    };

    fn sample_issue(id: &str, identifier: &str, state: &str) -> Issue {
        Issue {
            id: id.to_string(),
            identifier: identifier.to_string(),
            title: format!("Issue {identifier}"),
            description: None,
            priority: Some(1),
            state: state.to_string(),
            branch_name: None,
            url: None,
            labels: Vec::new(),
            blocked_by: Vec::new(),
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            assignee_id: None,
        }
    }

    #[test]
    fn state_payload_exposes_paused_and_held_entries() {
        let issue = sample_issue("1", "repo#1", "open");
        let snapshot = Snapshot {
            running: Vec::new(),
            retrying: vec![RetryEntry {
                issue_id: issue.id.clone(),
                identifier: issue.identifier.clone(),
                state: issue.state.clone(),
                issue: issue.clone(),
                attempt: 2,
                workspace_path: "/tmp/retry".to_string(),
                due_at: Utc::now(),
                error: Some("retry".to_string()),
                telemetry: RunTelemetry {
                    phase: RunPhase::Retrying,
                    ..RunTelemetry::default()
                },
            }],
            paused: vec![BlockedEntry {
                issue_id: issue.id.clone(),
                identifier: issue.identifier.clone(),
                state: issue.state.clone(),
                issue: issue.clone(),
                attempt: Some(1),
                workspace_path: "/tmp/paused".to_string(),
                blocked_at: Utc::now(),
                session: LiveSession::default(),
                telemetry: RunTelemetry {
                    phase: RunPhase::Paused,
                    ..RunTelemetry::default()
                },
            }],
            held: vec![BlockedEntry {
                issue_id: issue.id.clone(),
                identifier: issue.identifier.clone(),
                state: issue.state.clone(),
                issue: issue.clone(),
                attempt: Some(1),
                workspace_path: "/tmp/held".to_string(),
                blocked_at: Utc::now(),
                session: LiveSession::default(),
                telemetry: RunTelemetry {
                    phase: RunPhase::Held,
                    ..RunTelemetry::default()
                },
            }],
            needs_review: vec![BlockedEntry {
                issue_id: issue.id.clone(),
                identifier: issue.identifier.clone(),
                state: issue.state.clone(),
                issue: issue.clone(),
                attempt: Some(1),
                workspace_path: "/tmp/review".to_string(),
                blocked_at: Utc::now(),
                session: LiveSession::default(),
                telemetry: RunTelemetry {
                    phase: RunPhase::NeedsReview,
                    review_reason: Some("budget exceeded".to_string()),
                    ..RunTelemetry::default()
                },
            }],
            codex_totals: CodexTotals::default(),
            rate_limits: None,
            polling: Default::default(),
        };

        let payload = state_payload(Some(&snapshot));

        assert_eq!(payload.counts.as_ref().unwrap().paused, 1);
        assert_eq!(payload.counts.as_ref().unwrap().held, 1);
        assert_eq!(payload.counts.as_ref().unwrap().needs_review, 1);
        assert_eq!(
            payload.paused.as_ref().unwrap()[0].workspace_path,
            "/tmp/paused"
        );
        assert_eq!(
            payload.held.as_ref().unwrap()[0].workspace_path,
            "/tmp/held"
        );
        assert_eq!(
            payload.needs_review.as_ref().unwrap()[0]
                .review_reason
                .as_deref(),
            Some("budget exceeded")
        );
    }
}
