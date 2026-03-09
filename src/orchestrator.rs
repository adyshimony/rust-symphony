use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use liquid::{object, ParserBuilder};
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep};
use tracing::{error, info, warn};

use crate::codex;
use crate::config::ServiceConfig;
use crate::model::{
    duration_to_seconds, BlockedEntry, CodexTotals, ControlActionResult, ControlActionStatus,
    DiffStats, DueRetry, Issue, LiveSession, RefreshResult, RetryEntry, RunPhase, RunTelemetry,
    RunningEntry, RuntimeState, Snapshot, WorkerEvent, WorkerUpdate,
};
use crate::tracker::{issue_routable, TrackerClient};
use crate::workflow::WorkflowStore;
use crate::workspace;

#[derive(Clone)]
pub struct OrchestratorHandle {
    cmd_tx: mpsc::UnboundedSender<Command>,
    snapshot_rx: watch::Receiver<Snapshot>,
}

enum Command {
    Refresh {
        reply: oneshot::Sender<RefreshResult>,
    },
    PauseIssue {
        issue_identifier: String,
        reply: oneshot::Sender<ControlActionResult>,
    },
    ResumeIssue {
        issue_identifier: String,
        reply: oneshot::Sender<ControlActionResult>,
    },
    StopIssue {
        issue_identifier: String,
        reply: oneshot::Sender<ControlActionResult>,
    },
    RetryIssue {
        issue_identifier: String,
        reply: oneshot::Sender<ControlActionResult>,
    },
    RetryDue(DueRetry),
    WorkerEvent(WorkerEvent),
}

struct RuntimeControl {
    handles: HashMap<String, JoinHandle<()>>,
}

#[derive(Debug, Deserialize)]
struct DiscoveryReport {
    summary: String,
    likely_files: Vec<String>,
    #[serde(default)]
    tests: Vec<String>,
    #[serde(default)]
    risks: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ProgressReport {
    summary: String,
    remaining_work: String,
    #[serde(default)]
    next_files: Vec<String>,
    #[serde(default)]
    tests_run: Vec<String>,
    on_scope: bool,
}

pub async fn spawn(
    config: Arc<ServiceConfig>,
    workflow_path: PathBuf,
    tracker: TrackerClient,
) -> OrchestratorHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (snapshot_tx, snapshot_rx) = watch::channel(Snapshot::default());
    let runtime = Arc::new(Mutex::new(RuntimeState::default()));
    let control = Arc::new(Mutex::new(RuntimeControl {
        handles: HashMap::new(),
    }));
    tokio::spawn(run_loop(
        config,
        workflow_path,
        tracker,
        runtime,
        control,
        cmd_rx,
        cmd_tx.clone(),
        snapshot_tx,
    ));
    OrchestratorHandle {
        cmd_tx,
        snapshot_rx,
    }
}

pub fn spawn_demo(_config: Arc<ServiceConfig>) -> OrchestratorHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
    let (snapshot_tx, snapshot_rx) = watch::channel(demo_snapshot(0));
    tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(1));
        let mut step = 0u64;
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    step = step.wrapping_add(1);
                    let _ = snapshot_tx.send(demo_snapshot(step));
                }
                Some(command) = cmd_rx.recv() => {
                    match command {
                        Command::Refresh { reply } => {
                            step = step.wrapping_add(1);
                            let _ = snapshot_tx.send(demo_snapshot(step));
                            let _ = reply.send(RefreshResult {
                                queued: true,
                                coalesced: false,
                                requested_at: Utc::now(),
                                operations: vec!["demo_refresh".to_string()],
                            });
                        }
                        Command::PauseIssue { issue_identifier, reply }
                        | Command::ResumeIssue { issue_identifier, reply }
                        | Command::StopIssue { issue_identifier, reply }
                        | Command::RetryIssue { issue_identifier, reply } => {
                            let _ = reply.send(conflict_result(
                                issue_identifier,
                                "operator controls are unavailable in demo mode",
                            ));
                        }
                        Command::RetryDue(_) | Command::WorkerEvent(_) => {}
                    }
                }
            }
        }
    });
    OrchestratorHandle {
        cmd_tx,
        snapshot_rx,
    }
}

impl OrchestratorHandle {
    pub async fn snapshot(&self) -> Snapshot {
        self.snapshot_rx.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<Snapshot> {
        self.snapshot_rx.clone()
    }

    pub async fn request_refresh(&self) -> RefreshResult {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(Command::Refresh { reply: tx });
        rx.await.unwrap_or(RefreshResult {
            queued: false,
            coalesced: true,
            requested_at: Utc::now(),
            operations: vec!["poll".to_string(), "reconcile".to_string()],
        })
    }

    pub async fn pause_issue(&self, issue_identifier: impl Into<String>) -> ControlActionResult {
        self.request_control(issue_identifier.into(), |issue_identifier, reply| {
            Command::PauseIssue {
                issue_identifier,
                reply,
            }
        })
        .await
    }

    pub async fn resume_issue(&self, issue_identifier: impl Into<String>) -> ControlActionResult {
        self.request_control(issue_identifier.into(), |issue_identifier, reply| {
            Command::ResumeIssue {
                issue_identifier,
                reply,
            }
        })
        .await
    }

    pub async fn stop_issue(&self, issue_identifier: impl Into<String>) -> ControlActionResult {
        self.request_control(issue_identifier.into(), |issue_identifier, reply| {
            Command::StopIssue {
                issue_identifier,
                reply,
            }
        })
        .await
    }

    pub async fn retry_issue(&self, issue_identifier: impl Into<String>) -> ControlActionResult {
        self.request_control(issue_identifier.into(), |issue_identifier, reply| {
            Command::RetryIssue {
                issue_identifier,
                reply,
            }
        })
        .await
    }

    async fn request_control(
        &self,
        issue_identifier: String,
        build: impl FnOnce(String, oneshot::Sender<ControlActionResult>) -> Command,
    ) -> ControlActionResult {
        let fallback_identifier = issue_identifier.clone();
        let (tx, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(build(issue_identifier, tx));
        rx.await
            .unwrap_or_else(|_| conflict_result(fallback_identifier, "orchestrator unavailable"))
    }
}

async fn run_loop(
    config: Arc<ServiceConfig>,
    workflow_path: PathBuf,
    tracker: TrackerClient,
    runtime: Arc<Mutex<RuntimeState>>,
    control: Arc<Mutex<RuntimeControl>>,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    cmd_tx: mpsc::UnboundedSender<Command>,
    snapshot_tx: watch::Sender<Snapshot>,
) {
    let mut ticker = interval(Duration::from_millis(config.poll_interval_ms));
    let mut refresh_pending = false;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                {
                    let mut state = runtime.lock().await;
                    state.polling.checking = true;
                    state.polling.next_poll_in_ms = None;
                }
                publish_snapshot(&runtime, &snapshot_tx).await;
                if let Err(err) = run_poll_cycle(&config, &workflow_path, &tracker, &runtime, &control, &cmd_tx).await {
                    error!("poll cycle failed: {err:#}");
                }
                {
                    let mut state = runtime.lock().await;
                    state.polling.checking = false;
                    state.polling.next_poll_in_ms = Some(config.poll_interval_ms);
                }
                publish_snapshot(&runtime, &snapshot_tx).await;
                refresh_pending = false;
            }
            Some(command) = cmd_rx.recv() => {
                match command {
                    Command::Refresh { reply } => {
                        let coalesced = refresh_pending;
                        let _ = reply.send(RefreshResult {
                            queued: true,
                            coalesced,
                            requested_at: Utc::now(),
                            operations: vec!["poll".to_string(), "reconcile".to_string()],
                        });
                        if let Err(err) = run_poll_cycle(&config, &workflow_path, &tracker, &runtime, &control, &cmd_tx).await {
                            error!("refresh poll cycle failed: {err:#}");
                        }
                        publish_snapshot(&runtime, &snapshot_tx).await;
                        refresh_pending = false;
                    }
                    Command::PauseIssue { issue_identifier, reply } => {
                        let result = pause_issue(&runtime, &control, issue_identifier).await;
                        let _ = reply.send(result);
                        publish_snapshot(&runtime, &snapshot_tx).await;
                    }
                    Command::ResumeIssue { issue_identifier, reply } => {
                        let result = resume_issue(&runtime, issue_identifier).await;
                        let _ = reply.send(result);
                        publish_snapshot(&runtime, &snapshot_tx).await;
                    }
                    Command::StopIssue { issue_identifier, reply } => {
                        let result = stop_issue(&runtime, &control, issue_identifier).await;
                        let _ = reply.send(result);
                        publish_snapshot(&runtime, &snapshot_tx).await;
                    }
                    Command::RetryIssue { issue_identifier, reply } => {
                        let result = retry_issue(
                            &config,
                            &workflow_path,
                            &tracker,
                            &runtime,
                            &control,
                            &cmd_tx,
                            issue_identifier,
                        )
                        .await;
                        let _ = reply.send(result);
                        publish_snapshot(&runtime, &snapshot_tx).await;
                    }
                    Command::RetryDue(retry) => {
                        let issue = tracker.fetch_issue_states_by_ids(&[retry.issue_id.clone()]).await.ok().and_then(|mut issues| issues.pop());
                        if let Some(issue) = issue {
                            let due_at = retry.due_at;
                            let mut state = runtime.lock().await;
                            if let Some(entry) = state.retrying.get(&issue.id) {
                                if entry.attempt == retry.attempt && entry.due_at == due_at {
                                    state.retrying.remove(&issue.id);
                                    drop(state);
                                    let _ = dispatch_issue(&config, &workflow_path, &tracker, &runtime, &control, &cmd_tx, issue, Some(retry.attempt)).await;
                                }
                            }
                        }
                    }
                    Command::WorkerEvent(worker_event) => {
                        handle_worker_event(&config, &runtime, &control, &cmd_tx, worker_event).await;
                        publish_snapshot(&runtime, &snapshot_tx).await;
                    }
                }
            }
        }
    }
}

async fn run_poll_cycle(
    config: &Arc<ServiceConfig>,
    workflow_path: &PathBuf,
    tracker: &TrackerClient,
    runtime: &Arc<Mutex<RuntimeState>>,
    control: &Arc<Mutex<RuntimeControl>>,
    cmd_tx: &mpsc::UnboundedSender<Command>,
) -> Result<()> {
    config.validate()?;
    reconcile_running_issues(config, tracker, runtime, control).await?;
    reconcile_stalled_running_issues(config, runtime, control, cmd_tx).await;
    reconcile_blocked_issues(config, tracker, runtime).await?;

    let issues = tracker.fetch_candidate_issues().await?;
    let mut issues = issues;
    issues.sort_by_key(|issue| {
        (
            priority_rank(issue.priority),
            issue.created_at,
            issue.identifier.clone(),
        )
    });

    for issue in issues {
        if should_dispatch_issue(config, runtime, &issue).await {
            dispatch_issue(
                config,
                workflow_path,
                tracker,
                runtime,
                control,
                cmd_tx,
                issue,
                None,
            )
            .await?;
        }
    }
    Ok(())
}

async fn dispatch_issue(
    config: &Arc<ServiceConfig>,
    workflow_path: &PathBuf,
    tracker: &TrackerClient,
    runtime: &Arc<Mutex<RuntimeState>>,
    control: &Arc<Mutex<RuntimeControl>>,
    cmd_tx: &mpsc::UnboundedSender<Command>,
    issue: Issue,
    attempt: Option<u32>,
) -> Result<()> {
    {
        let mut state = runtime.lock().await;
        state
            .claimed
            .insert(issue.id.clone(), issue.identifier.clone());
    }
    publish_debug_state(runtime).await;
    let config = Arc::clone(config);
    let tracker = tracker.clone();
    let runtime_cmd_tx = cmd_tx.clone();
    let workflow_path = workflow_path.clone();
    let issue_id = issue.id.clone();
    let task_issue_id = issue_id.clone();
    let task = tokio::spawn(async move {
        if let Err(err) = run_issue_worker(
            config,
            workflow_path,
            tracker,
            runtime_cmd_tx.clone(),
            issue,
            attempt,
        )
        .await
        {
            let _ = runtime_cmd_tx.send(Command::WorkerEvent(WorkerEvent {
                issue_id: task_issue_id.clone(),
                update: WorkerUpdate::Finished {
                    issue_id: task_issue_id.clone(),
                    success: false,
                    continuation: false,
                    issue: Issue {
                        id: task_issue_id.clone(),
                        ..Default::default()
                    },
                    attempt,
                    error: Some(err.to_string()),
                },
            }));
        }
    });
    control.lock().await.handles.insert(issue_id, task);
    Ok(())
}

async fn run_issue_worker(
    config: Arc<ServiceConfig>,
    workflow_path: PathBuf,
    tracker: TrackerClient,
    cmd_tx: mpsc::UnboundedSender<Command>,
    issue: Issue,
    attempt: Option<u32>,
) -> Result<()> {
    let workspace = workspace::create_for_issue(&config, &issue).await?;
    workspace::run_before_run_hook(&config, &workspace).await?;
    let report_paths = workspace_report_paths(&workspace.path);
    clear_report_file(&report_paths.discovery_report_path).await;
    clear_report_file(&report_paths.progress_report_path).await;
    clear_report_file(&report_paths.repo_memory_delta_path).await;
    let shared_repo_memory = load_repo_memory(&config).await;
    let started_at = Utc::now();
    let _ = cmd_tx.send(Command::WorkerEvent(WorkerEvent {
        issue_id: issue.id.clone(),
        update: WorkerUpdate::Started {
            issue: issue.clone(),
            attempt,
            workspace_path: workspace.path.clone(),
            started_at,
            telemetry: RunTelemetry {
                phase: if config.discovery_turn_required {
                    RunPhase::Discovering
                } else {
                    RunPhase::Editing
                },
                phase_detail: Some(if config.discovery_turn_required {
                    "awaiting discovery turn with shared repo memory context".to_string()
                } else {
                    "awaiting implementation turn with shared repo memory context".to_string()
                }),
                stdout_log_path: Some(report_paths.stdout_log_path.clone()),
                stderr_log_path: Some(report_paths.stderr_log_path.clone()),
                progress_report_path: Some(report_paths.progress_report_path.clone()),
                discovery_report_path: Some(report_paths.discovery_report_path.clone()),
                ..RunTelemetry::default()
            },
        },
    }));

    let mut workflow_store = WorkflowStore::new(workflow_path);
    let _ = workflow_store.current()?;
    let parser = ParserBuilder::with_stdlib().build()?;
    let mut session =
        codex::start_session(Arc::clone(&config), std::path::Path::new(&workspace.path)).await?;
    let mut current_issue = issue.clone();
    let mut discovery_context = None::<DiscoveryReport>;

    for turn_number in 1..=config.max_turns {
        let discovery_turn = config.discovery_turn_required && turn_number == 1;
        let prompt = if discovery_turn {
            clear_report_file(&report_paths.discovery_report_path).await;
            clear_report_file(&report_paths.repo_memory_delta_path).await;
            render_discovery_prompt(
                &parser,
                &config.prompt_template,
                &current_issue,
                attempt,
                &report_paths.discovery_report_path,
                &report_paths.repo_memory_delta_path,
                &shared_repo_memory,
            )?
        } else {
            clear_report_file(&report_paths.progress_report_path).await;
            clear_report_file(&report_paths.repo_memory_delta_path).await;
            render_execution_prompt(
                &parser,
                &config.prompt_template,
                &current_issue,
                attempt,
                turn_number,
                config.max_turns,
                discovery_context.as_ref(),
                &report_paths.progress_report_path,
                &report_paths.repo_memory_delta_path,
                &shared_repo_memory,
            )?
        };
        let _ = cmd_tx.send(Command::WorkerEvent(WorkerEvent {
            issue_id: current_issue.id.clone(),
            update: WorkerUpdate::Telemetry {
                phase: Some(if discovery_turn {
                    RunPhase::Discovering
                } else {
                    RunPhase::Editing
                }),
                phase_detail: Some(if discovery_turn {
                    format!("turn {turn_number}: repo discovery and scope estimation")
                } else {
                    format!("turn {turn_number}: implementation in progress")
                }),
                review_reason: None,
                last_command: None,
                last_file_touched: None,
                diff: None,
            },
        }));
        session
            .run_turn(
                &current_issue,
                prompt,
                &worker_tx(&cmd_tx),
                tracker.linear_client(),
            )
            .await?;

        let workspace_snapshot = collect_workspace_snapshot(&workspace.path).await;
        append_repo_memory(&config, &current_issue, &report_paths.repo_memory_delta_path).await?;
        let report_summary = if discovery_turn {
            let report = read_discovery_report(&report_paths.discovery_report_path).await?;
            let summary = format!(
                "{} | likely files: {} | tests: {} | risks: {}",
                report.summary,
                truncate_join(&report.likely_files, 4),
                truncate_join(&report.tests, 3),
                truncate_join(&report.risks, 3)
            );
            discovery_context = Some(report);
            summary
        } else {
            let report = read_progress_report(&report_paths.progress_report_path).await?;
            let summary = format!(
                "{} | remaining: {} | next: {} | tests: {}",
                report.summary,
                report.remaining_work,
                truncate_join(&report.next_files, 4),
                truncate_join(&report.tests_run, 3)
            );
            if !report.on_scope {
                session.shutdown().await;
                workspace::run_after_run_hook(&config, &workspace).await;
                let _ = cmd_tx.send(Command::WorkerEvent(WorkerEvent {
                    issue_id: current_issue.id.clone(),
                    update: WorkerUpdate::NeedsReview {
                        issue: current_issue.clone(),
                        attempt,
                        reason: format!(
                            "progress report marked the run off-scope: {}",
                            report.remaining_work
                        ),
                    },
                }));
                return Ok(());
            }
            summary
        };
        let _ = cmd_tx.send(Command::WorkerEvent(WorkerEvent {
            issue_id: current_issue.id.clone(),
            update: WorkerUpdate::Telemetry {
                phase: Some(RunPhase::Waiting),
                phase_detail: Some(report_summary),
                review_reason: None,
                last_command: None,
                last_file_touched: workspace_snapshot.last_file_touched.clone(),
                diff: Some(workspace_snapshot.diff.clone()),
            },
        }));

        let refreshed = tracker
            .fetch_issue_states_by_ids(&[current_issue.id.clone()])
            .await?
            .into_iter()
            .next();
        match refreshed {
            Some(issue)
                if active_issue_state(&config, &issue.state) && turn_number < config.max_turns =>
            {
                if let Some(reason) =
                    review_trigger_reason(&config, turn_number, started_at, &workspace_snapshot)
                {
                    session.shutdown().await;
                    workspace::run_after_run_hook(&config, &workspace).await;
                    let _ = cmd_tx.send(Command::WorkerEvent(WorkerEvent {
                        issue_id: issue.id.clone(),
                        update: WorkerUpdate::NeedsReview {
                            issue: issue.clone(),
                            attempt,
                            reason,
                        },
                    }));
                    return Ok(());
                }
                current_issue = issue;
            }
            Some(issue) if active_issue_state(&config, &issue.state) => {
                session.shutdown().await;
                workspace::run_after_run_hook(&config, &workspace).await;
                let _ = cmd_tx.send(Command::WorkerEvent(WorkerEvent {
                    issue_id: issue.id.clone(),
                    update: WorkerUpdate::NeedsReview {
                        issue,
                        attempt,
                        reason: format!(
                            "the issue is still active after {} turns; operator review required",
                            config.max_turns
                        ),
                    },
                }));
                return Ok(());
            }
            Some(issue) => {
                session.shutdown().await;
                workspace::run_after_run_hook(&config, &workspace).await;
                let _ = cmd_tx.send(Command::WorkerEvent(WorkerEvent {
                    issue_id: issue.id.clone(),
                    update: WorkerUpdate::Finished {
                        issue_id: issue.id.clone(),
                        success: true,
                        continuation: true,
                        issue,
                        attempt,
                        error: None,
                    },
                }));
                return Ok(());
            }
            None => break,
        }
    }

    session.shutdown().await;
    workspace::run_after_run_hook(&config, &workspace).await;
    let _ = cmd_tx.send(Command::WorkerEvent(WorkerEvent {
        issue_id: current_issue.id.clone(),
        update: WorkerUpdate::NeedsReview {
            issue: current_issue,
            attempt,
            reason: "worker loop exhausted its configured turn budget".to_string(),
        },
    }));
    Ok(())
}

#[derive(Debug, Clone)]
struct WorkspaceReportPaths {
    stdout_log_path: String,
    stderr_log_path: String,
    discovery_report_path: String,
    progress_report_path: String,
    repo_memory_delta_path: String,
}

#[derive(Debug, Clone)]
struct WorkspaceSnapshot {
    diff: DiffStats,
    last_file_touched: Option<String>,
}

fn workspace_report_paths(workspace_path: &str) -> WorkspaceReportPaths {
    let root = PathBuf::from(workspace_path).join(".symphony");
    WorkspaceReportPaths {
        stdout_log_path: root
            .join("codex-stdout.jsonl")
            .to_string_lossy()
            .into_owned(),
        stderr_log_path: root.join("codex-stderr.log").to_string_lossy().into_owned(),
        discovery_report_path: root.join("discovery.json").to_string_lossy().into_owned(),
        progress_report_path: root.join("progress.json").to_string_lossy().into_owned(),
        repo_memory_delta_path: root
            .join("repo-memory-delta.md")
            .to_string_lossy()
            .into_owned(),
    }
}

async fn clear_report_file(path: &str) {
    if let Some(parent) = PathBuf::from(path).parent().map(PathBuf::from) {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    let _ = tokio::fs::remove_file(path).await;
}

async fn load_repo_memory(config: &ServiceConfig) -> String {
    match tokio::fs::read_to_string(&config.repo_memory_path).await {
        Ok(contents) => tail_text(&contents, 6_000),
        Err(_) => "No shared repository memory has been recorded yet.".to_string(),
    }
}

async fn append_repo_memory(
    config: &ServiceConfig,
    issue: &Issue,
    delta_path: &str,
) -> Result<()> {
    let delta = match tokio::fs::read_to_string(delta_path).await {
        Ok(contents) => contents.trim().to_string(),
        Err(_) => String::new(),
    };
    if delta.is_empty() {
        return Ok(());
    }

    if let Some(parent) = config.repo_memory_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.repo_memory_path)
        .await
        .with_context(|| format!("failed to open {}", config.repo_memory_path.display()))?;
    let entry = format!(
        "\n## {} - {}\n{}\n",
        issue.identifier,
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
        delta
    );
    tokio::io::AsyncWriteExt::write_all(&mut file, entry.as_bytes()).await?;
    let _ = tokio::fs::remove_file(delta_path).await;
    Ok(())
}

fn render_discovery_prompt(
    parser: &liquid::Parser,
    prompt_template: &str,
    issue: &Issue,
    attempt: Option<u32>,
    report_path: &str,
    repo_memory_delta_path: &str,
    shared_repo_memory: &str,
) -> Result<String> {
    let base = render_prompt(parser, prompt_template, issue, attempt)?;
    Ok(format!(
        "Symphony discovery turn.\n\nInspect the repository and propose the implementation scope before making changes.\nDo not make code changes in this turn.\nWrite a JSON report to `{report_path}` with this exact shape:\n{{\"summary\":\"...\",\"likely_files\":[\"...\"],\"tests\":[\"...\"],\"risks\":[\"...\"]}}\nAlso overwrite `{repo_memory_delta_path}` with short Markdown bullets describing reusable repository knowledge learned during this turn.\n\nShared repository memory:\n{shared_repo_memory}\n\nOriginal task:\n\n{base}"
    ))
}

fn render_execution_prompt(
    parser: &liquid::Parser,
    prompt_template: &str,
    issue: &Issue,
    attempt: Option<u32>,
    turn_number: u32,
    max_turns: u32,
    discovery: Option<&DiscoveryReport>,
    progress_report_path: &str,
    repo_memory_delta_path: &str,
    shared_repo_memory: &str,
) -> Result<String> {
    let base = render_prompt(parser, prompt_template, issue, attempt)?;
    let discovery_block = discovery
        .map(|report| {
            format!(
                "Discovery summary: {}\nLikely files: {}\nSuggested tests: {}\nRisks: {}",
                report.summary,
                truncate_join(&report.likely_files, 6),
                truncate_join(&report.tests, 4),
                truncate_join(&report.risks, 4)
            )
        })
        .unwrap_or_else(|| "No discovery summary available.".to_string());
    Ok(format!(
        "{base}\n\nShared repository memory:\n{shared_repo_memory}\n\n{discovery_block}\n\nThis is execution turn {turn_number} of {max_turns}.\nAt the end of the turn, update `{progress_report_path}` with JSON in this exact shape:\n{{\"summary\":\"...\",\"remaining_work\":\"...\",\"next_files\":[\"...\"],\"tests_run\":[\"...\"],\"on_scope\":true}}\nAlso overwrite `{repo_memory_delta_path}` with short Markdown bullets describing reusable repository knowledge learned or confirmed during this turn.\nIf scope expanded materially, set `on_scope` to false and explain why in `remaining_work`."
    ))
}

async fn read_discovery_report(path: &str) -> Result<DiscoveryReport> {
    let contents = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("missing discovery report at {path}"))?;
    let report: DiscoveryReport = serde_json::from_str(&contents)
        .with_context(|| format!("invalid discovery report at {path}"))?;
    if report.summary.trim().is_empty() || report.likely_files.is_empty() {
        return Err(anyhow!(
            "discovery report must include a summary and likely_files"
        ));
    }
    Ok(report)
}

async fn read_progress_report(path: &str) -> Result<ProgressReport> {
    let contents = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("missing progress report at {path}"))?;
    let report: ProgressReport = serde_json::from_str(&contents)
        .with_context(|| format!("invalid progress report at {path}"))?;
    if report.summary.trim().is_empty() || report.remaining_work.trim().is_empty() {
        return Err(anyhow!(
            "progress report must include summary and remaining_work"
        ));
    }
    Ok(report)
}

async fn collect_workspace_snapshot(workspace_path: &str) -> WorkspaceSnapshot {
    let status_output = tokio::process::Command::new("git")
        .arg("status")
        .arg("--porcelain")
        .current_dir(workspace_path)
        .output()
        .await
        .ok();
    let status_text = status_output
        .and_then(|output| output.status.success().then_some(output))
        .map(|output| String::from_utf8_lossy(&output.stdout).to_string())
        .unwrap_or_default();
    let status_lines = status_text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    let last_file_touched = status_lines.last().map(|line| parse_status_path(line));

    let diff_output = tokio::process::Command::new("git")
        .arg("diff")
        .arg("--numstat")
        .current_dir(workspace_path)
        .output()
        .await
        .ok();
    let diff_text = diff_output
        .and_then(|output| output.status.success().then_some(output))
        .map(|output| String::from_utf8_lossy(&output.stdout).to_string())
        .unwrap_or_default();
    let mut diff = DiffStats {
        changed_files: status_lines.len() as u32,
        ..DiffStats::default()
    };
    for line in diff_text.lines() {
        let mut parts = line.split_whitespace();
        let added = parts
            .next()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let removed = parts
            .next()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        diff.added_lines += added;
        diff.removed_lines += removed;
    }

    WorkspaceSnapshot {
        diff,
        last_file_touched,
    }
}

fn parse_status_path(line: &str) -> String {
    let trimmed = line.get(3..).unwrap_or(line).trim();
    trimmed.rsplit(" -> ").next().unwrap_or(trimmed).to_string()
}

fn review_trigger_reason(
    config: &ServiceConfig,
    turn_number: u32,
    started_at: chrono::DateTime<Utc>,
    snapshot: &WorkspaceSnapshot,
) -> Option<String> {
    let runtime_minutes = (Utc::now() - started_at).num_minutes().max(0) as u64;
    let diff_lines = snapshot.diff.added_lines + snapshot.diff.removed_lines;
    if turn_number >= config.max_autonomous_turns_before_review {
        Some(format!(
            "run reached the autonomous turn budget ({}/{})",
            turn_number, config.max_autonomous_turns_before_review
        ))
    } else if runtime_minutes >= config.max_runtime_minutes_before_review {
        Some(format!(
            "run exceeded the runtime budget ({}m >= {}m)",
            runtime_minutes, config.max_runtime_minutes_before_review
        ))
    } else if snapshot.diff.changed_files as usize >= config.max_changed_files_before_review {
        Some(format!(
            "workspace changed-file budget exceeded ({} >= {})",
            snapshot.diff.changed_files, config.max_changed_files_before_review
        ))
    } else if diff_lines as usize >= config.max_diff_lines_before_review {
        Some(format!(
            "workspace diff budget exceeded ({} lines >= {})",
            diff_lines, config.max_diff_lines_before_review
        ))
    } else {
        None
    }
}

fn truncate_join(values: &[String], limit: usize) -> String {
    if values.is_empty() {
        "n/a".to_string()
    } else {
        values
            .iter()
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn tail_text(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        value.to_string()
    } else {
        let suffix = value
            .chars()
            .rev()
            .take(max_chars)
            .collect::<String>()
            .chars()
            .rev()
            .collect::<String>();
        format!("...\n{suffix}")
    }
}

fn workspace_path_for_identifier(config: &ServiceConfig, identifier: &str) -> String {
    config
        .workspace_root
        .join(workspace::safe_identifier(identifier))
        .to_string_lossy()
        .into_owned()
}

fn default_run_telemetry_for_workspace(workspace_path: &str, phase: RunPhase) -> RunTelemetry {
    let paths = workspace_report_paths(workspace_path);
    RunTelemetry {
        phase,
        stdout_log_path: Some(paths.stdout_log_path),
        stderr_log_path: Some(paths.stderr_log_path),
        progress_report_path: Some(paths.progress_report_path),
        discovery_report_path: Some(paths.discovery_report_path),
        ..RunTelemetry::default()
    }
}

fn blocked_entry_from_running(
    entry: RunningEntry,
    blocked_at: chrono::DateTime<Utc>,
) -> BlockedEntry {
    BlockedEntry {
        issue_id: entry.issue_id,
        identifier: entry.identifier,
        state: entry.state,
        issue: entry.issue,
        attempt: entry.attempt,
        workspace_path: entry.workspace_path,
        blocked_at,
        session: entry.session,
        telemetry: entry.telemetry,
    }
}

fn blocked_entry_from_retry(entry: RetryEntry, blocked_at: chrono::DateTime<Utc>) -> BlockedEntry {
    BlockedEntry {
        issue_id: entry.issue_id,
        identifier: entry.identifier,
        state: entry.state,
        issue: entry.issue,
        attempt: Some(entry.attempt),
        workspace_path: entry.workspace_path,
        blocked_at,
        session: LiveSession::default(),
        telemetry: entry.telemetry,
    }
}

fn finalize_running_entry(state: &mut RuntimeState, entry: RunningEntry) {
    let elapsed = Utc::now() - entry.started_at;
    state.completed_totals.input_tokens += entry.session.codex_input_tokens;
    state.completed_totals.output_tokens += entry.session.codex_output_tokens;
    state.completed_totals.total_tokens += entry.session.codex_total_tokens;
    state.completed_totals.seconds_running +=
        duration_to_seconds(elapsed.to_std().unwrap_or_default());
}

fn block_running_entry_for_review(
    state: &mut RuntimeState,
    issue_id: &str,
    reason: String,
    blocked_at: chrono::DateTime<Utc>,
) {
    if let Some(entry) = state.running.remove(issue_id) {
        state.claimed.remove(issue_id);
        finalize_running_entry(state, entry.clone());
        let mut blocked = blocked_entry_from_running(entry, blocked_at);
        blocked.telemetry.phase = RunPhase::NeedsReview;
        blocked.telemetry.review_reason = Some(reason);
        blocked.telemetry.phase_detail = Some("operator review required".to_string());
        state.needs_review.insert(issue_id.to_string(), blocked);
    }
}

fn worker_tx(
    cmd_tx: &mpsc::UnboundedSender<Command>,
) -> tokio::sync::mpsc::UnboundedSender<WorkerEvent> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let cmd_tx = cmd_tx.clone();
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let _ = cmd_tx.send(Command::WorkerEvent(event));
        }
    });
    tx
}

fn render_prompt(
    parser: &liquid::Parser,
    prompt_template: &str,
    issue: &Issue,
    attempt: Option<u32>,
) -> Result<String> {
    let template = parser.parse(prompt_template)?;
    let output = template.render(&object!({
        "attempt": attempt.map(|value| value as i64),
        "issue": {
            "id": issue.id.clone(),
            "identifier": issue.identifier.clone(),
            "title": issue.title.clone(),
            "description": issue.description.clone().unwrap_or_default(),
            "priority": issue.priority.unwrap_or_default(),
            "state": issue.state.clone(),
            "branch_name": issue.branch_name.clone().unwrap_or_default(),
            "url": issue.url.clone().unwrap_or_default(),
            "labels": issue.labels.clone(),
        }
    }))?;
    Ok(output)
}

async fn pause_issue(
    runtime: &Arc<Mutex<RuntimeState>>,
    control: &Arc<Mutex<RuntimeControl>>,
    issue_identifier: String,
) -> ControlActionResult {
    let now = Utc::now();
    let mut state = runtime.lock().await;

    if let Some(issue_id) = issue_id_by_identifier(&state, &issue_identifier) {
        if let Some(entry) = state.running.remove(&issue_id) {
            state.claimed.remove(&issue_id);
            finalize_running_entry(&mut state, entry.clone());
            state.retrying.remove(&issue_id);
            state.held.remove(&issue_id);
            state
                .paused
                .insert(issue_id.clone(), blocked_entry_from_running(entry, now));
            drop(state);
            abort_running_issue(control, &issue_id).await;
            return accepted_result(issue_identifier, "issue paused");
        }
        if let Some(entry) = state.retrying.remove(&issue_id) {
            state.held.remove(&issue_id);
            state
                .paused
                .insert(issue_id, blocked_entry_from_retry(entry, now));
            return accepted_result(issue_identifier, "issue paused");
        }
        if let Some(entry) = state.held.remove(&issue_id) {
            state.paused.insert(
                issue_id,
                BlockedEntry {
                    blocked_at: now,
                    ..entry
                },
            );
            return accepted_result(issue_identifier, "issue paused");
        }
        if state.needs_review.contains_key(&issue_id) {
            return conflict_result(issue_identifier, "issue already requires review");
        }
        if state.paused.contains_key(&issue_id) {
            return conflict_result(issue_identifier, "issue is already paused");
        }
    }

    not_found_result(issue_identifier, "issue is not running, retrying, or held")
}

async fn resume_issue(
    runtime: &Arc<Mutex<RuntimeState>>,
    issue_identifier: String,
) -> ControlActionResult {
    let mut state = runtime.lock().await;
    if let Some(issue_id) = issue_id_by_identifier(&state, &issue_identifier) {
        if state.paused.remove(&issue_id).is_some()
            || state.held.remove(&issue_id).is_some()
            || state.needs_review.remove(&issue_id).is_some()
        {
            return accepted_result(issue_identifier, "issue resumed");
        }
        if state.running.contains_key(&issue_id) || state.retrying.contains_key(&issue_id) {
            return conflict_result(
                issue_identifier,
                "issue is not paused, held, or awaiting review",
            );
        }
    }
    not_found_result(
        issue_identifier,
        "issue is not paused, held, or awaiting review",
    )
}

async fn stop_issue(
    runtime: &Arc<Mutex<RuntimeState>>,
    control: &Arc<Mutex<RuntimeControl>>,
    issue_identifier: String,
) -> ControlActionResult {
    let now = Utc::now();
    let mut state = runtime.lock().await;

    if let Some(issue_id) = issue_id_by_identifier(&state, &issue_identifier) {
        if let Some(entry) = state.running.remove(&issue_id) {
            state.claimed.remove(&issue_id);
            finalize_running_entry(&mut state, entry.clone());
            state.retrying.remove(&issue_id);
            state.paused.remove(&issue_id);
            state
                .held
                .insert(issue_id.clone(), blocked_entry_from_running(entry, now));
            drop(state);
            abort_running_issue(control, &issue_id).await;
            return accepted_result(issue_identifier, "issue stopped and held");
        }
        if state.retrying.contains_key(&issue_id)
            || state.held.contains_key(&issue_id)
            || state.paused.contains_key(&issue_id)
            || state.needs_review.contains_key(&issue_id)
        {
            return conflict_result(issue_identifier, "stop is only valid for running issues");
        }
    }

    not_found_result(issue_identifier, "issue is not running")
}

async fn retry_issue(
    config: &Arc<ServiceConfig>,
    workflow_path: &PathBuf,
    tracker: &TrackerClient,
    runtime: &Arc<Mutex<RuntimeState>>,
    control: &Arc<Mutex<RuntimeControl>>,
    cmd_tx: &mpsc::UnboundedSender<Command>,
    issue_identifier: String,
) -> ControlActionResult {
    let (issue_id, attempt) = {
        let mut state = runtime.lock().await;
        let Some(issue_id) = issue_id_by_identifier(&state, &issue_identifier) else {
            return not_found_result(issue_identifier, "issue is not queued for retry or held");
        };

        if state.paused.contains_key(&issue_id) {
            return conflict_result(
                issue_identifier,
                "paused issues must be resumed before retrying",
            );
        }

        if let Some(entry) = state.retrying.remove(&issue_id) {
            state.held.remove(&issue_id);
            state.needs_review.remove(&issue_id);
            (issue_id, Some(entry.attempt))
        } else if let Some(entry) = state.held.remove(&issue_id) {
            (issue_id, entry.attempt)
        } else if let Some(entry) = state.needs_review.remove(&issue_id) {
            (issue_id, entry.attempt)
        } else {
            return conflict_result(
                issue_identifier,
                "retry is only valid for retrying, held, or review-blocked issues",
            );
        }
    };

    let issue = tracker
        .fetch_issue_states_by_ids(&[issue_id.clone()])
        .await
        .ok()
        .and_then(|mut issues| issues.pop());
    let Some(issue) = issue else {
        return conflict_result(
            issue_identifier,
            "issue is no longer available from the tracker",
        );
    };
    if !candidate_issue(config, &issue) || !issue_routable(config, &issue) {
        return conflict_result(issue_identifier, "issue is no longer eligible for dispatch");
    }
    match dispatch_issue(
        config,
        workflow_path,
        tracker,
        runtime,
        control,
        cmd_tx,
        issue,
        attempt,
    )
    .await
    {
        Ok(()) => accepted_result(issue_identifier, "issue retry dispatched"),
        Err(err) => conflict_result(issue_identifier, err.to_string()),
    }
}

async fn abort_running_issue(control: &Arc<Mutex<RuntimeControl>>, issue_id: &str) {
    if let Some(handle) = control.lock().await.handles.remove(issue_id) {
        handle.abort();
    }
}

fn issue_id_by_identifier(state: &RuntimeState, issue_identifier: &str) -> Option<String> {
    state
        .running
        .iter()
        .find_map(|(issue_id, entry)| {
            (entry.identifier == issue_identifier).then(|| issue_id.clone())
        })
        .or_else(|| {
            state.retrying.iter().find_map(|(issue_id, entry)| {
                (entry.identifier == issue_identifier).then(|| issue_id.clone())
            })
        })
        .or_else(|| {
            state.paused.iter().find_map(|(issue_id, entry)| {
                (entry.identifier == issue_identifier).then(|| issue_id.clone())
            })
        })
        .or_else(|| {
            state.held.iter().find_map(|(issue_id, entry)| {
                (entry.identifier == issue_identifier).then(|| issue_id.clone())
            })
        })
        .or_else(|| {
            state.needs_review.iter().find_map(|(issue_id, entry)| {
                (entry.identifier == issue_identifier).then(|| issue_id.clone())
            })
        })
        .or_else(|| {
            state.claimed.iter().find_map(|(issue_id, identifier)| {
                (identifier == issue_identifier).then(|| issue_id.clone())
            })
        })
}

fn accepted_result(issue_identifier: String, message: impl Into<String>) -> ControlActionResult {
    ControlActionResult {
        status: ControlActionStatus::Accepted,
        issue_identifier,
        message: message.into(),
    }
}

fn not_found_result(issue_identifier: String, message: impl Into<String>) -> ControlActionResult {
    ControlActionResult {
        status: ControlActionStatus::NotFound,
        issue_identifier,
        message: message.into(),
    }
}

fn conflict_result(issue_identifier: String, message: impl Into<String>) -> ControlActionResult {
    ControlActionResult {
        status: ControlActionStatus::Conflict,
        issue_identifier,
        message: message.into(),
    }
}

async fn handle_worker_event(
    config: &Arc<ServiceConfig>,
    runtime: &Arc<Mutex<RuntimeState>>,
    control: &Arc<Mutex<RuntimeControl>>,
    cmd_tx: &mpsc::UnboundedSender<Command>,
    worker_event: WorkerEvent,
) {
    match worker_event.update {
        WorkerUpdate::Started {
            issue,
            attempt,
            workspace_path,
            started_at,
            telemetry,
        } => {
            let mut state = runtime.lock().await;
            state.running.insert(
                issue.id.clone(),
                RunningEntry {
                    issue_id: issue.id.clone(),
                    identifier: issue.identifier.clone(),
                    state: issue.state.clone(),
                    issue,
                    attempt,
                    workspace_path,
                    started_at,
                    session: LiveSession::default(),
                    telemetry,
                },
            );
        }
        WorkerUpdate::CodexMessage {
            event,
            timestamp,
            session_id,
            thread_id,
            turn_id,
            codex_app_server_pid,
            message,
            usage,
            rate_limits,
            phase,
            phase_detail,
            last_command,
            last_file_touched,
        } => {
            let mut token_budget_reason = None;
            let mut state = runtime.lock().await;
            if let Some(entry) = state.running.get_mut(&worker_event.issue_id) {
                entry.session.last_codex_event = Some(event);
                entry.session.last_codex_timestamp = Some(timestamp);
                entry.session.last_codex_message = message;
                entry.session.session_id = session_id;
                entry.session.thread_id = thread_id;
                entry.session.turn_id = turn_id;
                entry.session.codex_app_server_pid = codex_app_server_pid;
                if let Some(usage) = usage {
                    entry.session.codex_input_tokens = usage.input_tokens;
                    entry.session.codex_output_tokens = usage.output_tokens;
                    entry.session.codex_total_tokens = usage.total_tokens;
                    if usage.total_tokens >= config.max_tokens_before_review {
                        token_budget_reason = Some(format!(
                            "token budget exceeded ({} >= {})",
                            usage.total_tokens, config.max_tokens_before_review
                        ));
                    }
                }
                entry.session.turn_count = entry.session.turn_count.max(1);
                if let Some(phase) = phase {
                    entry.telemetry.phase = phase;
                }
                if let Some(detail) = phase_detail {
                    entry.telemetry.phase_detail = Some(detail);
                }
                if let Some(command) = last_command {
                    entry.telemetry.last_command = Some(command);
                }
                if let Some(path) = last_file_touched {
                    entry.telemetry.last_file_touched = Some(path);
                }
                if let Some(rate_limits) = rate_limits {
                    state.rate_limits = Some(rate_limits);
                }
            }
            if let Some(reason) = token_budget_reason {
                block_running_entry_for_review(
                    &mut state,
                    &worker_event.issue_id,
                    reason,
                    Utc::now(),
                );
                drop(state);
                control.lock().await.handles.remove(&worker_event.issue_id);
            }
        }
        WorkerUpdate::Telemetry {
            phase,
            phase_detail,
            review_reason,
            last_command,
            last_file_touched,
            diff,
        } => {
            let mut state = runtime.lock().await;
            if let Some(entry) = state.running.get_mut(&worker_event.issue_id) {
                if let Some(phase) = phase {
                    entry.telemetry.phase = phase;
                }
                if let Some(detail) = phase_detail {
                    entry.telemetry.phase_detail = Some(detail);
                }
                if let Some(reason) = review_reason {
                    entry.telemetry.review_reason = Some(reason);
                }
                if let Some(command) = last_command {
                    entry.telemetry.last_command = Some(command);
                }
                if let Some(path) = last_file_touched {
                    entry.telemetry.last_file_touched = Some(path);
                }
                if let Some(diff) = diff {
                    entry.telemetry.diff = diff;
                }
            }
        }
        WorkerUpdate::NeedsReview {
            issue,
            attempt,
            reason,
        } => {
            let mut state = runtime.lock().await;
            let now = Utc::now();
            if state.running.contains_key(&worker_event.issue_id) {
                block_running_entry_for_review(
                    &mut state,
                    &worker_event.issue_id,
                    reason.clone(),
                    now,
                );
            } else {
                let workspace_path = workspace_path_for_identifier(config, &issue.identifier);
                state.needs_review.insert(
                    issue.id.clone(),
                    BlockedEntry {
                        issue_id: issue.id.clone(),
                        identifier: issue.identifier.clone(),
                        state: issue.state.clone(),
                        issue,
                        attempt,
                        workspace_path,
                        blocked_at: now,
                        session: LiveSession::default(),
                        telemetry: RunTelemetry {
                            phase: RunPhase::NeedsReview,
                            review_reason: Some(reason),
                            ..RunTelemetry::default()
                        },
                    },
                );
            }
            drop(state);
            control.lock().await.handles.remove(&worker_event.issue_id);
        }
        WorkerUpdate::Finished {
            issue_id,
            success,
            continuation,
            issue,
            attempt,
            error,
        } => {
            let mut state = runtime.lock().await;
            let entry = state.running.remove(&issue_id);
            state.claimed.remove(&issue_id);
            if let Some(entry) = entry {
                finalize_running_entry(&mut state, entry);
            }
            drop(state);
            control.lock().await.handles.remove(&issue_id);
            if success && continuation {
                schedule_retry(
                    config,
                    runtime,
                    cmd_tx,
                    issue.clone(),
                    attempt.unwrap_or(0) + 1,
                    Some("continuation".to_string()),
                    Duration::from_secs(1),
                )
                .await;
            } else if !success {
                let next_attempt = attempt.unwrap_or(0) + 1;
                let delay_ms = failure_backoff_ms(config, next_attempt);
                schedule_retry(
                    config,
                    runtime,
                    cmd_tx,
                    issue.clone(),
                    next_attempt,
                    error,
                    Duration::from_millis(delay_ms),
                )
                .await;
            } else if terminal_issue_state(config, &issue.state) {
                workspace::remove_issue_workspace(config, &issue.identifier).await;
            }
        }
    }
}

async fn schedule_retry(
    config: &Arc<ServiceConfig>,
    runtime: &Arc<Mutex<RuntimeState>>,
    cmd_tx: &mpsc::UnboundedSender<Command>,
    issue: Issue,
    attempt: u32,
    error: Option<String>,
    delay: Duration,
) {
    let due_at = Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default();
    let workspace_path = workspace_path_for_identifier(config, &issue.identifier);
    {
        let mut state = runtime.lock().await;
        state.retrying.insert(
            issue.id.clone(),
            RetryEntry {
                issue_id: issue.id.clone(),
                identifier: issue.identifier.clone(),
                state: issue.state.clone(),
                issue: issue.clone(),
                attempt,
                workspace_path: workspace_path.clone(),
                due_at,
                error: error.clone(),
                telemetry: RunTelemetry {
                    phase: RunPhase::Retrying,
                    phase_detail: error.clone(),
                    ..default_run_telemetry_for_workspace(&workspace_path, RunPhase::Retrying)
                },
            },
        );
    }
    let cmd_tx = cmd_tx.clone();
    tokio::spawn(async move {
        sleep(delay).await;
        let _ = cmd_tx.send(Command::RetryDue(DueRetry {
            issue_id: issue.id,
            attempt,
            due_at,
            error,
        }));
    });
}

async fn reconcile_running_issues(
    config: &Arc<ServiceConfig>,
    tracker: &TrackerClient,
    runtime: &Arc<Mutex<RuntimeState>>,
    control: &Arc<Mutex<RuntimeControl>>,
) -> Result<()> {
    let issue_ids = {
        let state = runtime.lock().await;
        state.running.keys().cloned().collect::<Vec<_>>()
    };
    if issue_ids.is_empty() {
        return Ok(());
    }
    let issues = tracker.fetch_issue_states_by_ids(&issue_ids).await?;
    let mut current = HashMap::new();
    for issue in issues {
        current.insert(issue.id.clone(), issue);
    }

    let mut to_abort = Vec::new();
    {
        let mut state = runtime.lock().await;
        for issue_id in issue_ids {
            if let Some(issue) = current.get(&issue_id) {
                if state.paused.contains_key(&issue_id)
                    || state.held.contains_key(&issue_id)
                    || state.needs_review.contains_key(&issue_id)
                    || terminal_issue_state(config, &issue.state)
                    || !active_issue_state(config, &issue.state)
                    || !issue_routable(config, issue)
                {
                    to_abort.push((
                        issue.id.clone(),
                        issue.identifier.clone(),
                        terminal_issue_state(config, &issue.state),
                    ));
                } else if let Some(entry) = state.running.get_mut(&issue_id) {
                    entry.state = issue.state.clone();
                    entry.issue = issue.clone();
                }
            }
        }
    }

    for (issue_id, identifier, cleanup) in to_abort {
        if let Some(handle) = control.lock().await.handles.remove(&issue_id) {
            handle.abort();
        }
        {
            let mut state = runtime.lock().await;
            state.running.remove(&issue_id);
            state.claimed.remove(&issue_id);
            state.retrying.remove(&issue_id);
        }
        if cleanup {
            workspace::remove_issue_workspace(config, &identifier).await;
        }
    }
    Ok(())
}

async fn reconcile_blocked_issues(
    config: &Arc<ServiceConfig>,
    tracker: &TrackerClient,
    runtime: &Arc<Mutex<RuntimeState>>,
) -> Result<()> {
    let issue_ids = {
        let state = runtime.lock().await;
        state
            .paused
            .keys()
            .chain(state.held.keys())
            .chain(state.needs_review.keys())
            .cloned()
            .collect::<Vec<_>>()
    };
    if issue_ids.is_empty() {
        return Ok(());
    }

    let issues = tracker.fetch_issue_states_by_ids(&issue_ids).await?;
    let current = issues
        .into_iter()
        .map(|issue| (issue.id.clone(), issue))
        .collect::<HashMap<_, _>>();
    let mut cleanup = Vec::new();

    {
        let mut state = runtime.lock().await;
        for issue_id in issue_ids {
            match current.get(&issue_id) {
                Some(issue)
                    if active_issue_state(config, &issue.state)
                        && issue_routable(config, issue) =>
                {
                    if let Some(entry) = state.paused.get_mut(&issue_id) {
                        entry.state = issue.state.clone();
                        entry.issue = issue.clone();
                    }
                    if let Some(entry) = state.held.get_mut(&issue_id) {
                        entry.state = issue.state.clone();
                        entry.issue = issue.clone();
                    }
                    if let Some(entry) = state.needs_review.get_mut(&issue_id) {
                        entry.state = issue.state.clone();
                        entry.issue = issue.clone();
                    }
                }
                Some(issue) if terminal_issue_state(config, &issue.state) => {
                    state.paused.remove(&issue_id);
                    state.held.remove(&issue_id);
                    state.needs_review.remove(&issue_id);
                    cleanup.push(issue.identifier.clone());
                }
                Some(_) | None => {
                    state.paused.remove(&issue_id);
                    state.held.remove(&issue_id);
                    state.needs_review.remove(&issue_id);
                }
            }
        }
    }

    for identifier in cleanup {
        workspace::remove_issue_workspace(config, &identifier).await;
    }

    Ok(())
}

async fn reconcile_stalled_running_issues(
    config: &Arc<ServiceConfig>,
    runtime: &Arc<Mutex<RuntimeState>>,
    control: &Arc<Mutex<RuntimeControl>>,
    cmd_tx: &mpsc::UnboundedSender<Command>,
) {
    let now = Utc::now();
    let timeout_ms = config.codex_stall_timeout_ms as i64;
    let entries = {
        let state = runtime.lock().await;
        state.running.values().cloned().collect::<Vec<_>>()
    };
    for entry in entries {
        let last = entry
            .session
            .last_codex_timestamp
            .unwrap_or(entry.started_at);
        let elapsed = now - last;
        if elapsed.num_milliseconds() > timeout_ms {
            warn!("issue stalled: {}", entry.identifier);
            if let Some(handle) = control.lock().await.handles.remove(&entry.issue_id) {
                handle.abort();
            }
            {
                let mut state = runtime.lock().await;
                state.running.remove(&entry.issue_id);
                state.claimed.remove(&entry.issue_id);
            }
            if (elapsed.num_minutes().max(0) as u64) >= config.max_idle_minutes_before_review {
                let mut state = runtime.lock().await;
                let mut blocked = blocked_entry_from_running(entry.clone(), Utc::now());
                blocked.telemetry.phase = RunPhase::NeedsReview;
                blocked.telemetry.review_reason = Some(format!(
                    "no meaningful Codex activity for {} minutes",
                    elapsed.num_minutes()
                ));
                state.needs_review.insert(entry.issue_id.clone(), blocked);
            } else {
                schedule_retry(
                    config,
                    runtime,
                    cmd_tx,
                    entry.issue.clone(),
                    entry.attempt.unwrap_or(0) + 1,
                    Some(format!(
                        "stalled for {}ms without codex activity",
                        elapsed.num_milliseconds()
                    )),
                    Duration::from_millis(failure_backoff_ms(
                        config,
                        entry.attempt.unwrap_or(0) + 1,
                    )),
                )
                .await;
            }
        }
    }
}

async fn publish_snapshot(
    runtime: &Arc<Mutex<RuntimeState>>,
    snapshot_tx: &watch::Sender<Snapshot>,
) {
    let state = runtime.lock().await;
    let mut running = state.running.values().cloned().collect::<Vec<_>>();
    running.sort_by_key(|entry| entry.identifier.clone());
    let mut retrying = state.retrying.values().cloned().collect::<Vec<_>>();
    retrying.sort_by_key(|entry| entry.due_at);
    let mut paused = state.paused.values().cloned().collect::<Vec<_>>();
    paused.sort_by_key(|entry| entry.identifier.clone());
    let mut held = state.held.values().cloned().collect::<Vec<_>>();
    held.sort_by_key(|entry| entry.identifier.clone());
    let mut needs_review = state.needs_review.values().cloned().collect::<Vec<_>>();
    needs_review.sort_by_key(|entry| entry.identifier.clone());
    let active_totals = running
        .iter()
        .fold(CodexTotals::default(), |mut acc, entry| {
            acc.input_tokens += entry.session.codex_input_tokens;
            acc.output_tokens += entry.session.codex_output_tokens;
            acc.total_tokens += entry.session.codex_total_tokens;
            acc.seconds_running +=
                duration_to_seconds((Utc::now() - entry.started_at).to_std().unwrap_or_default());
            acc
        });
    let snapshot = Snapshot {
        running,
        retrying,
        paused,
        held,
        needs_review,
        codex_totals: CodexTotals {
            input_tokens: state.completed_totals.input_tokens + active_totals.input_tokens,
            output_tokens: state.completed_totals.output_tokens + active_totals.output_tokens,
            total_tokens: state.completed_totals.total_tokens + active_totals.total_tokens,
            seconds_running: state.completed_totals.seconds_running + active_totals.seconds_running,
        },
        rate_limits: state.rate_limits.clone(),
        polling: state.polling.clone(),
    };
    let _ = snapshot_tx.send(snapshot);
}

async fn publish_debug_state(runtime: &Arc<Mutex<RuntimeState>>) {
    let state = runtime.lock().await;
    info!(
        running = state.running.len(),
        retrying = state.retrying.len(),
        paused = state.paused.len(),
        held = state.held.len(),
        needs_review = state.needs_review.len(),
        claimed = state.claimed.len(),
        "orchestrator state updated"
    );
}

async fn should_dispatch_issue(
    config: &Arc<ServiceConfig>,
    runtime: &Arc<Mutex<RuntimeState>>,
    issue: &Issue,
) -> bool {
    let state = runtime.lock().await;
    candidate_issue(config, issue)
        && !todo_issue_blocked(config, issue)
        && !state.claimed.contains_key(&issue.id)
        && !state.running.contains_key(&issue.id)
        && !state.retrying.contains_key(&issue.id)
        && !state.paused.contains_key(&issue.id)
        && !state.held.contains_key(&issue.id)
        && !state.needs_review.contains_key(&issue.id)
        && state.running.len() < config.max_concurrent_agents
        && running_issue_count_for_state(&state.running, &issue.state)
            < config.state_limit(&issue.state)
}

fn candidate_issue(config: &ServiceConfig, issue: &Issue) -> bool {
    !issue.id.is_empty()
        && !issue.identifier.is_empty()
        && !issue.title.is_empty()
        && active_issue_state(config, &issue.state)
        && issue_routable(config, issue)
}

fn todo_issue_blocked(config: &ServiceConfig, issue: &Issue) -> bool {
    issue.state == "Todo"
        && issue.blocked_by.iter().any(|blocker| {
            blocker
                .state
                .as_deref()
                .map(|state| !terminal_issue_state(config, state))
                .unwrap_or(false)
        })
}

fn running_issue_count_for_state(running: &HashMap<String, RunningEntry>, state: &str) -> usize {
    running
        .values()
        .filter(|entry| entry.state.eq_ignore_ascii_case(state))
        .count()
}

fn active_issue_state(config: &ServiceConfig, state: &str) -> bool {
    config
        .linear_active_states
        .iter()
        .any(|active| active.eq_ignore_ascii_case(state))
}

fn terminal_issue_state(config: &ServiceConfig, state: &str) -> bool {
    config
        .linear_terminal_states
        .iter()
        .any(|terminal| terminal.eq_ignore_ascii_case(state))
}

fn priority_rank(priority: Option<i64>) -> i64 {
    match priority {
        Some(value @ 1..=4) => value,
        _ => 5,
    }
}

fn failure_backoff_ms(config: &ServiceConfig, attempt: u32) -> u64 {
    let shift = attempt.saturating_sub(1).min(10);
    let delay = 10_000u64.saturating_mul(1 << shift);
    delay.min(config.max_retry_backoff_ms)
}

fn demo_snapshot(step: u64) -> Snapshot {
    let now = Utc::now();
    let runtime_a = 90 + step as i64;
    let runtime_b = 420 + (step as i64 * 2);
    Snapshot {
        running: vec![
            RunningEntry {
                issue_id: "demo-1".to_string(),
                identifier: "SYM-101".to_string(),
                state: "In Progress".to_string(),
                issue: Issue {
                    id: "demo-1".to_string(),
                    identifier: "SYM-101".to_string(),
                    title: "Build Rust TUI".to_string(),
                    description: Some("Demo issue for the built-in dashboard preview".to_string()),
                    priority: Some(1),
                    state: "In Progress".to_string(),
                    branch_name: Some("sym-101-rust-tui".to_string()),
                    url: Some("https://linear.app/demo/issue/SYM-101".to_string()),
                    labels: vec!["demo".to_string(), "rust".to_string()],
                    blocked_by: Vec::new(),
                    created_at: Some(now - chrono::Duration::hours(2)),
                    updated_at: Some(now),
                    assignee_id: None,
                },
                attempt: None,
                workspace_path: "/tmp/symphony_demo_workspaces/SYM-101".to_string(),
                started_at: now - chrono::Duration::seconds(runtime_a),
                session: LiveSession {
                    session_id: Some("thread-demo-alpha-0001".to_string()),
                    thread_id: Some("thread-demo-alpha".to_string()),
                    turn_id: Some(format!("turn-{step}")),
                    codex_app_server_pid: Some("4242".to_string()),
                    last_codex_event: Some("codex/event/task_started".to_string()),
                    last_codex_timestamp: Some(now),
                    last_codex_message: Some(format!(
                        "running cargo test and updating UI ({step})"
                    )),
                    codex_input_tokens: 18_000 + step * 16,
                    codex_output_tokens: 2_600 + step * 6,
                    codex_total_tokens: 20_600 + step * 22,
                    turn_count: 3 + (step % 3),
                },
                telemetry: RunTelemetry {
                    phase: RunPhase::Editing,
                    phase_detail: Some("demo editing run".to_string()),
                    last_command: Some("cargo test".to_string()),
                    last_file_touched: Some("src/tui.rs".to_string()),
                    diff: DiffStats {
                        changed_files: 3,
                        added_lines: 84,
                        removed_lines: 21,
                    },
                    ..default_run_telemetry_for_workspace(
                        "/tmp/symphony_demo_workspaces/SYM-101",
                        RunPhase::Editing,
                    )
                },
            },
            RunningEntry {
                issue_id: "demo-2".to_string(),
                identifier: "SYM-204".to_string(),
                state: "Rework".to_string(),
                issue: Issue {
                    id: "demo-2".to_string(),
                    identifier: "SYM-204".to_string(),
                    title: "Mirror web observability".to_string(),
                    description: Some(
                        "Second demo issue to keep the dashboard populated".to_string(),
                    ),
                    priority: Some(2),
                    state: "Rework".to_string(),
                    branch_name: Some("sym-204-web-observability".to_string()),
                    url: Some("https://linear.app/demo/issue/SYM-204".to_string()),
                    labels: vec!["demo".to_string(), "web".to_string()],
                    blocked_by: Vec::new(),
                    created_at: Some(now - chrono::Duration::hours(4)),
                    updated_at: Some(now),
                    assignee_id: None,
                },
                attempt: Some(2),
                workspace_path: "/tmp/symphony_demo_workspaces/SYM-204".to_string(),
                started_at: now - chrono::Duration::seconds(runtime_b),
                session: LiveSession {
                    session_id: Some("thread-demo-beta-0002".to_string()),
                    thread_id: Some("thread-demo-beta".to_string()),
                    turn_id: Some(format!("turn-{}", step + 10)),
                    codex_app_server_pid: Some("5252".to_string()),
                    last_codex_event: Some("notification".to_string()),
                    last_codex_timestamp: Some(now - chrono::Duration::seconds(3)),
                    last_codex_message: Some(
                        "agent message content streaming: websocket patch applied".to_string(),
                    ),
                    codex_input_tokens: 42_000 + step * 10,
                    codex_output_tokens: 5_000 + step * 3,
                    codex_total_tokens: 47_000 + step * 13,
                    turn_count: 7,
                },
                telemetry: RunTelemetry {
                    phase: RunPhase::Testing,
                    phase_detail: Some("running verification commands".to_string()),
                    last_command: Some("cargo test".to_string()),
                    last_file_touched: Some("src/web.rs".to_string()),
                    diff: DiffStats {
                        changed_files: 5,
                        added_lines: 132,
                        removed_lines: 47,
                    },
                    ..default_run_telemetry_for_workspace(
                        "/tmp/symphony_demo_workspaces/SYM-204",
                        RunPhase::Testing,
                    )
                },
            },
        ],
        retrying: vec![
            RetryEntry {
                issue_id: "demo-3".to_string(),
                identifier: "SYM-305".to_string(),
                state: "In Progress".to_string(),
                issue: Issue {
                    id: "demo-3".to_string(),
                    identifier: "SYM-305".to_string(),
                    title: "Add retry telemetry".to_string(),
                    description: Some("Retry queue demo entry".to_string()),
                    priority: Some(3),
                    state: "In Progress".to_string(),
                    branch_name: Some("sym-305-retry-telemetry".to_string()),
                    url: Some("https://linear.app/demo/issue/SYM-305".to_string()),
                    labels: vec!["demo".to_string(), "ops".to_string()],
                    blocked_by: Vec::new(),
                    created_at: Some(now - chrono::Duration::hours(1)),
                    updated_at: Some(now),
                    assignee_id: None,
                },
                attempt: 4,
                workspace_path: "/tmp/symphony_demo_workspaces/SYM-305".to_string(),
                due_at: now + chrono::Duration::seconds(12),
                error: Some("rate limit exhausted".to_string()),
                telemetry: RunTelemetry {
                    phase: RunPhase::Retrying,
                    phase_detail: Some("backing off after rate limit".to_string()),
                    ..default_run_telemetry_for_workspace(
                        "/tmp/symphony_demo_workspaces/SYM-305",
                        RunPhase::Retrying,
                    )
                },
            },
            RetryEntry {
                issue_id: "demo-4".to_string(),
                identifier: "SYM-401".to_string(),
                state: "Todo".to_string(),
                issue: Issue {
                    id: "demo-4".to_string(),
                    identifier: "SYM-401".to_string(),
                    title: "Recover stalled worker".to_string(),
                    description: Some("Second retry queue demo entry".to_string()),
                    priority: Some(2),
                    state: "Todo".to_string(),
                    branch_name: Some("sym-401-worker-recovery".to_string()),
                    url: Some("https://linear.app/demo/issue/SYM-401".to_string()),
                    labels: vec!["demo".to_string(), "ops".to_string()],
                    blocked_by: Vec::new(),
                    created_at: Some(now - chrono::Duration::hours(3)),
                    updated_at: Some(now),
                    assignee_id: None,
                },
                attempt: 2,
                workspace_path: "/tmp/symphony_demo_workspaces/SYM-401".to_string(),
                due_at: now + chrono::Duration::seconds(27),
                error: Some("worker crashed; restarting cleanly".to_string()),
                telemetry: RunTelemetry {
                    phase: RunPhase::Retrying,
                    phase_detail: Some("waiting for retry window".to_string()),
                    ..default_run_telemetry_for_workspace(
                        "/tmp/symphony_demo_workspaces/SYM-401",
                        RunPhase::Retrying,
                    )
                },
            },
        ],
        paused: Vec::new(),
        held: Vec::new(),
        needs_review: vec![BlockedEntry {
            issue_id: "demo-5".to_string(),
            identifier: "SYM-512".to_string(),
            state: "In Progress".to_string(),
            issue: Issue {
                id: "demo-5".to_string(),
                identifier: "SYM-512".to_string(),
                title: "Review oversized refactor".to_string(),
                description: Some("Demo needs-review issue".to_string()),
                priority: Some(1),
                state: "In Progress".to_string(),
                branch_name: Some("sym-512-review-refactor".to_string()),
                url: Some("https://linear.app/demo/issue/SYM-512".to_string()),
                labels: vec!["demo".to_string(), "ops".to_string()],
                blocked_by: Vec::new(),
                created_at: Some(now - chrono::Duration::hours(5)),
                updated_at: Some(now),
                assignee_id: None,
            },
            attempt: Some(1),
            workspace_path: "/tmp/symphony_demo_workspaces/SYM-512".to_string(),
            blocked_at: now - chrono::Duration::seconds(18),
            session: LiveSession {
                session_id: Some("thread-demo-review-0003".to_string()),
                thread_id: Some("thread-demo-review".to_string()),
                turn_id: Some("turn-review".to_string()),
                codex_app_server_pid: Some("6363".to_string()),
                last_codex_event: Some("turn_completed".to_string()),
                last_codex_timestamp: Some(now - chrono::Duration::seconds(18)),
                last_codex_message: Some("completed".to_string()),
                codex_input_tokens: 12_000,
                codex_output_tokens: 1_600,
                codex_total_tokens: 13_600,
                turn_count: 3,
            },
            telemetry: RunTelemetry {
                phase: RunPhase::NeedsReview,
                phase_detail: Some("paused for operator review".to_string()),
                review_reason: Some(
                    "workspace changed-file budget exceeded (16 >= 12)".to_string(),
                ),
                last_command: Some("cargo test".to_string()),
                last_file_touched: Some("src/orchestrator.rs".to_string()),
                diff: DiffStats {
                    changed_files: 16,
                    added_lines: 540,
                    removed_lines: 87,
                },
                ..default_run_telemetry_for_workspace(
                    "/tmp/symphony_demo_workspaces/SYM-512",
                    RunPhase::NeedsReview,
                )
            },
        }],
        codex_totals: CodexTotals {
            input_tokens: 60_000 + step * 26,
            output_tokens: 7_600 + step * 9,
            total_tokens: 67_600 + step * 35,
            seconds_running: (runtime_a + runtime_b) as f64,
        },
        rate_limits: Some(serde_json::json!({
            "limit_id": "gpt-5",
            "primary": { "remaining": 12345u64.saturating_sub(step * 3), "limit": 20000, "reset_in_seconds": 30 },
            "secondary": { "remaining": 52u64.saturating_sub((step % 10) as u64), "limit": 60, "reset_in_seconds": 12 },
            "credits": { "has_credits": true, "balance": 9876.5 }
        })),
        polling: crate::model::PollingState {
            checking: step % 8 == 0,
            next_poll_in_ms: Some(5_000 - ((step % 5) * 1_000)),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tokio::sync::Mutex;

    use super::*;
    use crate::tracker::GitHubClient;

    fn github_test_config() -> Arc<ServiceConfig> {
        let mut config = ServiceConfig::demo(PathBuf::from("WORKFLOW.md"), None);
        config.tracker_kind = Some("github".to_string());
        config.github_owner = Some("owner".to_string());
        config.github_repo = Some("repo".to_string());
        config.linear_active_states = vec!["open".to_string()];
        config.linear_terminal_states = vec!["closed".to_string()];
        Arc::new(config)
    }

    fn sample_issue(id: &str, identifier: &str, state: &str) -> Issue {
        Issue {
            id: id.to_string(),
            identifier: identifier.to_string(),
            title: format!("Issue {identifier}"),
            description: Some("test issue".to_string()),
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

    fn sample_running_entry(issue: Issue) -> RunningEntry {
        RunningEntry {
            issue_id: issue.id.clone(),
            identifier: issue.identifier.clone(),
            state: issue.state.clone(),
            issue,
            attempt: Some(2),
            workspace_path: "/tmp/test-workspace".to_string(),
            started_at: Utc::now() - chrono::Duration::seconds(30),
            session: LiveSession {
                codex_input_tokens: 11,
                codex_output_tokens: 7,
                codex_total_tokens: 18,
                ..LiveSession::default()
            },
            telemetry: default_run_telemetry_for_workspace(
                "/tmp/test-workspace",
                RunPhase::Editing,
            ),
        }
    }

    fn sample_retry_entry(issue: Issue) -> RetryEntry {
        RetryEntry {
            issue_id: issue.id.clone(),
            identifier: issue.identifier.clone(),
            state: issue.state.clone(),
            issue,
            attempt: 3,
            workspace_path: "/tmp/test-workspace".to_string(),
            due_at: Utc::now() + chrono::Duration::seconds(10),
            error: Some("boom".to_string()),
            telemetry: default_run_telemetry_for_workspace(
                "/tmp/test-workspace",
                RunPhase::Retrying,
            ),
        }
    }

    #[tokio::test]
    async fn pause_running_issue_moves_it_to_paused() {
        let issue = sample_issue("1", "repo#1", "open");
        let runtime = Arc::new(Mutex::new(RuntimeState::default()));
        let control = Arc::new(Mutex::new(RuntimeControl {
            handles: HashMap::new(),
        }));

        {
            let mut state = runtime.lock().await;
            state
                .running
                .insert(issue.id.clone(), sample_running_entry(issue.clone()));
            state
                .claimed
                .insert(issue.id.clone(), issue.identifier.clone());
        }
        control.lock().await.handles.insert(
            issue.id.clone(),
            tokio::spawn(async { sleep(Duration::from_secs(60)).await }),
        );

        let result = pause_issue(&runtime, &control, issue.identifier.clone()).await;

        assert_eq!(result.status, ControlActionStatus::Accepted);
        let state = runtime.lock().await;
        assert!(state.running.is_empty());
        assert!(!state.claimed.contains_key(&issue.id));
        assert!(state.paused.contains_key(&issue.id));
        assert_eq!(state.completed_totals.total_tokens, 18);
        drop(state);
        assert!(control.lock().await.handles.is_empty());
    }

    #[tokio::test]
    async fn stop_requires_running_issue() {
        let issue = sample_issue("2", "repo#2", "open");
        let runtime = Arc::new(Mutex::new(RuntimeState::default()));
        let control = Arc::new(Mutex::new(RuntimeControl {
            handles: HashMap::new(),
        }));

        runtime
            .lock()
            .await
            .retrying
            .insert(issue.id.clone(), sample_retry_entry(issue.clone()));

        let result = stop_issue(&runtime, &control, issue.identifier.clone()).await;

        assert_eq!(result.status, ControlActionStatus::Conflict);
        let state = runtime.lock().await;
        assert!(state.retrying.contains_key(&issue.id));
        assert!(state.held.is_empty());
    }

    #[tokio::test]
    async fn resume_clears_paused_issue() {
        let issue = sample_issue("3", "repo#3", "open");
        let runtime = Arc::new(Mutex::new(RuntimeState::default()));
        runtime.lock().await.paused.insert(
            issue.id.clone(),
            BlockedEntry {
                issue_id: issue.id.clone(),
                identifier: issue.identifier.clone(),
                state: issue.state.clone(),
                issue: issue.clone(),
                attempt: Some(1),
                workspace_path: "/tmp/test-workspace".to_string(),
                blocked_at: Utc::now(),
                session: LiveSession::default(),
                telemetry: default_run_telemetry_for_workspace(
                    "/tmp/test-workspace",
                    RunPhase::Paused,
                ),
            },
        );

        let result = resume_issue(&runtime, issue.identifier.clone()).await;

        assert_eq!(result.status, ControlActionStatus::Accepted);
        assert!(runtime.lock().await.paused.is_empty());
    }

    #[tokio::test]
    async fn retry_rejects_paused_issue() {
        let config = github_test_config();
        let tracker = TrackerClient::GitHub(GitHubClient::new(Arc::clone(&config)));
        let issue = sample_issue("4", "repo#4", "open");
        let runtime = Arc::new(Mutex::new(RuntimeState::default()));
        let control = Arc::new(Mutex::new(RuntimeControl {
            handles: HashMap::new(),
        }));
        runtime.lock().await.paused.insert(
            issue.id.clone(),
            BlockedEntry {
                issue_id: issue.id.clone(),
                identifier: issue.identifier.clone(),
                state: issue.state.clone(),
                issue,
                attempt: Some(1),
                workspace_path: "/tmp/test-workspace".to_string(),
                blocked_at: Utc::now(),
                session: LiveSession::default(),
                telemetry: default_run_telemetry_for_workspace(
                    "/tmp/test-workspace",
                    RunPhase::NeedsReview,
                ),
            },
        );

        let result = retry_issue(
            &config,
            &PathBuf::from("WORKFLOW.md"),
            &tracker,
            &runtime,
            &control,
            &mpsc::unbounded_channel().0,
            "repo#4".to_string(),
        )
        .await;

        assert_eq!(result.status, ControlActionStatus::Conflict);
        assert!(runtime.lock().await.paused.contains_key("4"));
    }

    #[tokio::test]
    async fn dispatch_skips_operator_blocked_issues() {
        let config = github_test_config();
        let issue = sample_issue("5", "repo#5", "open");
        let runtime = Arc::new(Mutex::new(RuntimeState::default()));
        {
            let mut state = runtime.lock().await;
            state.paused.insert(
                issue.id.clone(),
                BlockedEntry {
                    issue_id: issue.id.clone(),
                    identifier: issue.identifier.clone(),
                    state: issue.state.clone(),
                    issue: issue.clone(),
                    attempt: Some(1),
                    workspace_path: "/tmp/test-workspace".to_string(),
                    blocked_at: Utc::now(),
                    session: LiveSession::default(),
                    telemetry: default_run_telemetry_for_workspace(
                        "/tmp/test-workspace",
                        RunPhase::Paused,
                    ),
                },
            );
        }

        assert!(!should_dispatch_issue(&config, &runtime, &issue).await);
    }
}
