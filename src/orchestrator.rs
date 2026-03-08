use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use liquid::{object, ParserBuilder};
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep};
use tracing::{error, info, warn};

use crate::codex;
use crate::config::ServiceConfig;
use crate::model::{
    duration_to_seconds, CodexTotals, DueRetry, Issue, LiveSession, RefreshResult, RetryEntry, RunningEntry,
    RuntimeState, Snapshot, WorkerEvent, WorkerUpdate,
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
    RetryDue(DueRetry),
    WorkerEvent(WorkerEvent),
}

struct RuntimeControl {
    handles: HashMap<String, JoinHandle<()>>,
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
    OrchestratorHandle { cmd_tx, snapshot_rx }
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
                        Command::RetryDue(_) | Command::WorkerEvent(_) => {}
                    }
                }
            }
        }
    });
    OrchestratorHandle { cmd_tx, snapshot_rx }
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

    let issues = tracker.fetch_candidate_issues().await?;
    let mut issues = issues;
    issues.sort_by_key(|issue| (priority_rank(issue.priority), issue.created_at, issue.identifier.clone()));

    for issue in issues {
        if should_dispatch_issue(config, runtime, &issue).await {
            dispatch_issue(config, workflow_path, tracker, runtime, control, cmd_tx, issue, None).await?;
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
        state.claimed.insert(issue.id.clone(), issue.identifier.clone());
    }
    publish_debug_state(runtime).await;
    let config = Arc::clone(config);
    let tracker = tracker.clone();
    let runtime_cmd_tx = cmd_tx.clone();
    let workflow_path = workflow_path.clone();
    let issue_id = issue.id.clone();
    let task_issue_id = issue_id.clone();
    let task = tokio::spawn(async move {
        if let Err(err) = run_issue_worker(config, workflow_path, tracker, runtime_cmd_tx.clone(), issue, attempt).await {
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
    let started_at = Utc::now();
    let _ = cmd_tx.send(Command::WorkerEvent(WorkerEvent {
        issue_id: issue.id.clone(),
        update: WorkerUpdate::Started {
            issue: issue.clone(),
            attempt,
            workspace_path: workspace.path.clone(),
            started_at,
        },
    }));

    let mut workflow_store = WorkflowStore::new(workflow_path);
    let _ = workflow_store.current()?;
    let parser = ParserBuilder::with_stdlib().build()?;
    let mut session = codex::start_session(Arc::clone(&config), std::path::Path::new(&workspace.path)).await?;
    let mut current_issue = issue.clone();

    for turn_number in 1..=config.max_turns {
        let prompt = if turn_number == 1 {
            render_prompt(&parser, &config.prompt_template, &current_issue, attempt)?
        } else {
            format!(
                "Continuation guidance:\n\n- The previous Codex turn completed normally, but the Linear issue is still in an active state.\n- This is continuation turn #{turn_number} of {} for the current agent run.\n- Resume from the current workspace and workpad state instead of restarting from scratch.\n- The original task instructions and prior turn context are already present in this thread, so do not restate them before acting.\n- Focus on the remaining ticket work and do not end the turn while the issue stays active unless you are truly blocked.",
                config.max_turns
            )
        };
        session
            .run_turn(&current_issue, prompt, &worker_tx(&cmd_tx), tracker.linear_client())
            .await?;

        let refreshed = tracker
            .fetch_issue_states_by_ids(&[current_issue.id.clone()])
            .await?
            .into_iter()
            .next();
        match refreshed {
            Some(issue) if active_issue_state(&config, &issue.state) && turn_number < config.max_turns => {
                current_issue = issue;
            }
            Some(issue) if active_issue_state(&config, &issue.state) => {
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
            Some(issue) => {
                session.shutdown().await;
                workspace::run_after_run_hook(&config, &workspace).await;
                let _ = cmd_tx.send(Command::WorkerEvent(WorkerEvent {
                    issue_id: issue.id.clone(),
                    update: WorkerUpdate::Finished {
                        issue_id: issue.id.clone(),
                        success: true,
                        continuation: false,
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
    Ok(())
}

fn worker_tx(cmd_tx: &mpsc::UnboundedSender<Command>) -> tokio::sync::mpsc::UnboundedSender<WorkerEvent> {
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
        } => {
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
                }
                entry.session.turn_count = entry.session.turn_count.max(1);
                if let Some(rate_limits) = rate_limits {
                    state.rate_limits = Some(rate_limits);
                }
            }
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
                let elapsed = Utc::now() - entry.started_at;
                state.completed_totals.input_tokens += entry.session.codex_input_tokens;
                state.completed_totals.output_tokens += entry.session.codex_output_tokens;
                state.completed_totals.total_tokens += entry.session.codex_total_tokens;
                state.completed_totals.seconds_running += duration_to_seconds(elapsed.to_std().unwrap_or_default());
            }
            drop(state);
            control.lock().await.handles.remove(&issue_id);
            if success && continuation {
                schedule_retry(config, runtime, cmd_tx, issue_id.clone(), issue.identifier.clone(), attempt.unwrap_or(0) + 1, Some("continuation".to_string()), Duration::from_secs(1)).await;
            } else if !success {
                let next_attempt = attempt.unwrap_or(0) + 1;
                let delay_ms = failure_backoff_ms(config, next_attempt);
                schedule_retry(config, runtime, cmd_tx, issue_id.clone(), issue.identifier.clone(), next_attempt, error, Duration::from_millis(delay_ms)).await;
            } else if terminal_issue_state(config, &issue.state) {
                workspace::remove_issue_workspace(config, &issue.identifier).await;
            }
        }
    }
}

async fn schedule_retry(
    _config: &Arc<ServiceConfig>,
    runtime: &Arc<Mutex<RuntimeState>>,
    cmd_tx: &mpsc::UnboundedSender<Command>,
    issue_id: String,
    identifier: String,
    attempt: u32,
    error: Option<String>,
    delay: Duration,
) {
    let due_at = Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default();
    {
        let mut state = runtime.lock().await;
        state.retrying.insert(
            issue_id.clone(),
            RetryEntry {
                issue_id: issue_id.clone(),
                identifier,
                attempt,
                due_at,
                error: error.clone(),
            },
        );
    }
    let cmd_tx = cmd_tx.clone();
    tokio::spawn(async move {
        sleep(delay).await;
        let _ = cmd_tx.send(Command::RetryDue(DueRetry {
            issue_id,
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
                if terminal_issue_state(config, &issue.state)
                    || !active_issue_state(config, &issue.state)
                    || !issue_routable(config, issue)
                {
                    to_abort.push((issue.id.clone(), issue.identifier.clone(), terminal_issue_state(config, &issue.state)));
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
            schedule_retry(
                config,
                runtime,
                cmd_tx,
                entry.issue_id.clone(),
                entry.identifier.clone(),
                entry.attempt.unwrap_or(0) + 1,
                Some(format!(
                    "stalled for {}ms without codex activity",
                    elapsed.num_milliseconds()
                )),
                Duration::from_millis(failure_backoff_ms(config, entry.attempt.unwrap_or(0) + 1)),
            )
            .await;
        }
    }
}

async fn publish_snapshot(runtime: &Arc<Mutex<RuntimeState>>, snapshot_tx: &watch::Sender<Snapshot>) {
    let state = runtime.lock().await;
    let mut running = state.running.values().cloned().collect::<Vec<_>>();
    running.sort_by_key(|entry| entry.identifier.clone());
    let mut retrying = state.retrying.values().cloned().collect::<Vec<_>>();
    retrying.sort_by_key(|entry| entry.due_at);
    let active_totals = running.iter().fold(CodexTotals::default(), |mut acc, entry| {
        acc.input_tokens += entry.session.codex_input_tokens;
        acc.output_tokens += entry.session.codex_output_tokens;
        acc.total_tokens += entry.session.codex_total_tokens;
        acc.seconds_running += duration_to_seconds((Utc::now() - entry.started_at).to_std().unwrap_or_default());
        acc
    });
    let snapshot = Snapshot {
        running,
        retrying,
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
        claimed = state.claimed.len(),
        "orchestrator state updated"
    );
}

async fn should_dispatch_issue(config: &Arc<ServiceConfig>, runtime: &Arc<Mutex<RuntimeState>>, issue: &Issue) -> bool {
    let state = runtime.lock().await;
    candidate_issue(config, issue)
        && !todo_issue_blocked(config, issue)
        && !state.claimed.contains_key(&issue.id)
        && !state.running.contains_key(&issue.id)
        && state.running.len() < config.max_concurrent_agents
        && running_issue_count_for_state(&state.running, &issue.state) < config.state_limit(&issue.state)
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
        && issue
            .blocked_by
            .iter()
            .any(|blocker| blocker.state.as_deref().map(|state| !terminal_issue_state(config, state)).unwrap_or(false))
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
                    last_codex_message: Some(format!("running cargo test and updating UI ({step})")),
                    codex_input_tokens: 18_000 + step * 16,
                    codex_output_tokens: 2_600 + step * 6,
                    codex_total_tokens: 20_600 + step * 22,
                    turn_count: 3 + (step % 3),
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
                    description: Some("Second demo issue to keep the dashboard populated".to_string()),
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
                    last_codex_message: Some("agent message content streaming: websocket patch applied".to_string()),
                    codex_input_tokens: 42_000 + step * 10,
                    codex_output_tokens: 5_000 + step * 3,
                    codex_total_tokens: 47_000 + step * 13,
                    turn_count: 7,
                },
            },
        ],
        retrying: vec![
            RetryEntry {
                issue_id: "demo-3".to_string(),
                identifier: "SYM-305".to_string(),
                attempt: 4,
                due_at: now + chrono::Duration::seconds(12),
                error: Some("rate limit exhausted".to_string()),
            },
            RetryEntry {
                issue_id: "demo-4".to_string(),
                identifier: "SYM-401".to_string(),
                attempt: 2,
                due_at: now + chrono::Duration::seconds(27),
                error: Some("worker crashed; restarting cleanly".to_string()),
            },
        ],
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
