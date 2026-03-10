use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use futures_util::StreamExt;
use tokio::sync::watch;

use crate::config::ServiceConfig;
use crate::orchestrator::OrchestratorHandle;
use crate::presenter;

const DASHBOARD_CSS: &str = include_str!("../assets/dashboard.css");
const DASHBOARD_JS: &str = include_str!("../assets/dashboard.js");

#[derive(Clone)]
struct WebState {
    orchestrator: OrchestratorHandle,
    config: Arc<ServiceConfig>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct TailQuery {
    tail: Option<usize>,
}

pub async fn spawn_server(
    config: Arc<ServiceConfig>,
    orchestrator: OrchestratorHandle,
) -> anyhow::Result<()> {
    let Some(port) = config.server_port else {
        return Ok(());
    };
    let state = WebState {
        orchestrator,
        config: config.clone(),
    };
    let app = Router::new()
        .route("/", get(index))
        .route("/dashboard.css", get(styles))
        .route("/dashboard.js", get(script))
        .route("/ws", get(ws))
        .route("/api/v1/state", get(state_api))
        .route("/api/v1/refresh", post(refresh_api))
        .route(
            "/api/v1/issues/{issue_identifier}/pause",
            post(pause_issue_api),
        )
        .route(
            "/api/v1/issues/{issue_identifier}/resume",
            post(resume_issue_api),
        )
        .route(
            "/api/v1/issues/{issue_identifier}/stop",
            post(stop_issue_api),
        )
        .route(
            "/api/v1/issues/{issue_identifier}/retry",
            post(retry_issue_api),
        )
        .route(
            "/api/v1/issues/{issue_identifier}/logs/stdout",
            get(stdout_log_api),
        )
        .route(
            "/api/v1/issues/{issue_identifier}/logs/stderr",
            get(stderr_log_api),
        )
        .route(
            "/api/v1/issues/{issue_identifier}/reports/discovery",
            get(discovery_report_api),
        )
        .route(
            "/api/v1/issues/{issue_identifier}/reports/progress",
            get(progress_report_api),
        )
        .route("/api/v1/{issue_identifier}", get(issue_api))
        .fallback(not_found)
        .with_state(state);
    let addr: SocketAddr = format!("{}:{port}", config.server_host).parse()?;
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("bind web listener");
        axum::serve(listener, app).await.expect("serve web ui");
    });
    Ok(())
}

async fn index(State(state): State<WebState>) -> Html<String> {
    let snapshot = state.orchestrator.snapshot().await;
    Html(render_dashboard_html(&state.config, &snapshot))
}

async fn styles() -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/css")],
        DASHBOARD_CSS,
    )
        .into_response()
}

async fn script() -> Response {
    (
        StatusCode::OK,
        [("content-type", "application/javascript")],
        DASHBOARD_JS,
    )
        .into_response()
}

async fn ws(ws: WebSocketUpgrade, State(state): State<WebState>) -> Response {
    ws.on_upgrade(move |socket| websocket_loop(socket, state.orchestrator.subscribe()))
}

async fn state_api(State(state): State<WebState>) -> Json<presenter::StatePayload> {
    let snapshot = state.orchestrator.snapshot().await;
    Json(presenter::state_payload(Some(&snapshot)))
}

async fn refresh_api(State(state): State<WebState>) -> impl IntoResponse {
    let refresh = state.orchestrator.request_refresh().await;
    (
        StatusCode::ACCEPTED,
        Json(presenter::refresh_payload(refresh)),
    )
}

async fn pause_issue_api(
    Path(issue_identifier): Path<String>,
    State(state): State<WebState>,
) -> impl IntoResponse {
    control_response(state.orchestrator.pause_issue(issue_identifier).await)
}

async fn resume_issue_api(
    Path(issue_identifier): Path<String>,
    State(state): State<WebState>,
) -> impl IntoResponse {
    control_response(state.orchestrator.resume_issue(issue_identifier).await)
}

async fn stop_issue_api(
    Path(issue_identifier): Path<String>,
    State(state): State<WebState>,
) -> impl IntoResponse {
    control_response(state.orchestrator.stop_issue(issue_identifier).await)
}

async fn retry_issue_api(
    Path(issue_identifier): Path<String>,
    State(state): State<WebState>,
) -> impl IntoResponse {
    control_response(state.orchestrator.retry_issue(issue_identifier).await)
}

async fn issue_api(
    Path(issue_identifier): Path<String>,
    State(state): State<WebState>,
) -> Result<Json<presenter::IssuePayload>, (StatusCode, Json<serde_json::Value>)> {
    let snapshot = state.orchestrator.snapshot().await;
    issue_payload_or_404(&issue_identifier, &state.config, &snapshot).map(Json)
}

async fn stdout_log_api(
    Path(issue_identifier): Path<String>,
    Query(query): Query<TailQuery>,
    State(state): State<WebState>,
) -> Response {
    artifact_response(
        artifact_path(&state, &issue_identifier, |payload| payload.logs.stdout_log_path.clone()).await,
        query.tail,
    )
    .await
}

async fn stderr_log_api(
    Path(issue_identifier): Path<String>,
    Query(query): Query<TailQuery>,
    State(state): State<WebState>,
) -> Response {
    artifact_response(
        artifact_path(&state, &issue_identifier, |payload| payload.logs.stderr_log_path.clone()).await,
        query.tail,
    )
    .await
}

async fn discovery_report_api(
    Path(issue_identifier): Path<String>,
    Query(query): Query<TailQuery>,
    State(state): State<WebState>,
) -> Response {
    artifact_response(
        artifact_path(&state, &issue_identifier, |payload| payload.logs.discovery_report_path.clone()).await,
        query.tail,
    )
    .await
}

async fn progress_report_api(
    Path(issue_identifier): Path<String>,
    Query(query): Query<TailQuery>,
    State(state): State<WebState>,
) -> Response {
    artifact_response(
        artifact_path(&state, &issue_identifier, |payload| payload.logs.progress_report_path.clone()).await,
        query.tail,
    )
    .await
}

async fn not_found() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "error": { "code": "not_found", "message": "Route not found" }
        })),
    )
}

fn control_response(result: crate::model::ControlActionResult) -> impl IntoResponse {
    let status = match result.status {
        crate::model::ControlActionStatus::Accepted => StatusCode::ACCEPTED,
        crate::model::ControlActionStatus::NotFound => StatusCode::NOT_FOUND,
        crate::model::ControlActionStatus::Conflict => StatusCode::CONFLICT,
    };
    (status, Json(presenter::control_payload(result)))
}

fn issue_payload_or_404(
    issue_identifier: &str,
    config: &ServiceConfig,
    snapshot: &crate::model::Snapshot,
) -> Result<presenter::IssuePayload, (StatusCode, Json<serde_json::Value>)> {
    presenter::issue_payload(issue_identifier, config, snapshot).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": { "code": "issue_not_found", "message": "Issue not found" }
            })),
        )
    })
}

async fn artifact_path(
    state: &WebState,
    issue_identifier: &str,
    select: impl FnOnce(&presenter::IssuePayload) -> Option<String>,
) -> Result<String, (StatusCode, Json<serde_json::Value>)> {
    let snapshot = state.orchestrator.snapshot().await;
    let payload = issue_payload_or_404(issue_identifier, &state.config, &snapshot)?;
    select(&payload).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": { "code": "artifact_not_found", "message": "Artifact path unavailable" }
            })),
        )
    })
}

async fn artifact_response(
    path_result: Result<String, (StatusCode, Json<serde_json::Value>)>,
    tail: Option<usize>,
) -> Response {
    match path_result {
        Ok(path) => match tokio::fs::read_to_string(&path).await {
            Ok(contents) => {
                let body = match tail {
                    Some(lines) => tail_lines(&contents, lines),
                    None => contents,
                };
                (StatusCode::OK, [("content-type", "text/plain; charset=utf-8")], body)
                    .into_response()
            }
            Err(_) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": { "code": "artifact_missing", "message": "Artifact file not found" }
                })),
            )
                .into_response(),
        },
        Err(error) => error.into_response(),
    }
}

async fn websocket_loop(mut socket: WebSocket, mut rx: watch::Receiver<crate::model::Snapshot>) {
    let initial = rx.borrow().clone();
    let _ = socket
        .send(Message::Text(
            serde_json::to_string(&presenter::state_payload(Some(&initial)))
                .unwrap_or_else(|_| "{}".to_string())
                .into(),
        ))
        .await;
    loop {
        tokio::select! {
            changed = rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let payload = presenter::state_payload(Some(&rx.borrow().clone()));
                if socket.send(Message::Text(serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string()).into())).await.is_err() {
                    break;
                }
            }
            incoming = socket.next() => {
                if incoming.is_none() {
                    break;
                }
            }
        }
    }
}

fn render_dashboard_html(config: &ServiceConfig, snapshot: &crate::model::Snapshot) -> String {
    let payload = presenter::state_payload(Some(snapshot));
    let live_log_issue = payload
        .running
        .as_ref()
        .and_then(|entries| entries.first().map(|entry| entry.issue_identifier.clone()))
        .or_else(|| {
            payload
                .needs_review
                .as_ref()
                .and_then(|entries| entries.first().map(|entry| entry.issue_identifier.clone()))
        })
        .or_else(|| {
            payload
                .held
                .as_ref()
                .and_then(|entries| entries.first().map(|entry| entry.issue_identifier.clone()))
        })
        .or_else(|| {
            payload
                .paused
                .as_ref()
                .and_then(|entries| entries.first().map(|entry| entry.issue_identifier.clone()))
        })
        .unwrap_or_default();
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Symphony Operations Console</title>
  <link rel="stylesheet" href="/dashboard.css" />
</head>
<body data-project-url="{project_url}" data-runtime-mode="{runtime_mode}">
  <main class="app-shell">
    <section class="dashboard-shell">
      <header class="hero-card">
        <div class="hero-grid">
          <div class="hero-copy-wrap">
            <div class="hero-meta-row">
              <p class="eyebrow">Symphony Control Plane</p>
              <span class="mode-badge" id="mode-badge">{mode_label}</span>
            </div>
            <h1 class="hero-title">Operations Console</h1>
            <p class="hero-copy">Live orchestration state, retry pressure, rate limits, token burn, and agent activity across the current Symphony runtime.</p>
            <div class="hero-actions">
              <a class="action-pill action-pill-primary" href="{project_url}" target="_blank" rel="noreferrer">{project_link_label}</a>
              <button id="refresh-now" class="action-pill action-pill-secondary" type="button">Refresh Now</button>
            </div>
          </div>
          <div class="hero-sidecar">
            <div class="status-stack">
              <span class="status-badge status-badge-live" id="connection-live"><span class="status-badge-dot"></span>Connected</span>
              <span class="status-badge status-badge-offline" id="connection-offline"><span class="status-badge-dot"></span>Offline</span>
            </div>
            <div class="pulse-card">
              <p class="pulse-label">Tracker</p>
              <p class="pulse-value">{tracker_name}</p>
              <p class="pulse-detail">Last update <span id="generated-at">{generated_at}</span></p>
            </div>
          </div>
        </div>
      </header>
      <section class="metric-grid">
        <article class="metric-card metric-card-running"><p class="metric-label">Running</p><p class="metric-value numeric" id="metric-running">{running}</p><p class="metric-detail">Active issue sessions in the current runtime.</p></article>
        <article class="metric-card metric-card-retrying"><p class="metric-label">Retrying</p><p class="metric-value numeric" id="metric-retrying">{retrying}</p><p class="metric-detail">Issues waiting for the next retry window.</p></article>
        <article class="metric-card metric-card-runtime"><p class="metric-label">Paused</p><p class="metric-value numeric" id="metric-paused">{paused}</p><p class="metric-detail">Runs manually paused by the operator.</p></article>
        <article class="metric-card metric-card-runtime"><p class="metric-label">Held</p><p class="metric-value numeric" id="metric-held">{held}</p><p class="metric-detail">Runs stopped and waiting for resume or retry.</p></article>
        <article class="metric-card metric-card-runtime"><p class="metric-label">Needs Review</p><p class="metric-value numeric" id="metric-review">{needs_review}</p><p class="metric-detail">Runs paused for operator inspection.</p></article>
        <article class="metric-card metric-card-tokens"><p class="metric-label">Global tokens</p><p class="metric-value numeric" id="metric-total-tokens">{total_tokens}</p><p class="metric-detail numeric">In <span id="metric-input-tokens">{input_tokens}</span> / Out <span id="metric-output-tokens">{output_tokens}</span></p></article>
        <article class="metric-card metric-card-runtime"><p class="metric-label">Runtime</p><p class="metric-value numeric" id="metric-runtime">{runtime}</p><p class="metric-detail">Total Codex runtime across completed and active sessions.</p></article>
      </section>
      <section class="content-grid">
        <div class="content-primary">
          <section class="section-card monitor-strip">
            <div class="section-header"><div><h2 class="section-title">Runtime monitor</h2><p class="section-copy">Immediate health signals for active runs.</p></div></div>
            <div class="monitor-alert" id="runtime-alert">{runtime_alert}</div>
            <div class="operator-feedback" id="action-feedback">Operator actions will appear here.</div>
          </section>
          <section class="section-card section-card-large">
            <div class="section-header"><div><h2 class="section-title">Running sessions</h2><p class="section-copy">Active issues with phase, idle time, session details, workspace path, commands, diff stats, and telemetry health.</p></div><div class="section-pill">{running} active</div></div>
            <div class="table-wrap" id="running-wrap"><table class="data-table data-table-running"><thead><tr><th>Issue</th><th>Status</th><th>Timing</th><th>Session</th><th>Codex update</th><th>Tokens / workspace</th><th>Actions</th></tr></thead><tbody id="running-body" data-colspan="7"></tbody></table></div>
          </section>
          <section class="section-card">
            <div class="section-header"><div><h2 class="section-title">Paused</h2><p class="section-copy">Runs that are blocked until you resume them.</p></div><div class="section-pill">{paused} paused</div></div>
            <div class="table-wrap" id="paused-wrap"><table class="data-table"><thead><tr><th>Issue</th><th>Status</th><th>Execution context</th><th>Actions</th></tr></thead><tbody id="paused-body" data-colspan="4"></tbody></table></div>
          </section>
          <section class="section-card">
            <div class="section-header"><div><h2 class="section-title">Held</h2><p class="section-copy">Runs stopped by the operator and waiting for follow-up.</p></div><div class="section-pill section-pill-warm">{held} held</div></div>
            <div class="table-wrap" id="held-wrap"><table class="data-table"><thead><tr><th>Issue</th><th>Status</th><th>Execution context</th><th>Actions</th></tr></thead><tbody id="held-body" data-colspan="4"></tbody></table></div>
          </section>
          <section class="section-card">
            <div class="section-header"><div><h2 class="section-title">Needs Review</h2><p class="section-copy">Runs paused by guardrails so you can inspect them before continuing.</p></div><div class="section-pill section-pill-warm">{needs_review} waiting</div></div>
            <div class="table-wrap" id="review-wrap"><table class="data-table"><thead><tr><th>Issue</th><th>Status</th><th>Reason</th><th>Execution context</th><th>Actions</th></tr></thead><tbody id="review-body" data-colspan="5"></tbody></table></div>
          </section>
          <section class="section-card">
            <div class="section-header"><div><h2 class="section-title">Retry queue</h2><p class="section-copy">Issues waiting for the next retry window.</p></div><div class="section-pill section-pill-warm">{retrying} queued</div></div>
            <div class="table-wrap" id="retry-wrap"><table class="data-table"><thead><tr><th>Issue</th><th>Attempt</th><th>Due at</th><th>Error</th><th>Actions</th></tr></thead><tbody id="retry-body" data-colspan="5"></tbody></table></div>
          </section>
        </div>
        <aside class="content-secondary">
          <section class="section-card insight-card">
            <div class="section-header"><div><h2 class="section-title">Live stdout tail</h2><p class="section-copy">Latest Codex stdout lines for the primary active or review-blocked issue.</p></div></div>
            <div class="context-list">
              <div class="context-row"><span class="context-label">Issue</span><span class="context-value" id="log-issue">{live_log_issue}</span></div>
            </div>
            <pre class="code-panel" id="stdout-tail">No log stream selected.</pre>
          </section>
          <section class="section-card insight-card">
            <div class="section-header"><div><h2 class="section-title">Rate limits</h2><p class="section-copy">Latest upstream rate-limit snapshot, when available.</p></div></div>
            <pre class="code-panel" id="rate-limits">{rate_limits}</pre>
          </section>
          <section class="section-card insight-card">
            <div class="section-header"><div><h2 class="section-title">Context</h2><p class="section-copy">Runtime mode, tracker scope, and dashboard endpoints.</p></div></div>
            <div class="context-list">
              <div class="context-row"><span class="context-label">Mode</span><span class="context-value" id="context-mode">{mode_label}</span></div>
              <div class="context-row"><span class="context-label">Tracker</span><span class="context-value">{tracker_name}</span></div>
              <div class="context-row"><span class="context-label">Project</span><a class="context-link" href="{project_url}" target="_blank" rel="noreferrer">{project_context_label}</a></div>
              <div class="context-row"><span class="context-label">API</span><a class="context-link" href="/api/v1/state" target="_blank" rel="noreferrer">Current snapshot JSON</a></div>
            </div>
          </section>
        </aside>
      </section>
    </section>
  </main>
  <script>window.__SYMPHONY_INITIAL_STATE__ = {state_json};</script>
  <script src="/dashboard.js"></script>
</body>
</html>"#,
        project_url = escape_html(&project_url(config)),
        project_link_label = escape_html(project_link_label(config)),
        project_context_label = escape_html(project_context_label(config)),
        runtime_mode = escape_html(runtime_mode(config)),
        mode_label = escape_html(runtime_mode_label(config)),
        tracker_name = escape_html(&tracker_name(config)),
        generated_at = escape_html(&payload.generated_at),
        running = payload.counts.as_ref().map(|c| c.running).unwrap_or(0),
        retrying = payload.counts.as_ref().map(|c| c.retrying).unwrap_or(0),
        paused = payload.counts.as_ref().map(|c| c.paused).unwrap_or(0),
        held = payload.counts.as_ref().map(|c| c.held).unwrap_or(0),
        needs_review = payload.counts.as_ref().map(|c| c.needs_review).unwrap_or(0),
        total_tokens = payload
            .codex_totals
            .as_ref()
            .map(|totals| format_int(totals.total_tokens))
            .unwrap_or_else(|| "0".to_string()),
        input_tokens = payload
            .codex_totals
            .as_ref()
            .map(|totals| format_int(totals.input_tokens))
            .unwrap_or_else(|| "0".to_string()),
        output_tokens = payload
            .codex_totals
            .as_ref()
            .map(|totals| format_int(totals.output_tokens))
            .unwrap_or_else(|| "0".to_string()),
        runtime = payload
            .codex_totals
            .as_ref()
            .map(|totals| format!(
                "{:.0}m {:.0}s",
                (totals.seconds_running / 60.0).floor(),
                totals.seconds_running % 60.0
            ))
            .unwrap_or_else(|| "0m 0s".to_string()),
        runtime_alert = escape_html(&runtime_alert(
            payload.running.as_ref().unwrap_or(&Vec::new()),
            payload.needs_review.as_ref().unwrap_or(&Vec::new())
        )),
        rate_limits = escape_html(
            &serde_json::to_string_pretty(&payload.rate_limits)
                .unwrap_or_else(|_| "null".to_string())
        ),
        live_log_issue = escape_html(&live_log_issue),
        state_json = serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string()),
    )
}

fn runtime_mode(config: &ServiceConfig) -> &str {
    match config.tracker_kind.as_deref() {
        Some("memory") => "demo",
        _ => "live",
    }
}

fn runtime_mode_label(config: &ServiceConfig) -> &str {
    match config.tracker_kind.as_deref() {
        Some("memory") => "Demo Mode",
        Some("github") => "GitHub Runtime",
        _ => "Live Runtime",
    }
}

fn tracker_name(config: &ServiceConfig) -> String {
    match config.tracker_kind.as_deref() {
        Some("github") => format!(
            "GitHub / {}/{}",
            config.github_owner.as_deref().unwrap_or("unknown"),
            config.github_repo.as_deref().unwrap_or("unknown")
        ),
        Some("memory") => "Built-in Demo Feed".to_string(),
        _ => format!(
            "Linear / {}",
            config.linear_project_slug.as_deref().unwrap_or("unknown")
        ),
    }
}

fn project_url(config: &ServiceConfig) -> String {
    match config.tracker_kind.as_deref() {
        Some("github") => {
            let owner = config.github_owner.as_deref().unwrap_or("");
            let repo = config.github_repo.as_deref().unwrap_or("");
            format!("https://github.com/{owner}/{repo}/issues")
        }
        Some("memory") => "/api/v1/state".to_string(),
        _ => {
            let project = config.linear_project_slug.as_deref().unwrap_or("");
            format!("https://linear.app/project/{project}/issues")
        }
    }
}

fn project_link_label(config: &ServiceConfig) -> &'static str {
    match config.tracker_kind.as_deref() {
        Some("memory") => "Open Snapshot",
        _ => "Open Tracker",
    }
}

fn project_context_label(config: &ServiceConfig) -> &'static str {
    match config.tracker_kind.as_deref() {
        Some("memory") => "Runtime snapshot",
        _ => "Open source view",
    }
}

fn format_int(value: u64) -> String {
    let text = value.to_string();
    let mut out = String::with_capacity(text.len() + text.len() / 3);
    for (idx, ch) in text.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn parse_rfc3339(value: &str) -> Option<chrono::DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

struct RunHealth<'a> {
    label: &'a str,
}

fn run_health(entry: &presenter::RunningEntryPayload) -> RunHealth<'static> {
    let started = entry
        .started_at
        .as_deref()
        .and_then(parse_rfc3339)
        .unwrap_or_else(Utc::now);
    let last = entry
        .last_event_at
        .as_deref()
        .and_then(parse_rfc3339)
        .unwrap_or(started);
    let idle_secs = (Utc::now() - last).num_seconds().max(0);
    if entry.last_event_at.is_none() {
        RunHealth {
            label: "starting",
        }
    } else if idle_secs >= 300 {
        RunHealth {
            label: "stalled?",
        }
    } else if entry.tokens.total_tokens == 0 {
        RunHealth {
            label: "active/no-usage",
        }
    } else {
        RunHealth {
            label: "active",
        }
    }
}

fn tail_lines(contents: &str, lines: usize) -> String {
    if lines == 0 {
        return String::new();
    }
    let collected = contents.lines().rev().take(lines).collect::<Vec<_>>();
    collected.into_iter().rev().collect::<Vec<_>>().join("\n")
}

fn runtime_alert(
    entries: &[presenter::RunningEntryPayload],
    review_entries: &[presenter::BlockedEntryPayload],
) -> String {
    if !review_entries.is_empty() {
        "At least one run is paused for operator review.".to_string()
    } else if entries.is_empty() {
        "No active runs. The runtime is currently idle.".to_string()
    } else if entries
        .iter()
        .any(|entry| run_health(entry).label == "stalled?")
    {
        "At least one run looks stalled: last Codex update is older than 5 minutes.".to_string()
    } else if entries.iter().any(|entry| entry.tokens.total_tokens == 0) {
        "At least one run has started but has not reported token usage yet.".to_string()
    } else {
        "Live run telemetry is streaming normally.".to_string()
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    use super::control_response;
    use crate::model::{ControlActionResult, ControlActionStatus};

    #[test]
    fn control_response_maps_status_codes() {
        let accepted = control_response(ControlActionResult {
            status: ControlActionStatus::Accepted,
            issue_identifier: "repo#1".to_string(),
            message: "ok".to_string(),
        })
        .into_response();
        let not_found = control_response(ControlActionResult {
            status: ControlActionStatus::NotFound,
            issue_identifier: "repo#1".to_string(),
            message: "missing".to_string(),
        })
        .into_response();
        let conflict = control_response(ControlActionResult {
            status: ControlActionStatus::Conflict,
            issue_identifier: "repo#1".to_string(),
            message: "bad".to_string(),
        })
        .into_response();

        assert_eq!(accepted.status(), StatusCode::ACCEPTED);
        assert_eq!(not_found.status(), StatusCode::NOT_FOUND);
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
    }
}
