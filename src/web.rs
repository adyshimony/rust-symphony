use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Utc;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
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

pub async fn spawn_server(config: Arc<ServiceConfig>, orchestrator: OrchestratorHandle) -> anyhow::Result<()> {
    let Some(port) = config.server_port else {
        return Ok(());
    };
    let state = WebState { orchestrator, config: config.clone() };
    let app = Router::new()
        .route("/", get(index))
        .route("/dashboard.css", get(styles))
        .route("/dashboard.js", get(script))
        .route("/ws", get(ws))
        .route("/api/v1/state", get(state_api))
        .route("/api/v1/refresh", post(refresh_api))
        .route("/api/v1/{issue_identifier}", get(issue_api))
        .fallback(not_found)
        .with_state(state);
    let addr: SocketAddr = format!("{}:{port}", config.server_host).parse()?;
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(addr).await.expect("bind web listener");
        axum::serve(listener, app).await.expect("serve web ui");
    });
    Ok(())
}

async fn index(State(state): State<WebState>) -> Html<String> {
    let snapshot = state.orchestrator.snapshot().await;
    Html(render_dashboard_html(&state.config, &snapshot))
}

async fn styles() -> Response {
    (StatusCode::OK, [("content-type", "text/css")], DASHBOARD_CSS).into_response()
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
    (StatusCode::ACCEPTED, Json(presenter::refresh_payload(refresh)))
}

async fn issue_api(
    Path(issue_identifier): Path<String>,
    State(state): State<WebState>,
) -> Result<Json<presenter::IssuePayload>, (StatusCode, Json<serde_json::Value>)> {
    let snapshot = state.orchestrator.snapshot().await;
    presenter::issue_payload(&issue_identifier, &state.config, &snapshot)
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": { "code": "issue_not_found", "message": "Issue not found" }
                })),
            )
        })
}

async fn not_found() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "error": { "code": "not_found", "message": "Route not found" }
        })),
    )
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
    let running_rows = payload
        .running
        .as_ref()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|entry| {
            let health = run_health(entry);
            format!(
                r#"<tr><td><div class="issue-stack"><span class="issue-id">{}</span><span class="issue-title">{}</span><a class="issue-link" href="/api/v1/{}">JSON details</a></div></td><td><div class="detail-stack"><span class="state-text">{}</span><span class="health-chip {}">{}</span></div></td><td><div class="detail-stack"><span>{}</span><span class="muted">Last update {}</span><span class="muted">Idle {}</span></div></td><td><div class="detail-stack"><span>PID {} · turn {}</span><span class="muted mono">session {}</span><span class="muted mono">thread {}</span></div></td><td><div class="detail-stack"><span class="event-text">{}</span><span class="muted">{}</span></div></td><td><div class="token-stack numeric"><span>Total: {}</span><span class="muted">In {} / Out {}</span><span class="muted workspace-path">{}</span>{}</div></td></tr>"#,
                escape_html(&entry.issue_identifier),
                escape_html(&entry.issue_title),
                issue_api_path(&entry.issue_identifier),
                escape_html(&entry.state),
                health.class_name,
                health.label,
                format_runtime(&entry.started_at),
                format_runtime(&entry.last_event_at),
                format_idle(&entry.started_at, &entry.last_event_at),
                escape_html(entry.codex_app_server_pid.as_deref().unwrap_or("n/a")),
                entry.turn_count,
                escape_html(entry.session_id.as_deref().unwrap_or("n/a")),
                escape_html(entry.thread_id.as_deref().unwrap_or("n/a")),
                escape_html(entry.last_event.as_deref().unwrap_or("n/a")),
                escape_html(entry.last_message.as_deref().unwrap_or("n/a")),
                format_int(entry.tokens.total_tokens),
                format_int(entry.tokens.input_tokens),
                format_int(entry.tokens.output_tokens),
                escape_html(&entry.workspace_path),
                telemetry_warning_html(entry.tokens.total_tokens),
            )
        })
        .collect::<String>();
    let retry_rows = payload
        .retrying
        .as_ref()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|entry| {
            format!(
                r#"<tr><td><div class="issue-stack"><span class="issue-id">{}</span><a class="issue-link" href="/api/v1/{}">JSON details</a></div></td><td>{}</td><td class="mono">{}</td><td>{}</td></tr>"#,
                escape_html(&entry.issue_identifier),
                escape_html(&entry.issue_identifier),
                entry.attempt,
                escape_html(entry.due_at.as_deref().unwrap_or("n/a")),
                escape_html(entry.error.as_deref().unwrap_or("n/a")),
            )
        })
        .collect::<String>();
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
        <article class="metric-card metric-card-tokens"><p class="metric-label">Global tokens</p><p class="metric-value numeric" id="metric-total-tokens">{total_tokens}</p><p class="metric-detail numeric">In <span id="metric-input-tokens">{input_tokens}</span> / Out <span id="metric-output-tokens">{output_tokens}</span></p></article>
        <article class="metric-card metric-card-runtime"><p class="metric-label">Runtime</p><p class="metric-value numeric" id="metric-runtime">{runtime}</p><p class="metric-detail">Total Codex runtime across completed and active sessions.</p></article>
      </section>
      <section class="content-grid">
        <div class="content-primary">
          <section class="section-card monitor-strip">
            <div class="section-header"><div><h2 class="section-title">Runtime monitor</h2><p class="section-copy">Immediate health signals for active runs.</p></div></div>
            <div class="monitor-alert" id="runtime-alert">{runtime_alert}</div>
          </section>
          <section class="section-card section-card-large">
            <div class="section-header"><div><h2 class="section-title">Running sessions</h2><p class="section-copy">Active issues with status, idle time, session details, workspace path, and telemetry health.</p></div><div class="section-pill">{running} active</div></div>
            <div class="table-wrap" id="running-wrap"><table class="data-table data-table-running"><thead><tr><th>Issue</th><th>Status</th><th>Timing</th><th>Session</th><th>Codex update</th><th>Tokens / workspace</th></tr></thead><tbody id="running-body" data-colspan="6">{running_rows}</tbody></table></div>
          </section>
          <section class="section-card">
            <div class="section-header"><div><h2 class="section-title">Retry queue</h2><p class="section-copy">Issues waiting for the next retry window.</p></div><div class="section-pill section-pill-warm">{retrying} queued</div></div>
            <div class="table-wrap" id="retry-wrap"><table class="data-table"><thead><tr><th>Issue</th><th>Attempt</th><th>Due at</th><th>Error</th></tr></thead><tbody id="retry-body">{retry_rows}</tbody></table></div>
          </section>
        </div>
        <aside class="content-secondary">
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
            .map(|totals| format!("{:.0}m {:.0}s", (totals.seconds_running / 60.0).floor(), totals.seconds_running % 60.0))
            .unwrap_or_else(|| "0m 0s".to_string()),
        runtime_alert = escape_html(&runtime_alert(payload.running.as_ref().unwrap_or(&Vec::new()))),
        rate_limits = escape_html(&serde_json::to_string_pretty(&payload.rate_limits).unwrap_or_else(|_| "null".to_string())),
        running_rows = running_rows,
        retry_rows = retry_rows,
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

fn issue_api_path(issue_identifier: &str) -> String {
    format!(
        "/api/v1/{}",
        issue_identifier.replace('%', "%25").replace('/', "%2F").replace('#', "%23")
    )
}

fn format_runtime(started_at: &Option<String>) -> String {
    started_at.clone().unwrap_or_else(|| "n/a".to_string())
}

fn format_idle(started_at: &Option<String>, last_event_at: &Option<String>) -> String {
    let started = started_at
        .as_deref()
        .and_then(parse_rfc3339)
        .unwrap_or_else(Utc::now);
    let last = last_event_at
        .as_deref()
        .and_then(parse_rfc3339)
        .unwrap_or(started);
    human_duration((Utc::now() - last).num_seconds().max(0))
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

fn human_duration(total_secs: i64) -> String {
    let secs = total_secs.max(0);
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    let rem = secs % 60;
    if hours > 0 {
        format!("{hours}h {mins}m {rem}s")
    } else if mins > 0 {
        format!("{mins}m {rem}s")
    } else {
        format!("{rem}s")
    }
}

struct RunHealth<'a> {
    label: &'a str,
    class_name: &'a str,
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
            class_name: "health-chip-starting",
        }
    } else if idle_secs >= 300 {
        RunHealth {
            label: "stalled?",
            class_name: "health-chip-stalled",
        }
    } else if entry.tokens.total_tokens == 0 {
        RunHealth {
            label: "active/no-usage",
            class_name: "health-chip-no-usage",
        }
    } else {
        RunHealth {
            label: "active",
            class_name: "health-chip-active",
        }
    }
}

fn telemetry_warning_html(total_tokens: u64) -> String {
    if total_tokens == 0 {
        "<span class=\"warning-text\">No token telemetry yet</span>".to_string()
    } else {
        String::new()
    }
}

fn runtime_alert(entries: &[presenter::RunningEntryPayload]) -> String {
    if entries.is_empty() {
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
