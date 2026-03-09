(function () {
  const state = window.__SYMPHONY_INITIAL_STATE__ || null;
  const mode = document.body.dataset.runtimeMode || "live";

  function formatInt(value) {
    if (typeof value !== "number") return "0";
    return value.toLocaleString("en-US");
  }

  function formatRuntime(seconds) {
    if (typeof seconds !== "number") return "0m 0s";
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${mins}m ${secs}s`;
  }

  function formatTimestamp(value) {
    if (!value) return "n/a";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return date.toLocaleString([], {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit"
    });
  }

  function parseTimestampMs(value) {
    if (!value) return null;
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return null;
    return date.getTime();
  }

  function formatDurationSeconds(totalSeconds) {
    const secs = Math.max(0, Math.floor(totalSeconds || 0));
    const hours = Math.floor(secs / 3600);
    const mins = Math.floor((secs % 3600) / 60);
    const rem = secs % 60;
    if (hours > 0) return `${hours}h ${mins}m ${rem}s`;
    if (mins > 0) return `${mins}m ${rem}s`;
    return `${rem}s`;
  }

  function runHealth(entry) {
    const startedMs = parseTimestampMs(entry.started_at);
    const lastMs = parseTimestampMs(entry.last_event_at) || startedMs;
    const idleSeconds = lastMs ? (Date.now() - lastMs) / 1000 : 0;
    if (!entry.last_event_at) return { label: "starting", className: "health-chip-starting" };
    if (idleSeconds >= 300) return { label: "stalled?", className: "health-chip-stalled" };
    if ((entry.tokens?.total_tokens || 0) === 0) return { label: "active/no-usage", className: "health-chip-no-usage" };
    return { label: "active", className: "health-chip-active" };
  }

  function idleText(entry) {
    const startedMs = parseTimestampMs(entry.started_at);
    const lastMs = parseTimestampMs(entry.last_event_at) || startedMs;
    if (!lastMs) return "n/a";
    return formatDurationSeconds((Date.now() - lastMs) / 1000);
  }

  function telemetryWarning(entry) {
    return (entry.tokens?.total_tokens || 0) === 0
      ? `<span class="warning-text">No token telemetry yet</span>`
      : "";
  }

  function issueCell(identifier) {
    return `<div class="issue-stack"><span class="issue-id">${identifier}</span><a class="issue-link" href="/api/v1/${encodeURIComponent(identifier)}">JSON details</a></div>`;
  }

  function sessionCell(sessionId) {
    if (!sessionId) return `<span class="muted">n/a</span>`;
    return `<span class="session-text mono">${sessionId}</span>`;
  }

  function renderTableBody(target, rows, emptyText) {
    if (!rows.length) {
      target.innerHTML = `<tr><td colspan="6"><div class="empty-state">${emptyText}</div></td></tr>`;
      return;
    }
    target.innerHTML = rows.join("");
  }

  function render(payload) {
    if (!payload || payload.error) return;

    const counts = payload.counts || { running: 0, retrying: 0 };
    const totals = payload.codex_totals || {
      total_tokens: 0,
      input_tokens: 0,
      output_tokens: 0,
      seconds_running: 0
    };

    document.getElementById("metric-running").textContent = counts.running;
    document.getElementById("metric-retrying").textContent = counts.retrying;
    document.getElementById("metric-total-tokens").textContent = formatInt(totals.total_tokens);
    document.getElementById("metric-input-tokens").textContent = formatInt(totals.input_tokens);
    document.getElementById("metric-output-tokens").textContent = formatInt(totals.output_tokens);
    document.getElementById("metric-runtime").textContent = formatRuntime(totals.seconds_running);
    document.getElementById("generated-at").textContent = formatTimestamp(payload.generated_at);
    document.getElementById("rate-limits").textContent = JSON.stringify(payload.rate_limits, null, 2);
    const runtimeAlert = document.getElementById("runtime-alert");
    if (runtimeAlert) {
      if ((payload.running || []).length === 0) {
        runtimeAlert.textContent = "No active runs. The runtime is currently idle.";
      } else if ((payload.running || []).some((entry) => runHealth(entry).label === "stalled?")) {
        runtimeAlert.textContent = "At least one run looks stalled: last Codex update is older than 5 minutes.";
      } else if ((payload.running || []).some((entry) => (entry.tokens?.total_tokens || 0) === 0)) {
        runtimeAlert.textContent = "At least one run has started but has not reported token usage yet.";
      } else {
        runtimeAlert.textContent = "Live run telemetry is streaming normally.";
      }
    }

    const runningRows = (payload.running || []).map((entry) => `
      <tr>
        <td><div class="issue-stack"><span class="issue-id">${entry.issue_identifier}</span><span class="issue-title">${entry.issue_title || "Untitled issue"}</span><a class="issue-link" href="/api/v1/${encodeURIComponent(entry.issue_identifier)}">JSON details</a></div></td>
        <td><div class="detail-stack"><span class="state-text">${entry.state}</span><span class="health-chip ${runHealth(entry).className}">${runHealth(entry).label}</span></div></td>
        <td><div class="detail-stack"><span>${formatTimestamp(entry.started_at)}</span><span class="muted">Last update ${formatTimestamp(entry.last_event_at)}</span><span class="muted">Idle ${idleText(entry)}</span></div></td>
        <td><div class="detail-stack"><span>PID ${entry.codex_app_server_pid || "n/a"} · turn ${entry.turn_count ?? 0}</span><span class="muted mono">session ${entry.session_id || "n/a"}</span><span class="muted mono">thread ${entry.thread_id || "n/a"}</span></div></td>
        <td><div class="detail-stack"><span class="event-text">${entry.last_event || "n/a"}</span><span class="muted">${entry.last_message || "n/a"}</span></div></td>
        <td><div class="token-stack numeric"><span>Total: ${formatInt(entry.tokens.total_tokens)}</span><span class="muted">In ${formatInt(entry.tokens.input_tokens)} / Out ${formatInt(entry.tokens.output_tokens)}</span><span class="muted workspace-path">${entry.workspace_path || "n/a"}</span>${telemetryWarning(entry)}</div></td>
      </tr>
    `);

    const retryRows = (payload.retrying || []).map((entry) => `
      <tr>
        <td>${issueCell(entry.issue_identifier)}</td>
        <td>${entry.attempt}</td>
        <td class="mono">${formatTimestamp(entry.due_at)}</td>
        <td>${entry.error || "n/a"}</td>
      </tr>
    `);

    renderTableBody(
      document.getElementById("running-body"),
      runningRows,
      "No active sessions. The orchestrator is currently idle."
    );

    renderTableBody(
      document.getElementById("retry-body"),
      retryRows,
      "No issues are currently backing off."
    );

  }

  function setConnection(connected) {
    document.getElementById("connection-live").style.display = connected ? "inline-flex" : "none";
    document.getElementById("connection-offline").style.display = connected ? "none" : "inline-flex";
  }

  async function triggerRefresh() {
    const button = document.getElementById("refresh-now");
    const original = button.textContent;
    button.disabled = true;
    button.textContent = "Refreshing…";
    try {
      await fetch("/api/v1/refresh", { method: "POST" });
    } catch (_err) {
    } finally {
      window.setTimeout(() => {
        button.disabled = false;
        button.textContent = original;
      }, 900);
    }
  }

  document.getElementById("refresh-now")?.addEventListener("click", triggerRefresh);

  render(state);
  setConnection(true);

  if (mode === "demo") {
    document.title = "Symphony Demo Dashboard";
  }

  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const socket = new WebSocket(`${protocol}//${location.host}/ws`);
  socket.onopen = function () {
    setConnection(true);
  };
  socket.onclose = function () {
    setConnection(false);
  };
  socket.onerror = function () {
    setConnection(false);
  };
  socket.onmessage = function (event) {
    try {
      render(JSON.parse(event.data));
      setConnection(true);
    } catch (_err) {}
  };
})();
