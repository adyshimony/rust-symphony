(function () {
  const state = window.__SYMPHONY_INITIAL_STATE__ || null;
  const mode = document.body.dataset.runtimeMode || "live";
  let latestPayload = state;

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

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

  function renderTableBody(target, rows, emptyText) {
    if (!target) return;
    const colspan = target.dataset.colspan || "1";
    if (!rows.length) {
      target.innerHTML = `<tr><td colspan="${colspan}"><div class="empty-state">${escapeHtml(emptyText)}</div></td></tr>`;
      return;
    }
    target.innerHTML = rows.join("");
  }

  function issueCell(identifier, title) {
    return `<div class="issue-stack"><span class="issue-id">${escapeHtml(identifier)}</span><span class="issue-title">${escapeHtml(title || "Untitled issue")}</span><a class="issue-link" href="/api/v1/${encodeURIComponent(identifier)}">JSON details</a></div>`;
  }

  function actionButton(issueIdentifier, action, label, tone) {
    return `<button class="table-action table-action-${tone}" type="button" data-issue="${escapeHtml(issueIdentifier)}" data-action="${escapeHtml(action)}">${escapeHtml(label)}</button>`;
  }

  function actionsCell(issueIdentifier, actions) {
    return `<div class="action-stack">${actions.map((action) => actionButton(issueIdentifier, action.action, action.label, action.tone)).join("")}</div>`;
  }

  function blockedContextCell(entry) {
    return `<div class="detail-stack"><span class="muted mono">cmd ${escapeHtml(entry.last_command || "n/a")}</span><span class="muted mono">file ${escapeHtml(entry.last_file_touched || "n/a")}</span><span class="muted">diff ${entry.diff?.changed_files || 0} +${formatInt(entry.diff?.added_lines || 0)} -${formatInt(entry.diff?.removed_lines || 0)}</span></div>`;
  }

  function primaryLogIssue(payload) {
    return payload?.running?.[0]?.issue_identifier
      || payload?.needs_review?.[0]?.issue_identifier
      || payload?.held?.[0]?.issue_identifier
      || payload?.paused?.[0]?.issue_identifier
      || payload?.retrying?.[0]?.issue_identifier
      || null;
  }

  async function refreshStdoutTail(issueIdentifier) {
    const target = document.getElementById("stdout-tail");
    const label = document.getElementById("log-issue");
    if (!target || !label) return;
    if (!issueIdentifier) {
      label.textContent = "n/a";
      target.textContent = "No log stream selected.";
      return;
    }
    label.textContent = issueIdentifier;
    try {
      const response = await fetch(`/api/v1/issues/${encodeURIComponent(issueIdentifier)}/logs/stdout?tail=40`);
      if (!response.ok) {
        target.textContent = "Log stream unavailable.";
        return;
      }
      const text = await response.text();
      target.textContent = text || "Log stream is empty.";
    } catch (_err) {
      target.textContent = "Failed to load log stream.";
    }
  }

  async function refreshSnapshot() {
    try {
      const response = await fetch("/api/v1/state");
      if (!response.ok) return;
      render(await response.json());
    } catch (_err) {
    }
  }

  function showActionFeedback(kind, message) {
    const target = document.getElementById("action-feedback");
    if (!target) return;
    target.className = `operator-feedback operator-feedback-${kind}`;
    target.textContent = message;
  }

  async function controlAction(issueIdentifier, action) {
    if (action === "stop" && !window.confirm(`Stop ${issueIdentifier}?`)) {
      return;
    }
    showActionFeedback("pending", `${action} ${issueIdentifier}...`);
    try {
      const response = await fetch(`/api/v1/issues/${encodeURIComponent(issueIdentifier)}/${action}`, {
        method: "POST"
      });
      const payload = await response.json().catch(() => null);
      const message = payload?.message || `${action} ${issueIdentifier}`;
      if (response.ok) {
        showActionFeedback("success", message);
        await refreshSnapshot();
      } else {
        showActionFeedback("error", message);
      }
    } catch (_err) {
      showActionFeedback("error", `Failed to ${action} ${issueIdentifier}`);
    }
  }

  function render(payload) {
    if (!payload || payload.error) return;
    latestPayload = payload;

    const counts = payload.counts || {
      running: 0,
      retrying: 0,
      paused: 0,
      held: 0,
      needs_review: 0
    };
    const totals = payload.codex_totals || {
      total_tokens: 0,
      input_tokens: 0,
      output_tokens: 0,
      seconds_running: 0
    };

    document.getElementById("metric-running").textContent = counts.running;
    document.getElementById("metric-retrying").textContent = counts.retrying;
    document.getElementById("metric-paused").textContent = counts.paused || 0;
    document.getElementById("metric-held").textContent = counts.held || 0;
    document.getElementById("metric-review").textContent = counts.needs_review || 0;
    document.getElementById("metric-total-tokens").textContent = formatInt(totals.total_tokens);
    document.getElementById("metric-input-tokens").textContent = formatInt(totals.input_tokens);
    document.getElementById("metric-output-tokens").textContent = formatInt(totals.output_tokens);
    document.getElementById("metric-runtime").textContent = formatRuntime(totals.seconds_running);
    document.getElementById("generated-at").textContent = formatTimestamp(payload.generated_at);
    document.getElementById("rate-limits").textContent = JSON.stringify(payload.rate_limits, null, 2);

    const runtimeAlert = document.getElementById("runtime-alert");
    if (runtimeAlert) {
      if ((payload.needs_review || []).length > 0) {
        runtimeAlert.textContent = "At least one run is paused for operator review.";
      } else if ((payload.running || []).length === 0) {
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
        <td>${issueCell(entry.issue_identifier, entry.issue_title)}</td>
        <td><div class="detail-stack"><span class="state-text">${escapeHtml(entry.state)}</span><span class="health-chip ${runHealth(entry).className}">${escapeHtml(runHealth(entry).label)}</span><span class="muted">phase ${escapeHtml(entry.phase || "unknown")}</span></div></td>
        <td><div class="detail-stack"><span>${escapeHtml(formatTimestamp(entry.started_at))}</span><span class="muted">Last update ${escapeHtml(formatTimestamp(entry.last_event_at))}</span><span class="muted">Idle ${escapeHtml(idleText(entry))}</span><span class="muted">${escapeHtml(entry.phase_detail || "n/a")}</span></div></td>
        <td><div class="detail-stack"><span>PID ${escapeHtml(entry.codex_app_server_pid || "n/a")} · turn ${escapeHtml(entry.turn_count ?? 0)}</span><span class="muted mono">session ${escapeHtml(entry.session_id || "n/a")}</span><span class="muted mono">thread ${escapeHtml(entry.thread_id || "n/a")}</span></div></td>
        <td><div class="detail-stack"><span class="event-text">${escapeHtml(entry.last_event || "n/a")}</span><span class="muted">${escapeHtml(entry.last_message || "n/a")}</span><span class="muted mono">cmd ${escapeHtml(entry.last_command || "n/a")}</span><span class="muted mono">file ${escapeHtml(entry.last_file_touched || "n/a")}</span></div></td>
        <td><div class="token-stack numeric"><span>Total: ${formatInt(entry.tokens.total_tokens)}</span><span class="muted">In ${formatInt(entry.tokens.input_tokens)} / Out ${formatInt(entry.tokens.output_tokens)}</span><span class="muted">diff ${entry.diff?.changed_files || 0} +${formatInt(entry.diff?.added_lines || 0)} -${formatInt(entry.diff?.removed_lines || 0)}</span><span class="muted workspace-path">${escapeHtml(entry.workspace_path || "n/a")}</span><span class="muted workspace-path">${escapeHtml(entry.stdout_log_path || "n/a")}</span><span class="muted workspace-path">${escapeHtml(entry.progress_report_path || "n/a")}</span>${telemetryWarning(entry)}</div></td>
        <td>${actionsCell(entry.issue_identifier, [
          { action: "pause", label: "Pause", tone: "neutral" },
          { action: "stop", label: "Stop", tone: "danger" }
        ])}</td>
      </tr>
    `);

    const retryRows = (payload.retrying || []).map((entry) => `
      <tr>
        <td>${issueCell(entry.issue_identifier, entry.issue_title)}</td>
        <td>${escapeHtml(entry.attempt)}</td>
        <td class="mono">${escapeHtml(formatTimestamp(entry.due_at))}</td>
        <td>${escapeHtml(entry.error || "n/a")}</td>
        <td>${actionsCell(entry.issue_identifier, [
          { action: "pause", label: "Pause", tone: "neutral" },
          { action: "retry", label: "Retry", tone: "info" }
        ])}</td>
      </tr>
    `);

    const pausedRows = (payload.paused || []).map((entry) => `
      <tr>
        <td>${issueCell(entry.issue_identifier, entry.issue_title)}</td>
        <td><div class="detail-stack"><span class="state-text">${escapeHtml(entry.state)}</span><span class="health-chip health-chip-starting">${escapeHtml(entry.phase || "paused")}</span><span class="muted">${escapeHtml(formatTimestamp(entry.blocked_at))}</span></div></td>
        <td>${blockedContextCell(entry)}</td>
        <td>${actionsCell(entry.issue_identifier, [
          { action: "resume", label: "Resume", tone: "success" }
        ])}</td>
      </tr>
    `);

    const heldRows = (payload.held || []).map((entry) => `
      <tr>
        <td>${issueCell(entry.issue_identifier, entry.issue_title)}</td>
        <td><div class="detail-stack"><span class="state-text">${escapeHtml(entry.state)}</span><span class="health-chip health-chip-stalled">${escapeHtml(entry.phase || "held")}</span><span class="muted">${escapeHtml(formatTimestamp(entry.blocked_at))}</span></div></td>
        <td>${blockedContextCell(entry)}</td>
        <td>${actionsCell(entry.issue_identifier, [
          { action: "resume", label: "Resume", tone: "success" },
          { action: "retry", label: "Retry", tone: "info" }
        ])}</td>
      </tr>
    `);

    const reviewRows = (payload.needs_review || []).map((entry) => `
      <tr>
        <td>${issueCell(entry.issue_identifier, entry.issue_title)}</td>
        <td><div class="detail-stack"><span class="state-text">${escapeHtml(entry.state)}</span><span class="health-chip health-chip-stalled">${escapeHtml(entry.phase || "needs_review")}</span><span class="muted">${escapeHtml(formatTimestamp(entry.blocked_at))}</span></div></td>
        <td><div class="detail-stack"><span>${escapeHtml(entry.review_reason || "review required")}</span><span class="muted">${escapeHtml(entry.phase_detail || "n/a")}</span></div></td>
        <td>${blockedContextCell(entry)}</td>
        <td>${actionsCell(entry.issue_identifier, [
          { action: "resume", label: "Resume", tone: "success" },
          { action: "retry", label: "Retry", tone: "info" }
        ])}</td>
      </tr>
    `);

    renderTableBody(
      document.getElementById("running-body"),
      runningRows,
      "No active sessions. The orchestrator is currently idle."
    );
    renderTableBody(
      document.getElementById("paused-body"),
      pausedRows,
      "No paused issues."
    );
    renderTableBody(
      document.getElementById("held-body"),
      heldRows,
      "No held issues."
    );
    renderTableBody(
      document.getElementById("retry-body"),
      retryRows,
      "No issues are currently backing off."
    );
    renderTableBody(
      document.getElementById("review-body"),
      reviewRows,
      "No runs currently require operator review."
    );

    refreshStdoutTail(primaryLogIssue(payload));
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
      showActionFeedback("success", "Refresh queued.");
      await refreshSnapshot();
    } catch (_err) {
      showActionFeedback("error", "Refresh failed.");
    } finally {
      window.setTimeout(() => {
        button.disabled = false;
        button.textContent = original;
      }, 900);
    }
  }

  document.getElementById("refresh-now")?.addEventListener("click", triggerRefresh);
  document.addEventListener("click", (event) => {
    const button = event.target.closest("[data-action][data-issue]");
    if (!button) return;
    controlAction(button.dataset.issue, button.dataset.action);
  });

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

  window.setInterval(() => {
    refreshStdoutTail(primaryLogIssue(latestPayload));
  }, 3000);
})();
