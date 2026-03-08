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

  function issueCell(identifier) {
    return `<div class="issue-stack"><span class="issue-id">${identifier}</span><a class="issue-link" href="/api/v1/${identifier}">JSON details</a></div>`;
  }

  function sessionCell(sessionId) {
    if (!sessionId) return `<span class="muted">n/a</span>`;
    return `<button type="button" class="subtle-button" data-label="Copy ID" data-copy="${sessionId}">Copy ID</button>`;
  }

  function bindCopyButtons() {
    document.querySelectorAll(".subtle-button[data-copy]").forEach((button) => {
      button.onclick = function () {
        navigator.clipboard.writeText(button.dataset.copy);
        button.textContent = "Copied";
        clearTimeout(button._copyTimer);
        button._copyTimer = setTimeout(() => {
          button.textContent = button.dataset.label;
        }, 1200);
      };
    });
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

    const runningRows = (payload.running || []).map((entry) => `
      <tr>
        <td>${issueCell(entry.issue_identifier)}</td>
        <td><span class="state-badge">${entry.state}</span></td>
        <td>${sessionCell(entry.session_id)}</td>
        <td class="numeric">${formatTimestamp(entry.started_at)}</td>
        <td><div class="detail-stack"><span class="event-text">${entry.last_message || "n/a"}</span><span class="muted event-meta">${entry.last_event || "n/a"}${entry.last_event_at ? ` · ${formatTimestamp(entry.last_event_at)}` : ""}</span></div></td>
        <td><div class="token-stack numeric"><span>Total: ${formatInt(entry.tokens.total_tokens)}</span><span class="muted">In ${formatInt(entry.tokens.input_tokens)} / Out ${formatInt(entry.tokens.output_tokens)}</span></div></td>
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

    bindCopyButtons();
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
