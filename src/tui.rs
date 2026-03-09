use std::fs;
use std::io::{self, Stdout};
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};
use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use futures_util::StreamExt;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::{Frame, Terminal};

use crate::orchestrator::OrchestratorHandle;

pub async fn run(orchestrator: OrchestratorHandle) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut events = EventStream::new();
    let mut snapshot_rx = orchestrator.subscribe();
    loop {
        let snapshot = snapshot_rx.borrow().clone();
        terminal.draw(|frame| render(frame, &snapshot))?;

        tokio::select! {
            maybe_event = events.next() => {
                if let Some(Ok(Event::Key(key))) = maybe_event {
                    if key.code == KeyCode::Char('q')
                        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL)) {
                        break;
                    }
                }
            }
            changed = snapshot_rx.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }

    restore_terminal(terminal)?;
    Ok(())
}

fn restore_terminal(mut terminal: Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

fn render(frame: &mut Frame<'_>, snapshot: &crate::model::Snapshot) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Min(14),
            Constraint::Length(8),
            Constraint::Length(10),
        ])
        .split(frame.area());

    let header = header_text(snapshot);
    frame.render_widget(
        Paragraph::new(header)
            .block(
                Block::default()
                    .title("SYMPHONY STATUS")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: true }),
        layout[0],
    );

    let log_tail = log_tail_text(snapshot);
    frame.render_widget(
        Paragraph::new(log_tail)
            .block(
                Block::default()
                    .title("Live Log Tail")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false }),
        layout[1],
    );

    let running = running_text(snapshot);
    frame.render_widget(
        Paragraph::new(running)
            .block(Block::default().title("Running").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        layout[2],
    );

    let review = review_text(snapshot);
    frame.render_widget(
        Paragraph::new(review)
            .block(Block::default().title("Needs Review").borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        layout[3],
    );

    let retry = retry_text(snapshot);
    frame.render_widget(
        Paragraph::new(retry)
            .block(
                Block::default()
                    .title("Backoff Queue")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: true }),
        layout[4],
    );
}

fn header_text(snapshot: &crate::model::Snapshot) -> Text<'static> {
    let mut lines = Vec::new();
    let total_agents = snapshot.running.len()
        + snapshot.retrying.len()
        + snapshot.paused.len()
        + snapshot.held.len()
        + snapshot.needs_review.len();
    let telemetry_missing = snapshot.running.iter().any(|entry| {
        entry.session.codex_total_tokens == 0 && entry.session.last_codex_timestamp.is_some()
    });
    lines.push(Line::from(vec![
        Span::styled("Workload: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(
            format!("running {}", snapshot.running.len()),
            Style::default().fg(Color::Green),
        ),
        Span::raw(" | "),
        Span::styled(
            format!("retrying {}", snapshot.retrying.len()),
            Style::default().fg(Color::Yellow),
        ),
        Span::raw(" | "),
        Span::styled(
            format!("review {}", snapshot.needs_review.len()),
            Style::default().fg(Color::LightRed),
        ),
        Span::raw(" | "),
        Span::raw(format!("tracked {}", total_agents)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Runtime: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(
            format!(
                "{:.0}m {:.0}s",
                (snapshot.codex_totals.seconds_running / 60.0).floor(),
                snapshot.codex_totals.seconds_running % 60.0
            ),
            Style::default().fg(Color::Magenta),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled(
            "Global tokens: ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(
                "in {} | out {} | total {}",
                snapshot.codex_totals.input_tokens,
                snapshot.codex_totals.output_tokens,
                snapshot.codex_totals.total_tokens
            ),
            Style::default().fg(Color::Yellow),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled(
            "Per-run tokens: ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(if snapshot.running.is_empty() {
            "n/a".to_string()
        } else {
            snapshot
                .running
                .iter()
                .map(|entry| {
                    format!(
                        "{}={}",
                        truncate(&entry.identifier, 20),
                        entry.session.codex_total_tokens
                    )
                })
                .collect::<Vec<_>>()
                .join(" | ")
        }),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Telemetry: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(
            if telemetry_missing {
                "Codex usage has not been reported yet for at least one active run"
            } else if snapshot.running.is_empty() {
                "idle"
            } else {
                "streaming"
            },
            Style::default().fg(if telemetry_missing {
                Color::LightRed
            } else {
                Color::Green
            }),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled(
            "Rate Limits: ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(
            snapshot
                .rate_limits
                .as_ref()
                .map(|value| truncate(&value.to_string(), 96))
                .unwrap_or_else(|| "unavailable".to_string()),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled(
            "Next refresh: ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            if snapshot.polling.checking {
                "checking now...".to_string()
            } else {
                snapshot
                    .polling
                    .next_poll_in_ms
                    .map(|ms| format!("{}s", (ms + 999) / 1000))
                    .unwrap_or_else(|| "n/a".to_string())
            },
            Style::default().fg(Color::Cyan),
        ),
    ]));
    Text::from(lines)
}

fn running_text(snapshot: &crate::model::Snapshot) -> Text<'static> {
    let mut lines = Vec::new();
    if snapshot.running.is_empty() {
        lines.push(Line::from(vec![Span::styled(
            "No active agents",
            Style::default().fg(Color::DarkGray),
        )]));
    } else {
        for entry in &snapshot.running {
            let last_seen = entry
                .session
                .last_codex_timestamp
                .unwrap_or(entry.started_at);
            let idle_secs = (Utc::now() - last_seen).num_seconds().max(0);
            let age_secs = (Utc::now() - entry.started_at).num_seconds().max(0);
            let status = classify_run(entry);
            lines.push(Line::from(vec![
                Span::styled(
                    truncate(&entry.identifier, 48),
                    Style::default()
                        .add_modifier(Modifier::BOLD)
                        .fg(Color::Cyan),
                ),
                Span::raw("  "),
                Span::styled(status, Style::default().fg(status_color(status))),
                Span::raw("  "),
                Span::raw(format!(
                    "state {}  phase {}",
                    entry.state,
                    phase_label(&entry.telemetry.phase)
                )),
            ]));
            lines.push(Line::from(format!(
                "  title={} ",
                truncate(&entry.issue.title, 108),
            )));
            lines.push(Line::from(format!(
                "  age={}  idle={}  turn={}  pid={}  tokens=in {} / out {} / total {}",
                human_duration(age_secs),
                human_duration(idle_secs),
                entry.session.turn_count,
                entry
                    .session
                    .codex_app_server_pid
                    .as_deref()
                    .unwrap_or("n/a"),
                entry.session.codex_input_tokens,
                entry.session.codex_output_tokens,
                entry.session.codex_total_tokens,
            )));
            lines.push(Line::from(format!(
                "  started={}  last_update={}",
                format_timestamp(entry.started_at),
                format_timestamp(last_seen),
            )));
            lines.push(Line::from(format!(
                "  session={}  thread={}  turn_id={}",
                truncate(entry.session.session_id.as_deref().unwrap_or("n/a"), 40),
                truncate(entry.session.thread_id.as_deref().unwrap_or("n/a"), 28),
                truncate(entry.session.turn_id.as_deref().unwrap_or("n/a"), 28),
            )));
            lines.push(Line::from(format!(
                "  command={}  file={}  diff=files {} +{} -{}",
                truncate(entry.telemetry.last_command.as_deref().unwrap_or("n/a"), 48),
                truncate(
                    entry
                        .telemetry
                        .last_file_touched
                        .as_deref()
                        .unwrap_or("n/a"),
                    48
                ),
                entry.telemetry.diff.changed_files,
                entry.telemetry.diff.added_lines,
                entry.telemetry.diff.removed_lines,
            )));
            lines.push(Line::from(format!(
                "  workspace={}",
                truncate(&entry.workspace_path, 108),
            )));
            lines.push(Line::from(format!(
                "  logs={}  stderr={}  report={}",
                truncate(
                    entry.telemetry.stdout_log_path.as_deref().unwrap_or("n/a"),
                    42
                ),
                truncate(
                    entry.telemetry.stderr_log_path.as_deref().unwrap_or("n/a"),
                    42
                ),
                truncate(
                    entry
                        .telemetry
                        .progress_report_path
                        .as_deref()
                        .unwrap_or("n/a"),
                    42
                ),
            )));
            lines.push(Line::from(format!(
                "  detail={}",
                truncate(
                    entry.telemetry.phase_detail.as_deref().unwrap_or("n/a"),
                    108
                ),
            )));
            lines.push(Line::from(format!(
                "  event={}  message={}",
                truncate(
                    entry.session.last_codex_event.as_deref().unwrap_or("n/a"),
                    32
                ),
                truncate(
                    entry.session.last_codex_message.as_deref().unwrap_or("n/a"),
                    96,
                ),
            )));
            if entry.session.codex_total_tokens == 0 {
                lines.push(Line::from(vec![Span::styled(
                    "  warning=no token usage reported yet; this may mean Codex is still starting or usage telemetry is missing",
                    Style::default().fg(Color::LightRed),
                )]));
            }
            lines.push(Line::from(""));
        }
    }
    Text::from(lines)
}

fn review_text(snapshot: &crate::model::Snapshot) -> Text<'static> {
    let mut lines = Vec::new();
    if snapshot.needs_review.is_empty() {
        lines.push(Line::from(vec![Span::styled(
            "No review-blocked runs",
            Style::default().fg(Color::DarkGray),
        )]));
    } else {
        for entry in &snapshot.needs_review {
            lines.push(Line::from(vec![
                Span::styled("! ", Style::default().fg(Color::LightRed)),
                Span::styled(
                    truncate(&entry.identifier, 48),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!(
                    "  phase={}  blocked_at={}",
                    phase_label(&entry.telemetry.phase),
                    format_timestamp(entry.blocked_at),
                )),
            ]));
            lines.push(Line::from(format!(
                "  reason={}",
                truncate(
                    entry.telemetry.review_reason.as_deref().unwrap_or("n/a"),
                    108
                ),
            )));
            lines.push(Line::from(format!(
                "  command={}  file={}  diff=files {} +{} -{}",
                truncate(entry.telemetry.last_command.as_deref().unwrap_or("n/a"), 48),
                truncate(
                    entry
                        .telemetry
                        .last_file_touched
                        .as_deref()
                        .unwrap_or("n/a"),
                    48
                ),
                entry.telemetry.diff.changed_files,
                entry.telemetry.diff.added_lines,
                entry.telemetry.diff.removed_lines,
            )));
        }
    }
    Text::from(lines)
}

fn log_tail_text(snapshot: &crate::model::Snapshot) -> Text<'static> {
    let mut lines = Vec::new();
    let selected = snapshot
        .running
        .iter()
        .find_map(|entry| {
            entry.telemetry.stdout_log_path.as_ref().map(|path| {
                (
                    entry.identifier.as_str(),
                    path.as_str(),
                    entry.telemetry.phase_detail.as_deref(),
                )
            })
        })
        .or_else(|| {
            snapshot.needs_review.iter().find_map(|entry| {
                entry.telemetry.stdout_log_path.as_ref().map(|path| {
                    (
                        entry.identifier.as_str(),
                        path.as_str(),
                        entry.telemetry.review_reason.as_deref(),
                    )
                })
            })
        });

    let Some((issue_identifier, log_path, context)) = selected else {
        lines.push(Line::from(vec![Span::styled(
            "No active or review-blocked issue has a stdout log yet.",
            Style::default().fg(Color::DarkGray),
        )]));
        return Text::from(lines);
    };

    lines.push(Line::from(format!(
        "issue={}  path={}",
        truncate(issue_identifier, 48),
        truncate(log_path, 96),
    )));
    lines.push(Line::from(format!(
        "context={}",
        truncate(context.unwrap_or("live Codex stdout tail"), 108),
    )));
    lines.push(Line::from(""));

    match fs::read_to_string(log_path) {
        Ok(contents) => {
            let tail = tail_lines(&contents, 8);
            if tail.is_empty() {
                lines.push(Line::from(vec![Span::styled(
                    "Log file is present but empty.",
                    Style::default().fg(Color::DarkGray),
                )]));
            } else {
                for line in tail.lines() {
                    lines.push(Line::from(truncate(line, 144)));
                }
            }
        }
        Err(_) => {
            lines.push(Line::from(vec![Span::styled(
                "Log file is not readable yet.",
                Style::default().fg(Color::Yellow),
            )]));
        }
    }

    Text::from(lines)
}

fn retry_text(snapshot: &crate::model::Snapshot) -> Text<'static> {
    let mut lines = Vec::new();
    if snapshot.retrying.is_empty() {
        lines.push(Line::from(vec![Span::styled(
            "No queued retries",
            Style::default().fg(Color::DarkGray),
        )]));
    } else {
        for entry in &snapshot.retrying {
            let due_in_secs = (entry.due_at - Utc::now()).num_seconds().max(0);
            lines.push(Line::from(vec![
                Span::styled("↻ ", Style::default().fg(Color::Yellow)),
                Span::styled(
                    truncate(&entry.identifier, 48),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!(
                    "  attempt={}  due_in={}  due_at={}",
                    entry.attempt,
                    human_duration(due_in_secs),
                    format_timestamp(entry.due_at),
                )),
            ]));
            lines.push(Line::from(format!(
                "  error={}",
                truncate(entry.error.as_deref().unwrap_or("n/a"), 108)
            )));
        }
    }
    Text::from(lines)
}

fn classify_run(entry: &crate::model::RunningEntry) -> &'static str {
    let idle_secs = (Utc::now()
        - entry
            .session
            .last_codex_timestamp
            .unwrap_or(entry.started_at))
    .num_seconds()
    .max(0);
    if entry.session.last_codex_timestamp.is_none() {
        "starting"
    } else if idle_secs >= 300 {
        "stalled?"
    } else if entry.session.codex_total_tokens == 0 {
        "active/no-usage"
    } else {
        "active"
    }
}

fn status_color(status: &str) -> Color {
    match status {
        "starting" => Color::Yellow,
        "stalled?" => Color::LightRed,
        "active/no-usage" => Color::Magenta,
        _ => Color::Green,
    }
}

fn phase_label(phase: &crate::model::RunPhase) -> &'static str {
    match phase {
        crate::model::RunPhase::Starting => "starting",
        crate::model::RunPhase::Discovering => "discovering",
        crate::model::RunPhase::Editing => "editing",
        crate::model::RunPhase::Testing => "testing",
        crate::model::RunPhase::Waiting => "waiting",
        crate::model::RunPhase::Retrying => "retrying",
        crate::model::RunPhase::Paused => "paused",
        crate::model::RunPhase::Held => "held",
        crate::model::RunPhase::NeedsReview => "needs_review",
        crate::model::RunPhase::Stalled => "stalled",
        crate::model::RunPhase::Completed => "completed",
    }
}

fn format_timestamp(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S UTC").to_string()
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

fn truncate(value: &str, max: usize) -> String {
    if value.len() <= max {
        value.to_string()
    } else {
        format!("{}...", &value[..max.saturating_sub(3)])
    }
}

fn tail_lines(contents: &str, lines: usize) -> String {
    if lines == 0 {
        return String::new();
    }
    let collected = contents.lines().rev().take(lines).collect::<Vec<_>>();
    collected.into_iter().rev().collect::<Vec<_>>().join("\n")
}
