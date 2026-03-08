use std::io::{self, Stdout};
use std::time::Duration;

use anyhow::Result;
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
            Constraint::Length(9),
            Constraint::Min(10),
            Constraint::Length(8),
        ])
        .split(frame.area());

    let header = header_text(snapshot);
    frame.render_widget(
        Paragraph::new(header)
            .block(Block::default().title("SYMPHONY STATUS").borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        layout[0],
    );

    let running = running_text(snapshot);
    frame.render_widget(
        Paragraph::new(running)
            .block(Block::default().title("Running").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        layout[1],
    );

    let retry = retry_text(snapshot);
    frame.render_widget(
        Paragraph::new(retry)
            .block(Block::default().title("Backoff Queue").borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        layout[2],
    );
}

fn header_text(snapshot: &crate::model::Snapshot) -> Text<'static> {
    let mut lines = Vec::new();
    lines.push(Line::from(vec![
        Span::styled("Agents: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(
            format!("{}", snapshot.running.len()),
            Style::default().fg(Color::Green),
        ),
        Span::raw("/"),
        Span::raw("?"),
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
        Span::styled("Tokens: ", Style::default().add_modifier(Modifier::BOLD)),
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
        Span::styled("Rate Limits: ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(
            snapshot
                .rate_limits
                .as_ref()
                .map(|value| truncate(&value.to_string(), 96))
                .unwrap_or_else(|| "unavailable".to_string()),
        ),
    ]));
    lines.push(Line::from(vec![
        Span::styled("Next refresh: ", Style::default().add_modifier(Modifier::BOLD)),
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
    lines.push(Line::from("ID        STAGE          PID      AGE/TURN     TOKENS     SESSION        EVENT"));
    lines.push(Line::from("-------------------------------------------------------------------------------"));
    if snapshot.running.is_empty() {
        lines.push(Line::from(vec![Span::styled(
            "No active agents",
            Style::default().fg(Color::DarkGray),
        )]));
    } else {
        for entry in &snapshot.running {
            lines.push(Line::from(format!(
                "{:<8}  {:<14}  {:<8}  {:<11}  {:>8}  {:<12}  {}",
                truncate(&entry.identifier, 8),
                truncate(&entry.state, 14),
                truncate(entry.session.codex_app_server_pid.as_deref().unwrap_or("n/a"), 8),
                format!(
                    "{:.0}m/{:02}",
                    ((chrono::Utc::now() - entry.started_at).num_seconds().max(0) as f64 / 60.0).floor(),
                    entry.session.turn_count
                ),
                entry.session.codex_total_tokens,
                truncate(entry.session.session_id.as_deref().unwrap_or("n/a"), 12),
                truncate(entry.session.last_codex_message.as_deref().unwrap_or(entry.session.last_codex_event.as_deref().unwrap_or("n/a")), 48),
            )));
        }
    }
    Text::from(lines)
}

fn retry_text(snapshot: &crate::model::Snapshot) -> Text<'static> {
    if snapshot.retrying.is_empty() {
        Text::from(Line::from(vec![Span::styled(
            "No queued retries",
            Style::default().fg(Color::DarkGray),
        )]))
    } else {
        Text::from(
            snapshot
                .retrying
                .iter()
                .map(|entry| {
                    Line::from(format!(
                        "↻ {} attempt={} due={} error={}",
                        entry.identifier,
                        entry.attempt,
                        entry.due_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        truncate(entry.error.as_deref().unwrap_or("n/a"), 96)
                    ))
                })
                .collect::<Vec<_>>(),
        )
    }
}

fn truncate(value: &str, max: usize) -> String {
    if value.len() <= max {
        value.to_string()
    } else {
        format!("{}...", &value[..max.saturating_sub(3)])
    }
}
