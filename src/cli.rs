use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::config::ServiceConfig;
use crate::orchestrator;
use crate::tracker::TrackerClient;
use crate::web;
use crate::workflow::WorkflowStore;

#[derive(Debug, Parser)]
#[command(name = "symphony", version, about = "Rust reference implementation of Symphony")]
struct Args {
    #[arg(long = "i-understand-that-this-will-be-running-without-the-usual-guardrails")]
    ack: bool,

    #[arg(long)]
    logs_root: Option<PathBuf>,

    #[arg(long)]
    port: Option<u16>,

    #[arg(long)]
    demo: bool,

    #[arg()]
    workflow_path: Option<PathBuf>,
}

pub async fn run() -> Result<()> {
    let args = Args::parse();
    init_tracing(args.logs_root.as_deref())?;

    let requested_workflow = args
        .workflow_path
        .clone()
        .unwrap_or_else(|| PathBuf::from("WORKFLOW.md"));
    let workflow_path = requested_workflow
        .canonicalize()
        .unwrap_or(requested_workflow.clone());

    let use_demo = args.demo || !workflow_path.exists();

    let orchestrator;
    let config = if use_demo {
        if !args.demo && !workflow_path.exists() {
            tracing::warn!(
                "workflow file not found at {}; starting built-in demo mode",
                workflow_path.display()
            );
        }
        let config = Arc::new(ServiceConfig::demo(workflow_path, args.port));
        orchestrator = orchestrator::spawn_demo(Arc::clone(&config));
        config
    } else {
        if !args.ack {
            return Err(anyhow!(acknowledgement_banner()));
        }
        let mut workflow_store = WorkflowStore::new(workflow_path.clone());
        let workflow = workflow_store.load_current()?;
        let mut config = ServiceConfig::from_workflow(workflow_path.clone(), workflow)?;
        if args.port.is_some() {
            config.server_port = args.port;
        }
        config.validate()?;
        let config = Arc::new(config);
        let tracker = TrackerClient::new(Arc::clone(&config))?;
        orchestrator = orchestrator::spawn(Arc::clone(&config), workflow_path, tracker.clone()).await;
        config
    };

    web::spawn_server(Arc::clone(&config), orchestrator.clone()).await?;
    crate::tui::run(orchestrator).await?;
    Ok(())
}

fn init_tracing(logs_root: Option<&std::path::Path>) -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if let Some(logs_root) = logs_root {
        std::fs::create_dir_all(logs_root)?;
        let file_appender = tracing_appender::rolling::never(logs_root, "symphony.log");
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(file_appender)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    Ok(())
}

fn acknowledgement_banner() -> String {
    let lines = [
        "This Symphony implementation is a low key engineering preview.",
        "Codex will run without any guardrails.",
        "Symphony Rust is not a supported product and is presented as-is.",
        "To proceed, start with `--i-understand-that-this-will-be-running-without-the-usual-guardrails`.",
    ];
    let width = lines.iter().map(|line| line.len()).max().unwrap_or(0);
    let border = "─".repeat(width + 2);
    let mut message = format!("╭{}╮\n│ {:width$} │\n", border, "", width = width);
    for line in lines {
        message.push_str(&format!("│ {:width$} │\n", line, width = width));
    }
    message.push_str(&format!("│ {:width$} │\n╰{}╯", "", border, width = width));
    message
}
