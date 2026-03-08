use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::{timeout, Duration};

use crate::config::ServiceConfig;
use crate::model::{Issue, TokenUsage, WorkerEvent, WorkerUpdate};
use crate::tracker::LinearClient;

pub struct AppServerSession {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    config: Arc<ServiceConfig>,
    thread_id: String,
    workspace: String,
    app_server_pid: Option<String>,
}

pub async fn start_session(config: Arc<ServiceConfig>, workspace: &Path) -> Result<AppServerSession> {
    let mut child = Command::new("bash");
    child
        .arg("-lc")
        .arg(&config.codex_command)
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = child.spawn().context("failed to spawn codex app-server")?;
    let stdin = child.stdin.take().ok_or_else(|| anyhow!("missing codex stdin"))?;
    let stdout = child.stdout.take().ok_or_else(|| anyhow!("missing codex stdout"))?;
    let app_server_pid = child.id().map(|id| id.to_string());
    let mut session = AppServerSession {
        child,
        stdin,
        stdout: BufReader::new(stdout),
        config,
        thread_id: String::new(),
        workspace: workspace.to_string_lossy().into_owned(),
        app_server_pid,
    };
    session.initialize().await?;
    Ok(session)
}

impl AppServerSession {
    async fn initialize(&mut self) -> Result<()> {
        self.send(json!({
            "method": "initialize",
            "id": 1,
            "params": {
                "capabilities": { "experimentalApi": true },
                "clientInfo": {
                    "name": "symphony-rust",
                    "title": "Symphony Rust",
                    "version": "0.1.0"
                }
            }
        }))
        .await?;
        let _ = self.await_response(1).await?;
        self.send(json!({"method": "initialized", "params": {}})).await?;
        self.send(json!({
            "method": "thread/start",
            "id": 2,
            "params": {
                "approvalPolicy": self.config.codex_runtime.approval_policy,
                "sandbox": self.config.codex_runtime.thread_sandbox,
                "cwd": self.workspace,
                "dynamicTools": [{
                    "name": "linear_graphql",
                    "description": "Execute a raw GraphQL query or mutation against Linear using Symphony's configured auth.",
                    "inputSchema": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["query"],
                        "properties": {
                            "query": { "type": "string", "description": "GraphQL query or mutation document to execute against Linear." },
                            "variables": { "type": ["object", "null"], "description": "Optional GraphQL variables object.", "additionalProperties": true }
                        }
                    }
                }]
            }
        }))
        .await?;
        let response = self.await_response(2).await?;
        self.thread_id = response
            .get("thread")
            .and_then(|thread| thread.get("id"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("invalid thread/start response"))?
            .to_string();
        Ok(())
    }

    pub async fn run_turn(
        &mut self,
        issue: &Issue,
        prompt: String,
        worker_tx: &tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
        linear: Option<&LinearClient>,
    ) -> Result<()> {
        self.send(json!({
            "method": "turn/start",
            "id": 3,
            "params": {
                "threadId": self.thread_id,
                "input": [{ "type": "text", "text": prompt }],
                "cwd": self.workspace,
                "title": format!("{}: {}", issue.identifier, issue.title),
                "approvalPolicy": self.config.codex_runtime.approval_policy,
                "sandboxPolicy": self.config.codex_runtime.turn_sandbox_policy
            }
        }))
        .await?;
        let response = self.await_response(3).await?;
        let turn_id = response
            .get("turn")
            .and_then(|turn| turn.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string);
        loop {
            let line = self.read_line_with_timeout(self.config.codex_turn_timeout_ms).await?;
            let payload: Value = match serde_json::from_str(&line) {
                Ok(payload) => payload,
                Err(_) => continue,
            };
            if let Some(method) = payload.get("method").and_then(Value::as_str) {
                match method {
                    "turn/completed" => {
                        worker_tx.send(WorkerEvent {
                            issue_id: issue.id.clone(),
                            update: WorkerUpdate::CodexMessage {
                                event: "turn_completed".to_string(),
                                timestamp: Utc::now(),
                                session_id: turn_id.as_ref().map(|id| format!("{}-{id}", self.thread_id)),
                                thread_id: Some(self.thread_id.clone()),
                                turn_id: turn_id.clone(),
                                codex_app_server_pid: self.app_server_pid.clone(),
                                message: Some("completed".to_string()),
                                usage: usage_from_payload(&payload),
                                rate_limits: payload
                                    .get("usage")
                                    .and_then(|usage| usage.get("rate_limits").cloned().or_else(|| usage.get("rateLimits").cloned())),
                            },
                        }).ok();
                        return Ok(());
                    }
                    "turn/failed" => bail!("turn failed: {payload}"),
                    "turn/cancelled" => bail!("turn cancelled: {payload}"),
                    "item/tool/call" => {
                        self.handle_tool_call(&payload, linear).await?;
                    }
                    "item/commandExecution/requestApproval"
                    | "execCommandApproval"
                    | "applyPatchApproval"
                    | "item/fileChange/requestApproval" => {
                        let id = payload.get("id").cloned().ok_or_else(|| anyhow!("approval request missing id"))?;
                        let decision = if method == "execCommandApproval" || method == "applyPatchApproval" {
                            "approved_for_session"
                        } else {
                            "acceptForSession"
                        };
                        self.send(json!({ "id": id, "result": { "decision": decision } })).await?;
                    }
                    "item/tool/requestUserInput" => {
                        let id = payload.get("id").cloned().ok_or_else(|| anyhow!("tool input request missing id"))?;
                        let answers = payload
                            .get("params")
                            .and_then(|params| params.get("questions"))
                            .and_then(Value::as_array)
                            .map(|questions| {
                                let mut answers = serde_json::Map::new();
                                for question in questions {
                                    if let Some(id) = question.get("id").and_then(Value::as_str) {
                                        answers.insert(id.to_string(), json!({ "answers": ["This is a non-interactive session. Operator input is unavailable."] }));
                                    }
                                }
                                Value::Object(answers)
                            })
                            .unwrap_or_else(|| json!({}));
                        self.send(json!({ "id": id, "result": { "answers": answers } })).await?;
                    }
                    other => {
                        worker_tx.send(WorkerEvent {
                            issue_id: issue.id.clone(),
                            update: WorkerUpdate::CodexMessage {
                                event: other.to_string(),
                                timestamp: Utc::now(),
                                session_id: turn_id.as_ref().map(|id| format!("{}-{id}", self.thread_id)),
                                thread_id: Some(self.thread_id.clone()),
                                turn_id: turn_id.clone(),
                                codex_app_server_pid: self.app_server_pid.clone(),
                                message: summarize_payload(&payload),
                                usage: usage_from_payload(&payload),
                                rate_limits: payload
                                    .get("usage")
                                    .and_then(|usage| usage.get("rate_limits").cloned().or_else(|| usage.get("rateLimits").cloned())),
                            },
                        }).ok();
                    }
                }
            }
        }
    }

    async fn handle_tool_call(&mut self, payload: &Value, linear: Option<&LinearClient>) -> Result<()> {
        let id = payload
            .get("id")
            .cloned()
            .ok_or_else(|| anyhow!("tool call missing id"))?;
        let params = payload
            .get("params")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let tool_name = params
            .get("tool")
            .or_else(|| params.get("name"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let arguments = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
        let result = if tool_name == "linear_graphql" {
            if let Some(linear) = linear {
                let query = arguments
                    .get("query")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("linear_graphql missing query"))?;
                let variables = arguments
                    .get("variables")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                match linear.graphql(query, variables).await {
                    Ok(response) => json!({
                        "success": !response.get("errors").map(|errors| errors.is_array() && !errors.as_array().unwrap().is_empty()).unwrap_or(false),
                        "contentItems": [{ "type": "inputText", "text": serde_json::to_string_pretty(&response).unwrap_or_else(|_| response.to_string()) }]
                    }),
                    Err(err) => json!({
                        "success": false,
                        "contentItems": [{ "type": "inputText", "text": json!({ "error": { "message": err.to_string() } }).to_string() }]
                    }),
                }
            } else {
                json!({
                    "success": false,
                    "contentItems": [{ "type": "inputText", "text": json!({ "error": { "message": "linear_graphql is only available when tracker.kind is linear" } }).to_string() }]
                })
            }
        } else {
            json!({
                "success": false,
                "contentItems": [{ "type": "inputText", "text": json!({ "error": { "message": format!("Unsupported dynamic tool: {tool_name}") } }).to_string() }]
            })
        };
        self.send(json!({ "id": id, "result": result })).await?;
        Ok(())
    }

    async fn await_response(&mut self, request_id: u64) -> Result<Value> {
        loop {
            let line = self.read_line_with_timeout(self.config.codex_read_timeout_ms).await?;
            let payload: Value = match serde_json::from_str(&line) {
                Ok(payload) => payload,
                Err(_) => continue,
            };
            if payload.get("id").and_then(Value::as_u64) == Some(request_id) {
                if let Some(error) = payload.get("error") {
                    bail!("app-server response error: {error}");
                }
                return Ok(payload
                    .get("result")
                    .cloned()
                    .ok_or_else(|| anyhow!("app-server response missing result"))?);
            }
        }
    }

    async fn read_line_with_timeout(&mut self, timeout_ms: u64) -> Result<String> {
        let mut line = String::new();
        let count = timeout(Duration::from_millis(timeout_ms), self.stdout.read_line(&mut line))
            .await
            .map_err(|_| anyhow!("codex read timeout after {timeout_ms}ms"))?
            .context("failed to read codex output")?;
        if count == 0 {
            let status = self.child.wait().await.ok();
            bail!("codex app-server exited unexpectedly: {status:?}");
        }
        Ok(line)
    }

    async fn send(&mut self, value: Value) -> Result<()> {
        let line = serde_json::to_vec(&value)?;
        self.stdin.write_all(&line).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        Ok(())
    }

    pub async fn shutdown(mut self) {
        let _ = self.child.kill().await;
    }
}

fn usage_from_payload(payload: &Value) -> Option<TokenUsage> {
    let usage = payload.get("usage")?;
    Some(TokenUsage {
        input_tokens: usage
            .get("input_tokens")
            .or_else(|| usage.get("inputTokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        output_tokens: usage
            .get("output_tokens")
            .or_else(|| usage.get("outputTokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        total_tokens: usage
            .get("total_tokens")
            .or_else(|| usage.get("totalTokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    })
}

fn summarize_payload(payload: &Value) -> Option<String> {
    payload
        .get("params")
        .and_then(|params| params.get("msg"))
        .and_then(|msg| msg.get("content"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| payload.get("method").and_then(Value::as_str).map(str::to_string))
}
