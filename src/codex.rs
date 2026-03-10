use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use serde_json::{json, Value};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::time::{timeout, Duration};

use crate::config::ServiceConfig;
use crate::model::{Issue, RunPhase, TokenUsage, WorkerEvent, WorkerUpdate};
use crate::tracker::LinearClient;

pub struct AppServerSession {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    config: Arc<ServiceConfig>,
    thread_id: String,
    workspace: String,
    app_server_pid: Option<String>,
    stdout_log_path: PathBuf,
}

pub async fn start_session(
    config: Arc<ServiceConfig>,
    workspace: &Path,
) -> Result<AppServerSession> {
    let (stdout_log_path, stderr_log_path) = log_paths(workspace);
    if let Some(parent) = stdout_log_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    reset_log_file(&stdout_log_path).await?;
    reset_log_file(&stderr_log_path).await?;
    let mut child = Command::new("bash");
    child
        .arg("-lc")
        .arg(&config.codex_command)
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = child.spawn().context("failed to spawn codex app-server")?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("missing codex stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("missing codex stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("missing codex stderr"))?;
    let app_server_pid = child.id().map(|id| id.to_string());
    spawn_stderr_capture(stderr, stderr_log_path.clone());
    let mut session = AppServerSession {
        child,
        stdin,
        stdout: BufReader::new(stdout),
        config,
        thread_id: String::new(),
        workspace: workspace.to_string_lossy().into_owned(),
        app_server_pid,
        stdout_log_path,
    };
    session.initialize().await?;
    Ok(session)
}

fn log_paths(workspace: &Path) -> (PathBuf, PathBuf) {
    let root = workspace.join(".symphony");
    (
        root.join("codex-stdout.jsonl"),
        root.join("codex-stderr.log"),
    )
}

async fn reset_log_file(path: &Path) -> Result<()> {
    let _ = tokio::fs::remove_file(path).await;
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await
        .with_context(|| format!("failed to create {}", path.display()))?;
    Ok(())
}

fn spawn_stderr_capture(mut stderr: ChildStderr, path: PathBuf) {
    tokio::spawn(async move {
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
        {
            let _ = tokio::io::copy(&mut stderr, &mut file).await;
        }
    });
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
        self.send(json!({"method": "initialized", "params": {}}))
            .await?;
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
            let line = self
                .read_line_with_timeout(self.config.codex_turn_timeout_ms)
                .await?;
            let payload: Value = match serde_json::from_str(&line) {
                Ok(payload) => payload,
                Err(_) => continue,
            };
            if let Some(method) = payload.get("method").and_then(Value::as_str) {
                match method {
                    "turn/completed" => {
                        worker_tx
                            .send(WorkerEvent {
                                issue_id: issue.id.clone(),
                                update: WorkerUpdate::CodexMessage {
                                    event: "turn_completed".to_string(),
                                    timestamp: Utc::now(),
                                    session_id: turn_id
                                        .as_ref()
                                        .map(|id| format!("{}-{id}", self.thread_id)),
                                    thread_id: Some(self.thread_id.clone()),
                                    turn_id: turn_id.clone(),
                                    codex_app_server_pid: self.app_server_pid.clone(),
                                    message: Some("completed".to_string()),
                                    usage: usage_from_payload(&payload),
                                    rate_limits: rate_limits_from_payload(&payload),
                                    phase: Some(RunPhase::Waiting),
                                    phase_detail: Some(
                                        "turn completed; reconciling issue state".to_string(),
                                    ),
                                    last_command: command_from_payload(&payload),
                                    last_file_touched: file_from_payload(&payload),
                                },
                            })
                            .ok();
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
                        let id = payload
                            .get("id")
                            .cloned()
                            .ok_or_else(|| anyhow!("approval request missing id"))?;
                        let decision =
                            if method == "execCommandApproval" || method == "applyPatchApproval" {
                                "approved_for_session"
                            } else {
                                "acceptForSession"
                            };
                        self.send(json!({ "id": id, "result": { "decision": decision } }))
                            .await?;
                    }
                    "item/tool/requestUserInput" => {
                        let id = payload
                            .get("id")
                            .cloned()
                            .ok_or_else(|| anyhow!("tool input request missing id"))?;
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
                        self.send(json!({ "id": id, "result": { "answers": answers } }))
                            .await?;
                    }
                    other => {
                        worker_tx
                            .send(WorkerEvent {
                                issue_id: issue.id.clone(),
                                update: WorkerUpdate::CodexMessage {
                                    event: other.to_string(),
                                    timestamp: Utc::now(),
                                    session_id: turn_id
                                        .as_ref()
                                        .map(|id| format!("{}-{id}", self.thread_id)),
                                    thread_id: Some(self.thread_id.clone()),
                                    turn_id: turn_id.clone(),
                                    codex_app_server_pid: self.app_server_pid.clone(),
                                    message: summarize_payload(&payload),
                                    usage: usage_from_payload(&payload),
                                    rate_limits: rate_limits_from_payload(&payload),
                                    phase: infer_phase(other, &payload),
                                    phase_detail: summarize_payload(&payload),
                                    last_command: command_from_payload(&payload),
                                    last_file_touched: file_from_payload(&payload),
                                },
                            })
                            .ok();
                    }
                }
            }
        }
    }

    async fn handle_tool_call(
        &mut self,
        payload: &Value,
        linear: Option<&LinearClient>,
    ) -> Result<()> {
        let id = payload
            .get("id")
            .cloned()
            .ok_or_else(|| anyhow!("tool call missing id"))?;
        let params = payload.get("params").cloned().unwrap_or_else(|| json!({}));
        let tool_name = params
            .get("tool")
            .or_else(|| params.get("name"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
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
            let line = self
                .read_line_with_timeout(self.config.codex_read_timeout_ms)
                .await?;
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
        let count = timeout(
            Duration::from_millis(timeout_ms),
            self.stdout.read_line(&mut line),
        )
        .await
        .map_err(|_| anyhow!("codex read timeout after {timeout_ms}ms"))?
        .context("failed to read codex output")?;
        if count == 0 {
            let status = self.child.wait().await.ok();
            bail!("codex app-server exited unexpectedly: {status:?}");
        }
        self.append_stdout_log(&line).await?;
        Ok(line)
    }

    async fn append_stdout_log(&self, line: &str) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.stdout_log_path)
            .await
            .with_context(|| format!("failed to open {}", self.stdout_log_path.display()))?;
        file.write_all(line.as_bytes()).await?;
        Ok(())
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
    usage_candidates(payload)
        .into_iter()
        .find_map(token_usage_from_value)
}

fn usage_candidates<'a>(payload: &'a Value) -> Vec<&'a Value> {
    vec![
        payload.get("usage"),
        payload.pointer("/params/usage"),
        payload.pointer("/params/tokenUsage"),
        payload.pointer("/params/token_usage"),
        payload.pointer("/params/msg/usage"),
        payload.pointer("/params/msg/tokenUsage"),
        payload.pointer("/params/msg/token_usage"),
        payload.pointer("/params/info"),
        payload.pointer("/params/msg/info"),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn token_usage_from_value(value: &Value) -> Option<TokenUsage> {
    let input_tokens = find_first_u64(
        value,
        &[
            "input_tokens",
            "inputTokens",
            "input_token_count",
            "inputTokenCount",
        ],
    )
    .unwrap_or_default();
    let output_tokens = find_first_u64(
        value,
        &[
            "output_tokens",
            "outputTokens",
            "output_token_count",
            "outputTokenCount",
        ],
    )
    .unwrap_or_default();
    let total_tokens = find_first_u64(
        value,
        &[
            "total_tokens",
            "totalTokens",
            "total_token_usage",
            "totalTokenUsage",
            "totalUsage",
        ],
    )
    .unwrap_or(input_tokens + output_tokens);

    if input_tokens == 0 && output_tokens == 0 && total_tokens == 0 {
        None
    } else {
        Some(TokenUsage {
            input_tokens,
            output_tokens,
            total_tokens,
        })
    }
}

fn rate_limits_from_payload(payload: &Value) -> Option<Value> {
    [
        payload.pointer("/usage/rate_limits"),
        payload.pointer("/usage/rateLimits"),
        payload.pointer("/params/rate_limits"),
        payload.pointer("/params/rateLimits"),
        payload.pointer("/params/msg/info/rate_limits"),
        payload.pointer("/params/msg/info/rateLimits"),
        payload.pointer("/params/msg/rate_limits"),
        payload.pointer("/params/msg/rateLimits"),
    ]
    .into_iter()
    .flatten()
    .next()
    .cloned()
}

fn summarize_payload(payload: &Value) -> Option<String> {
    payload
        .get("params")
        .and_then(|params| params.get("msg"))
        .and_then(|msg| msg.get("content"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            payload
                .get("method")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

fn command_from_payload(payload: &Value) -> Option<String> {
    find_first_string(payload, &["command", "cmd", "rawCommand", "raw_command"])
}

fn file_from_payload(payload: &Value) -> Option<String> {
    find_first_string(
        payload,
        &["filePath", "file_path", "path", "targetPath", "target_path"],
    )
}

fn infer_phase(method: &str, payload: &Value) -> Option<RunPhase> {
    let command = command_from_payload(payload)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if command.contains("cargo test")
        || command.contains("cargo clippy")
        || command.contains("cargo check")
        || command.contains("pytest")
        || command.contains("npm test")
    {
        Some(RunPhase::Testing)
    } else if method.contains("commandExecution") {
        Some(RunPhase::Editing)
    } else if method.contains("fileChange") {
        Some(RunPhase::Editing)
    } else if method.contains("turn/completed") {
        Some(RunPhase::Waiting)
    } else {
        None
    }
}

fn find_first_string(value: &Value, keys: &[&str]) -> Option<String> {
    match value {
        Value::Object(map) => {
            for key in keys {
                if let Some(text) = map.get(*key).and_then(Value::as_str) {
                    if !text.trim().is_empty() {
                        return Some(text.to_string());
                    }
                }
            }
            map.values()
                .find_map(|child| find_first_string(child, keys))
        }
        Value::Array(items) => items.iter().find_map(|item| find_first_string(item, keys)),
        _ => None,
    }
}

fn find_first_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    match value {
        Value::Object(map) => {
            for key in keys {
                if let Some(number) = map.get(*key).and_then(Value::as_u64) {
                    return Some(number);
                }
            }
            map.values().find_map(|child| find_first_u64(child, keys))
        }
        Value::Array(items) => items.iter().find_map(|item| find_first_u64(item, keys)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{rate_limits_from_payload, usage_from_payload};

    #[test]
    fn usage_from_payload_reads_nested_token_usage_events() {
        let payload = json!({
            "method": "thread/tokenUsage/updated",
            "params": {
                "tokenUsage": {
                    "inputTokens": 1200,
                    "outputTokens": 340,
                    "totalTokens": 1540
                }
            }
        });

        let usage = usage_from_payload(&payload).expect("usage");
        assert_eq!(usage.input_tokens, 1200);
        assert_eq!(usage.output_tokens, 340);
        assert_eq!(usage.total_tokens, 1540);
    }

    #[test]
    fn usage_from_payload_reads_msg_info_token_count_events() {
        let payload = json!({
            "method": "codex/event/token_count",
            "params": {
                "msg": {
                    "info": {
                        "total_token_usage": 9876
                    }
                }
            }
        });

        let usage = usage_from_payload(&payload).expect("usage");
        assert_eq!(usage.total_tokens, 9876);
    }

    #[test]
    fn rate_limits_from_payload_reads_nested_shapes() {
        let payload = json!({
            "method": "account/rateLimits/updated",
            "params": {
                "rateLimits": {
                    "primary": { "usedPercent": 1 }
                }
            }
        });

        let rate_limits = rate_limits_from_payload(&payload).expect("rate limits");
        assert_eq!(rate_limits["primary"]["usedPercent"], 1);
    }
}
