use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tokio::process::Command;
use tokio::time::timeout;

use crate::config::ServiceConfig;
use crate::model::{Issue, WorkspaceState};

pub async fn create_for_issue(config: &ServiceConfig, issue: &Issue) -> Result<WorkspaceState> {
    let workspace_key = safe_identifier(&issue.identifier);
    let path = config.workspace_root.join(&workspace_key);
    validate_workspace_path(&config.workspace_root, &path)?;

    let created_now = if path.is_dir() {
        clean_tmp_artifacts(&path).await?;
        false
    } else {
        if path.exists() {
            tokio::fs::remove_file(&path).await.ok();
            tokio::fs::remove_dir_all(&path).await.ok();
        }
        tokio::fs::create_dir_all(&path)
            .await
            .with_context(|| format!("failed to create workspace {}", path.display()))?;
        true
    };

    let workspace = WorkspaceState {
        path: path.to_string_lossy().into_owned(),
        workspace_key,
        created_now,
    };

    if created_now {
        if let Some(command) = &config.workspace_hooks.after_create {
            run_hook(command, &path, config.workspace_hooks.timeout_ms).await?;
        }
    }

    Ok(workspace)
}

pub async fn run_before_run_hook(config: &ServiceConfig, workspace: &WorkspaceState) -> Result<()> {
    if let Some(command) = &config.workspace_hooks.before_run {
        run_hook(command, Path::new(&workspace.path), config.workspace_hooks.timeout_ms).await?;
    }
    Ok(())
}

pub async fn run_after_run_hook(config: &ServiceConfig, workspace: &WorkspaceState) {
    if let Some(command) = &config.workspace_hooks.after_run {
        let _ = run_hook(command, Path::new(&workspace.path), config.workspace_hooks.timeout_ms).await;
    }
}

pub async fn remove_issue_workspace(config: &ServiceConfig, identifier: &str) {
    let workspace_key = safe_identifier(identifier);
    let path = config.workspace_root.join(workspace_key);
    if path.is_dir() {
        if let Some(command) = &config.workspace_hooks.before_remove {
            let _ = run_hook(command, &path, config.workspace_hooks.timeout_ms).await;
        }
        let _ = tokio::fs::remove_dir_all(&path).await;
    }
}

pub fn safe_identifier(identifier: &str) -> String {
    let mut output = String::with_capacity(identifier.len());
    for ch in identifier.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            output.push(ch);
        } else {
            output.push('_');
        }
    }
    if output.is_empty() {
        "issue".to_string()
    } else {
        output
    }
}

async fn clean_tmp_artifacts(workspace: &Path) -> Result<()> {
    for entry in [".elixir_ls", "tmp"] {
        let path = workspace.join(entry);
        if path.exists() {
            let _ = tokio::fs::remove_dir_all(&path).await;
            let _ = tokio::fs::remove_file(&path).await;
        }
    }
    Ok(())
}

async fn run_hook(command: &str, workspace: &Path, timeout_ms: u64) -> Result<()> {
    let mut child = Command::new("sh");
    child.arg("-lc").arg(command).current_dir(workspace);
    let output = timeout(Duration::from_millis(timeout_ms), child.output())
        .await
        .map_err(|_| anyhow!("workspace hook timed out after {timeout_ms}ms"))?
        .context("failed to run workspace hook")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        bail!(
            "workspace hook failed with status {} stdout={} stderr={}",
            output.status,
            truncate(&stdout, 512),
            truncate(&stderr, 512)
        );
    }
    Ok(())
}

fn truncate(value: &str, max: usize) -> String {
    if value.len() <= max {
        value.to_string()
    } else {
        format!("{}...", &value[..max])
    }
}

pub fn validate_workspace_path(root: &Path, workspace: &Path) -> Result<()> {
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let workspace = if workspace.exists() {
        workspace.canonicalize().unwrap_or_else(|_| workspace.to_path_buf())
    } else {
        workspace.to_path_buf()
    };
    if workspace == root {
        bail!("workspace path equals workspace root");
    }
    if !workspace.starts_with(&root) {
        bail!("workspace path is outside workspace root");
    }

    let relative = workspace
        .strip_prefix(&root)
        .unwrap_or(workspace.as_path());
    let mut current = PathBuf::from(&root);
    for component in relative.components() {
        if let Component::Normal(segment) = component {
            current.push(segment);
            if let Ok(metadata) = std::fs::symlink_metadata(&current) {
                if metadata.file_type().is_symlink() {
                    bail!("workspace path contains symlink component");
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{safe_identifier, validate_workspace_path};

    #[test]
    fn sanitizes_identifier() {
        assert_eq!(safe_identifier("MT 123/abc"), "MT_123_abc");
    }

    #[test]
    fn rejects_root_workspace() {
        let dir = tempdir().unwrap();
        let err = validate_workspace_path(dir.path(), dir.path()).unwrap_err();
        assert!(err.to_string().contains("equals"));
    }
}
