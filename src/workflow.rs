use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde_yaml::Value;
use sha2::{Digest, Sha256};

use crate::model::{WorkflowDefinition, WorkflowStamp};

#[derive(Debug, Clone)]
pub struct WorkflowStore {
    path: PathBuf,
    last_good: Option<(WorkflowStamp, WorkflowDefinition)>,
}

impl WorkflowStore {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            last_good: None,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load_current(&mut self) -> Result<WorkflowDefinition> {
        let content = fs::read_to_string(&self.path)
            .with_context(|| format!("missing workflow file: {}", self.path.display()))?;
        let parsed = parse_workflow(&content)?;
        let stamp = current_stamp(&self.path, &content)?;
        self.last_good = Some((stamp, parsed.clone()));
        Ok(parsed)
    }

    pub fn reload_last_good(&mut self) -> Result<WorkflowDefinition> {
        let content = fs::read_to_string(&self.path)
            .with_context(|| format!("missing workflow file: {}", self.path.display()))?;
        let parsed = parse_workflow(&content)?;
        let stamp = current_stamp(&self.path, &content)?;
        self.last_good = Some((stamp, parsed.clone()));
        Ok(parsed)
    }

    pub fn current(&mut self) -> Result<WorkflowDefinition> {
        match self.reload_last_good() {
            Ok(definition) => Ok(definition),
            Err(err) => {
                if let Some((_, definition)) = &self.last_good {
                    Ok(definition.clone())
                } else {
                    Err(err)
                }
            }
        }
    }
}

pub fn parse_workflow(content: &str) -> Result<WorkflowDefinition> {
    let lines: Vec<&str> = content.lines().collect();
    let (front_matter_lines, prompt_lines) = if lines.first().copied() == Some("---") {
        let mut end_idx = None;
        for (idx, line) in lines.iter().enumerate().skip(1) {
            if *line == "---" {
                end_idx = Some(idx);
                break;
            }
        }
        match end_idx {
            Some(idx) => (&lines[1..idx], &lines[(idx + 1)..]),
            None => (&lines[1..], &[][..]),
        }
    } else {
        (&[][..], &lines[..])
    };

    let yaml = front_matter_lines.join("\n");
    let config = if yaml.trim().is_empty() {
        Value::Mapping(Default::default())
    } else {
        let value: Value = serde_yaml::from_str(&yaml).context("failed to parse workflow front matter")?;
        if !matches!(value, Value::Mapping(_)) {
            return Err(anyhow!("workflow front matter must decode to a map"));
        }
        value
    };
    let prompt_template = prompt_lines.join("\n").trim().to_string();
    Ok(WorkflowDefinition {
        config,
        prompt_template,
    })
}

fn current_stamp(path: &Path, content: &str) -> Result<WorkflowStamp> {
    let metadata = fs::metadata(path)?;
    let modified_epoch_secs = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    let content_sha256 = format!("{:x}", hasher.finalize());
    Ok(WorkflowStamp {
        modified_epoch_secs,
        size_bytes: metadata.len(),
        content_sha256,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_workflow;

    #[test]
    fn parses_front_matter_and_prompt() {
        let workflow = parse_workflow(
            r#"---
tracker:
  kind: linear
---
Hello {{ issue.identifier }}
"#,
        )
        .unwrap();
        assert_eq!(workflow.prompt_template, "Hello {{ issue.identifier }}");
    }

    #[test]
    fn requires_map_front_matter() {
        let err = parse_workflow(
            r#"---
- nope
---
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("must decode to a map"));
    }
}
