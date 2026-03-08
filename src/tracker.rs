use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Value};

use crate::config::ServiceConfig;
use crate::model::{Issue, IssueBlocker};

#[derive(Clone)]
pub enum TrackerClient {
    Linear(LinearClient),
    GitHub(GitHubClient),
}

impl TrackerClient {
    pub fn new(config: Arc<ServiceConfig>) -> Result<Self> {
        match config.tracker_kind.as_deref() {
            Some("linear") => Ok(Self::Linear(LinearClient::new(config))),
            Some("github") => Ok(Self::GitHub(GitHubClient::new(config))),
            Some("memory") => Err(anyhow!("memory tracker is only available in demo mode")),
            Some(other) => Err(anyhow!("unsupported tracker kind: {other}")),
            None => Err(anyhow!("missing tracker kind")),
        }
    }

    pub async fn fetch_candidate_issues(&self) -> Result<Vec<Issue>> {
        match self {
            Self::Linear(client) => client.fetch_candidate_issues().await,
            Self::GitHub(client) => client.fetch_candidate_issues().await,
        }
    }

    pub async fn fetch_issue_states_by_ids(&self, ids: &[String]) -> Result<Vec<Issue>> {
        match self {
            Self::Linear(client) => client.fetch_issue_states_by_ids(ids).await,
            Self::GitHub(client) => client.fetch_issue_states_by_ids(ids).await,
        }
    }

    pub fn linear_client(&self) -> Option<&LinearClient> {
        match self {
            Self::Linear(client) => Some(client),
            Self::GitHub(_) => None,
        }
    }
}

#[derive(Clone)]
pub struct LinearClient {
    http: reqwest::Client,
    config: Arc<ServiceConfig>,
}

impl LinearClient {
    pub fn new(config: Arc<ServiceConfig>) -> Self {
        Self {
            http: reqwest::Client::new(),
            config,
        }
    }

    pub async fn fetch_candidate_issues(&self) -> Result<Vec<Issue>> {
        let states = self.config.linear_active_states.clone();
        self.fetch_issues_by_states(states).await
    }

    pub async fn fetch_issues_by_states(&self, states: Vec<String>) -> Result<Vec<Issue>> {
        let project_slug = self
            .config
            .linear_project_slug
            .clone()
            .ok_or_else(|| anyhow!("missing linear project slug"))?;
        let query = r#"
query SymphonyLinearPoll($projectSlug: String!, $stateNames: [String!]!, $first: Int!, $relationFirst: Int!, $after: String) {
  issues(filter: {project: {slugId: {eq: $projectSlug}}, state: {name: {in: $stateNames}}}, first: $first, after: $after) {
    nodes {
      id
      identifier
      title
      description
      priority
      state { name }
      branchName
      url
      assignee { id }
      labels { nodes { name } }
      inverseRelations(first: $relationFirst) {
        nodes {
          type
          issue {
            id
            identifier
            state { name }
          }
        }
      }
      createdAt
      updatedAt
    }
    pageInfo { hasNextPage endCursor }
  }
}"#;
        let mut after = Value::Null;
        let mut issues = Vec::new();
        loop {
            let response = self
                .graphql(
                    query,
                    json!({
                        "projectSlug": project_slug,
                        "stateNames": states,
                        "first": 50,
                        "relationFirst": 50,
                        "after": after,
                    }),
                )
                .await?;
            let page = response
                .get("data")
                .and_then(|d| d.get("issues"))
                .ok_or_else(|| anyhow!("invalid Linear issues response"))?;
            let nodes = page
                .get("nodes")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("missing issues.nodes"))?;
            for node in nodes {
                if let Some(issue) = normalize_linear_issue(node) {
                    if issue_routable(&self.config, &issue) {
                        issues.push(issue);
                    }
                }
            }
            let page_info = page
                .get("pageInfo")
                .and_then(Value::as_object)
                .ok_or_else(|| anyhow!("missing pageInfo"))?;
            if page_info.get("hasNextPage").and_then(Value::as_bool) == Some(true) {
                after = page_info.get("endCursor").cloned().unwrap_or(Value::Null);
            } else {
                break;
            }
        }
        Ok(issues)
    }

    pub async fn fetch_issue_states_by_ids(&self, ids: &[String]) -> Result<Vec<Issue>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let query = r#"
query SymphonyLinearIssuesById($ids: [ID!]!, $first: Int!, $relationFirst: Int!) {
  issues(filter: {id: {in: $ids}}, first: $first) {
    nodes {
      id
      identifier
      title
      description
      priority
      state { name }
      branchName
      url
      assignee { id }
      labels { nodes { name } }
      inverseRelations(first: $relationFirst) {
        nodes {
          type
          issue {
            id
            identifier
            state { name }
          }
        }
      }
      createdAt
      updatedAt
    }
  }
}"#;
        let response = self
            .graphql(
                query,
                json!({
                    "ids": ids,
                    "first": ids.len().min(50),
                    "relationFirst": 50,
                }),
            )
            .await?;
        let nodes = response
            .get("data")
            .and_then(|d| d.get("issues"))
            .and_then(|d| d.get("nodes"))
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("invalid Linear issue state response"))?;
        Ok(nodes.iter().filter_map(normalize_linear_issue).collect())
    }

    pub async fn graphql(&self, query: &str, variables: Value) -> Result<Value> {
        let token = self
            .config
            .linear_api_token
            .as_deref()
            .ok_or_else(|| anyhow!("missing linear api token"))?;
        let payload = json!({ "query": query, "variables": variables });
        let response = self
            .http
            .post(&self.config.linear_endpoint)
            .header(CONTENT_TYPE, "application/json")
            .header(AUTHORIZATION, token)
            .json(&payload)
            .send()
            .await
            .context("failed to send Linear GraphQL request")?;
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .context("failed to decode Linear GraphQL response")?;
        if !status.is_success() {
            return Err(anyhow!("Linear GraphQL request failed with status {status}: {body}"));
        }
        Ok(body)
    }
}

#[derive(Clone)]
pub struct GitHubClient {
    http: reqwest::Client,
    config: Arc<ServiceConfig>,
}

impl GitHubClient {
    pub fn new(config: Arc<ServiceConfig>) -> Self {
        Self {
            http: reqwest::Client::new(),
            config,
        }
    }

    pub async fn fetch_candidate_issues(&self) -> Result<Vec<Issue>> {
        let owner = self
            .config
            .github_owner
            .as_deref()
            .ok_or_else(|| anyhow!("missing github owner"))?;
        let repo = self
            .config
            .github_repo
            .as_deref()
            .ok_or_else(|| anyhow!("missing github repo"))?;

        let linked_prs = self.fetch_linked_pull_requests().await?;
        let mut page = 1usize;
        let mut issues = Vec::new();

        loop {
            let body = self
                .request_json(
                    format!("/repos/{owner}/{repo}/issues"),
                    &[
                        ("state", "all".to_string()),
                        ("sort", "created".to_string()),
                        ("direction", "asc".to_string()),
                        ("per_page", "100".to_string()),
                        ("page", page.to_string()),
                    ],
                )
                .await?;

            let items = body
                .as_array()
                .ok_or_else(|| anyhow!("invalid GitHub issues response"))?;

            if items.is_empty() {
                break;
            }

            for item in items {
                if let Some(issue) = normalize_github_issue(owner, repo, item, linked_prs.get(&github_issue_number(item))) {
                    if candidate_github_issue(&self.config, &issue) {
                        issues.push(issue);
                    }
                }
            }

            if items.len() < 100 {
                break;
            }

            page += 1;
        }

        Ok(issues)
    }

    pub async fn fetch_issue_states_by_ids(&self, ids: &[String]) -> Result<Vec<Issue>> {
        let owner = self
            .config
            .github_owner
            .as_deref()
            .ok_or_else(|| anyhow!("missing github owner"))?;
        let repo = self
            .config
            .github_repo
            .as_deref()
            .ok_or_else(|| anyhow!("missing github repo"))?;
        let linked_prs = self.fetch_linked_pull_requests().await?;
        let mut issues = Vec::new();

        for id in ids {
            let Ok(number) = id.parse::<u64>() else {
                continue;
            };
            let body = self
                .request_json(format!("/repos/{owner}/{repo}/issues/{number}"), &[])
                .await?;
            if let Some(issue) = normalize_github_issue(owner, repo, &body, linked_prs.get(&number)) {
                issues.push(issue);
            }
        }

        Ok(issues)
    }

    async fn fetch_linked_pull_requests(&self) -> Result<HashMap<u64, LinkedPullRequest>> {
        let owner = self
            .config
            .github_owner
            .as_deref()
            .ok_or_else(|| anyhow!("missing github owner"))?;
        let repo = self
            .config
            .github_repo
            .as_deref()
            .ok_or_else(|| anyhow!("missing github repo"))?;
        let mut page = 1usize;
        let mut linked = HashMap::new();

        loop {
            let body = self
                .request_json(
                    format!("/repos/{owner}/{repo}/pulls"),
                    &[
                        ("state", "open".to_string()),
                        ("sort", "created".to_string()),
                        ("direction", "asc".to_string()),
                        ("per_page", "100".to_string()),
                        ("page", page.to_string()),
                    ],
                )
                .await?;
            let pulls = body
                .as_array()
                .ok_or_else(|| anyhow!("invalid GitHub pulls response"))?;

            if pulls.is_empty() {
                break;
            }

            for pull in pulls {
                let Some(pr) = normalize_pull_request(pull) else {
                    continue;
                };
                for number in linked_issue_numbers(&pr.title, &pr.body) {
                    linked.entry(number).or_insert_with(|| pr.clone());
                }
            }

            if pulls.len() < 100 {
                break;
            }

            page += 1;
        }

        Ok(linked)
    }

    async fn request_json(&self, path: String, query: &[(&str, String)]) -> Result<Value> {
        let url = format!("{}{}", self.config.github_endpoint.trim_end_matches('/'), path);
        let mut request = self
            .http
            .get(url)
            .query(query)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .header("User-Agent", "symphony-rust");
        if let Some(token) = self.config.github_api_token.as_deref() {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.context("failed to send GitHub request")?;
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .context("failed to decode GitHub response")?;
        if !status.is_success() {
            return Err(anyhow!("GitHub request failed with status {status}: {body}"));
        }
        Ok(body)
    }
}

#[derive(Clone, Debug)]
struct LinkedPullRequest {
    head_ref: String,
    html_url: Option<String>,
    title: String,
    body: String,
}

fn normalize_pull_request(pull: &Value) -> Option<LinkedPullRequest> {
    Some(LinkedPullRequest {
        head_ref: pull.get("head")?.get("ref")?.as_str()?.to_string(),
        html_url: pull.get("html_url").and_then(Value::as_str).map(str::to_string),
        title: pull.get("title")?.as_str()?.to_string(),
        body: pull
            .get("body")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    })
}

pub fn issue_routable(config: &ServiceConfig, issue: &Issue) -> bool {
    match config.tracker_kind.as_deref() {
        Some("github") => match config.github_assignee.as_deref() {
            None => true,
            Some(assignee) => issue.assignee_id.as_deref() == Some(assignee),
        },
        _ => match config.linear_assignee.as_deref() {
            None => true,
            Some(assignee) => issue.assignee_id.as_deref() == Some(assignee),
        },
    }
}

fn candidate_github_issue(config: &ServiceConfig, issue: &Issue) -> bool {
    config
        .linear_active_states
        .iter()
        .any(|state| state.eq_ignore_ascii_case(&issue.state))
        && issue_routable(config, issue)
        && (config.github_labels.is_empty()
            || config.github_labels.iter().all(|required| {
                issue
                    .labels
                    .iter()
                    .any(|label| label.eq_ignore_ascii_case(required))
            }))
}

pub fn normalize_linear_issue(issue: &Value) -> Option<Issue> {
    let id = issue.get("id")?.as_str()?.to_string();
    let identifier = issue.get("identifier")?.as_str()?.to_string();
    let title = issue.get("title")?.as_str()?.to_string();
    let state = issue
        .get("state")
        .and_then(|state| state.get("name"))
        .and_then(Value::as_str)?
        .to_string();
    let labels = issue
        .get("labels")
        .and_then(|v| v.get("nodes"))
        .and_then(Value::as_array)
        .map(|nodes| {
            nodes.iter()
                .filter_map(|node| node.get("name").and_then(Value::as_str))
                .map(|label| label.to_lowercase())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let blocked_by = issue
        .get("inverseRelations")
        .and_then(|v| v.get("nodes"))
        .and_then(Value::as_array)
        .map(|nodes| {
            nodes.iter()
                .filter(|node| node.get("type").and_then(Value::as_str) == Some("blocks"))
                .filter_map(|node| node.get("issue"))
                .map(|related| IssueBlocker {
                    id: related.get("id").and_then(Value::as_str).map(str::to_string),
                    identifier: related
                        .get("identifier")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    state: related
                        .get("state")
                        .and_then(|s| s.get("name"))
                        .and_then(Value::as_str)
                        .map(str::to_string),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Some(Issue {
        id,
        identifier,
        title,
        description: issue
            .get("description")
            .and_then(Value::as_str)
            .map(str::to_string),
        priority: issue.get("priority").and_then(Value::as_i64),
        state,
        branch_name: issue.get("branchName").and_then(Value::as_str).map(str::to_string),
        url: issue.get("url").and_then(Value::as_str).map(str::to_string),
        labels,
        blocked_by,
        created_at: parse_datetime(issue.get("createdAt")),
        updated_at: parse_datetime(issue.get("updatedAt")),
        assignee_id: issue
            .get("assignee")
            .and_then(|a| a.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

fn normalize_github_issue(
    owner: &str,
    repo: &str,
    issue: &Value,
    linked_pr: Option<&LinkedPullRequest>,
) -> Option<Issue> {
    if issue.get("pull_request").is_some() {
        return None;
    }
    let number = github_issue_number(issue);
    let mut description = issue
        .get("body")
        .and_then(Value::as_str)
        .map(str::to_string);

    if let Some(pr) = linked_pr {
        let suffix = format!(
            "\n\nLinked PR:\n- Title: {}\n- Branch: {}\n- URL: {}\n",
            pr.title,
            pr.head_ref,
            pr.html_url.as_deref().unwrap_or("n/a")
        );
        match &mut description {
            Some(body) => body.push_str(&suffix),
            None => description = Some(suffix),
        }
    }

    Some(Issue {
        id: number.to_string(),
        identifier: format!("{owner}/{repo}#{number}"),
        title: issue.get("title")?.as_str()?.to_string(),
        description,
        priority: None,
        state: issue.get("state")?.as_str()?.to_string(),
        branch_name: linked_pr.map(|pr| pr.head_ref.clone()),
        url: issue
            .get("html_url")
            .and_then(Value::as_str)
            .map(str::to_string),
        labels: issue
            .get("labels")
            .and_then(Value::as_array)
            .map(|items| {
                items.iter()
                    .filter_map(|label| label.get("name").and_then(Value::as_str))
                    .map(|label| label.to_lowercase())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        blocked_by: Vec::new(),
        created_at: parse_datetime(issue.get("created_at")),
        updated_at: parse_datetime(issue.get("updated_at")),
        assignee_id: issue
            .get("assignee")
            .and_then(|assignee| assignee.get("login"))
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

fn github_issue_number(issue: &Value) -> u64 {
    issue.get("number").and_then(Value::as_u64).unwrap_or_default()
}

fn linked_issue_numbers(title: &str, body: &str) -> Vec<u64> {
    let text = format!("{}\n{}", title.to_lowercase(), body.to_lowercase());
    let mut numbers = Vec::new();
    for token in text.split(|ch: char| ch.is_whitespace() || ch == ',' || ch == '.' || ch == ')' || ch == '(') {
        if let Some(rest) = token.strip_prefix('#') {
            if let Ok(number) = rest.parse::<u64>() {
                if closing_reference_nearby(&text, number) {
                    numbers.push(number);
                }
            }
        }
    }
    numbers.sort_unstable();
    numbers.dedup();
    numbers
}

fn closing_reference_nearby(text: &str, number: u64) -> bool {
    let needle = format!("#{number}");
    for keyword in ["close", "closes", "closed", "fix", "fixes", "fixed", "resolve", "resolves", "resolved"] {
        let pattern = format!("{keyword} {needle}");
        if text.contains(&pattern) {
            return true;
        }
    }
    false
}

fn parse_datetime(value: Option<&Value>) -> Option<DateTime<Utc>> {
    value
        .and_then(Value::as_str)
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{closing_reference_nearby, linked_issue_numbers, normalize_github_issue, LinkedPullRequest};

    #[test]
    fn finds_closing_references() {
        let linked = linked_issue_numbers("Fix parser", "This closes #42 and resolves #50.");
        assert_eq!(linked, vec![42, 50]);
        assert!(closing_reference_nearby("fixes #42", 42));
    }

    #[test]
    fn normalizes_github_issue_with_pr_awareness() {
        let issue = normalize_github_issue(
            "openai",
            "symphony",
            &json!({
                "number": 42,
                "title": "Support GitHub issues",
                "body": "Body",
                "state": "open",
                "html_url": "https://github.com/openai/symphony/issues/42",
                "labels": [{ "name": "orchestration" }],
                "assignee": { "login": "adys-x1" },
                "created_at": "2026-03-08T00:00:00Z",
                "updated_at": "2026-03-08T00:00:00Z"
            }),
            Some(&LinkedPullRequest {
                head_ref: "feature/github-issues".to_string(),
                html_url: Some("https://github.com/openai/symphony/pull/77".to_string()),
                title: "closes #42".to_string(),
                body: "closes #42".to_string(),
            }),
        )
        .unwrap();
        assert_eq!(issue.id, "42");
        assert_eq!(issue.identifier, "openai/symphony#42");
        assert_eq!(issue.branch_name.as_deref(), Some("feature/github-issues"));
        assert!(issue.description.as_deref().unwrap_or("").contains("Linked PR"));
    }
}
