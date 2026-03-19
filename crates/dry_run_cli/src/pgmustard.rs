use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct PgMustardClient {
    api_key: String,
    client: reqwest::Client,
}

// -- Score endpoint types --

#[derive(Debug, Clone, Serialize)]
struct ScoreRequest<'a> {
    plan: &'a serde_json::Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ScoreResponse {
    pub query_identifier: Option<serde_json::Value>,
    pub query_time: Option<f64>,
    pub query_blocks: Option<i64>,
    pub best_tips: Vec<Tip>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Tip {
    pub tip_category: String,
    pub tip_title: String,
    pub score: f64,
    pub tip_explanation: Vec<String>,
    pub learn_more_links: Vec<String>,
}

// -- Save endpoint types --

#[derive(Debug, Clone, Serialize)]
struct SaveRequest<'a> {
    plan: &'a serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_text: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SaveResponse {
    pub id: String,
    pub explore_url: String,
    pub duration_ms: Option<f64>,
    pub buffers_kb: Option<i64>,
    pub top_tip_score: Option<f64>,
}

impl PgMustardClient {
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("PGMUSTARD_API_KEY").ok()?;
        Some(Self {
            api_key,
            client: reqwest::Client::new(),
        })
    }

    /// Call the score endpoint — returns 0-3 deterministic tips with scores and explanations.
    pub async fn score(
        &self,
        plan_json: &serde_json::Value,
    ) -> Result<ScoreResponse, PgMustardError> {
        let resp = self
            .client
            .post("https://app.pgmustard.com/api/v1/score")
            .bearer_auth(&self.api_key)
            .json(&ScoreRequest { plan: plan_json })
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(PgMustardError::Request)?;

        match resp.status().as_u16() {
            200 => resp.json().await.map_err(PgMustardError::Request),
            402 => Err(PgMustardError::CreditsExhausted),
            403 => Err(PgMustardError::AuthFailed),
            code => Err(PgMustardError::Api(
                code,
                resp.text().await.unwrap_or_default(),
            )),
        }
    }

    /// Call the save endpoint — saves plan to pgMustard, returns explore_url for deep-dive UI.
    pub async fn save(
        &self,
        plan_json: &serde_json::Value,
        sql: Option<&str>,
        name: Option<&str>,
    ) -> Result<SaveResponse, PgMustardError> {
        let resp = self
            .client
            .post("https://app.pgmustard.com/api/v1/save")
            .bearer_auth(&self.api_key)
            .json(&SaveRequest {
                plan: plan_json,
                query_text: sql,
                name,
            })
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(PgMustardError::Request)?;

        match resp.status().as_u16() {
            200 => resp.json().await.map_err(PgMustardError::Request),
            402 => Err(PgMustardError::CreditsExhausted),
            403 => Err(PgMustardError::AuthFailed),
            code => Err(PgMustardError::Api(
                code,
                resp.text().await.unwrap_or_default(),
            )),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PgMustardError {
    #[error("pgMustard API request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("pgMustard API credits exhausted")]
    CreditsExhausted,
    #[error("pgMustard authentication failed or subscription inactive")]
    AuthFailed,
    #[error("pgMustard API error ({0}): {1}")]
    Api(u16, String),
}
