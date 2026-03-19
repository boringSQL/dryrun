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
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            client: reqwest::Client::new(),
        }
    }

    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("PGMUSTARD_API_KEY").ok()?;
        Some(Self::new(api_key))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_score_response() {
        let json = serde_json::json!({
            "query-identifier": null,
            "query-time": 3200.5,
            "query-blocks": 80000,
            "best-tips": [
                {
                    "tip-category": "index-potential",
                    "tip-title": "Potential index on orders.customer_id",
                    "score": 4.2,
                    "tip-explanation": ["Sequential scan on orders reading 50M rows..."],
                    "learn-more-links": ["https://www.pgmustard.com/docs/tips/seq-scan"]
                }
            ]
        });
        let resp: ScoreResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.query_time, Some(3200.5));
        assert_eq!(resp.query_blocks, Some(80000));
        assert_eq!(resp.best_tips.len(), 1);
        assert_eq!(resp.best_tips[0].tip_category, "index-potential");
        assert_eq!(resp.best_tips[0].score, 4.2);
    }

    #[test]
    fn deserialize_score_response_empty_tips() {
        let json = serde_json::json!({
            "query-identifier": "abc123",
            "query-time": 1.2,
            "query-blocks": 10,
            "best-tips": []
        });
        let resp: ScoreResponse = serde_json::from_value(json).unwrap();
        assert!(resp.best_tips.is_empty());
        assert_eq!(resp.query_time, Some(1.2));
    }

    #[test]
    fn deserialize_save_response() {
        let json = serde_json::json!({
            "id": "40d6478e-abcd-1234-5678-aabbccddeeff",
            "explore_url": "https://app.pgmustard.com/#/explore/40d6478e",
            "duration_ms": 150.3,
            "buffers_kb": 512,
            "top_tip_score": 3.8
        });
        let resp: SaveResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.id, "40d6478e-abcd-1234-5678-aabbccddeeff");
        assert!(resp.explore_url.contains("pgmustard.com"));
        assert_eq!(resp.duration_ms, Some(150.3));
        assert_eq!(resp.top_tip_score, Some(3.8));
    }

    #[test]
    fn deserialize_save_response_minimal() {
        let json = serde_json::json!({
            "id": "abc",
            "explore_url": "https://app.pgmustard.com/#/explore/abc",
            "duration_ms": null,
            "buffers_kb": null,
            "top_tip_score": null
        });
        let resp: SaveResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.id, "abc");
        assert!(resp.duration_ms.is_none());
    }

    #[test]
    fn serialize_score_request() {
        let plan = serde_json::json!([{"Plan": {"Node Type": "Seq Scan"}}]);
        let req = ScoreRequest { plan: &plan };
        let json = serde_json::to_value(&req).unwrap();
        assert!(json.get("plan").is_some());
    }

    #[test]
    fn serialize_save_request_skips_none() {
        let plan = serde_json::json!([{"Plan": {"Node Type": "Seq Scan"}}]);
        let req = SaveRequest {
            plan: &plan,
            query_text: None,
            name: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert!(json.get("plan").is_some());
        assert!(json.get("query_text").is_none());
        assert!(json.get("name").is_none());
    }

    #[test]
    fn serialize_save_request_includes_optionals() {
        let plan = serde_json::json!([{"Plan": {"Node Type": "Seq Scan"}}]);
        let req = SaveRequest {
            plan: &plan,
            query_text: Some("SELECT 1"),
            name: Some("test-plan"),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["query_text"], "SELECT 1");
        assert_eq!(json["name"], "test-plan");
    }

    #[test]
    fn from_env_returns_none_without_key() {
        // ensure env var is not set for this test
        std::env::remove_var("PGMUSTARD_API_KEY");
        assert!(PgMustardClient::from_env().is_none());
    }

    #[test]
    fn error_display() {
        let err = PgMustardError::CreditsExhausted;
        assert_eq!(err.to_string(), "pgMustard API credits exhausted");

        let err = PgMustardError::AuthFailed;
        assert!(err.to_string().contains("authentication failed"));

        let err = PgMustardError::Api(500, "internal error".into());
        assert!(err.to_string().contains("500"));
        assert!(err.to_string().contains("internal error"));
    }
}
