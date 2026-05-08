package pgmustard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

const defaultEndpoint = "https://app.pgmustard.com/api/query-plans"

type (
	Client struct {
		apiKey   string
		endpoint string
		http     *http.Client
	}

	AnalysisResult struct {
		Tips []Tip `json:"tips"`
	}

	Tip struct {
		Category    string `json:"category"`
		Description string `json:"description"`
		Impact      string `json:"impact"`
	}
)

// Empty apiKey falls back to PGMUSTARD_API_KEY env
func NewClient(apiKey string) *Client {
	if apiKey == "" {
		apiKey = os.Getenv("PGMUSTARD_API_KEY")
	}
	return &Client{
		apiKey:   apiKey,
		endpoint: defaultEndpoint,
		http:     &http.Client{},
	}
}

func (c *Client) HasKey() bool {
	return c.apiKey != ""
}

func (c *Client) AnalyzePlan(planJSON json.RawMessage) (*AnalysisResult, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("pgMustard API key not configured")
	}

	body := map[string]any{
		"plan": planJSON,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", c.endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("pgMustard API request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pgMustard API returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result AnalysisResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return &result, nil
}
