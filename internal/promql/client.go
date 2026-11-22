// Package promql is a minimal Prometheus instant-query client shared by the
// API's SLO gate and the controller's rollout analysis.
package promql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// maxResponseBytes caps a single query response.
const maxResponseBytes = 1 << 20

// Querier runs an instant query and returns its first sample.
type Querier interface {
	// Query returns found=false when the result is empty or the sample is
	// NaN or infinite. Transport and query errors return err.
	Query(ctx context.Context, query string) (value float64, found bool, err error)
}

type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient returns a client for the Prometheus server at baseURL.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		http:    &http.Client{Timeout: 5 * time.Second},
	}
}

// WithHTTPClient replaces the HTTP client, for tests.
func (c *Client) WithHTTPClient(h *http.Client) *Client {
	c.http = h
	return c
}

func (c *Client) Query(ctx context.Context, query string) (float64, bool, error) {
	if c.baseURL == "" {
		return 0, false, errors.New("prometheus URL is empty")
	}
	values := url.Values{}
	values.Set("query", query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/query?"+values.Encode(), nil)
	if err != nil {
		return 0, false, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, false, fmt.Errorf("prometheus returned %s", resp.Status)
	}
	var payload struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return 0, false, err
	}
	if payload.Status != "success" {
		if payload.Error == "" {
			payload.Error = "query failed"
		}
		return 0, false, errors.New(payload.Error)
	}
	if len(payload.Data.Result) == 0 {
		return 0, false, nil
	}
	if len(payload.Data.Result[0].Value) < 2 {
		return 0, false, fmt.Errorf("query returned no scalar result")
	}
	raw, ok := payload.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, false, fmt.Errorf("query scalar result was not a string")
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse scalar result %q: %w", raw, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false, nil
	}
	return value, true, nil
}
