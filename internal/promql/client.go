// Package promql is a minimal Prometheus instant-query client shared by the
// API's SLO gate and the controller's rollout analysis.
package promql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
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
	baseURL      string
	http         *http.Client
	auth         func(*http.Request)
	queryTimeout time.Duration
}

// Options configure access to a Prometheus server that needs it.
type Options struct {
	// BearerToken is sent as "Authorization: Bearer <token>".
	BearerToken string
	// Username and Password, if Username is set, are sent as basic auth.
	Username, Password string
	// CAFile is a PEM bundle to trust for the server's TLS certificate, in
	// addition to the system roots.
	CAFile string
	// QueryTimeout bounds each query; 0 means DefaultQueryTimeout.
	QueryTimeout time.Duration
}

// DefaultQueryTimeout bounds a single query when Options does not.
const DefaultQueryTimeout = 3 * time.Second

// NewClient returns a client for an unauthenticated Prometheus at baseURL.
func NewClient(baseURL string) *Client {
	c, _ := New(baseURL, Options{})
	return c
}

// New returns a client for the Prometheus server at baseURL. One client is
// meant to be shared: it keeps its connections between queries.
func New(baseURL string, opts Options) (*Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if opts.CAFile != "" {
		pem, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read Prometheus CA file: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no PEM certificates in Prometheus CA file %s", opts.CAFile)
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	c := &Client{
		baseURL:      strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		http:         &http.Client{Transport: transport},
		auth:         func(*http.Request) {},
		queryTimeout: opts.QueryTimeout,
	}
	if c.queryTimeout <= 0 {
		c.queryTimeout = DefaultQueryTimeout
	}
	switch {
	case opts.BearerToken != "" && opts.Username != "":
		return nil, errors.New("configure either a Prometheus bearer token or basic auth, not both")
	case opts.BearerToken != "":
		token := opts.BearerToken
		c.auth = func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
	case opts.Username != "":
		user, pass := opts.Username, opts.Password
		c.auth = func(r *http.Request) { r.SetBasicAuth(user, pass) }
	}
	return c, nil
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
	ctx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()
	values := url.Values{}
	values.Set("query", query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/query?"+values.Encode(), nil)
	if err != nil {
		return 0, false, err
	}
	c.auth(req)
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
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return 0, false, err
	}
	if len(body) > maxResponseBytes {
		return 0, false, fmt.Errorf("prometheus response exceeds %d bytes", maxResponseBytes)
	}
	if err := json.Unmarshal(body, &payload); err != nil {
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
