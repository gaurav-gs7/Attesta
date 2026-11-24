// Package archive sends each RolloutDecision to an archive outside the
// cluster, so a decision survives even if the guard and every record in the
// cluster are deleted. attesta-api is one such archive (POST
// /api/v1/decision-archive); any HTTP endpoint that accepts the record and
// answers 2xx works.
package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
)

// ErrRefused means the archive answered that it will not take the record
// (4xx): most often it already holds a different record at that position,
// which is what tampering in the cluster looks like from outside.
var ErrRefused = errors.New("archive refused the record")

// Client posts records to an archive.
type Client struct {
	url   string
	token string
	http  *http.Client
}

// New returns a client for the archive at url. token, if set, is sent as a
// bearer token.
func New(url, token string) *Client {
	return &Client{url: url, token: strings.TrimSpace(token), http: &http.Client{Timeout: 5 * time.Second}}
}

// Record is what is sent: the decision as stored in the cluster.
type Record struct {
	APIVersion string                        `json:"apiVersion"`
	Kind       string                        `json:"kind"`
	Metadata   Metadata                      `json:"metadata"`
	Spec       attestav1.RolloutDecisionSpec `json:"spec"`
}

// Metadata identifies the record in the cluster.
type Metadata struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// RecordOf converts a stored decision.
func RecordOf(d attestav1.RolloutDecision) Record {
	return Record{APIVersion: attestav1.GroupVersion.String(), Kind: "RolloutDecision",
		Metadata: Metadata{Name: d.Name, Namespace: d.Namespace}, Spec: d.Spec}
}

// Archive sends one record. Sending a record the archive already holds is
// not an error: archives answer it as a duplicate.
func (c *Client) Archive(ctx context.Context, d attestav1.RolloutDecision) error {
	body, err := json.Marshal(RecordOf(d))
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", d.Spec.Hash)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("archive %s: %w", d.Name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	switch {
	case resp.StatusCode/100 == 2:
		return nil
	case resp.StatusCode/100 == 4 && resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusUnauthorized:
		return fmt.Errorf("%w: %s: %d %s", ErrRefused, d.Name, resp.StatusCode, strings.TrimSpace(string(msg)))
	default:
		return fmt.Errorf("archive %s: %d %s", d.Name, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
}
