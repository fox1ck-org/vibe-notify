// Package notify emits notification envelopes to vibe-platform's
// webhook-listener, which queues them for the user-notification-worker to fan
// out per each recipient's preferences.
//
// It is a separate module rather than a package in vibe-common on purpose.
// Consumers of vibe-common currently span v1.2.0 to v1.23.0, and two of the
// services that need to emit (vibe-cards, vibe-tasks) do not depend on it at
// all — so putting an emitter there would force a fleet-wide version bump,
// dragging 21 minor versions of unrelated auth/keycloak/repository churn into
// vibe-fb for the sake of a 200-line HTTP client. The build graph is stdlib
// plus uuid, so taking this module costs a service nothing. (The outbox
// subpackage's integration tests import lib/pq, but Go's module graph pruning
// means a consumer never builds or resolves a test-only dependency.)
//
// The envelope shape is the single canonical contract across every source app.
// Keep it in step with vibe-platform/packages/notifications/src/types/event.ts.
package notify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Priority mirrors the canonical ladder. There are deliberately four rungs and
// no "success": a green checkmark is a Tone, and adding a fifth priority would
// break the ordinal comparison the router uses to apply a user's minimum.
type Priority string

const (
	PriorityInfo    Priority = "info"
	PriorityWarning Priority = "warning"
	PriorityError   Priority = "error"
	PriorityUrgent  Priority = "urgent"
)

// Tone is presentation only — it never affects whether something is delivered.
type Tone string

const (
	ToneNeutral Tone = "neutral"
	ToneSuccess Tone = "success"
	ToneFailure Tone = "failure"
)

// SoundClass is a request, not a decision: the hub gates it on the recipient's
// sound_enabled preference and current mode before any client hears it.
type SoundClass string

const (
	SoundNone   SoundClass = "none"
	SoundSoft   SoundClass = "soft"
	SoundAlert  SoundClass = "alert"
	SoundUrgent SoundClass = "urgent"
)

// Channel names must match the hub's enum.
type Channel string

const (
	ChannelInApp      Channel = "in_app"
	ChannelWebPush    Channel = "web_push"
	ChannelEmail      Channel = "email"
	ChannelMattermost Channel = "mattermost"
)

// Envelope is the wire contract. Field names are snake_case to match the Zod
// schema on the receiving side; a mismatch here is a 400 the source will only
// notice in a log, so the struct tags are load-bearing.
type Envelope struct {
	ID         string         `json:"id"`
	Source     string         `json:"source"`
	EventType  string         `json:"event_type"`
	OccurredAt string         `json:"occurred_at"`
	ActorSub   string         `json:"actor_sub,omitempty"`
	Recipients []string       `json:"recipients"`
	Priority   Priority       `json:"priority"`
	Category   string         `json:"category"`
	Title      string         `json:"title"`
	Body       string         `json:"body"`
	Deeplink   string         `json:"deeplink"`
	Payload    map[string]any `json:"payload,omitempty"`

	// ThreadKey groups every step of one story — an order and the onboarding
	// run answering it share a key — so the history renders a thread rather
	// than a dozen loose pings. Rendered by the source, because only the source
	// knows the request id.
	ThreadKey string `json:"thread_key,omitempty"`
	// CollapseKey makes a newer event supersede older unread ones sharing it,
	// which is what stops a twelve-step order badging as twelve. It also
	// becomes the Web Push tag, so the OS replaces rather than stacks.
	CollapseKey string     `json:"collapse_key,omitempty"`
	Tone        Tone       `json:"tone,omitempty"`
	SoundClass  SoundClass `json:"sound_class,omitempty"`
}

// Validate mirrors the receiving Zod schema so a malformed envelope fails here,
// with a useful message, instead of as an anonymous 400 in a log nobody reads.
func (e Envelope) Validate() error {
	switch {
	case e.ID == "":
		return fmt.Errorf("notify: id is required")
	case e.Source == "":
		return fmt.Errorf("notify: source is required")
	case e.EventType == "":
		return fmt.Errorf("notify: event_type is required")
	case len(e.Recipients) == 0:
		return fmt.Errorf("notify: at least one recipient is required")
	case e.Title == "":
		return fmt.Errorf("notify: title is required — it is what a human reads")
	case len(e.Title) > 200:
		return fmt.Errorf("notify: title exceeds 200 characters")
	case len(e.Body) > 2000:
		return fmt.Errorf("notify: body exceeds 2000 characters")
	case e.Deeplink == "":
		return fmt.Errorf("notify: deeplink is required")
	}
	switch e.Priority {
	case PriorityInfo, PriorityWarning, PriorityError, PriorityUrgent:
	default:
		return fmt.Errorf("notify: invalid priority %q", e.Priority)
	}
	return nil
}

type Option func(*Client)

// WithHMAC signs the body as sha256(secret + body), which is what the listener
// verifies. Leave it unset when the listener has no secret configured —
// sending a signature it cannot check is harmless, but enabling the secret on
// the listener before every caller signs will 401 them all.
func WithHMAC(secret string) Option { return func(c *Client) { c.secret = secret } }

func WithTimeout(d time.Duration) Option { return func(c *Client) { c.http.Timeout = d } }

func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

type Client struct {
	baseURL string
	source  string
	secret  string
	http    *http.Client
	logger  *slog.Logger
}

// NewClient returns nil when baseURL is empty, and every method is nil-safe.
// This is the reason a developer can run the service locally, or a test can
// construct it, without a hub reachable: calls become no-ops rather than
// errors. Keep it.
func NewClient(baseURL, source string, logger *slog.Logger, opts ...Option) *Client {
	if baseURL == "" {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	c := &Client{
		baseURL: baseURL,
		source:  source,
		http:    &http.Client{Timeout: 5 * time.Second},
		logger:  logger.With("component", "notify"),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// NewEnvelope stamps the fields every envelope needs and that no caller should
// have to remember: a fresh id, the source, and the time.
func (c *Client) NewEnvelope(eventType string, recipients []string) Envelope {
	src := ""
	if c != nil {
		src = c.source
	}
	return Envelope{
		ID:         uuid.NewString(),
		Source:     src,
		EventType:  eventType,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Recipients: recipients,
		Priority:   PriorityInfo,
	}
}

// Emit delivers the envelope and returns the error. Use it where the caller can
// do something useful with a failure — inside a Temporal activity that will be
// retried, or an outbox drainer that will try again.
func (c *Client) Emit(ctx context.Context, env Envelope) error {
	if c == nil {
		return nil
	}
	if err := env.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("notify: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/webhook/notifications", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("notify: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.secret != "" {
		// The listener computes a plain sha256(secret + body) digest, not a
		// keyed HMAC. Match it exactly; "close enough" here is a silent 401.
		h := sha256.New()
		h.Write([]byte(c.secret))
		h.Write(body)
		req.Header.Set("x-webhook-signature", "sha256="+hex.EncodeToString(h.Sum(nil)))
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("notify: send: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("notify: %s returned %d: %s", env.EventType, resp.StatusCode, string(b))
	}
	return nil
}

// EmitAsync is fire-and-forget: it logs a failure and never blocks or breaks
// the business operation that triggered it. Correct only where losing the
// notification costs nothing because the state is visible elsewhere — never
// for something a person is waiting on. For that, use an outbox.
func (c *Client) EmitAsync(ctx context.Context, env Envelope) {
	if c == nil {
		return
	}
	if err := c.Emit(ctx, env); err != nil {
		c.logger.Warn("emit failed",
			"event_type", env.EventType,
			"recipients", len(env.Recipients),
			"error", err)
	}
}
