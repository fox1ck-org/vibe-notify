// Package notifytest is a fake notification hub for a product's tests: it
// accepts envelopes on the hub's real path, validates them the way the hub
// would, records them, and lets a test assert what was emitted.
//
//	hub := notifytest.New(t)
//	svc := mysvc.New(hub.Client("vibe-cards"))
//	svc.DisableCard(ctx, id)
//	hub.RequireEmitted(t, "cards.card.disabled",
//		notifytest.Org(orgID), notifytest.ResourceIs("card", id))
package notifytest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	notify "github.com/fox1ck-org/vibe-notify"
)

// Hub is a running fake hub. URL is what NOTIFY_HUB_URL would be.
type Hub struct {
	URL string

	srv    *httptest.Server
	secret string

	mu     sync.Mutex
	events []notify.Envelope
	status int
}

// Option configures a Hub.
type Option func(*Hub)

// WithSecret makes the hub verify x-webhook-signature as the real listener
// does, answering 401 on a mismatch.
func WithSecret(secret string) Option { return func(h *Hub) { h.secret = secret } }

// New starts a fake hub and stops it when the test ends.
func New(t testing.TB, opts ...Option) *Hub {
	t.Helper()
	h := &Hub{status: http.StatusAccepted}
	for _, o := range opts {
		o(h)
	}
	h.srv = httptest.NewServer(http.HandlerFunc(h.serve))
	h.URL = h.srv.URL
	t.Cleanup(h.srv.Close)
	return h
}

// Client returns a notify client pointed at this hub, signing when the hub
// has a secret.
func (h *Hub) Client(source string, opts ...notify.Option) *notify.Client {
	if h.secret != "" {
		opts = append([]notify.Option{notify.WithHMAC(h.secret)}, opts...)
	}
	return notify.NewClient(h.URL, source, nil, opts...)
}

// Respond makes every later valid envelope answer with status — e.g. 503 to
// test an outage, 404 to test a permanent refusal. A non-2xx answer is not
// recorded as emitted.
func (h *Hub) Respond(status int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.status = status
}

// Events returns a copy of every accepted envelope, in arrival order.
func (h *Hub) Events() []notify.Envelope {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.events)
}

// Reset forgets every recorded envelope.
func (h *Hub) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = nil
}

func (h *Hub) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/webhook/notifications" {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if h.secret != "" {
		sum := sha256.Sum256(append([]byte(h.secret), body...))
		if r.Header.Get("x-webhook-signature") != "sha256="+hex.EncodeToString(sum[:]) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
	}
	var env notify.Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := env.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	status := h.status
	if status < 300 {
		h.events = append(h.events, env)
	}
	h.mu.Unlock()
	w.WriteHeader(status)
}

// Matcher narrows which envelope RequireEmitted accepts.
type Matcher struct {
	desc  string
	match func(notify.Envelope) bool
}

// Match is a custom matcher; desc says what it checks in a failure message.
func Match(desc string, fn func(notify.Envelope) bool) Matcher {
	return Matcher{desc: desc, match: fn}
}

// To matches an envelope whose recipients include sub.
func To(sub string) Matcher {
	return Match("recipient "+sub, func(e notify.Envelope) bool { return slices.Contains(e.Recipients, sub) })
}

// Org matches org_id.
func Org(orgID string) Matcher {
	return Match("org_id "+orgID, func(e notify.Envelope) bool { return e.OrgID == orgID })
}

// Visible matches visibility; an empty visibility counts as personal.
func Visible(v notify.Visibility) Matcher {
	return Match("visibility "+string(v), func(e notify.Envelope) bool {
		got := e.Visibility
		if got == "" {
			got = notify.VisibilityPersonal
		}
		return got == v
	})
}

// WithPriority matches priority.
func WithPriority(p notify.Priority) Matcher {
	return Match("priority "+string(p), func(e notify.Envelope) bool { return e.Priority == p })
}

// ResourceIs matches resource kind and id.
func ResourceIs(kind, id string) Matcher {
	return Match("resource "+kind+"/"+id, func(e notify.Envelope) bool {
		return e.Resource != nil && e.Resource.Kind == kind && e.Resource.ID == id
	})
}

// HasFact matches a fact with this label and value.
func HasFact(label, value string) Matcher {
	return Match(fmt.Sprintf("fact %q=%q", label, value), func(e notify.Envelope) bool {
		return slices.Contains(e.Facts, notify.F(label, value))
	})
}

// HasAction matches an action with this id.
func HasAction(id string) Matcher {
	return Match("action "+id, func(e notify.Envelope) bool {
		return slices.ContainsFunc(e.Actions, func(a notify.Action) bool { return a.ID == id })
	})
}

// ToDestination matches an envelope naming this destination in DestinationIDs.
func ToDestination(id string) Matcher {
	return Match("destination "+id, func(e notify.Envelope) bool { return slices.Contains(e.DestinationIDs, id) })
}

// RequireEmitted fails the test unless an accepted envelope has eventType and
// satisfies every matcher, and returns the first one that does.
func (h *Hub) RequireEmitted(t testing.TB, eventType string, matchers ...Matcher) notify.Envelope {
	t.Helper()
	events := h.Events()
	for _, e := range events {
		if e.EventType == eventType && all(e, matchers) {
			return e
		}
	}
	t.Fatalf("notifytest: no %q envelope%s; got %s", eventType, describe(matchers), summary(events))
	return notify.Envelope{}
}

// RequireNotEmitted fails the test if any accepted envelope has eventType and
// satisfies every matcher.
func (h *Hub) RequireNotEmitted(t testing.TB, eventType string, matchers ...Matcher) {
	t.Helper()
	for _, e := range h.Events() {
		if e.EventType == eventType && all(e, matchers) {
			t.Fatalf("notifytest: unexpected %q envelope%s (id %s)", eventType, describe(matchers), e.ID)
		}
	}
}

func all(e notify.Envelope, ms []Matcher) bool {
	for _, m := range ms {
		if !m.match(e) {
			return false
		}
	}
	return true
}

func describe(ms []Matcher) string {
	if len(ms) == 0 {
		return ""
	}
	d := make([]string, len(ms))
	for i, m := range ms {
		d[i] = m.desc
	}
	return " with " + strings.Join(d, ", ")
}

func summary(events []notify.Envelope) string {
	if len(events) == 0 {
		return "nothing"
	}
	types := make([]string, len(events))
	for i, e := range events {
		types[i] = e.EventType
	}
	return "[" + strings.Join(types, ", ") + "]"
}
