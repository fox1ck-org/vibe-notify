package notify

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNilClientIsSafe(t *testing.T) {
	// The whole reason NewClient returns nil for an empty base URL: a service
	// must run locally, and its tests must run in CI, without a hub.
	var c *Client
	c = NewClient("", "vibe-accounts", nil)
	if c != nil {
		t.Fatal("expected nil client for an empty base URL")
	}
	if err := c.Emit(context.Background(), Envelope{}); err != nil {
		t.Fatalf("nil client Emit should be a no-op, got %v", err)
	}
	c.EmitAsync(context.Background(), Envelope{}) // must not panic
	env := c.NewEnvelope("x", nil)
	if env.ID == "" {
		t.Fatal("NewEnvelope should still mint an id on a nil client")
	}
}

func validEnvelope() Envelope {
	return Envelope{
		ID:         "11111111-1111-1111-1111-111111111111",
		Source:     "vibe-accounts",
		EventType:  "accounts.request.opened",
		OccurredAt: "2026-09-04T12:00:00Z",
		Recipients: []string{"22222222-2222-2222-2222-222222222222"},
		Priority:   PriorityWarning,
		Category:   "request",
		Title:      "New order to fulfil",
		Deeplink:   "https://accounts.igrudsky.dev/requests/42",
	}
}

func TestValidateRejectsWhatTheHubWould(t *testing.T) {
	cases := map[string]func(*Envelope){
		"no id":          func(e *Envelope) { e.ID = "" },
		"no source":      func(e *Envelope) { e.Source = "" },
		"no event type":  func(e *Envelope) { e.EventType = "" },
		"no recipients":  func(e *Envelope) { e.Recipients = nil },
		"no title":       func(e *Envelope) { e.Title = "" },
		"no deeplink":    func(e *Envelope) { e.Deeplink = "" },
		"long title":     func(e *Envelope) { e.Title = strings.Repeat("x", 201) },
		"long body":      func(e *Envelope) { e.Body = strings.Repeat("x", 2001) },
		"bad priority":   func(e *Envelope) { e.Priority = "critical" },
		"empty priority": func(e *Envelope) { e.Priority = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			e := validEnvelope()
			mutate(&e)
			if err := e.Validate(); err == nil {
				t.Fatalf("expected %s to be rejected", name)
			}
		})
	}
	if err := validEnvelope().Validate(); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}
}

func TestEmitPostsTheCanonicalShape(t *testing.T) {
	var got map[string]any
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &got)
		w.WriteHeader(202)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "vibe-accounts", nil)
	env := validEnvelope()
	env.ThreadKey = "accounts.request:42"
	env.CollapseKey = "accounts.request:42:status"
	env.Tone = ToneFailure
	env.SoundClass = SoundAlert
	if err := c.Emit(context.Background(), env); err != nil {
		t.Fatalf("emit: %v", err)
	}

	if path != "/webhook/notifications" {
		t.Fatalf("wrong path %q", path)
	}
	// snake_case is the contract; a silent rename here is a 400 the source
	// would only ever see in a log.
	for _, k := range []string{"event_type", "occurred_at", "thread_key", "collapse_key", "sound_class", "deeplink"} {
		if _, ok := got[k]; !ok {
			t.Errorf("payload missing %q", k)
		}
	}
	if got["tone"] != "failure" {
		t.Errorf("tone = %v", got["tone"])
	}
}

func TestEmitSignsWhenASecretIsSet(t *testing.T) {
	var sig string
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sig = r.Header.Get("x-webhook-signature")
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(202)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "vibe-accounts", nil, WithHMAC("s3cret"))
	if err := c.Emit(context.Background(), validEnvelope()); err != nil {
		t.Fatalf("emit: %v", err)
	}

	// Must match the listener's sha256(secret + body) exactly — a keyed HMAC
	// would look right and fail closed.
	h := sha256.New()
	h.Write([]byte("s3cret"))
	h.Write(body)
	want := "sha256=" + hex.EncodeToString(h.Sum(nil))
	if sig != want {
		t.Fatalf("signature mismatch:\n got %s\nwant %s", sig, want)
	}
}

func TestEmitDoesNotSignWithoutASecret(t *testing.T) {
	var sig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sig = r.Header.Get("x-webhook-signature")
		w.WriteHeader(202)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "vibe-accounts", nil)
	_ = c.Emit(context.Background(), validEnvelope())
	if sig != "" {
		t.Fatalf("unexpected signature %q", sig)
	}
}

func TestEmitSurfacesNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte("invalid envelope"))
	}))
	defer srv.Close()
	err := NewClient(srv.URL, "vibe-accounts", nil).Emit(context.Background(), validEnvelope())
	if err == nil {
		t.Fatal("expected an error for a 400")
	}
	if !strings.Contains(err.Error(), "invalid envelope") {
		t.Fatalf("error should carry the response body, got %v", err)
	}
}

func TestEmitAsyncSwallowsFailure(t *testing.T) {
	// Fire-and-forget must never break the operation that triggered it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()
	NewClient(srv.URL, "vibe-accounts", nil).EmitAsync(context.Background(), validEnvelope())
}

func TestEmitRejectsInvalidBeforeSending(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer srv.Close()
	bad := validEnvelope()
	bad.Title = ""
	if err := NewClient(srv.URL, "vibe-accounts", nil).Emit(context.Background(), bad); err == nil {
		t.Fatal("expected validation to reject")
	}
	if called {
		t.Fatal("a malformed envelope must not reach the network")
	}
}

// A refusal that will be given again forever must be distinguishable from an
// outage. Without this the vibe-fb relay retried the same Kafka batch 65 times
// against a host that had no backend at all (2026-09-16), and no notification
// from any of the three emitting services was ever delivered.
func TestAPermanentRefusalIsNotAnOutage(t *testing.T) {
	cases := []struct {
		status    int
		permanent bool
		why       string
	}{
		{404, true, "nobody is home at that address"},
		{400, true, "the envelope is wrong and will stay wrong"},
		{401, true, "the secret is wrong until somebody changes it"},
		{408, false, "a timeout is explicitly later, not never"},
		{429, false, "rate limiting is explicitly later"},
		{500, false, "the hub is down"},
		{503, false, "the hub is down"},
	}
	for _, c := range cases {
		err := error(&HTTPError{EventType: "fb.adaccount.disabled", StatusCode: c.status, Body: "x"})
		if got := IsPermanent(err); got != c.permanent {
			t.Errorf("status %d: permanent=%v, want %v — %s", c.status, got, c.permanent, c.why)
		}
	}
	if IsPermanent(errors.New("dial tcp: connection refused")) {
		t.Error("a transport failure is an outage, never a permanent refusal")
	}
	if IsPermanent(nil) {
		t.Error("no error is not a permanent refusal")
	}
}
