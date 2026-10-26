package notify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const orgID = "33333333-3333-3333-3333-333333333333"

func TestValidateRejectsBadOrgFields(t *testing.T) {
	cases := map[string]func(*Envelope){
		"org visibility without org": func(e *Envelope) { e.Visibility = VisibilityOrg },
		"unknown visibility":         func(e *Envelope) { e.Visibility = "team" },
		"org id not a uuid":          func(e *Envelope) { e.OrgID = "acme" },
		"empty recipients personal":  func(e *Envelope) { e.Recipients = nil; e.OrgID = orgID },
		"resource without kind":      func(e *Envelope) { e.Resource = Res("", "1", "", "") },
		"resource without id":        func(e *Envelope) { e.Resource = Res("card", "", "", "") },
		"resource relative url":      func(e *Envelope) { e.Resource = Res("card", "1", "", "/cards/1") },
		"eleven facts":               func(e *Envelope) { e.Facts = make([]Fact, 11); fill(e.Facts) },
		"fact without label":         func(e *Envelope) { e.Facts = []Fact{F("", "x")} },
		"five actions":               func(e *Envelope) { e.Actions = actions(5) },
		"action without label":       func(e *Envelope) { e.Actions = []Action{Callback("ok", "")} },
		"action label too long":      func(e *Envelope) { e.Actions = []Action{Callback("ok", strings.Repeat("я", 41))} },
		"duplicate action id":        func(e *Envelope) { e.Actions = []Action{Callback("ok", "A"), Callback("ok", "B")} },
		"bad action style":           func(e *Envelope) { e.Actions = []Action{{ID: "ok", Label: "OK", Style: "loud"}} },
		"action link not absolute":   func(e *Envelope) { e.Actions = []Action{Link("open", "Open", "cards/1")} },
		"destinations on personal":   func(e *Envelope) { e.DestinationIDs = []string{orgID} },
		"destination not a uuid": func(e *Envelope) {
			e.OrgID, e.Visibility, e.DestinationIDs = orgID, VisibilityOrg, []string{"~alerts"}
		},
		"eleven destinations": func(e *Envelope) {
			e.OrgID, e.Visibility = orgID, VisibilityOrg
			for range 11 {
				e.DestinationIDs = append(e.DestinationIDs, orgID)
			}
		},
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
}

func TestValidateAcceptsARichOrgEvent(t *testing.T) {
	e := validEnvelope()
	e.OrgID = orgID
	e.Visibility = VisibilityOrg
	e.Recipients = nil // shared-only: no personal recipient
	e.Resource = Res("card", "c-1", "Visa *4242", "https://cards.igrudsky.dev/cards/c-1")
	e.Facts = []Fact{F("Balance", "$12.40"), F("Reason", "")}
	e.Actions = []Action{
		Link("open", "Open", "https://cards.igrudsky.dev/cards/c-1").Primary(),
		Callback("freeze", "Freeze").Danger(),
		Callback("ack", "OK"),
		Callback("snooze", "Later"),
	}
	e.DestinationIDs = []string{"44444444-4444-4444-4444-444444444444"}
	e.Title = strings.Repeat("я", 200) // characters, not bytes
	if err := e.Validate(); err != nil {
		t.Fatalf("valid org event rejected: %v", err)
	}
}

func TestSharedOnlyEventSendsEmptyArrayNotNull(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &got)
		w.WriteHeader(202)
	}))
	defer srv.Close()

	e := validEnvelope()
	e.OrgID, e.Visibility, e.Recipients = orgID, VisibilityOrg, nil
	e.Actions = []Action{Callback("freeze", "Freeze").Danger()}
	if err := NewClient(srv.URL, "vibe-cards", nil).Emit(context.Background(), e); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if r, ok := got["recipients"].([]any); !ok || len(r) != 0 {
		t.Fatalf("recipients = %#v, want []", got["recipients"])
	}
	if got["org_id"] != orgID || got["visibility"] != "org" {
		t.Fatalf("org fields = %v / %v", got["org_id"], got["visibility"])
	}
	acts := got["actions"].([]any)
	a := acts[0].(map[string]any)
	if a["id"] != "freeze" || a["style"] != "danger" {
		t.Fatalf("action = %v", a)
	}
	if _, hasURL := a["url"]; hasURL {
		t.Fatal("a callback must not carry a url")
	}
}

func TestNewClientFromEnv(t *testing.T) {
	t.Setenv(EnvHubURL, "")
	if NewClientFromEnv("vibe-cards", nil) != nil {
		t.Fatal("no NOTIFY_HUB_URL must give the nil no-op client")
	}

	var sig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sig = r.Header.Get("x-webhook-signature")
		w.WriteHeader(202)
	}))
	defer srv.Close()
	t.Setenv(EnvHubURL, srv.URL)
	t.Setenv(EnvHMACSecret, "s3cret")
	c := NewClientFromEnv("vibe-cards", nil)
	if err := c.Emit(context.Background(), validEnvelope()); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if !strings.HasPrefix(sig, "sha256=") {
		t.Fatalf("NOTIFY_HMAC_SECRET was not applied, signature %q", sig)
	}
}

func fill(fs []Fact) {
	for i := range fs {
		fs[i] = F("k", "v")
	}
}

func actions(n int) []Action {
	out := make([]Action, n)
	for i := range out {
		out[i] = Callback(string(rune('a'+i)), "x")
	}
	return out
}
