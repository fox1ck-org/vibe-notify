package notifytest_test

import (
	"context"
	"testing"

	notify "github.com/fox1ck-org/vibe-notify"
	"github.com/fox1ck-org/vibe-notify/notifytest"
)

const (
	org  = "33333333-3333-3333-3333-333333333333"
	user = "22222222-2222-2222-2222-222222222222"
)

func emit(t *testing.T, c *notify.Client, mutate func(*notify.Envelope)) error {
	t.Helper()
	e := c.NewEnvelope("cards.card.frozen", []string{user})
	e.Category = "card"
	e.Title = "Card frozen"
	e.Deeplink = "https://cards.igrudsky.dev/cards/c-1"
	if mutate != nil {
		mutate(&e)
	}
	return c.Emit(context.Background(), e)
}

func TestRequireEmittedFindsTheMatchingEnvelope(t *testing.T) {
	hub := notifytest.New(t, notifytest.WithSecret("s3cret"))
	c := hub.Client("vibe-cards")
	if err := emit(t, c, func(e *notify.Envelope) {
		e.OrgID, e.Visibility = org, notify.VisibilityOrg
		e.Resource = notify.Res("card", "c-1", "Visa *4242", "")
		e.Facts = []notify.Fact{notify.F("Balance", "$12.40")}
		e.Actions = []notify.Action{notify.Callback("unfreeze", "Unfreeze")}
	}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	got := hub.RequireEmitted(t, "cards.card.frozen",
		notifytest.To(user), notifytest.Org(org), notifytest.Visible(notify.VisibilityOrg),
		notifytest.ResourceIs("card", "c-1"), notifytest.HasFact("Balance", "$12.40"),
		notifytest.HasAction("unfreeze"), notifytest.WithPriority(notify.PriorityInfo))
	if got.Source != "vibe-cards" {
		t.Fatalf("source = %q", got.Source)
	}
	hub.RequireNotEmitted(t, "cards.card.frozen", notifytest.Org("44444444-4444-4444-4444-444444444444"))
	hub.RequireNotEmitted(t, "cards.card.deleted")
}

func TestHubRejectsWhatTheRealHubWould(t *testing.T) {
	hub := notifytest.New(t, notifytest.WithSecret("s3cret"))
	unsigned := notify.NewClient(hub.URL, "vibe-cards", nil)
	if err := emit(t, unsigned, nil); !notify.IsPermanent(err) {
		t.Fatalf("unsigned emit: want a permanent 401, got %v", err)
	}
	if n := len(hub.Events()); n != 0 {
		t.Fatalf("recorded %d rejected envelopes", n)
	}
}

func TestRespondSimulatesAnOutage(t *testing.T) {
	hub := notifytest.New(t)
	hub.Respond(503)
	err := emit(t, hub.Client("vibe-cards"), nil)
	if err == nil || notify.IsPermanent(err) {
		t.Fatalf("want a retryable error, got %v", err)
	}
	hub.Respond(202)
	if err := emit(t, hub.Client("vibe-cards"), nil); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if len(hub.Events()) != 1 {
		t.Fatalf("events = %d, want 1", len(hub.Events()))
	}
	hub.Reset()
	hub.RequireNotEmitted(t, "cards.card.frozen")
}
