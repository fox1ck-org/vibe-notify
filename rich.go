package notify

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Limits mirror the hub's Zod schema (vibe-platform
// packages/notifications/src/types/event.ts). Change both in lockstep.
const (
	MaxFacts   = 10
	MaxActions = 4

	maxResourceKind  = 64
	maxResourceID    = 160
	maxResourceLabel = 200
	maxFactLabel     = 64
	maxFactValue     = 500
	maxActionID      = 64
	maxActionLabel   = 40
	maxURL           = 2048
)

// Resource is what an event is about — an order, a card, a campaign. The hub
// threads, filters and renders by it.
type Resource struct {
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	Label string `json:"label,omitempty"`
	URL   string `json:"url,omitempty"`
}

// Fact is one key/value line rendered by every channel.
type Fact struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

// ActionStyle is presentation only.
type ActionStyle string

const (
	ActionDefault ActionStyle = "default"
	ActionPrimary ActionStyle = "primary"
	ActionDanger  ActionStyle = "danger"
)

// Action is a button. With a URL it opens the link; without one it is a
// callback: the hub calls the product back with the action id, and only for a
// linked account that has the rights to do it.
type Action struct {
	ID    string      `json:"id"`
	Label string      `json:"label"`
	URL   string      `json:"url,omitempty"`
	Style ActionStyle `json:"style,omitempty"`
}

// Res builds a Resource. label and url may be empty.
func Res(kind, id, label, url string) *Resource {
	return &Resource{Kind: kind, ID: id, Label: label, URL: url}
}

// F builds a Fact.
func F(label, value string) Fact { return Fact{Label: label, Value: value} }

// Link builds a button that opens url.
func Link(id, label, url string) Action { return Action{ID: id, Label: label, URL: url} }

// Callback builds a button the hub answers by calling the product back with id.
func Callback(id, label string) Action { return Action{ID: id, Label: label} }

// Primary returns the action styled as the main choice.
func (a Action) Primary() Action { a.Style = ActionPrimary; return a }

// Danger returns the action styled as destructive.
func (a Action) Danger() Action { a.Style = ActionDanger; return a }

// Env names read by NewClientFromEnv. One name across every product, so a
// manifest never has to know which service spells it which way.
const (
	EnvHubURL     = "NOTIFY_HUB_URL"
	EnvHMACSecret = "NOTIFY_HMAC_SECRET"
)

// NewClientFromEnv reads NOTIFY_HUB_URL and, when set, NOTIFY_HMAC_SECRET.
// Like NewClient it returns nil (a no-op client) when the URL is unset.
// Options passed here are applied after the env ones, so they win.
func NewClientFromEnv(source string, logger *slog.Logger, opts ...Option) *Client {
	var all []Option
	if s := os.Getenv(EnvHMACSecret); s != "" {
		all = append(all, WithHMAC(s))
	}
	return NewClient(os.Getenv(EnvHubURL), source, logger, append(all, opts...)...)
}

func (e Envelope) validateOrgFields() error {
	if e.OrgID != "" {
		if _, err := uuid.Parse(e.OrgID); err != nil {
			return fmt.Errorf("notify: org_id must be a uuid, got %q", e.OrgID)
		}
	}
	switch e.Visibility {
	case "", VisibilityPersonal:
	case VisibilityOrg:
		if e.OrgID == "" {
			return fmt.Errorf("notify: visibility %q requires org_id", e.Visibility)
		}
	default:
		return fmt.Errorf("notify: invalid visibility %q", e.Visibility)
	}
	if r := e.Resource; r != nil {
		if err := text("resource.kind", r.Kind, 1, maxResourceKind); err != nil {
			return err
		}
		if err := text("resource.id", r.ID, 1, maxResourceID); err != nil {
			return err
		}
		if err := text("resource.label", r.Label, 0, maxResourceLabel); err != nil {
			return err
		}
		if err := optionalURL("resource.url", r.URL); err != nil {
			return err
		}
	}
	if len(e.Facts) > MaxFacts {
		return fmt.Errorf("notify: %d facts, at most %d", len(e.Facts), MaxFacts)
	}
	for i, f := range e.Facts {
		if err := text(fmt.Sprintf("facts[%d].label", i), f.Label, 1, maxFactLabel); err != nil {
			return err
		}
		if err := text(fmt.Sprintf("facts[%d].value", i), f.Value, 0, maxFactValue); err != nil {
			return err
		}
	}
	if len(e.Actions) > MaxActions {
		return fmt.Errorf("notify: %d actions, at most %d", len(e.Actions), MaxActions)
	}
	seen := make(map[string]bool, len(e.Actions))
	for i, a := range e.Actions {
		if err := text(fmt.Sprintf("actions[%d].id", i), a.ID, 1, maxActionID); err != nil {
			return err
		}
		if seen[a.ID] {
			return fmt.Errorf("notify: duplicate action id %q", a.ID)
		}
		seen[a.ID] = true
		if err := text(fmt.Sprintf("actions[%d].label", i), a.Label, 1, maxActionLabel); err != nil {
			return err
		}
		if err := optionalURL(fmt.Sprintf("actions[%d].url", i), a.URL); err != nil {
			return err
		}
		switch a.Style {
		case "", ActionDefault, ActionPrimary, ActionDanger:
		default:
			return fmt.Errorf("notify: actions[%d].style %q is not primary, danger or default", i, a.Style)
		}
	}
	return nil
}

// text checks a length in characters, as Zod's .min/.max do.
func text(field, v string, minLen, maxLen int) error {
	n := utf8.RuneCountInString(v)
	switch {
	case n < minLen:
		return fmt.Errorf("notify: %s is required", field)
	case n > maxLen:
		return fmt.Errorf("notify: %s exceeds %d characters", field, maxLen)
	}
	return nil
}

func optionalURL(field, v string) error {
	if v == "" {
		return nil
	}
	if len(v) > maxURL {
		return fmt.Errorf("notify: %s exceeds %d characters", field, maxURL)
	}
	u, err := url.Parse(v)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("notify: %s must be an absolute URL, got %q", field, v)
	}
	return nil
}
