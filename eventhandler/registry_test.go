package eventhandler

import (
	"github.com/edgejumps/sportstalk-pubsub/pubsub"
	"testing"
)

type mockHandler struct {
	action string
}

func (m *mockHandler) Action() string {
	return pubsub.NormalizeActionPath(m.action)
}

func (m *mockHandler) Handle(event pubsub.Event) error {
	return nil
}

func (m *mockHandler) Build(payload interface{}, ttl int) (pubsub.EventData, error) {
	return nil, nil
}

var r Registry

func TestRegistry_Register(t *testing.T) {
	if r == nil {
		r = NewRegistry()
	}

	handler := &mockHandler{action: "/test"}

	err := r.Register(handler)

	if err != nil {
		t.Errorf("failed to register handler: %v", err)
	}

	handler2 := &mockHandler{action: "/test"}

	err = r.Register(handler2)

	if err == nil {
		t.Errorf("expected error registering duplicate handler")
	}
}

func TestRegistry_Has(t *testing.T) {

	if !r.Has("/test") {
		t.Errorf("expected handler to exist")
	}

	if r.Has("test") {
		t.Errorf("expected handler to not exist")
	}

	if r.Has("/test2") {
		t.Errorf("expected handler to not exist")
	}
}

func TestRegistry_Route(t *testing.T) {

	event, err := pubsub.NewIncomingEvent(&pubsub.EventID{Topic: "mock-topic"}, map[string]interface{}{
		"action": "/test",
		"ttl":    1,
	})

	if err != nil {
		t.Errorf("failed to create event: %v", err)
	}

	err = r.Route(event)

	if err != nil {
		t.Errorf("failed to route event: %v", err)
	}
}

func TestRegistry_BuildData(t *testing.T) {

	_, err := r.BuildData("/test", nil, 1)

	if err != nil {
		t.Errorf("failed to build data: %v", err)
	}
}

func TestRegistry_Unregister(t *testing.T) {

	err := r.Unregister("/test")

	if err != nil {
		t.Errorf("failed to unregister handler: %v", err)
	}

	if r.Has("/test") {
		t.Errorf("expected handler to not exist")
	}
}
