package eventbus

import (
	"testing"
	"time"
)

type event struct {
	Name string
}

func TestEventBusEmit(t *testing.T) {
	bus, err := NewEventBus()
	if err != nil {
		t.Errorf("Expected to initialize EventBus %s", err)
	}

	e := event{Name: "event"}
	if err := bus.Emit("topic", &e); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}
}

func TestEventBusRequest(t *testing.T) {
	bus, err := NewEventBus()
	if err != nil {
		t.Errorf("Expected to initialize EventBus %s", err)
	}

	replyHandler := func(payload interface{}) (interface{}, error) {
		m, ok := payload.(map[string]interface{})
		if !ok {
			t.Errorf("Expected to cast payload in handler")
		}

		if m["Name"] != "event_reply" {
			t.Errorf("Expected name to be equal event %s", m["Name"])
		}

		return nil, nil
	}

	handler := func(payload interface{}) (interface{}, error) {
		_, ok := payload.(map[string]interface{})
		if !ok {
			t.Errorf("Expected to cast payload in handler")
		}

		return &event{Name: "event_reply"}, nil
	}

	if err := bus.On("topic", "chan", handler); err != nil {
		t.Errorf("Expected to listen a message %s", err)
	}

	e := event{Name: "event"}
	if err := bus.Request("topic", &e, replyHandler); err != nil {
		t.Errorf("Expected to request a message %s", err)
	}

	time.Sleep(500 * time.Millisecond)
}

func TestEventBusOn(t *testing.T) {
	bus, err := NewEventBus()
	if err != nil {
		t.Errorf("Expected to initialize EventBus %s", err)
	}

	handler := func(payload interface{}) (interface{}, error) {
		m, ok := payload.(map[string]interface{})
		if !ok {
			t.Errorf("Expected to cast payload in handler")
		}

		if m["Name"] != "event" {
			t.Errorf("Expected name to be equal event %s", m["Name"])
		}

		return nil, nil
	}

	e := event{Name: "event"}
	if err := bus.Emit("topic", &e); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}

	if err := bus.On("topic", "channel", handler); err != nil {
		t.Errorf("Expected to listen a message %s", err)
	}

	time.Sleep(200 * time.Millisecond)
}
