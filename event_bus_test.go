package eventbus

import (
	"encoding/json"
	"testing"
	"time"
)

type payload struct {
	Name string
}

func TestEventBusEmit(t *testing.T) {
	bus, err := NewEventBus()
	if err != nil {
		t.Errorf("Expected to initialize EventBus %s", err)
	}

	p := payload{Name: "event"}
	if err := bus.Emit("topic", &Message{Payload: p}); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}
}

func TestEventBusRequest(t *testing.T) {
	bus, err := NewEventBus()
	if err != nil {
		t.Errorf("Expected to initialize EventBus %s", err)
	}

	testReplyHandler := func(msg interface{}) (interface{}, error) {
		p := payload{}
		if err := json.Unmarshal(msg.([]byte), &p); err != nil {
			t.Errorf("Expected to unmarshal a message %s", err)
		}
		reply := payload{Name: "Reply"}

		return &Message{Payload: reply}, nil
	}

	if err := bus.Request("topic", &Message{}, testReplyHandler); err != nil {
		t.Errorf("Expected to request a message %s", err)
	}

	time.Sleep(200 * time.Millisecond)
}

func TestEventBusOn(t *testing.T) {
	bus, err := NewEventBus()
	if err != nil {
		t.Errorf("Expected to initialize EventBus %s", err)
	}

	testHandler := func(msg interface{}) (interface{}, error) {
		p := payload{}
		if err := json.Unmarshal(msg.([]byte), &p); err != nil {
			t.Errorf("Expected to unmarshal a message %s", err)
		}

		return nil, nil
	}

	if err := bus.On("topic", "channel", testHandler); err != nil {
		t.Errorf("Expected to listen a message %s", err)
	}

	time.Sleep(200 * time.Millisecond)

	p := payload{Name: "event"}
	if err := bus.Emit("topic", &Message{Payload: p}); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}

	time.Sleep(200 * time.Millisecond)
}
