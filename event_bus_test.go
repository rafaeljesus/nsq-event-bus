package eventbus

import (
	"encoding/json"
	"log"
	"testing"
	"time"
)

type message struct {
	Name string
}

func TestEventBusEmit(t *testing.T) {
	bus, err := NewEventBus()
	if err != nil {
		t.Errorf("Expected to initialize EventBus %s", err)
	}

	if err := bus.Emit("topic", &message{Name: "event"}); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}
}

func TestEventBusOn(t *testing.T) {
	bus, err := NewEventBus()
	if err != nil {
		t.Errorf("Expected to initialize EventBus %s", err)
	}

	testHandler := func(msg []byte) error {
		log.Print("on handler")
		m := message{}
		if err := json.Unmarshal(msg, &m); err != nil {
			t.Errorf("Expected to unmarshal a message %s", err)
		}

		return nil
	}

	if err := bus.On("topic", "channel", testHandler); err != nil {
		t.Errorf("Expected to listen a message %s", err)
	}

	time.Sleep(200 * time.Millisecond)

	if err := bus.Emit("topic", &message{Name: "event"}); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}

	time.Sleep(200 * time.Millisecond)
}
