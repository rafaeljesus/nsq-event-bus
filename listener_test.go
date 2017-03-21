package bus

import (
	"sync"
	"testing"
)

func TestListenerOn(t *testing.T) {
	type event struct{ Name string }

	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Errorf("Expected to initialize emitter %s", err)
	}

	e := event{"event"}
	if err := emitter.Emit("ltopic", &e); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(message *Message) (reply interface{}, err error) {
		e := event{}
		if err = message.DecodePayload(&e); err != nil {
			t.Errorf("Expected to unmarshal payload")
		}

		if e.Name != "event" {
			t.Errorf("Expected name to be equal event %s", e.Name)
		}

		message.Finish()
		wg.Done()
		return
	}

	if err := On(ListenerConfig{
		Topic:       "ltopic",
		Channel:     "test_on",
		HandlerFunc: handler,
	}); err != nil {
		t.Errorf("Expected to listen a message %s", err)
	}

	wg.Wait()
}

func TestListenerOnRequiresTopic(t *testing.T) {
	if err := On(ListenerConfig{
		Topic:   "",
		Channel: "test_on",
		HandlerFunc: func(message *Message) (reply interface{}, err error) {
			return
		},
	}); err == nil {
		t.Errorf("Expected to pass a topic %s", err)
	}
}

func TestListenerOnRequiresChannel(t *testing.T) {
	if err := On(ListenerConfig{
		Topic:   "ltopic",
		Channel: "",
		HandlerFunc: func(message *Message) (reply interface{}, err error) {
			return
		},
	}); err == nil {
		t.Errorf("Expected to pass a channel %s", err)
	}
}

func TestListenerOnRequiresHandler(t *testing.T) {
	if err := On(ListenerConfig{
		Topic:   "ltopic",
		Channel: "test_on",
	}); err == nil {
		t.Errorf("Expected to pass a handler %s", err)
	}
}
