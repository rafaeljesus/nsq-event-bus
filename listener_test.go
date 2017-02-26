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
