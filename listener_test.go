package bus

import (
	"encoding/json"
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
	if err := emitter.Emit("topic", &e); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(payload []byte) (reply interface{}, err error) {
		e := event{}
		if err = json.Unmarshal(payload, &e); err != nil {
			t.Errorf("Expected to unmarshal payload")
		}

		if e.Name != "event" {
			t.Errorf("Expected name to be equal event %s", e.Name)
		}

		wg.Done()
		return
	}

	if err := On(ListenerConfig{
		Topic:       "topic",
		Channel:     "test_on",
		HandlerFunc: handler,
	}); err != nil {
		t.Errorf("Expected to listen a message %s", err)
	}

	wg.Wait()
}
