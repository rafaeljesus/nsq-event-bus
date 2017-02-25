package bus

import (
	"encoding/json"
	"sync"
	"testing"
)

func TestEmitterEmit(t *testing.T) {
	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Errorf("Expected to initialize emitter %s", err)
	}

	type event struct{ Name string }
	e := event{"event"}

	if err := emitter.Emit("etopic", &e); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}
}

func TestEmitterEmitAsync(t *testing.T) {
	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Errorf("Expected to initialize emitter %s", err)
	}

	type event struct{ Name string }
	e := event{"event"}

	if err := emitter.EmitAsync("etopic", &e); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}
}

func TestEmitterRequest(t *testing.T) {
	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Errorf("Expected to initialize emitter %s", err)
	}

	type event struct{ Name string }

	var wg sync.WaitGroup
	wg.Add(1)

	replyHandler := func(payload []byte) (reply interface{}, err error) {
		e := event{}
		if err = json.Unmarshal(payload, &e); err != nil {
			t.Errorf("Expected to unmarshal payload")
		}

		if e.Name != "event_reply" {
			t.Errorf("Expected name to be equal event %s", e.Name)
		}

		wg.Done()
		return
	}

	handler := func(payload []byte) (reply interface{}, err error) {
		e := event{}
		if err = json.Unmarshal(payload, &e); err != nil {
			t.Errorf("Expected to unmarshal payload")
		}
		reply = &event{"event_reply"}
		return
	}

	if err := On(ListenerConfig{
		Topic:       "etopic",
		Channel:     "test_request",
		HandlerFunc: handler,
	}); err != nil {
		t.Errorf("Expected to listen a message %s", err)
	}

	e := event{"event"}
	if err := emitter.Request("etopic", &e, replyHandler); err != nil {
		t.Errorf("Expected to request a message %s", err)
	}

	wg.Wait()
}
