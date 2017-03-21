package bus

import (
	"sync"
	"testing"
)

func TestNewEmitter(t *testing.T) {
	var addressesTest = []struct {
		addr   string
		expect string
	}{
		{"", "localhost:4150"},
		{"new-address", "new-address"},
	}

	for _, a := range addressesTest {
		emitter, err := NewEmitter(EmitterConfig{
			Address: a.addr,
		})

		if err != nil {
			t.Errorf("Expected to initialize emitter %s", err)
		}

		e := emitter.(*eventEmitter)
		if e.String() != a.expect {
			t.Errorf("Expected emitter address %s, got %s", a.expect, e.String())
		}
	}
}

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

	if err := emitter.Emit("", &e); err == nil {
		t.Fail()
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

	if err := emitter.EmitAsync("", &e); err == nil {
		t.Fail()
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

	replyHandler := func(message *Message) (reply interface{}, err error) {
		e := event{}
		if err = message.DecodePayload(&e); err != nil {
			t.Errorf("Expected to unmarshal payload")
		}

		if e.Name != "event_reply" {
			t.Errorf("Expected name to be equal event %s", e.Name)
		}

		message.Finish()
		wg.Done()
		return
	}

	handler := func(message *Message) (reply interface{}, err error) {
		e := event{}
		if err = message.DecodePayload(&e); err != nil {
			t.Errorf("Expected to unmarshal payload")
		}
		reply = &event{"event_reply"}
		message.Finish()
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

	if err := emitter.Request("", &e, replyHandler); err == nil {
		t.Fail()
	}

	if err := emitter.Request("etopic", &e, nil); err == nil {
		t.Fail()
	}

	wg.Wait()
}
