package eventbus

import (
	"testing"
)

func TestEmitterEmit(t *testing.T) {
	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Errorf("Expected to initialize emitter %s", err)
	}

	type event struct{ Name string }
	e := event{"event"}

	if err := emitter.Emit("topic", &e); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}
}
