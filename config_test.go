package bus

import (
	"testing"
)

func TestNewEmitterConfig(t *testing.T) {
	c := newEmitterConfig(EmitterConfig{})

	if c == nil {
		t.Fail()
	}
}

func TestNewListenerConfig(t *testing.T) {
	c := newListenerConfig(ListenerConfig{})

	if c == nil {
		t.Fail()
	}
}
