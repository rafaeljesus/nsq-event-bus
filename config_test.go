package bus

import (
	"testing"
)

func TestnewEmitterConfig(t *testing.T) {
	c := newEmitterConfig(EmitterConfig{})

	if c != nil {
		t.Fail()
	}
}

func TestnewListenerConfig(t *testing.T) {
	c := newListenerConfig(ListenerConfig{})

	if c != nil {
		t.Fail()
	}
}
