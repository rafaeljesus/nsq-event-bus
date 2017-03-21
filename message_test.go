package bus

import (
	"encoding/json"
	"testing"
)

func TestMessageDecodePayload(t *testing.T) {
	type event struct{ Name string }

	e := &event{"event"}
	payload, err := json.Marshal(e)
	if err != nil {
		t.Fail()
	}

	m := &Message{Payload: payload}
	v := &event{}
	if err := m.DecodePayload(v); err != nil {
		t.Errorf("Expected to decode payload message %s", err)
	}
}
