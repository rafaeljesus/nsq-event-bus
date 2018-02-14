package bus

import (
	"encoding/json"

	nsq "github.com/nsqio/go-nsq"
)

// Message carries nsq.Message fields and methods and
// adds extra fields for handling messages internally.
type (
	Message struct {
		*nsq.Message
		ReplyTo string
		Payload []byte
	}
)

// NewMessage returns a new bus.Message.
func NewMessage(p []byte, r string) *Message {
	return &Message{Payload: p, ReplyTo: r}
}

// DecodePayload deserializes data (as []byte) and creates a new struct passed by parameter.
func (m *Message) DecodePayload(v interface{}) (err error) {
	return json.Unmarshal(m.Payload, v)
}
