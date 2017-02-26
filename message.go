package bus

import (
	"encoding/json"
	nsq "github.com/nsqio/go-nsq"
)

type Message struct {
	*nsq.Message
	ReplyTo string
	Payload []byte
}

func (m *Message) DecodePayload(v interface{}) (err error) {
	err = json.Unmarshal(m.Payload, v)

	return
}
