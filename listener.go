package bus

import (
	"encoding/json"
	"errors"

	nsq "github.com/nsqio/go-nsq"
)

var (
	// ErrTopicRequired is returned when topic is not passed as parameter.
	ErrTopicRequired = errors.New("topic is mandatory")
	// ErrHandlerRequired is returned when handler is not passed as parameter.
	ErrHandlerRequired = errors.New("handler is mandatory")
	// ErrChannelRequired is returned when channel is not passed as parameter in bus.ListenerConfig.
	ErrChannelRequired = errors.New("channel is mandatory")
)

type handlerFunc func(m *Message) (interface{}, error)

// On listen to a message from a specific topic using nsq consumer, returns
// an error if topic and channel not passed or if an error occurred while creating
// nsq consumer.
func On(lc ListenerConfig) (err error) {
	if len(lc.Topic) == 0 {
		err = ErrTopicRequired
		return
	}

	if len(lc.Channel) == 0 {
		err = ErrChannelRequired
		return
	}

	if lc.HandlerFunc == nil {
		err = ErrHandlerRequired
		return
	}

	if len(lc.Lookup) == 0 {
		lc.Lookup = []string{"localhost:4161"}
	}

	if lc.HandlerConcurrency == 0 {
		lc.HandlerConcurrency = 1
	}

	config := newListenerConfig(lc)
	consumer, err := nsq.NewConsumer(lc.Topic, lc.Channel, config)
	if err != nil {
		return
	}

	handler := handleMessage(lc)
	consumer.AddConcurrentHandlers(handler, lc.HandlerConcurrency)
	err = consumer.ConnectToNSQLookupds(lc.Lookup)

	return
}

func handleMessage(lc ListenerConfig) nsq.HandlerFunc {
	return nsq.HandlerFunc(func(message *nsq.Message) (err error) {
		m := Message{Message: message}
		if err = json.Unmarshal(message.Body, &m); err != nil {
			return
		}

		res, err := lc.HandlerFunc(&m)
		if err != nil {
			return
		}

		if m.ReplyTo == "" {
			return
		}

		emitter, err := NewEmitter(EmitterConfig{})
		if err != nil {
			return
		}

		err = emitter.Emit(m.ReplyTo, res)

		return
	})
}
