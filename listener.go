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

// HandlerFunc is the handler function to handle the massage.
type HandlerFunc func(m *Message) (interface{}, error)

// On listen to a message from a specific topic using nsq consumer, returns
// an error if topic and channel not passed or if an error occurred while creating
// nsq consumer.
func On(lc ListenerConfig) error {
	if len(lc.Topic) == 0 {
		return ErrTopicRequired
	}

	if len(lc.Channel) == 0 {
		return ErrChannelRequired
	}

	if lc.HandlerFunc == nil {
		return ErrHandlerRequired
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
		return err
	}

	handler := handleMessage(lc)
	consumer.AddConcurrentHandlers(handler, lc.HandlerConcurrency)
	return consumer.ConnectToNSQLookupds(lc.Lookup)
}

func handleMessage(lc ListenerConfig) nsq.HandlerFunc {
	return nsq.HandlerFunc(func(message *nsq.Message) error {
		m := Message{Message: message}
		if err := json.Unmarshal(message.Body, &m); err != nil {
			return err
		}

		res, err := lc.HandlerFunc(&m)
		if err != nil {
			return err
		}

		if m.ReplyTo == "" {
			return nil
		}

		emitter, err := NewEmitter(EmitterConfig{})
		if err != nil {
			return err
		}

		return emitter.Emit(m.ReplyTo, res)
	})
}
