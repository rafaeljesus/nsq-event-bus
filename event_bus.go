package eventbus

import (
	"encoding/json"
	nsq "github.com/nsqio/go-nsq"
	"os"
)

var NSQ_URL = os.Getenv("NSQ_URL")
var NSQ_LOOKUPD_URL = os.Getenv("NSQ_LOOKUPD_URL")

type fnHandler func(message []byte) error

type EventBus interface {
	Emit(topic string, message interface{}) error
	On(topic, channel string, handler fnHandler) error
}

type Bus struct {
	Producer *nsq.Producer
	Config   *nsq.Config
}

func init() {
	if NSQ_URL == "" {
		NSQ_URL = "localhost:4150"
	}

	if NSQ_LOOKUPD_URL == "" {
		NSQ_LOOKUPD_URL = "localhost:4161"
	}
}

func NewEventBus() (EventBus, error) {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(NSQ_URL, config)
	if err != nil {
		return nil, err
	}

	return &Bus{producer, config}, nil
}

func (bus *Bus) Emit(topic string, message interface{}) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return bus.Producer.Publish(topic, payload)
}

func (bus *Bus) On(topic, channel string, handler fnHandler) error {
	consumer, err := nsq.NewConsumer(topic, channel, bus.Config)
	if err != nil {
		return err
	}

	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		if err := handler(message.Body); err != nil {
			return err
		}
		return nil
	}))

	if err := consumer.ConnectToNSQLookupd(NSQ_LOOKUPD_URL); err != nil {
		return err
	}

	return nil
}
