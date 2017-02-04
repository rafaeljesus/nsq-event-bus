package eventbus

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	nsq "github.com/nsqio/go-nsq"
	"os"
)

var NSQ_URL = os.Getenv("NSQ_URL")
var NSQ_LOOKUPD_URL = os.Getenv("NSQ_LOOKUPD_URL")

type EventBus interface {
	Emit(topic string, message *Message) error
	Request(topic string, message *Message, handler fnHandler) error
	On(topic, channel string, handler fnHandler) error
}

type Bus struct {
	Producer *nsq.Producer
	Config   *nsq.Config
}

type Message struct {
	ReplyTo string
	Payload interface{}
}

type fnHandler func(v interface{}) (interface{}, error)

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

func (bus *Bus) Emit(topic string, message *Message) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return bus.Producer.Publish(topic, payload)
}

func (bus *Bus) Request(topic string, message *Message, handler fnHandler) error {
	reply, err := bus.genReplyQueue()
	if err != nil {
		return err
	}

	if err := bus.On(reply, reply, handler); err != nil {
		return err
	}

	if err := bus.Emit(topic, message); err != nil {
		return err
	}

	return nil
}

func (bus *Bus) On(topic, channel string, handler fnHandler) error {
	consumer, err := nsq.NewConsumer(topic, channel, bus.Config)
	if err != nil {
		return err
	}

	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		m := Message{}
		if err := json.Unmarshal(message.Body, &m); err != nil {
			return err
		}

		res, err := handler(m.Payload)
		if err != nil {
			return err
		}

		if m.ReplyTo == "" {
			return nil
		}

		reply := Message{Payload: res}
		if err := bus.Emit(m.ReplyTo, &reply); err != nil {
			return err
		}

		return nil
	}))

	if err := consumer.ConnectToNSQLookupd(NSQ_LOOKUPD_URL); err != nil {
		return err
	}

	return nil
}

func (bus *Bus) genReplyQueue() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	hash := hex.EncodeToString(b)
	return hash, nil
}
