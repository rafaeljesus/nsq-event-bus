package eventbus

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	nsq "github.com/nsqio/go-nsq"
	"os"
)

var NSQ_URL = os.Getenv("NSQ_URL")
var NSQ_LOOKUPD_URL = os.Getenv("NSQ_LOOKUPD_URL")

var ErrInvalidPayload = errors.New("Invalid Payload")

type EventBus interface {
	Emit(topic string, payload interface{}) error
	Request(topic string, payload interface{}, handler fnHandler) error
	On(topic, channel string, handler fnHandler) error
}

type Bus struct {
	Producer *nsq.Producer
	Config   *nsq.Config
}

type Message struct {
	ReplyTo string
	Payload []byte
}

type fnHandler func(payload []byte) (interface{}, error)

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

func (bus *Bus) Emit(topic string, payload interface{}) error {
	p, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	message := Message{Payload: p}
	body, err := json.Marshal(&message)
	if err != nil {
		return err
	}

	return bus.Producer.Publish(topic, body)
}

func (bus *Bus) Request(topic string, payload interface{}, handler fnHandler) error {
	replyTo, err := bus.genReplyQueue()
	if err != nil {
		return err
	}

	if err := bus.On(replyTo, replyTo, handler); err != nil {
		return err
	}

	p, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	message := Message{replyTo, p}
	body, err := json.Marshal(&message)
	if err != nil {
		return err
	}

	if err := bus.Producer.Publish(topic, body); err != nil {
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

		if err := bus.Emit(m.ReplyTo, res); err != nil {
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
