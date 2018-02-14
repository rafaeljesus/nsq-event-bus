package bus

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	nsq "github.com/nsqio/go-nsq"
	"github.com/sony/gobreaker"
)

type (
	// Emitter is the emitter wrapper over nsq.
	Emitter struct {
		producer *nsq.Producer
		address  string
		breaker  *gobreaker.CircuitBreaker
	}
)

// NewEmitter returns a new Emitter configured with the
// variables from the config parameter, or returning an non-nil err
// if an error occurred while creating nsq producer.
func NewEmitter(ec EmitterConfig) (*Emitter, error) {
	config := newEmitterConfig(ec)

	address := ec.Address
	if len(address) == 0 {
		address = "localhost:4150"
	}

	producer, err := nsq.NewProducer(address, config)
	if err != nil {
		return nil, err
	}

	return &Emitter{
		producer: producer,
		address:  address,
		breaker:  gobreaker.NewCircuitBreaker(newBreakerSettings(ec.Breaker)),
	}, nil
}

// Emit emits a message to a specific topic using nsq producer, returning
// an error if encoding payload fails or if an error occurred while publishing
// the message.
func (e *Emitter) Emit(topic string, payload interface{}) error {
	if len(topic) == 0 {
		return ErrTopicRequired
	}

	body, err := e.encodeMessage(payload, "")
	if err != nil {
		return err
	}

	_, err = e.breaker.Execute(func() (interface{}, error) {
		return nil, e.producer.Publish(topic, body)
	})

	return err
}

// Emit emits a message to a specific topic using nsq producer, but does not wait for
// the response from `nsqd`. Returns an error if encoding payload fails and
// logs to console if an error occurred while publishing the message.
func (e *Emitter) EmitAsync(topic string, payload interface{}) error {
	if len(topic) == 0 {
		return ErrTopicRequired
	}

	body, err := e.encodeMessage(payload, "")
	if err != nil {
		return err
	}

	responseChan := make(chan *nsq.ProducerTransaction, 1)
	go func(responseChan chan *nsq.ProducerTransaction) {
		for {
			trans, ok := <-responseChan
			if ok && trans.Error != nil {
				log.Fatalf(trans.Error.Error())
			}
		}
	}(responseChan)

	_, err = e.breaker.Execute(func() (interface{}, error) {
		return nil, e.producer.PublishAsync(topic, body, responseChan, "")
	})

	return err
}

// Request a RPC like method which implements request/reply pattern using nsq producer and consumer.
// Returns an non-nil err if an error occurred while creating or listening to the internal
// reply topic or encoding the message payload fails or while publishing the message.
func (e *Emitter) Request(topic string, payload interface{}, handler HandlerFunc) error {
	if len(topic) == 0 {
		return ErrTopicRequired
	}

	if handler == nil {
		return ErrHandlerRequired
	}

	replyTo, err := e.genReplyQueue()
	if err != nil {
		return err
	}

	if err := e.createTopic(replyTo); err != nil {
		return err
	}

	if err := On(ListenerConfig{
		Topic:       replyTo,
		Channel:     replyTo,
		HandlerFunc: handler,
	}); err != nil {
		return err
	}

	body, err := e.encodeMessage(payload, replyTo)
	if err != nil {
		return err
	}

	_, err = e.breaker.Execute(func() (interface{}, error) {
		return nil, e.producer.Publish(topic, body)
	})

	return err
}

func (e *Emitter) encodeMessage(payload interface{}, replyTo string) ([]byte, error) {
	p, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	message := NewMessage(p, replyTo)
	return json.Marshal(message)
}

func (e *Emitter) genReplyQueue() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	hash := hex.EncodeToString(b)
	return fmt.Sprint(hash, ".ephemeral"), nil
}

func (e *Emitter) createTopic(topic string) error {
	s := strings.Split(e.address, ":")
	port, err := strconv.Atoi(s[1])
	if err != nil {
		return err
	}

	uri := fmt.Sprintf("http://%s:%s/topic/create?topic=%s", s[0], strconv.Itoa(port+1), topic)
	_, err = http.Post(uri, "application/json; charset=utf-8", nil)
	return err
}

func newBreakerSettings(c Breaker) gobreaker.Settings {
	return gobreaker.Settings{
		Name:     "nsq-emitter-circuit-breaker",
		Interval: c.Interval,
		Timeout:  c.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures > c.Threshold
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			c.OnStateChange(name, from.String(), to.String())
		},
	}
}
