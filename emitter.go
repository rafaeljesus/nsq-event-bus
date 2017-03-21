package bus

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	nsq "github.com/nsqio/go-nsq"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Emitter exposes a interface for emitting and listening for events.
type Emitter interface {
	Emit(topic string, payload interface{}) error
	EmitAsync(topic string, payload interface{}) error
	Request(topic string, payload interface{}, handler handlerFunc) error
}

// EmitterConfig carries the different variables to tune a newly started emitter,
// it exposes the same configuration available from official nsq go client.
type EmitterConfig struct {
	Address                 string
	DialTimeout             time.Duration
	ReadTimeout             time.Duration
	WriteTimeout            time.Duration
	LocalAddr               net.Addr
	LookupdPollInterval     time.Duration
	LookupdPollJitter       float64
	MaxRequeueDelay         time.Duration
	DefaultRequeueDelay     time.Duration
	BackoffStrategy         nsq.BackoffStrategy
	MaxBackoffDuration      time.Duration
	BackoffMultiplier       time.Duration
	MaxAttempts             uint16
	LowRdyIdleTimeout       time.Duration
	RDYRedistributeInterval time.Duration
	ClientID                string
	Hostname                string
	UserAgent               string
	HeartbeatInterval       time.Duration
	SampleRate              int32
	TLSV1                   bool
	TLSConfig               *tls.Config
	Deflate                 bool
	DeflateLevel            int
	Snappy                  bool
	OutputBufferSize        int64
	OutputBufferTimeout     time.Duration
	MaxInFlight             int
	MsgTimeout              time.Duration
	AuthSecret              string
}

type eventEmitter struct {
	*nsq.Producer
	address string
}

// NewEmitter returns a new eventEmitter configured with the
// variables from the config parameter, or returning an non-nil err
// if an error ocurred while creating nsq producer.
func NewEmitter(ec EmitterConfig) (emitter Emitter, err error) {
	config := newEmitterConfig(ec)

	address := ec.Address
	if len(address) == 0 {
		address = "localhost:4150"
	}

	producer, err := nsq.NewProducer(address, config)
	if err != nil {
		return
	}

	emitter = &eventEmitter{producer, address}

	return
}

// Emit emits a message to a specific topic using nsq producer, returning
// an error if encoding payload fails or if an error ocurred while publishing
// the message.
func (ee eventEmitter) Emit(topic string, payload interface{}) (err error) {
	if len(topic) == 0 {
		err = ErrTopicRequired
		return
	}

	body, err := ee.encodeMessage(payload, "")
	if err != nil {
		return
	}

	err = ee.Publish(topic, body)

	return
}

// Emit emits a message to a specific topic using nsq producer, but does not wait for
// the response from `nsqd`. Returns an error if encoding payload fails and
// logs to console if an error ocurred while publishing the message.
func (ee eventEmitter) EmitAsync(topic string, payload interface{}) (err error) {
	if len(topic) == 0 {
		err = ErrTopicRequired
		return
	}

	body, err := ee.encodeMessage(payload, "")
	if err != nil {
		return
	}

	responseChan := make(chan *nsq.ProducerTransaction, 1)

	if err = ee.PublishAsync(topic, body, responseChan, ""); err != nil {
		return
	}

	go func(responseChan chan *nsq.ProducerTransaction) {
		for {
			select {
			case trans := <-responseChan:
				if trans.Error != nil {
					log.Fatalf(trans.Error.Error())
				}
			}
		}
	}(responseChan)

	return
}

// Request a RPC like method which implements request/reply pattern using nsq producer and consumer.
// Returns an non-nil err if an error ocurred while creating or listening to the internal
// reply topic or encoding the message payload fails or while publishing the message.
func (ee eventEmitter) Request(topic string, payload interface{}, handler handlerFunc) (err error) {
	if len(topic) == 0 {
		err = ErrTopicRequired
		return
	}

  if handler == nil {
		err = ErrHandlerRequired
		return
  }

	replyTo, err := ee.genReplyQueue()
	if err != nil {
		return
	}

	if err = ee.createTopic(replyTo); err != nil {
		return
	}

	if err = On(ListenerConfig{
		Topic:       replyTo,
		Channel:     replyTo,
		HandlerFunc: handler,
	}); err != nil {
		return
	}

	body, err := ee.encodeMessage(payload, replyTo)
	if err != nil {
		return
	}

	err = ee.Publish(topic, body)

	return
}

func (ee eventEmitter) encodeMessage(payload interface{}, replyTo string) (body []byte, err error) {
	p, err := json.Marshal(payload)
	if err != nil {
		return
	}

	message := Message{Payload: p, ReplyTo: replyTo}
	body, err = json.Marshal(&message)

	return
}

func (ee eventEmitter) genReplyQueue() (replyTo string, err error) {
	b := make([]byte, 8)
	_, err = rand.Read(b)
	if err != nil {
		return
	}

	hash := hex.EncodeToString(b)
	replyTo = fmt.Sprint(hash, ".ephemeral")

	return
}

func (ee eventEmitter) createTopic(topic string) (err error) {
	s := strings.Split(ee.address, ":")
	port, err := strconv.Atoi(s[1])
	if err != nil {
		return
	}

	uri := "http://" + s[0] + ":" + strconv.Itoa(port+1) + "/topic/create?topic=" + topic
	_, err = http.Post(uri, "application/json; charset=utf-8", nil)

	return
}

func newEmitterConfig(ec EmitterConfig) (config *nsq.Config) {
	config = nsq.NewConfig()

	if ec.DialTimeout != 0 {
		config.DialTimeout = ec.DialTimeout
	}

	if ec.ReadTimeout != 0 {
		config.ReadTimeout = ec.ReadTimeout
	}

	if ec.LocalAddr != nil {
		config.LocalAddr = ec.LocalAddr
	}

	if ec.LookupdPollInterval != 0 {
		config.LookupdPollInterval = ec.LookupdPollInterval
	}

	if ec.LookupdPollJitter != 0 {
		config.LookupdPollJitter = ec.LookupdPollJitter
	}

	if ec.MaxRequeueDelay != 0 {
		config.MaxRequeueDelay = ec.MaxRequeueDelay
	}

	if ec.DefaultRequeueDelay != 0 {
		config.DefaultRequeueDelay = ec.DefaultRequeueDelay
	}

	if ec.BackoffStrategy != nil {
		config.BackoffStrategy = ec.BackoffStrategy
	}

	if ec.MaxBackoffDuration != 0 {
		config.MaxBackoffDuration = ec.MaxBackoffDuration
	}

	if ec.BackoffMultiplier != 0 {
		config.BackoffMultiplier = ec.BackoffMultiplier
	}

	if ec.MaxAttempts != 0 {
		config.MaxAttempts = ec.MaxAttempts
	}

	if ec.LowRdyIdleTimeout != 0 {
		config.LowRdyIdleTimeout = ec.LowRdyIdleTimeout
	}

	if ec.RDYRedistributeInterval != 0 {
		config.RDYRedistributeInterval = ec.RDYRedistributeInterval
	}

	if ec.ClientID != "" {
		config.ClientID = ec.ClientID
	}

	if ec.Hostname != "" {
		config.Hostname = ec.Hostname
	}

	if ec.UserAgent != "" {
		config.UserAgent = ec.UserAgent
	}

	if ec.HeartbeatInterval != 0 {
		config.HeartbeatInterval = ec.HeartbeatInterval
	}

	if ec.SampleRate != 0 {
		config.SampleRate = ec.SampleRate
	}

	if ec.TLSV1 {
		config.TlsV1 = ec.TLSV1
	}

	if ec.TLSConfig != nil {
		config.TlsConfig = ec.TLSConfig
	}

	if ec.Deflate {
		config.Deflate = ec.Deflate
	}

	if ec.OutputBufferSize != 0 {
		config.OutputBufferSize = ec.OutputBufferSize
	}

	if ec.OutputBufferTimeout != 0 {
		config.OutputBufferTimeout = ec.OutputBufferTimeout
	}

	if ec.MaxInFlight != 0 {
		config.MaxInFlight = ec.MaxInFlight
	}

	if ec.MsgTimeout != 0 {
		config.MsgTimeout = ec.MsgTimeout
	}

	if ec.AuthSecret != "" {
		config.AuthSecret = ec.AuthSecret
	}

	return
}
