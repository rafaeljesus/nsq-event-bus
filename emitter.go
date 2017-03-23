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
// if an error occurred while creating nsq producer.
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
// an error if encoding payload fails or if an error occurred while publishing
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
// logs to console if an error occurred while publishing the message.
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
// Returns an non-nil err if an error occurred while creating or listening to the internal
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

	message := NewMessage(p, replyTo)
	body, err = json.Marshal(message)

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

	setDialTimeout(config, ec.DialTimeout)
	setReadTimeout(config, ec.ReadTimeout)
	setLocalAddr(config, ec.LocalAddr)
	setLookupPollInterval(config, ec.LookupdPollInterval)
	setLookupPollJitter(config, ec.LookupdPollJitter)
	setMaxRequeueDelay(config, ec.MaxRequeueDelay)
	setDefaultRequeueDelay(config, ec.DefaultRequeueDelay)
	setBackoffStrategy(config, ec.BackoffStrategy)
	setMaxBackoffDuration(config, ec.MaxBackoffDuration)
	setBackoffMultiplier(config, ec.BackoffMultiplier)
	setMaxAttempts(config, ec.MaxAttempts)
	setLowRdyIdleTimeout(config, ec.LowRdyIdleTimeout)
	setRDYRedistributeInterval(config, ec.RDYRedistributeInterval)
	setClientID(config, ec.ClientID)
	setHostname(config, ec.Hostname)
	setUserAgent(config, ec.UserAgent)
	setHeartbeatInterval(config, ec.HeartbeatInterval)
	setSampleRate(config, ec.SampleRate)
	setTLSV1(config, ec.TLSV1)
	setTLSConfig(config, ec.TLSConfig)
	setDeflate(config, ec.Deflate)
	setOutputBufferSize(config, ec.OutputBufferSize)
	setOutputBufferTimeout(config, ec.OutputBufferTimeout)
	setMaxInFlight(config, ec.MaxInFlight)
	setMsgTimeout(config, ec.MsgTimeout)
	setAuthSecret(config, ec.AuthSecret)

	return
}
