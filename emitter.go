package eventbus

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	nsq "github.com/nsqio/go-nsq"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Emitter interface {
	Emit(topic string, payload interface{}) error
	Request(topic string, payload interface{}, handler handlerFunc) error
}

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
	TlsV1                   bool
	TlsConfig               *tls.Config
	Deflate                 bool
	DeflateLevel            int
	Snappy                  bool
	OutputBufferSize        int64
	OutputBufferTimeout     time.Duration
	MaxInFlight             int
	MsgTimeout              time.Duration
	AuthSecret              string
}

type EventEmitter struct {
	*nsq.Producer
	address string
}

func NewEmitter(ec EmitterConfig) (emitter Emitter, err error) {
	config := newEmitterConfig(ec)

	var address string
	if len(ec.Address) == 0 {
		address = "localhost:4150"
	}

	producer, err := nsq.NewProducer(address, config)
	if err != nil {
		return
	}

	emitter = &EventEmitter{producer, address}

	return
}

func (ee EventEmitter) Emit(topic string, payload interface{}) (err error) {
	p, err := json.Marshal(payload)
	if err != nil {
		return
	}

	message := Message{Payload: p}
	body, err := json.Marshal(&message)
	if err != nil {
		return
	}

	err = ee.Publish(topic, body)

	return
}

func (ee EventEmitter) Request(topic string, payload interface{}, handler handlerFunc) (err error) {
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

	p, err := json.Marshal(payload)
	if err != nil {
		return
	}

	message := Message{replyTo, p}
	body, err := json.Marshal(&message)
	if err != nil {
		return
	}

	if err = ee.Publish(topic, body); err != nil {
		return
	}

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

	if ec.TlsV1 {
		config.TlsV1 = ec.TlsV1
	}

	if ec.TlsConfig != nil {
		config.TlsConfig = ec.TlsConfig
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

func (ee EventEmitter) genReplyQueue() (replyTo string, err error) {
	b := make([]byte, 8)
	_, err = rand.Read(b)
	if err != nil {
		return
	}

	hash := hex.EncodeToString(b)
	replyTo = fmt.Sprint(hash, ".ephemeral")

	return
}

func (ee EventEmitter) createTopic(topic string) (err error) {
	s := strings.Split(ee.address, ":")
	port, err := strconv.Atoi(s[1])
	if err != nil {
		return
	}

	uri := "http://" + s[0] + ":" + strconv.Itoa(port+1) + "/topic/create?topic=" + topic
	res, err := http.Post(uri, "application/json; charset=utf-8", nil)
	if err != nil {
		return
	}

	if res.StatusCode != 200 {
		return
	}

	return
}
