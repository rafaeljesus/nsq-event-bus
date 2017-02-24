package eventbus

import (
	"crypto/tls"
	"encoding/json"
	nsq "github.com/nsqio/go-nsq"
	"net"
	"time"
)

type Emitter interface {
	Emit(topic string, payload interface{}) error
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

	emitter = &EventEmitter{producer}

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
