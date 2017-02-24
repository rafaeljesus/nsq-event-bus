package eventbus

import (
	"encoding/json"
	nsq "github.com/nsqio/go-nsq"
)

type Emitter interface {
	Emit(topic string, payload interface{}) error
}

type EmitterConfig struct {
	*nsq.Config
	Address string
}

type EventEmitter struct {
	*nsq.Producer
}

func NewEmitter(ec EmitterConfig) (emitter Emitter, err error) {
	config := newEmitterConfig(ec)
	producer, err := nsq.NewProducer(ec.Address, config)
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
	config.DialTimeout = ec.DialTimeout
	config.ReadTimeout = ec.ReadTimeout
	config.LocalAddr = ec.LocalAddr
	config.LookupdPollInterval = ec.LookupdPollInterval
	config.LookupdPollJitter = ec.LookupdPollJitter
	config.MaxRequeueDelay = ec.MaxRequeueDelay
	config.DefaultRequeueDelay = ec.DefaultRequeueDelay
	config.BackoffStrategy = ec.BackoffStrategy
	config.MaxBackoffDuration = ec.MaxBackoffDuration
	config.BackoffMultiplier = ec.BackoffMultiplier
	config.MaxAttempts = ec.MaxAttempts
	config.LowRdyIdleTimeout = ec.LowRdyIdleTimeout
	config.RDYRedistributeInterval = ec.RDYRedistributeInterval
	config.ClientID = ec.ClientID
	config.Hostname = ec.Hostname
	config.UserAgent = ec.UserAgent
	config.HeartbeatInterval = ec.HeartbeatInterval
	config.SampleRate = ec.SampleRate
	config.TlsV1 = ec.TlsV1
	config.Deflate = ec.Deflate
	config.OutputBufferSize = ec.OutputBufferSize
	config.OutputBufferTimeout = ec.OutputBufferTimeout
	config.MaxInFlight = ec.MaxInFlight
	config.MsgTimeout = ec.MsgTimeout
	config.AuthSecret = ec.AuthSecret
	return
}
