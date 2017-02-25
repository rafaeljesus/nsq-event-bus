package bus

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	nsq "github.com/nsqio/go-nsq"
	"net"
	"time"
)

var (
	TopicRequired   = errors.New("creating a new consumer requires a non-empty topic")
	ChannelRequired = errors.New("creating a new consumer requires a non-empty channel")
)

type handlerFunc func(payload []byte) (interface{}, error)

type Listener interface {
	On(lc ListenerConfig) error
}

type ListenerConfig struct {
	Topic                   string
	Channel                 string
	Lookup                  []string
	HandlerFunc             handlerFunc
	HandlerConcurrency      int
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

func On(lc ListenerConfig) (err error) {
	if len(lc.Topic) == 0 {
		err = TopicRequired
		return
	}

	if len(lc.Channel) == 0 {
		err = ChannelRequired
		return
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
		return
	}

	handler := handleMessage(consumer, lc)
	consumer.AddConcurrentHandlers(handler, lc.HandlerConcurrency)

	if err = consumer.ConnectToNSQLookupds(lc.Lookup); err != nil {
		return
	}

	return
}

func handleMessage(consumer *nsq.Consumer, lc ListenerConfig) nsq.HandlerFunc {
	return nsq.HandlerFunc(func(message *nsq.Message) (err error) {
		m := Message{}
		if err = json.Unmarshal(message.Body, &m); err != nil {
			return
		}

		res, err := lc.HandlerFunc(m.Payload)
		if err != nil {
			return
		}

		message.Finish()

		if m.ReplyTo == "" {
			return
		}

		emitter, err := NewEmitter(EmitterConfig{})
		if err != nil {
			return
		}

		err = emitter.Emit(m.ReplyTo, res)

		return
	})
}

func newListenerConfig(lc ListenerConfig) (config *nsq.Config) {
	config = nsq.NewConfig()

	if lc.DialTimeout != 0 {
		config.DialTimeout = lc.DialTimeout
	}

	if lc.ReadTimeout != 0 {
		config.ReadTimeout = lc.ReadTimeout
	}

	if lc.LocalAddr != nil {
		config.LocalAddr = lc.LocalAddr
	}

	if lc.LookupdPollInterval != 0 {
		config.LookupdPollInterval = lc.LookupdPollInterval
	}

	if lc.LookupdPollJitter != 0 {
		config.LookupdPollJitter = lc.LookupdPollJitter
	}

	if lc.MaxRequeueDelay != 0 {
		config.MaxRequeueDelay = lc.MaxRequeueDelay
	}

	if lc.DefaultRequeueDelay != 0 {
		config.DefaultRequeueDelay = lc.DefaultRequeueDelay
	}

	if lc.BackoffStrategy != nil {
		config.BackoffStrategy = lc.BackoffStrategy
	}

	if lc.MaxBackoffDuration != 0 {
		config.MaxBackoffDuration = lc.MaxBackoffDuration
	}

	if lc.BackoffMultiplier != 0 {
		config.BackoffMultiplier = lc.BackoffMultiplier
	}

	if lc.MaxAttempts != 0 {
		config.MaxAttempts = lc.MaxAttempts
	}

	if lc.LowRdyIdleTimeout != 0 {
		config.LowRdyIdleTimeout = lc.LowRdyIdleTimeout
	}

	if lc.RDYRedistributeInterval != 0 {
		config.RDYRedistributeInterval = lc.RDYRedistributeInterval
	}

	if lc.ClientID != "" {
		config.ClientID = lc.ClientID
	}

	if lc.Hostname != "" {
		config.Hostname = lc.Hostname
	}

	if lc.UserAgent != "" {
		config.UserAgent = lc.UserAgent
	}

	if lc.HeartbeatInterval != 0 {
		config.HeartbeatInterval = lc.HeartbeatInterval
	}

	if lc.SampleRate != 0 {
		config.SampleRate = lc.SampleRate
	}

	if lc.TlsV1 {
		config.TlsV1 = lc.TlsV1
	}

	if lc.TlsConfig != nil {
		config.TlsConfig = lc.TlsConfig
	}

	if lc.Deflate {
		config.Deflate = lc.Deflate
	}

	if lc.OutputBufferSize != 0 {
		config.OutputBufferSize = lc.OutputBufferSize
	}

	if lc.OutputBufferTimeout != 0 {
		config.OutputBufferTimeout = lc.OutputBufferTimeout
	}

	if lc.MaxInFlight != 0 {
		config.MaxInFlight = lc.MaxInFlight
	}

	if lc.MsgTimeout != 0 {
		config.MsgTimeout = lc.MsgTimeout
	}

	if lc.AuthSecret != "" {
		config.AuthSecret = lc.AuthSecret
	}

	return
}
