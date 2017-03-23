package bus

import (
	"crypto/tls"
	"net"
	"time"

	nsq "github.com/nsqio/go-nsq"
)

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

// ListenerConfig carries the different variables to tune a newly started consumer,
// it exposes the same configuration available from official nsq go client.
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

func newListenerConfig(lc ListenerConfig) (config *nsq.Config) {
	config = nsq.NewConfig()

	setDialTimeout(config, lc.DialTimeout)
	setReadTimeout(config, lc.ReadTimeout)
	setLocalAddr(config, lc.LocalAddr)
	setLookupPollInterval(config, lc.LookupdPollInterval)
	setLookupPollJitter(config, lc.LookupdPollJitter)
	setMaxRequeueDelay(config, lc.MaxRequeueDelay)
	setDefaultRequeueDelay(config, lc.DefaultRequeueDelay)
	setBackoffStrategy(config, lc.BackoffStrategy)
	setMaxBackoffDuration(config, lc.MaxBackoffDuration)
	setBackoffMultiplier(config, lc.BackoffMultiplier)
	setMaxAttempts(config, lc.MaxAttempts)
	setLowRdyIdleTimeout(config, lc.LowRdyIdleTimeout)
	setRDYRedistributeInterval(config, lc.RDYRedistributeInterval)
	setClientID(config, lc.ClientID)
	setHostname(config, lc.Hostname)
	setUserAgent(config, lc.UserAgent)
	setHeartbeatInterval(config, lc.HeartbeatInterval)
	setSampleRate(config, lc.SampleRate)
	setTLSV1(config, lc.TLSV1)
	setTLSConfig(config, lc.TLSConfig)
	setDeflate(config, lc.Deflate)
	setOutputBufferSize(config, lc.OutputBufferSize)
	setOutputBufferTimeout(config, lc.OutputBufferTimeout)
	setMaxInFlight(config, lc.MaxInFlight)
	setMsgTimeout(config, lc.MsgTimeout)
	setAuthSecret(config, lc.AuthSecret)

	return
}

func setDialTimeout(config *nsq.Config, dialTimeout time.Duration) {
	if dialTimeout != 0 {
		config.DialTimeout = dialTimeout
	}
}

func setReadTimeout(config *nsq.Config, readTimeout time.Duration) {
	if readTimeout != 0 {
		config.ReadTimeout = readTimeout
	}
}

func setLocalAddr(config *nsq.Config, localAddr net.Addr) {
	if localAddr != nil {
		config.LocalAddr = localAddr
	}
}

func setLookupPollInterval(config *nsq.Config, lookupdPollInterval time.Duration) {
	if lookupdPollInterval != 0 {
		config.LookupdPollInterval = lookupdPollInterval
	}
}

func setLookupPollJitter(config *nsq.Config, lookupdPollJitter float64) {
	if lookupdPollJitter != 0 {
		config.LookupdPollJitter = lookupdPollJitter
	}
}

func setMaxRequeueDelay(config *nsq.Config, maxRequeueDelay time.Duration) {
	if maxRequeueDelay != 0 {
		config.MaxRequeueDelay = maxRequeueDelay
	}
}

func setDefaultRequeueDelay(config *nsq.Config, defaultRequeueDelay time.Duration) {
	if defaultRequeueDelay != 0 {
		config.DefaultRequeueDelay = defaultRequeueDelay
	}
}

func setBackoffStrategy(config *nsq.Config, backoffStrategy nsq.BackoffStrategy) {
	if backoffStrategy != nil {
		config.BackoffStrategy = backoffStrategy
	}
}

func setMaxBackoffDuration(config *nsq.Config, maxBackoffDuration time.Duration) {
	if maxBackoffDuration != 0 {
		config.MaxBackoffDuration = maxBackoffDuration
	}
}

func setBackoffMultiplier(config *nsq.Config, backoffMultiplier time.Duration) {
	if backoffMultiplier != 0 {
		config.BackoffMultiplier = backoffMultiplier
	}
}

func setMaxAttempts(config *nsq.Config, maxAttempts uint16) {
	if maxAttempts != 0 {
		config.MaxAttempts = maxAttempts
	}
}

func setLowRdyIdleTimeout(config *nsq.Config, lowRdyIdleTimeout time.Duration) {
	if lowRdyIdleTimeout != 0 {
		config.LowRdyIdleTimeout = lowRdyIdleTimeout
	}
}

func setRDYRedistributeInterval(config *nsq.Config, rdyRedistributeInterval time.Duration) {
	if rdyRedistributeInterval != 0 {
		config.LowRdyIdleTimeout = rdyRedistributeInterval
	}
}

func setClientID(config *nsq.Config, clientID string) {
	if clientID != "" {
		config.ClientID = clientID
	}
}

func setHostname(config *nsq.Config, hostname string) {
	if hostname != "" {
		config.Hostname = hostname
	}
}

func setUserAgent(config *nsq.Config, userAgent string) {
	if userAgent != "" {
		config.UserAgent = userAgent
	}
}

func setHeartbeatInterval(config *nsq.Config, heartbeatInterval time.Duration) {
	if heartbeatInterval != 0 {
		config.HeartbeatInterval = heartbeatInterval
	}
}

func setSampleRate(config *nsq.Config, sampleRate int32) {
	if sampleRate != 0 {
		config.SampleRate = sampleRate
	}
}

func setTLSV1(config *nsq.Config, tlsv1 bool) {
	if tlsv1 {
		config.TlsV1 = tlsv1
	}
}

func setTLSConfig(config *nsq.Config, tlsConfig *tls.Config) {
	if tlsConfig != nil {
		config.TlsConfig = tlsConfig
	}
}

func setDeflate(config *nsq.Config, deflate bool) {
	if deflate {
		config.Deflate = deflate
	}
}

func setOutputBufferSize(config *nsq.Config, out int64) {
	if out != 0 {
		config.OutputBufferSize = out
	}
}

func setOutputBufferTimeout(config *nsq.Config, out time.Duration) {
	if out != 0 {
		config.OutputBufferTimeout = out
	}
}

func setMaxInFlight(config *nsq.Config, maxInFlight int) {
	if maxInFlight != 0 {
		config.MaxInFlight = maxInFlight
	}
}

func setMsgTimeout(config *nsq.Config, msgTimeout time.Duration) {
	if msgTimeout != 0 {
		config.MsgTimeout = msgTimeout
	}
}

func setAuthSecret(config *nsq.Config, authSecret string) {
	if authSecret != "" {
		config.AuthSecret = authSecret
	}
}
