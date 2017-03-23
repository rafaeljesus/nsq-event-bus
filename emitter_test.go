package bus

import (
	"crypto/tls"
	"sync"
	"testing"
	"time"
)

func TestNewEmitter(t *testing.T) {
	_, err := NewEmitter(EmitterConfig{
		Address:                 "localhost:4150",
		DialTimeout:             time.Second * 5,
		ReadTimeout:             time.Second * 5,
		WriteTimeout:            time.Second * 5,
		LocalAddr:               &localAddrMock{},
		LookupdPollInterval:     time.Second * 5,
		LookupdPollJitter:       1,
		MaxRequeueDelay:         time.Second * 5,
		DefaultRequeueDelay:     time.Second * 5,
		BackoffStrategy:         &backoffStrategyMock{},
		MaxBackoffDuration:      time.Second * 5,
		BackoffMultiplier:       time.Second * 5,
		MaxAttempts:             5,
		LowRdyIdleTimeout:       time.Second * 5,
		RDYRedistributeInterval: time.Second * 5,
		ClientID:                "foo",
		Hostname:                "foo",
		UserAgent:               "foo",
		HeartbeatInterval:       time.Second * 5,
		SampleRate:              10,
		TLSV1:                   true,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		Deflate:             true,
		DeflateLevel:        1,
		Snappy:              true,
		OutputBufferSize:    1,
		OutputBufferTimeout: time.Second * 5,
		MaxInFlight:         1,
		MsgTimeout:          time.Second * 5,
		AuthSecret:          "foo",
	})

	if err != nil {
		t.Errorf("Expected to initialize emitter %s", err)
	}
}

func TestEmitterEmit(t *testing.T) {
	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Errorf("Expected to initialize emitter %s", err)
	}

	type event struct{ Name string }
	e := event{"event"}
	if err := emitter.Emit("etopic", &e); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}

	if err := emitter.Emit("", &e); err == nil {
		t.Fail()
	}
}

func TestEmitterEmitAsync(t *testing.T) {
	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Errorf("Expected to initialize emitter %s", err)
	}

	type event struct{ Name string }
	e := event{"event"}

	if err := emitter.EmitAsync("etopic", &e); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}

	if err := emitter.EmitAsync("", &e); err == nil {
		t.Fail()
	}
}

func TestEmitterRequest(t *testing.T) {
	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Errorf("Expected to initialize emitter %s", err)
	}

	type event struct{ Name string }

	var wg sync.WaitGroup
	wg.Add(1)

	replyHandler := func(message *Message) (reply interface{}, err error) {
		e := event{}
		if err = message.DecodePayload(&e); err != nil {
			t.Errorf("Expected to unmarshal payload")
		}

		if e.Name != "event_reply" {
			t.Errorf("Expected name to be equal event %s", e.Name)
		}

		message.Finish()
		wg.Done()
		return
	}

	handler := func(message *Message) (reply interface{}, err error) {
		e := event{}
		if err = message.DecodePayload(&e); err != nil {
			t.Errorf("Expected to unmarshal payload")
		}
		reply = &event{"event_reply"}
		message.Finish()
		return
	}

	if err := On(ListenerConfig{
		Topic:       "etopic",
		Channel:     "test_request",
		HandlerFunc: handler,
	}); err != nil {
		t.Errorf("Expected to listen a message %s", err)
	}

	e := event{"event"}
	if err := emitter.Request("etopic", &e, replyHandler); err != nil {
		t.Errorf("Expected to request a message %s", err)
	}

	if err := emitter.Request("", &e, replyHandler); err == nil {
		t.Fail()
	}

	if err := emitter.Request("etopic", &e, nil); err == nil {
		t.Fail()
	}

	wg.Wait()
}

type localAddrMock struct{}

func (a *localAddrMock) Network() (s string) {
	return
}

func (a *localAddrMock) String() (s string) {
	return
}

type backoffStrategyMock struct{}

func (b *backoffStrategyMock) Calculate(attempt int) (v time.Duration) {
	return
}
