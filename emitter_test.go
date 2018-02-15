package bus

import (
	"crypto/tls"
	"sync"
	"testing"
	"time"
)

func TestEmitter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			"create new emitter",
			testNewEmitter,
		},
		{
			"emit message",
			testEmitMessage,
		},
		{
			"emit async message",
			testEmitAsyncMessage,
		},
		{
			"request message",
			testRequestMessage,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t)
		})
	}
}

func testNewEmitter(t *testing.T) {
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
		t.Fatalf("expected to initialize emitter %v", err)
	}
}

func testEmitMessage(t *testing.T) {
	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Fatalf("expected to initialize emitter %v", err)
	}

	type event struct{ Name string }
	e := event{"event"}
	if err := emitter.Emit("etopic", &e); err != nil {
		t.Fatalf("expected to emit message %v", err)
	}

	if err := emitter.Emit("", &e); err != ErrTopicRequired {
		t.Fatalf("unexpected error value %v", err)
	}
}

func testEmitAsyncMessage(t *testing.T) {
	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Fatalf("expected to initialize emitter %v", err)
	}

	type event struct{ Name string }
	e := event{"event"}
	if err := emitter.EmitAsync("etopic", &e); err != nil {
		t.Fatalf("expected to emit message %v", err)
	}

	if err := emitter.Emit("", &e); err != ErrTopicRequired {
		t.Fatalf("unexpected error value %v", err)
	}
}

func testRequestMessage(t *testing.T) {
	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Fatalf("expected to initialize emitter %v", err)
	}

	type event struct{ Name string }

	var wg sync.WaitGroup
	wg.Add(1)
	replyHandler := func(message *Message) (reply interface{}, err error) {
		defer wg.Done()
		e := event{}
		if err = message.DecodePayload(&e); err != nil {
			t.Errorf("Expected to unmarshal payload")
		}
		if e.Name != "event_reply" {
			t.Errorf("Expected name to be equal event %s", e.Name)
		}
		message.Finish()
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
		t.Fatalf("expected to listen a message %v", err)
	}

	cases := []struct {
		topic   string
		event   event
		replyh  func(message *Message) (interface{}, error)
		wantErr bool
	}{
		{
			"etopic", event{"event"}, replyHandler, true,
		},
		{
			"", event{"event"}, replyHandler, false,
		},
		{
			"etopic", event{"event"}, nil, false,
		},
	}

	for _, c := range cases {
		if c.wantErr {
			if err := emitter.Request(c.topic, c.event, c.replyh); err != nil {
				t.Errorf("expected to request a message %v", err)
			}
		} else {
			if err := emitter.Request(c.topic, c.event, c.replyh); err == nil {
				t.Errorf("unexpected error value %v", err)
			}
		}
	}

	wg.Wait()
}

type localAddrMock struct{}

func (a *localAddrMock) Network() (s string) { return }
func (a *localAddrMock) String() (s string)  { return }

type backoffStrategyMock struct{}

func (b *backoffStrategyMock) Calculate(attempt int) (v time.Duration) { return }
