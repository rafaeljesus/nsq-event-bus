package bus

import (
	"crypto/tls"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestListener(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			"listener on",
			testOn,
		},
		{
			"listener on validation",
			testOnValidation,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t)
		})
	}
}

func testOn(t *testing.T) {
	type event struct{ Name string }

	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Fatalf("expected to initialize emitter %v", err)
	}

	e := event{"event"}
	if err := emitter.Emit("ltopic", &e); err != nil {
		t.Fatalf("expected to emit message %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	handler := func(message *Message) (reply interface{}, err error) {
		defer wg.Done()
		e := event{}
		if err = message.DecodePayload(&e); err != nil {
			t.Errorf("Expected to unmarshal payload")
		}
		if e.Name != "event" {
			t.Errorf("Expected name to be equal event %s", e.Name)
		}
		message.Finish()
		return
	}

	if err := On(ListenerConfig{
		Topic:                   "ltopic",
		Channel:                 "test_on",
		HandlerFunc:             handler,
		HandlerConcurrency:      1,
		Lookup:                  []string{"localhost:4161"},
		DialTimeout:             time.Second * 1,
		ReadTimeout:             time.Second * 60,
		WriteTimeout:            time.Second * 1,
		LookupdPollInterval:     time.Second * 60,
		LookupdPollJitter:       0.3,
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
		HeartbeatInterval:       time.Second * 30,
		SampleRate:              99,
		TLSV1:                   true,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		Deflate:             true,
		DeflateLevel:        5,
		Snappy:              true,
		OutputBufferSize:    16384,
		OutputBufferTimeout: time.Millisecond * 350,
		MaxInFlight:         2,
		MsgTimeout:          time.Second * 5,
		AuthSecret:          "foo",
	}); err != nil {
		t.Errorf("expected to listen a message %v", err)
	}

	wg.Wait()
}

func testOnValidation(t *testing.T) {
	cases := []struct {
		msg    string
		config ListenerConfig
	}{
		{
			"unexpected topic",
			ListenerConfig{
				Topic:   "",
				Channel: "test_on",
				HandlerFunc: func(message *Message) (reply interface{}, err error) {
					return
				},
			},
		},
		{
			"unexpected channel",
			ListenerConfig{
				Topic:   "ltopic",
				Channel: "",
				HandlerFunc: func(message *Message) (reply interface{}, err error) {
					return
				},
			},
		},
		{
			"unexpected handler",
			ListenerConfig{
				Topic:   "ltopic",
				Channel: "test_on",
			},
		},
	}

	for _, c := range cases {
		if err := On(c.config); err == nil {
			t.Fatalf(fmt.Sprintf("%s: %v", c.msg, err))
		}
	}
}
