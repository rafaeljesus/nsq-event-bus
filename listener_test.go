package bus

import (
	"crypto/tls"
	"sync"
	"testing"
	"time"
)

func TestListenerOn(t *testing.T) {
	type event struct{ Name string }

	emitter, err := NewEmitter(EmitterConfig{})
	if err != nil {
		t.Errorf("Expected to initialize emitter %s", err)
	}

	e := event{"event"}
	if err := emitter.Emit("ltopic", &e); err != nil {
		t.Errorf("Expected to emit message %s", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(message *Message) (reply interface{}, err error) {
		e := event{}
		if err = message.DecodePayload(&e); err != nil {
			t.Errorf("Expected to unmarshal payload")
		}

		if e.Name != "event" {
			t.Errorf("Expected name to be equal event %s", e.Name)
		}

		message.Finish()
		wg.Done()
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
		t.Errorf("Expected to listen a message %s", err)
	}

	wg.Wait()
}

func TestListenerOnRequiresTopic(t *testing.T) {
	if err := On(ListenerConfig{
		Topic:   "",
		Channel: "test_on",
		HandlerFunc: func(message *Message) (reply interface{}, err error) {
			return
		},
	}); err == nil {
		t.Errorf("Expected to pass a topic %s", err)
	}
}

func TestListenerOnRequiresChannel(t *testing.T) {
	if err := On(ListenerConfig{
		Topic:   "ltopic",
		Channel: "",
		HandlerFunc: func(message *Message) (reply interface{}, err error) {
			return
		},
	}); err == nil {
		t.Errorf("Expected to pass a channel %s", err)
	}
}

func TestListenerOnRequiresHandler(t *testing.T) {
	if err := On(ListenerConfig{
		Topic:   "ltopic",
		Channel: "test_on",
	}); err == nil {
		t.Errorf("Expected to pass a handler %s", err)
	}
}
