## Event Bus NSQ

* A tiny wrapper around [go-nsq](https://github.com/nsqio/go-nsq) topic and channel.
* Protect nsq calls with [gobreaker](https://github.com/sony/gobreaker).

## Installation
```bash
go get -u github.com/rafaeljesus/nsq-event-bus
```

## Usage
The nsq-event-bus package exposes a interface for emitting and listening events.

### Emitter
```go
import "github.com/rafaeljesus/nsq-event-bus"

topic := "events"
emitter, err := bus.NewEmitter(bus.EmitterConfig{
  Address: "localhost:4150",
  MaxInFlight: 25,
})

e := event{}
if err = emitter.Emit(topic, &e); err != nil {
  // handle failure to emit message
}

// emitting messages on a async fashion
if err = emitter.EmitAsync(topic, &e); err != nil {
  // handle failure to emit message
}

```

### Listener
```go
import "github.com/rafaeljesus/nsq-event-bus"

if err = bus.On(bus.ListenerConfig{
  Topic:              "topic",
  Channel:            "test_on",
  HandlerFunc:        handler,
  HandlerConcurrency: 4,
}); err != nil {
  // handle failure to listen a message
}

func handler(message *Message) (reply interface{}, err error) {
  e := event{}
  if err = message.DecodePayload(&e); err != nil {
    message.Finish()
    return
  }

  if message.Attempts > MAX_DELIVERY_ATTEMPTS {
    message.Finish()
    return
  }

  err, _ = doWork(&e)
  if err != nil {
    message.Requeue(BACKOFF_TIME)
    return
  }

  message.Finish()
  return
}
```

### Request (Request/Reply)
```go
import "github.com/rafaeljesus/nsq-event-bus"

topic := "user_signup"
emitter, err = bus.NewEmitter(bus.EmitterConfig{})

e := event{Login: "rafa", Password: "ilhabela_is_the_place"}
if err = bus.Request(topic, &e, handler); err != nil {
  // handle failure to listen a message
}

func handler(message *Message) (reply interface{}, err error) {
  e := event{}
  if err = message.DecodePayload(&e); err != nil {
    message.Finish()
    return
  }

  reply = &Reply{}
  message.Finish()
  return
}
```

## Contributing
- Fork it
- Create your feature branch (`git checkout -b my-new-feature`)
- Commit your changes (`git commit -am 'Add some feature'`)
- Push to the branch (`git push origin my-new-feature`)
- Create new Pull Request

## Badges

[![Build Status](https://circleci.com/gh/rafaeljesus/nsq-event-bus.svg?style=svg)](https://circleci.com/gh/rafaeljesus/nsq-event-bus)
[![Go Report Card](https://goreportcard.com/badge/github.com/rafaeljesus/nsq-event-bus)](https://goreportcard.com/report/github.com/rafaeljesus/nsq-event-bus)
[![Go Doc](https://godoc.org/github.com/rafaeljesus/nsq-event-bus?status.svg)](https://godoc.org/github.com/rafaeljesus/nsq-event-bus)

---

> GitHub [@rafaeljesus](https://github.com/rafaeljesus) &nbsp;&middot;&nbsp;
> Medium [@_jesus_rafael](https://medium.com/@_jesus_rafael) &nbsp;&middot;&nbsp;
> Twitter [@_jesus_rafael](https://twitter.com/_jesus_rafael)
