## Event Bus NSQ

* A tiny wrapper around [go-nsq](https://github.com/nsqio/go-nsq) topic and channel.

## Installation
```bash
go get -u https://github.com/rafaeljesus/nsq-event-bus
```

## Environment Variables
```bash
export NSQ_URL=localhost:4150
export NSQ_LOOKUPD_URL=localhost:4161
```

## Usage
The nsq-event-bus package exposes a interface for emitting and listening events.

### Emitter
```go
import "github.com/rafaeljesus/nsq-event-bus"

topic := "events"
var event struct{}
eventBus, _ := eventbus.NewEventBus()

e := event{}
if err := eventBus.Emit(topic, &e); err != nil {
  // handle failure to emit message
}

```

### Listener
```go
import "github.com/rafaeljesus/nsq-event-bus"

topic := "events"
metricsChannel := "metrics"
notificationsChannel := "notifications"
eventBus, _ := eventbus.NewEventBus()

if err := eventBus.On(topic, metricsChannel, metricsHandler); err != nil {
  // handle failure to listen a message
}

if err := eventBus.On(topic, notificationsChannel, notificationsHandler); err != nil {
  // handle failure to listen a message
}

func metricsHandler(payload interface{}) (interface{}, error) {
  v, ok := payload.(map[string]interface{})
  if !ok {
    return nil, ErrPayloadInvalid
  }
  // handle message
  return nil, nil
}

func notificationsHandler(payload interface{}) (interface{}, error) {
  v, ok := payload.(map[string]interface{})
  if !ok {
    return nil, ErrPayloadInvalid
  }
  // handle message
  return nil, nil
}

```

### Request (Request/Reply)
```go
import "github.com/rafaeljesus/nsq-event-bus"

topic := "user_signup"
eventBus, _ := eventbus.NewEventBus()

e := event{Login: "rafa", Password: "ilhabela_is_the_place"}
if err := eventBus.Request(topic, &e, replyHandler); err != nil {
  // handle failure to listen a message
}

func replyHandler(message interface{}) (interface{}, error) {
  v, ok := payload.(map[string]interface{})
  if !ok {
    return nil, ErrPayloadInvalid
  }
  // handle message
  return nil, nil
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

---

> GitHub [@rafaeljesus](https://github.com/rafaeljesus) &nbsp;&middot;&nbsp;
> Medium [@_jesus_rafael](https://medium.com/@_jesus_rafael) &nbsp;&middot;&nbsp;
> Twitter [@_jesus_rafael](https://twitter.com/_jesus_rafael)
