package registry

import "github.com/edgejumps/sportstalk-pubsub/pubsub"

// EventHandler is an interface that defines the behavior of an event handler.
// An event handler is responsible for handling an event matching a specific Action.
// Please ensure the Action is normalized, e.g., /action/path.
type EventHandler interface {
	Action() string
	Handle(data pubsub.EventData) error
}

type EventDataBuilder interface {
	Build(payload interface{}, ttl int) (pubsub.EventData, error)
}
