package registry

import "github.com/edgejumps/sportstalk-pubsub/pubsub"

type EventHandler interface {
	Action() string
	Handle(data pubsub.EventData) error
}

type EventDataBuilder interface {
	Build(payload interface{}, ttl int) (pubsub.EventData, error)
}
