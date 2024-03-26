package registry

import (
	"errors"
	"github.com/edgejumps/sportstalk-pubsub/pubsub"
	cmap "github.com/orcaman/concurrent-map/v2"
)

var (
	ErrHandlerRegistered           = errors.New("handler already registered")
	ErrHandlerNotFound             = errors.New("handler not found")
	ErrNoAvailableEventDataBuilder = errors.New("no available event data builder")
)

type EventHandlerRegistry interface {
	Register(handler EventHandler) error
	Unregister(action string) error
	Route(event pubsub.Event) error
	Has(action string) bool

	BuildData(action string, payload interface{}, ttl int) (pubsub.EventData, error)
}

type registry struct {
	handlers cmap.ConcurrentMap[string, EventHandler]
}

func NewRegistry() EventHandlerRegistry {
	return &registry{
		handlers: cmap.New[EventHandler](),
	}
}

func (r *registry) Register(handler EventHandler) error {
	if r.Has(handler.Action()) {
		return ErrHandlerRegistered
	}

	r.handlers.Set(handler.Action(), handler)

	return nil
}

func (r *registry) Unregister(action string) error {
	if !r.Has(action) {
		return ErrHandlerNotFound
	}

	r.handlers.Remove(action)

	return nil
}

func (r *registry) Route(event pubsub.Event) error {
	data, err := event.Data()

	if err != nil {
		return err
	}

	h, ok := r.handlers.Get(data.Action())

	if !ok {
		return ErrHandlerNotFound
	}

	return h.Handle(data)
}

func (r *registry) Has(action string) bool {
	return r.handlers.Has(action)
}

func (r *registry) BuildData(action string, payload interface{}, ttl int) (pubsub.EventData, error) {
	h, ok := r.handlers.Get(action)

	if !ok {
		return nil, ErrHandlerNotFound
	}

	b, ok := h.(EventDataBuilder)

	if !ok {
		return nil, ErrNoAvailableEventDataBuilder
	}

	return b.Build(payload, ttl)
}
