package pubsub

import (
	"context"
	"github.com/edgejumps/sportstalk-common-utils/logger"
	"github.com/redis/go-redis/v9"
	"sync"
)

type pubSubImpl struct {
	client *redis.Client

	working bool
	mu      *sync.Mutex
}

func NewPubSub(c *redis.Client) UnifiedPubSub {
	return &pubSubImpl{
		client: c,
		mu:     &sync.Mutex{},
	}
}

func (ps *pubSubImpl) Publish(context context.Context, event Event) error {

	data, err := event.Data()

	if err != nil {
		return err
	}

	value := ""

	err = data.NormalizeInto(&value)

	if err != nil {
		return err
	}

	count, err := ps.client.Publish(context, event.ID().Topic, value).Result()

	if err != nil {
		return err
	}

	if count == 0 {
		return ErrNoSubscriberConsumed
	}

	return nil
}

func (ps *pubSubImpl) Subscribe(context context.Context, topics ...TargetTopic) (<-chan Event, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.working {
		return nil, ErrAlreadySubscribed
	}

	targets := make([]string, len(topics))

	for i, t := range topics {
		targets[i] = t.Key
	}

	sub := ps.client.Subscribe(context, targets...)

	_, err := sub.Receive(context)

	if err != nil {
		return nil, err
	}

	msgs := make(chan Event)

	go ps.listen(context, sub, msgs)

	ps.working = true

	return msgs, nil
}

func (ps *pubSubImpl) DumpSyncPoint(path string) error {
	return nil
}

func (ps *pubSubImpl) listen(ctx context.Context, pubsub *redis.PubSub, receiver chan<- Event) {
	defer func() {
		pubsub.Close()
		close(receiver)
		ps.working = false
		logger.Info("pubsub closed")
	}()

	ch := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:

			event := NewIncomingEvent(&EventID{
				Topic: msg.Channel,
			}, msg.Payload)

			receiver <- event
		}
	}
}
