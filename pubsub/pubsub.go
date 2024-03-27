package pubsub

import (
	"context"
	"github.com/redis/go-redis/v9"
	"sync"
)

func New(c *redis.Client) UnifiedPubSub {
	return &pubSubImpl{
		client:    c,
		eventChan: make(chan Event),
		topics:    make(map[string]Topic),
		workers:   make([]Worker, 0),
		mu:        &sync.Mutex{},
	}
}

// Since Redis PUBSUB would not be able to persistent messages,
// so we would create a new worker for newly added topics when calling Subscribe,
// instead of using the same worker for all topics.
type pubSubImpl struct {
	client *redis.Client

	eventChan chan Event

	topics map[string]Topic

	workers []Worker

	mu *sync.Mutex
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

func (ps *pubSubImpl) Subscribe(topics ...Topic) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	newTopics := make([]Topic, 0)

	for _, topic := range topics {

		name := topic.Name()

		if _, ok := ps.topics[name]; !ok {
			newTopics = append(newTopics, topic)
		}

		ps.topics[name] = topic
	}

	if len(newTopics) == 0 {
		return nil
	}

	worker := NewWorker(ps.client)
	ps.workers = append(ps.workers, worker)

	return worker.Run(newTopics, ps.eventChan)
}

func (ps *pubSubImpl) Events() <-chan Event {
	return ps.eventChan
}

func (ps *pubSubImpl) Errors() <-chan error {
	return nil
}

func (ps *pubSubImpl) Topics() []string {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	topics := make([]string, 0)

	for _, topic := range ps.topics {
		topics = append(topics, topic.Name())
	}

	return topics
}

func (ps *pubSubImpl) Stop() (SyncPoint, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if len(ps.workers) == 0 {
		return SyncPoint{}, nil
	}

	defer func() {
		ps.workers = make([]Worker, 0)
		close(ps.eventChan)
	}()

	for _, worker := range ps.workers {
		worker.Stop()
	}

	return SyncPoint{}, nil
}
