package pubsub

import (
	"context"
	"github.com/redis/go-redis/v9"
	"sync"
)

// Since Redis PUBSUB would not be able to persistent messages,
// so we would create a new worker for newly added topics when calling Subscribe,
// instead of using the same worker for all topics.
type pubSubImpl struct {
	client *redis.Client

	eventChan chan Event

	topics map[string]TargetTopic

	workers []Worker

	mu *sync.Mutex
}

func NewPubSub(c *redis.Client) UnifiedPubSub {
	return &pubSubImpl{
		client:    c,
		eventChan: make(chan Event),
		topics:    make(map[string]TargetTopic),
		workers:   make([]Worker, 0),
		mu:        &sync.Mutex{},
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

func (ps *pubSubImpl) Subscribe(topics ...TargetTopic) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	newTopics := make([]string, 0)

	for _, topic := range topics {

		if _, ok := ps.topics[topic.Key]; !ok {
			newTopics = append(newTopics, topic.Key)
		}

		ps.topics[topic.Key] = topic
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

func (ps *pubSubImpl) Topics() []TargetTopic {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	topics := make([]TargetTopic, 0)

	for _, topic := range ps.topics {
		topics = append(topics, topic)
	}

	return topics
}

func (ps *pubSubImpl) Stop() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if len(ps.workers) == 0 {
		return nil
	}

	defer func() {
		ps.workers = make([]Worker, 0)
		close(ps.eventChan)
	}()

	for _, worker := range ps.workers {
		worker.Stop()
	}

	return nil
}
