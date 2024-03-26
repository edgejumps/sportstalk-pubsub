package pubsub

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"sync"
)

var (
	ErrNoEntryID = errors.New("no entry id")
)

type pubSubStreamImpl struct {
	client *redis.Client

	eventChan chan Event

	topics map[string]TargetTopic

	mu    *sync.Mutex
	point *SyncPoint

	pointPath string

	worker Worker
}

func NewPubSubStream(c *redis.Client, pointPath string) UnifiedPubSub {

	point, err := LoadSyncPoint(pointPath)

	if err != nil {
		point = &SyncPoint{
			Offsets: make(map[string]string),
		}
	}

	return &pubSubStreamImpl{
		client:    c,
		point:     point,
		pointPath: pointPath,
		eventChan: make(chan Event),
		topics:    make(map[string]TargetTopic),
		mu:        &sync.Mutex{},
	}
}

// Publish publishes a message to a topic
func (ps *pubSubStreamImpl) Publish(context context.Context, event Event) error {

	data, err := event.Data()

	if err != nil {
		return err
	}

	id := event.ID()

	if id.EntryID == "" {
		id.EntryID = string(AutoGeneratedID)
	}

	jsonData := make(map[string]interface{})

	err = data.NormalizeInto(&jsonData)

	if err != nil {
		return err
	}

	entryID, err := ps.client.XAdd(context, &redis.XAddArgs{
		Stream:     id.Topic,
		NoMkStream: true,
		Approx:     true,
		ID:         id.EntryID,
		Values:     jsonData,
	}).Result()

	if err != nil {
		return err
	}

	if entryID == "" {
		return ErrNoEntryID
	}

	return nil
}

func (ps *pubSubStreamImpl) Subscribe(topics ...TargetTopic) error {

	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.worker != nil {
		oldWorker := ps.worker
		ps.point.Merge(oldWorker.Stop())
		ps.worker = nil
	}

	for _, topic := range topics {
		ps.topics[topic.Key] = topic
	}

	ps.worker = NewStreamWorker(ps.client)

	return ps.worker.Run(ps.collectTopics(), ps.eventChan)

}

func (ps *pubSubStreamImpl) Events() <-chan Event {
	return ps.eventChan
}

func (ps *pubSubStreamImpl) Errors() <-chan error {
	return nil
}

func (ps *pubSubStreamImpl) Topics() []TargetTopic {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	topics := make([]TargetTopic, 0)

	for _, topic := range ps.topics {
		topics = append(topics, topic)
	}

	return topics
}

func (ps *pubSubStreamImpl) Stop() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.worker == nil {
		return nil
	}

	defer func() {
		ps.worker = nil
		close(ps.eventChan)
	}()

	fmt.Printf("Stopping StreamPubSub: %v\n", ps.worker)

	ps.point.Merge(ps.worker.Stop())

	if ps.pointPath == "" {
		return nil
	}

	return DumpSyncPoint(ps.pointPath, ps.point)
}

// collectTopics collects the topics and their last read offset.
// it would return a slice of strings where the first half is the topics
// and the second half is the last read offset for each topic.
func (ps *pubSubStreamImpl) collectTopics() []string {
	topics := make([]string, 0)
	ids := make([]string, 0)

	for _, topic := range ps.topics {
		v, ok := ps.point.Offsets[topic.Key]

		if !ok {
			v = string(MinimumID)
		}

		topics = append(topics, topic.Key)
		ids = append(ids, v)
	}

	return append(topics, ids...)
}
