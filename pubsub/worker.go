package pubsub

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
)

var (
	ErrWorkerAlreadyStarted = errors.New("worker already started")
)

// Worker is an interface that defines the behavior of a worker that consumes messages from a topic.
// For Redis Stream worker, it will normalize the topic's offset to the latest message ID if not set.
// For Redis PubSub worker, it will subscribe to the topic directly without caring the offset.
// For Kafka worker, it will subscribe to the topic with the offset (groupID) provided.
type Worker interface {
	Run(topics []Topic, receiver chan<- Event) error
	Stop()
}

type workerImpl struct {
	client *redis.Client

	ctx    context.Context
	cancel context.CancelFunc

	rpb *redis.PubSub
}

func NewWorker(c *redis.Client) Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &workerImpl{
		client: c,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (w *workerImpl) Run(topics []Topic, receiver chan<- Event) error {

	if w.rpb != nil {
		return ErrWorkerAlreadyStarted
	}

	channels := make([]string, 0)

	for _, topic := range topics {
		channels = append(channels, topic.Name())
	}

	sub := w.client.Subscribe(w.ctx, channels...)

	_, err := sub.Receive(w.ctx)

	if err != nil {
		return err
	}

	w.rpb = sub

	ch := sub.Channel()

	go func() {

		for {
			select {
			case <-w.ctx.Done():
				return
			case msg := <-ch:
				event := NewIncomingEvent(&EventID{
					Topic: msg.Channel,
				}, msg.Payload)

				receiver <- event
			}
		}

	}()

	return nil
}

func (w *workerImpl) Stop() {
	if w.rpb != nil {
		_ = w.rpb.Close()
		w.rpb = nil
	}

	w.cancel()
}

type streamWorkerImpl struct {
	client *redis.Client

	ctx      context.Context
	canceler context.CancelFunc

	topics map[string]Topic
}

func NewStreamWorker(c *redis.Client) Worker {
	ctx, canceler := context.WithCancel(context.Background())

	return &streamWorkerImpl{
		client:   c,
		ctx:      ctx,
		canceler: canceler,
		topics:   make(map[string]Topic),
	}
}

func (w *streamWorkerImpl) Run(topics []Topic, receiver chan<- Event) error {

	if len(w.topics) > 0 {
		return ErrWorkerAlreadyStarted
	}

	for _, topic := range topics {
		w.topics[topic.Name()] = topic
	}

	go func() {
		defer func() {

			if r := recover(); r != nil {
				fmt.Printf("panic recovered\n")
			}
		}()

		for {

			keys := make([]string, 0)
			ids := make([]string, 0)

			for _, topic := range w.topics {
				keys = append(keys, topic.Name())

				offset := topic.Offset()

				if offset == "" {
					offset = string(MinimumID)
				}

				ids = append(ids, offset)
			}

			select {
			case <-w.ctx.Done():
				fmt.Printf("Worker done: %v\n", w)
				return
			default:
				streamMessages, err := w.client.XRead(w.ctx, &redis.XReadArgs{
					Streams: append(keys, ids...),
				}).Result()

				if err != nil {
					fmt.Printf("Error reading stream: %v\n", err)
					return
				}

				for _, stream := range streamMessages {
					for _, msg := range stream.Messages {
						event := NewIncomingEvent(&EventID{
							Topic:   stream.Stream,
							EntryID: msg.ID,
						}, msg.Values)

						receiver <- event
					}

					if topic, ok := w.topics[stream.Stream]; ok {
						topic.SyncOffset(stream.Messages[len(stream.Messages)-1].ID)
					}
				}
			}
		}

	}()

	return nil
}

// Stop stops the worker from consuming messages from the topic.
// we will not wait for the worker to stop completely, as we may re-create a new worker to replace it.
// However, we would copy and merge its SyncPoint with pubSubStreamImpl's SyncPoint
func (w *streamWorkerImpl) Stop() {
	w.canceler()
}
