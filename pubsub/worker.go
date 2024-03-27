package pubsub

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

var (
	ErrWorkerAlreadyStarted = errors.New("worker already started")
)

type Worker interface {
	Run(topics []string, receiver chan<- Event) error
	Stop() SyncPoint
}

type workerImpl struct {
	client *redis.Client

	ctx    context.Context
	cancel context.CancelFunc
}

func NewWorker(c *redis.Client) Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &workerImpl{
		client: c,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (w *workerImpl) Run(topics []string, receiver chan<- Event) error {
	sub := w.client.Subscribe(w.ctx, topics...)

	_, err := sub.Receive(w.ctx)

	if err != nil {
		return err
	}

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

func (w *workerImpl) Stop() SyncPoint {
	w.cancel()
	return SyncPoint{
		Timestamp: time.Now().UnixMilli(),
	}
}

type streamWorkerImpl struct {
	client *redis.Client

	ctx      context.Context
	canceler context.CancelFunc

	point *SyncPoint
}

func NewStreamWorker(c *redis.Client) Worker {
	ctx, canceler := context.WithCancel(context.Background())

	return &streamWorkerImpl{
		client:   c,
		ctx:      ctx,
		canceler: canceler,
	}
}

func (w *streamWorkerImpl) Run(topics []string, receiver chan<- Event) error {

	if w.point != nil {
		return ErrWorkerAlreadyStarted
	}

	w.point = &SyncPoint{
		Timestamp: time.Now().UnixMilli(),
		Offsets:   make(map[string]string),
	}

	go func() {
		defer func() {

			if r := recover(); r != nil {
				fmt.Printf("panic recovered\n")
			}
		}()

		for {
			select {
			case <-w.ctx.Done():
				fmt.Printf("Worker done: %v\n", w)
				return
			default:
				streamMessages, err := w.client.XRead(w.ctx, &redis.XReadArgs{
					Streams: topics,
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

					w.point.Offsets[stream.Stream] = stream.Messages[len(stream.Messages)-1].ID
				}
			}
		}

	}()

	return nil
}

// Stop stops the worker from consuming messages from the topic.
// we will not wait for the worker to stop completely, as we may re-create a new worker to replace it.
// However, we would copy and merge its SyncPoint with pubSubStreamImpl's SyncPoint
func (w *streamWorkerImpl) Stop() SyncPoint {
	w.canceler()
	return SyncPoint{
		Timestamp: time.Now().UnixMilli(),
		Offsets:   w.point.Offsets,
	}
}
