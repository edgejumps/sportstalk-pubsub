package pubsub

import "context"

func WithKafka() UnifiedPubSub {
	return &kafkaPubSub{}
}

type kafkaPubSub struct{}

func (ps *kafkaPubSub) Publish(ctx context.Context, event Event) error {
	return nil
}

func (ps *kafkaPubSub) Subscribe(...Topic) error {
	return nil
}

func (ps *kafkaPubSub) Events() <-chan Event {
	return nil
}

func (ps *kafkaPubSub) Errors() <-chan error {
	return nil
}

func (ps *kafkaPubSub) Stop() (SyncPoint, error) {
	return SyncPoint{}, nil
}

func (ps *kafkaPubSub) Topics() []string {
	return nil
}
