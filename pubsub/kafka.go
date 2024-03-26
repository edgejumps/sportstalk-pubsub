package pubsub

import "context"

func NewKafkaPubSub() UnifiedPubSub {
	return &kafkaPubSub{}
}

type kafkaPubSub struct{}

func (ps *kafkaPubSub) Publish(ctx context.Context, event Event) error {
	return nil
}

func (ps *kafkaPubSub) Subscribe(...TargetTopic) error {
	return nil
}

func (ps *kafkaPubSub) Events() <-chan Event {
	return nil
}

func (ps *kafkaPubSub) Errors() <-chan error {
	return nil
}

func (ps *kafkaPubSub) Stop() error {
	return nil
}

func (ps *kafkaPubSub) Topics() []TargetTopic {
	return nil
}
