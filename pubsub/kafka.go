package pubsub

import "context"

func NewKafkaPubSub() UnifiedPubSub {
	return &kafkaPubSub{}
}

type kafkaPubSub struct{}

func (ps *kafkaPubSub) Publish(ctx context.Context, event Event) error {
	return nil
}

func (ps *kafkaPubSub) Subscribe(context.Context, ...TargetTopic) (<-chan Event, error) {
	return nil, nil
}

func (ps *kafkaPubSub) DumpSyncPoint(path string) error {
	return nil
}
