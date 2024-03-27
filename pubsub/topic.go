package pubsub

// Topic represents a unified topic that can be used in Redis/Kafka.
type Topic interface {
	// Name returns the name of the topic.
	// For redis PubSub, it would be the channel name.
	// For Redis stream, it would be the stream key.
	// For kafka, it would be the topic name.
	Name() string

	// Offset returns the configuration of the topic.
	// For redis PubSub, it would be ignored.
	// For Redis stream, it may return the last delivered ID (string).
	// For kafka, it is the consumer group ID (so that it can be used to resume the last committed offset in this group).
	Offset() string

	// SyncOffset updates the offset of the topic.
	// For redis PubSub, it would be ignored.
	// For Redis stream, it may update the last delivered ID.
	// For kafka, it is the consumer group ID (so that it can be used to resume the last committed offset in this group).
	SyncOffset(offset string)
}

type topicImpl struct {
	name   string
	offset string
}

func (t *topicImpl) Name() string {
	return t.name
}

func (t *topicImpl) Offset() string {
	return t.offset
}

func (t *topicImpl) SyncOffset(offset string) {
	t.offset = offset
}

// NewTopic creates a new topic with the given name and offset.
// For Redis PubSub, the offset is ignored.
// For Redis Stream, the offset should be the last delivered ID; if empty, it will be the first entry (MinimumID).
// For Kafka, the offset should be the consumer group ID, as Kafka will store the last committed offset for a consumer group (
// with this design, we would only support one consumer for a consumer group in a Worker).
// Therefore, it is important to provide an acceptable offset for the topic when using different pub/sub systems.
func NewTopic(name, offset string) Topic {
	return &topicImpl{
		name:   name,
		offset: offset,
	}
}
