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

// NewRedisTopic creates a new Redis topic with the given name.
// The offset is ignored in Redis PubSub.
// The offset defaults to the minimum ID in Redis stream; however, it may be reset according to the local SyncPoint
func NewRedisTopic(name string) Topic {
	return &topicImpl{
		name:   name,
		offset: string(MinimumID),
	}
}

// NewKafkaTopic creates a new Kafka topic with the given name and group ID.
// We recommend assigning a group ID to the consumer so that it can resume the last committed offset in this group.
// Otherwise, it will always start consuming from the last stored offset.
func NewKafkaTopic(name string, groupID string) Topic {
	return &topicImpl{
		name:   name,
		offset: groupID,
	}
}
