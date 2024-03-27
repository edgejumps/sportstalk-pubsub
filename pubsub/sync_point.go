package pubsub

import (
	"github.com/edgejumps/sportstalk-common-utils/fileloader"
)

type SyncPoint struct {
	// Timestamp is the time when the sync point was created.
	// It is unix milliseconds since epoch
	Timestamp int64 `json:"timestamp"`

	// Offsets is the last read message IDs for each stream.
	// Only used for streams.
	Offsets map[string]string `json:"offsets"`
}

func (s *SyncPoint) AsTopics() []Topic {
	topics := make([]Topic, 0)

	for topic, offset := range s.Offsets {
		topics = append(topics, NewTopic(topic, offset))
	}

	return topics
}

func DumpSyncPoint(path string, point *SyncPoint) error {

	err := fileloader.WriteInto(point, path, true)

	if err != nil {
		return err
	}

	return nil
}

func LoadSyncPoint(path string) (*SyncPoint, error) {

	point := &SyncPoint{}

	err := fileloader.ReadInto(path, point)

	if err != nil {
		return nil, err
	}

	return point, nil
}
