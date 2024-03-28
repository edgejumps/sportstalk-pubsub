package pubsub

// Event represents an event that is sent to/from a topic.
// UnifiedPubSub would drop the received data if it cannot be parsed as Event via NewIncomingEvent.
// Users should ensure the data can ben converted to Event via NewOutgoingEvent when publishing.
// Through this way, we can guarantee consumers can always receive and process events conforming to the expected format.
type Event interface {
	// ID returns the identifier of the event.
	// including the topic (where the event is sent to/from) and the entry ID if available.
	ID() EventID

	EventData
}

// EventID represents the identifier of an event.
// It contains the entry ID and the topic.
// EntryID is optional and can be empty.
//
// For Redis PubSub, EntryID is ignored;
// For Kafka, EntryID is used as the message key;
// For Redis Stream, EntryID is used as the stream entry ID (auto-generated if not provided).
type EventID struct {
	EntryID string
	Topic   string
}

type eventImpl struct {
	id *EventID
	EventData
}

func (e *eventImpl) ID() EventID {
	return *e.id
}

// NewIncomingEvent parses the given payload and creates a new incoming event with the given ID.
// The given data must conform to the expected format:
// - []byte -> unmarshal to map[string]interface{}
// - string -> []byte(string) -> unmarshal to map[string]interface{}
// - map[string]interface{} -> must have EventActionKey, and optional EventTTLKey, EventPayloadKey, EventTimestampKey
func NewIncomingEvent(id *EventID, data interface{}) (Event, error) {
	parsed, err := ParseIncomingEventData(data)

	if err != nil {
		return nil, err
	}

	return &eventImpl{
		id:        id,
		EventData: parsed,
	}, nil
}

// NewOutgoingEvent creates a new outgoing event with the given ID, action, TTL, and payload.
// The payload eventually would be converted into string for Redis compatibility:
// - struct/map -> json.Marshal -> []byte -> string
// - []byte -> string
// - string -> string
func NewOutgoingEvent(id *EventID, action string, ttl int, payload interface{}) (Event, error) {

	data, err := NewEventData(action, ttl, payload)

	if err != nil {
		return nil, err
	}

	return &eventImpl{
		id:        id,
		EventData: data,
	}, nil
}
