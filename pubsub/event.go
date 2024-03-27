package pubsub

// Event represents a message that is sent to/from a topic.
// It contains an ID and a payload.
type Event interface {
	// ID returns the identifier of the event.
	// including the topic (where the event is sent to/from) and the entry ID if available.
	ID() EventID

	// Data returns the payload of the event.
	// It returns an error if the payload format is unsupported/invalid.
	Data() (EventData, error)
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

type incomingEvent struct {
	id      *EventID
	payload interface{}
}

func (e *incomingEvent) ID() EventID {
	return *e.id
}

func (e *incomingEvent) Data() (EventData, error) {
	return ParseIncomingEventData(e.payload)
}

// NewIncomingEvent creates a new incoming event with the given ID and payload.
// It will build EventData via ParseIncomingEventData.
func NewIncomingEvent(id *EventID, payload interface{}) Event {
	return &incomingEvent{
		id:      id,
		payload: payload,
	}
}

type outgoingEvent struct {
	id *EventID

	ttl     int
	action  string
	payload interface{}
}

func (e *outgoingEvent) ID() EventID {
	return *e.id
}

func (e *outgoingEvent) Data() (EventData, error) {
	return NewEventData(e.action, e.ttl, e.payload)
}

// NewOutgoingEvent creates a new outgoing event with the given ID, action, TTL, and payload.
// It will build EventData via NewEventData.
// The given payload must be serializable, like map[string]interface{}, []byte, string, or custom struct that can be marshaled into bytes
func NewOutgoingEvent(id *EventID, action string, ttl int, payload interface{}) Event {
	return &outgoingEvent{
		id:      id,
		action:  action,
		ttl:     ttl,
		payload: payload,
	}
}
