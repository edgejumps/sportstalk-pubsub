package pubsub

import (
	"encoding/json"
	"github.com/edgejumps/sportstalk-common-utils/logger"
	"time"
)

// EventData represents the data associated with an event.
// It contains the action, time-to-live (TTL), and the payload.
// The payload can be of type map[string]interface{}, []byte, or string.
type EventData interface {

	// Action returns the action associated with the event.
	// The action is a string that represents the type of event.
	// e.g., /user/created, /user/updated, /user/deleted
	Action() string

	// TTL returns the time-to-live (TTL) associated with the event.
	// The TTL is an integer that represents the duration of the event.
	// the receiver will not try to forward an event if TTL <= MinimumTTL
	TTL() int

	// Timestamp returns the timestamp associated with the event.
	// The timestamp is an integer that represents the time when the event was created.
	// It is unix milliseconds since epoch.
	Timestamp() int64

	// UnmarshalPayload deserializes the RawPayload into the target.
	// The target must be a pointer to a struct, similar to json.Unmarshal.
	// Typically, it is used for deserializing the payload into a struct.
	UnmarshalPayload(target interface{}) error

	// NormalizeInto normalizes the event data into the target.
	// The target can be of type *map[string]interface{}, *[]byte, or *string;
	// otherwise, it will return ErrUnsupportedNormalizeTarget.
	// Typically, it is used for normalizing the event data and then publishing it.
	// It will construct the interval as
	// ```
	// {
	// 	"action": "/event/action",
	// 	"ttl": 1,
	// 	"timestamp": 1630000000000,
	// 	"payload": <raw payload>
	// }
	// ```
	// then, marshal it into the target.
	// e.g., Redis Pub/Sub stores the value as string.
	// Redis Stream stores the value as map[string]interface{}.
	// kafka stores the value as []byte.
	NormalizeInto(target interface{}) error

	// RawPayload returns the payload associated with the event.
	// The payload can be of type map[string]interface{}, []byte, or string.
	RawPayload() interface{}
}

type baseEventData struct {
	action    string
	ttl       int
	timestamp int64
}

func (e *baseEventData) Action() string {
	return NormalizeActionPath(e.action)
}

func (e *baseEventData) TTL() int {
	return e.ttl
}

func (e *baseEventData) Timestamp() int64 {
	return e.timestamp
}

func (e *baseEventData) format(payload interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	result[EventActionKey] = e.action
	result[EventTTLKey] = e.ttl
	result[EventTimestampKey] = e.timestamp

	if payload != nil {

		parsed := ""

		switch payload := payload.(type) {
		case []byte:
			parsed = string(payload)
		case string:
			parsed = payload
		default:
			bytes, err := json.Marshal(payload)

			if err != nil {
				return nil, err
			}

			parsed = string(bytes)
		}

		if parsed != "" {
			result[EventPayloadKey] = parsed
		}
	}

	return result, nil
}

func (e *baseEventData) normalize(target interface{}, payload interface{}) error {
	jsonData, err := e.format(payload)

	if err != nil {
		return err
	}

	switch target := target.(type) {
	case *map[string]interface{}:
		*target = jsonData
	case *string:
		bytes, err := json.Marshal(jsonData)

		if err != nil {
			return err
		}

		*target = string(bytes)

	case *[]byte:
		bytes, err := json.Marshal(jsonData)

		if err != nil {
			return err
		}

		*target = bytes
	default:
		return ErrUnsupportedNormalizeTarget
	}

	return nil
}

type jsonEventData struct {
	baseEventData
	payload map[string]interface{}
}

func (e *jsonEventData) UnmarshalPayload(target interface{}) error {
	bytes, err := json.Marshal(e.payload)

	if err != nil {
		return err
	}

	return json.Unmarshal(bytes, target)
}

func (e *jsonEventData) NormalizeInto(target interface{}) error {
	return e.baseEventData.normalize(target, e.payload)
}

func (e *jsonEventData) RawPayload() interface{} {
	return e.payload
}

type binaryEventData struct {
	baseEventData
	payload []byte
}

func (e *binaryEventData) UnmarshalPayload(target interface{}) error {
	return json.Unmarshal(e.payload, target)
}

func (e *binaryEventData) NormalizeInto(target interface{}) error {
	return e.baseEventData.normalize(target, e.payload)
}

func (e *binaryEventData) RawPayload() interface{} {
	return e.payload
}

type stringEventData struct {
	baseEventData
	payload string
}

func (e *stringEventData) UnmarshalPayload(target interface{}) error {
	return json.Unmarshal([]byte(e.payload), target)
}

func (e *stringEventData) NormalizeInto(target interface{}) error {
	return e.baseEventData.normalize(target, e.payload)
}

func (e *stringEventData) RawPayload() interface{} {
	return e.payload
}

// NewEventData creates a new event data with the given action, TTL, and payload.
// The payload eventually would be converted into string for Redis compatibility:
// - struct/map -> json.Marshal -> []byte -> string
// - []byte -> string
// - string -> string
func NewEventData(action string, ttl int, payload interface{}) (EventData, error) {
	return buildEventData(action, ttl, time.Now().UnixMilli(), payload)
}

// ParseIncomingEventData parses the incoming data into an EventData.
// The given data must conform to the expected format:
// - []byte -> unmarshal to map[string]interface{}
// - string -> []byte(string) -> unmarshal to map[string]interface{}
// - map[string]interface{} -> must have EventActionKey, and optional EventTTLKey, EventPayloadKey, EventTimestampKey
func ParseIncomingEventData(data interface{}) (EventData, error) {

	jsonMap := make(map[string]interface{})

	switch v := data.(type) {
	case map[string]interface{}:
		jsonMap = v
	case string:
		err := json.Unmarshal([]byte(v), &jsonMap)
		if err != nil {
			return nil, err
		}

	case []byte:
		err := json.Unmarshal(v, &jsonMap)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrUnsupportedEventPayload
	}

	action, ok := jsonMap[EventActionKey].(string)

	if !ok {
		return nil, ErrUnknownEventAction
	}

	ttl := NormalizeTTL(jsonMap[EventTTLKey])

	payload, ok := jsonMap[EventPayloadKey]

	if !ok {
		logger.Info("No data associated with event")
	}

	timestamp := ParseTimestamp(jsonMap[EventTimestampKey])

	return buildEventData(action, ttl, timestamp, payload)
}

func buildEventData(action string, ttl int, timestamp int64, payload interface{}) (EventData, error) {
	base := baseEventData{
		action:    NormalizeActionPath(action),
		ttl:       NormalizeTTL(ttl),
		timestamp: timestamp,
	}

	if payload == nil {
		return &binaryEventData{
			baseEventData: base,
		}, nil
	}

	switch payload := payload.(type) {
	case map[string]interface{}:
		return &jsonEventData{
			baseEventData: base,
			payload:       payload,
		}, nil
	case []byte:
		return &binaryEventData{
			baseEventData: base,
			payload:       payload,
		}, nil
	case string:
		return &stringEventData{
			baseEventData: base,
			payload:       payload,
		}, nil
	default:
		bytes, err := json.Marshal(payload)

		if err != nil {
			return nil, err
		}
		return &binaryEventData{
			baseEventData: base,
			payload:       bytes,
		}, nil
	}
}
