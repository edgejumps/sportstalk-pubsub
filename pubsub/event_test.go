package pubsub

import (
	"fmt"
	"testing"
	"time"
)

type NestedStruct struct {
	Number int                    `json:"nested_number"`
	Data   map[string]interface{} `json:"nested_data"`
}

type CustomStruct struct {
	Name   string       `json:"name"`
	Age    int          `json:"age"`
	Nested NestedStruct `json:"nested"`
}

var (
	customStruct = CustomStruct{
		Name: "John",
		Age:  30,
		Nested: NestedStruct{
			Number: 1,
			Data: map[string]interface{}{
				"key1": "value1",
			},
		},
	}
	customStructString = `{"name":"John","age":30,"nested":{"nested_number":1, "nested_data":{"key1":"value1"}}}`

	customSturctBytes = []byte(customStructString)
)

func TestParseIncomingEventData(t *testing.T) {
	timestamp := time.Now().Unix()

	jsonMap := map[string]interface{}{
		"action":    "custom",
		"ttl":       1,
		"timestamp": fmt.Sprintf("%d", timestamp),
		"payload":   customStructString,
	}

	eventData, err := ParseIncomingEventData(jsonMap)

	if err != nil {
		t.Errorf("Error parsing incoming event data: %v", err)
	}

	if eventData.Action() != "/custom" {
		t.Errorf("Expected action to be 'custom', got '%s'", eventData.Action())
	}

	if eventData.TTL() != 1 {
		t.Errorf("Expected TTL to be 1, got %d", eventData.TTL())
	}

	if eventData.Timestamp() != timestamp {
		t.Errorf("Expected timestamp to be %d, got %d", timestamp, eventData.Timestamp())
	}

	customStruct := CustomStruct{}

	err = eventData.UnmarshalPayload(&customStruct)

	if err != nil {
		t.Errorf("Error unmarshaling payload: %v", err)
	}

	err = compareStruct(&customStruct)

	if err != nil {
		t.Errorf("Error comparing received data: %v", err)
	}

}

func TestNewEventData_EmptyPayload(t *testing.T) {
	eventData, err := NewEventData("/test", 0, nil)

	if err != nil {
		t.Errorf("Error creating new event data: %v", err)
	}

	if eventData.Action() != "/test" {
		t.Errorf("Expected action to be '/test', got '%s'", eventData.Action())
	}

	if eventData.TTL() != 0 {
		t.Errorf("Expected TTL to be 0, got %d", eventData.TTL())
	}

	target := make(map[string]interface{})

	err = eventData.NormalizeInto(&target)

	if err != nil {
		t.Errorf("Error normalizing data: %v", err)
	}

	if target[EventActionKey] != "/test" {
		t.Errorf("Expected action to be '/test', got '%s'", target[EventActionKey])
	}

	if target[EventTTLKey] != 0 {
		t.Errorf("Expected TTL to be 0, got %d", target[EventTTLKey])
	}

	_, ok := target[EventPayloadKey]

	if ok {
		t.Errorf("Expected payload to be nil, got %v", target[EventPayloadKey])
	}

}

func TestNewEventData_CustomStruct(t *testing.T) {
	eventData, err := NewEventData("/test", 0, customStruct)

	if err != nil {
		t.Errorf("Error creating new event data: %v", err)
	}

	if eventData.Action() != "/test" {
		t.Errorf("Expected action to be '/test', got '%s'", eventData.Action())
	}

	if eventData.TTL() != 0 {
		t.Errorf("Expected TTL to be 0, got %d", eventData.TTL())
	}

	customStruct2 := CustomStruct{}

	err = eventData.UnmarshalPayload(&customStruct2)

	if err != nil {
		t.Errorf("Error unmarshaling payload: %v", err)
	}

	err = compareStruct(&customStruct2)

	if err != nil {
		t.Errorf("Error comparing received data: %v", err)
	}
}

func TestNewEventData_CustomString(t *testing.T) {
	eventData, err := NewEventData("/test", 0, customStructString)

	if err != nil {
		t.Errorf("Error creating new event data: %v", err)
	}

	if eventData.Action() != "/test" {
		t.Errorf("Expected action to be '/test', got '%s'", eventData.Action())
	}

	if eventData.TTL() != 0 {
		t.Errorf("Expected TTL to be 0, got %d", eventData.TTL())
	}

	customStruct2 := CustomStruct{}

	err = eventData.UnmarshalPayload(&customStruct2)

	if err != nil {
		t.Errorf("Error unmarshaling payload: %v", err)
	}

	err = compareStruct(&customStruct2)

	if err != nil {
		t.Errorf("Error comparing received data: %v", err)
	}

}

func TestOutgoingEvent_CustomStruct(t *testing.T) {

	event := NewOutgoingEvent(&EventID{
		Topic: "stream-key",
	}, "custom", 1, customStruct)

	if event.ID().EntryID != "" {
		t.Errorf("Expected entry ID to be empty, got '%s'", event.ID().EntryID)
	}

	data, err := event.Data()

	if err != nil {
		t.Errorf("Error marshaling value from message: %v", err)
	}

	if data.Action() != "/custom" {
		t.Errorf("Expected action to be 'custom', got '%s'", data.Action())
	}

	if data.TTL() != 1 {
		t.Errorf("Expected TTL to be 0, got %d", data.TTL())
	}

	customStruct2 := CustomStruct{}

	err = data.UnmarshalPayload(&customStruct2)

	if err != nil {
		t.Errorf("Error unmarshaling payload: %v", err)
	}

	err = compareStruct(&customStruct2)

	if err != nil {
		t.Errorf("Error comparing received data: %v", err)
	}
}

func TestOutgoingEvent_CustomBytes(t *testing.T) {

	event := NewOutgoingEvent(&EventID{
		Topic: "stream-key",
	}, "custom", 0, customSturctBytes)

	if event.ID().EntryID != "" {
		t.Errorf("Expected entry ID to be empty, got '%s'", event.ID().EntryID)
	}

	data, err := event.Data()

	if err != nil {
		t.Errorf("Error marshaling value from message: %v", err)
	}

	if data.Action() != "/custom" {
		t.Errorf("Expected action to be 'custom', got '%s'", data.Action())
	}

	if data.TTL() != 0 {
		t.Errorf("Expected TTL to be 0, got %d", data.TTL())
	}

	jsonData := make(map[string]interface{})

	err = data.UnmarshalPayload(&jsonData)

	if err != nil {
		t.Errorf("Error unmarshaling data: %v", err)
	}

	err = compareJson(jsonData)

	if err != nil {
		t.Errorf("Error comparing received data: %v", err)
	}
}

func TestIncomingEvent_CustomBytes(t *testing.T) {
	jsonMap := map[string]interface{}{
		"action":  "custom",
		"ttl":     1,
		"payload": customSturctBytes,
	}

	event := NewIncomingEvent(&EventID{
		Topic:   "stream-key",
		EntryID: "1",
	}, jsonMap)

	data, err := event.Data()

	if err != nil {
		t.Errorf("Error marshaling value from payload: %v", err)
	}

	if data.Action() != "/custom" {
		t.Errorf("Expected action to be 'custom', got '%s'", data.Action())
	}

	if data.TTL() != 1 {
		t.Errorf("Expected TTL to be 1, got %d", data.TTL())
	}

	customStruct := CustomStruct{}

	err = data.UnmarshalPayload(&customStruct)

	if err != nil {
		t.Errorf("Error unmarshaling payload: %v", err)
	}

	err = compareStruct(&customStruct)

	if err != nil {
		t.Errorf("Error comparing received data: %v", err)
	}
}

func TestIncomingEvent_CustomString(t *testing.T) {
	jsonMap := map[string]interface{}{
		"action":  "custom",
		"ttl":     1,
		"payload": customStructString,
	}

	event := NewIncomingEvent(&EventID{
		Topic:   "stream-key",
		EntryID: "1",
	}, jsonMap)

	data, err := event.Data()

	if err != nil {
		t.Errorf("Error marshaling value from payload: %v", err)
	}

	if data.Action() != "/custom" {
		t.Errorf("Expected action to be 'custom', got '%s'", data.Action())
	}

	if data.TTL() != 1 {
		t.Errorf("Expected TTL to be 1, got %d", data.TTL())
	}

	customStruct := CustomStruct{}

	err = data.UnmarshalPayload(&customStruct)

	if err != nil {
		t.Errorf("Error unmarshaling payload: %v", err)
	}

	err = compareStruct(&customStruct)

	if err != nil {
		t.Errorf("Error comparing received data: %v", err)
	}

}

func compareStruct(s *CustomStruct) error {
	if s.Name != customStruct.Name {
		return fmt.Errorf("expected name to be 'John', got '%s'", s.Name)
	}

	if s.Age != customStruct.Age {
		return fmt.Errorf("expected age to be 30, got '%d'", s.Age)
	}

	if s.Nested.Number != customStruct.Nested.Number {
		return fmt.Errorf("expected nested number to be 1, got '%d'", s.Nested.Number)
	}

	if s.Nested.Data["key1"] != customStruct.Nested.Data["key1"] {
		return fmt.Errorf("expected nested data key1 to be 'value1', got '%s'", s.Nested.Data["key1"])
	}

	return nil
}

func compareJson(s map[string]interface{}) error {
	if s["name"] != "John" {
		return fmt.Errorf("expected name to be 'John', got '%s'", s["name"])
	}

	if s["age"] != float64(30) {
		return fmt.Errorf("expected age to be 30, got '%d'", s["age"])
	}

	nested, ok := s["nested"].(map[string]interface{})

	if !ok {
		return fmt.Errorf("expected nested_data to be a map, got %T", s["nested"])
	}

	if nested["nested_number"] != float64(1) {
		return fmt.Errorf("expected nested number to be 1, got '%d'", nested["nested_number"])
	}

	nestedData, ok := nested["nested_data"].(map[string]interface{})

	if !ok {
		return fmt.Errorf("expected nested_data to be a map, got %T", nested["nested_data"])
	}

	if nestedData["key1"] != "value1" {
		return fmt.Errorf("expected nested data key1 to be 'value1', got '%s'", nestedData["key1"])
	}

	return nil
}
