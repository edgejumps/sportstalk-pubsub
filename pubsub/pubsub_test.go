package pubsub

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var rdb *redis.Client

func init() {
	rdb = redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "simonwang",
		//ContextTimeoutEnabled: true,
	})

	err := rdb.Ping(context.Background()).Err()

	if err != nil {
		panic(err)
	}
}

func TestRedisStream(t *testing.T) {
	ps := WithStream(rdb)

	action := fmt.Sprintf("/custom-struct-%d", rand.Intn(100))

	event, err := NewOutgoingEvent(&EventID{
		Topic: "data-case-test",
	}, action, 0, customStruct)

	if err != nil {
		t.Errorf("Error creating new event: %v", err)
	}

	err = ps.Publish(context.Background(), event)

	if err != nil {
		t.Errorf("Error publishing message: %v", err)
	}

	err = ps.Subscribe(NewTopic("data-case-test", ""))

	if err != nil {
		t.Errorf("Error subscribing: %v", err)
	}

	for e := range ps.Events() {

		if e.Action() == action {

			s := &CustomStruct{}

			err := e.UnmarshalPayload(s)

			if err != nil {
				t.Errorf("Error deserialize EventData: %v", err)
			}

			err = compareStruct(s)

			if err != nil {
				t.Errorf("Error comparing received data: %v", err)
			}

			break
		}
	}

	ps.Stop()
}

func TestRedisPubSub(t *testing.T) {
	ps := New(rdb)

	action := fmt.Sprintf("/custom-struct-%d", rand.Intn(100))
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {

		defer wg.Done()

		err := ps.Subscribe(NewTopic("pubsub-key", ""))

		if err != nil {
			t.Errorf("Error subscribing: %v", err)
		}

		for e := range ps.Events() {

			if e.Action() == action {
				s := &CustomStruct{}

				err := e.UnmarshalPayload(s)

				if err != nil {
					t.Errorf("Error deserialize EventData: %v", err)
				}

				err = compareStruct(s)

				if err != nil {
					t.Errorf("Error comparing received data: %v", err)
				}
				break
			}
		}
	}()

	time.Sleep(1 * time.Second)

	event, err := NewOutgoingEvent(&EventID{
		Topic: "pubsub-key",
	}, action, 0, customStruct)

	if err != nil {
		t.Errorf("Error creating new event: %v", err)
	}

	err = ps.Publish(context.Background(), event)

	if err != nil {
		t.Errorf("Error publishing message: %v", err)
	}
	wg.Wait()
	ps.Stop()
}

func TestStreamPubSub_MultipleTopics(t *testing.T) {
	ps := WithStream(rdb)

	action := fmt.Sprintf("/custom-struct-%d", rand.Intn(100))

	event, err := NewOutgoingEvent(&EventID{
		Topic: "data-case-test",
	}, action, 0, customStruct)

	if err != nil {
		t.Errorf("Error creating new event: %v", err)
	}

	err = ps.Publish(context.Background(), event)

	if err != nil {
		t.Errorf("Error publishing message: %v", err)
	}

	err = ps.Subscribe(NewTopic("data-case-test-2", ""))

	if err != nil {
		t.Errorf("Error subscribing: %v", err)
	}

	err = ps.Subscribe(NewTopic("data-case-test", ""))

	if err != nil {
		t.Errorf("Error subscribing: %v", err)
	}

	for e := range ps.Events() {

		fmt.Printf("Received event: %v\n", e.ID().Topic)

		if e.Action() == action {

			s := &CustomStruct{}

			err := e.UnmarshalPayload(s)

			if err != nil {
				t.Errorf("Error deserialize EventData: %v", err)
			}

			err = compareStruct(s)

			if err != nil {
				t.Errorf("Error comparing received data: %v", err)
			}

			break
		}
	}

	ps.Stop()
}
