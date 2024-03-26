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
	})

	err := rdb.Ping(context.Background()).Err()

	if err != nil {
		panic(err)
	}
}

func TestRedisStream(t *testing.T) {
	ps := NewPubSubStream(rdb, nil)

	action := fmt.Sprintf("/custom-struct-%d", rand.Intn(100))

	event := NewOutgoingEvent(&EventID{
		Topic: "data-case-test",
	}, action, 0, customStruct)

	err := ps.Publish(context.Background(), event)

	if err != nil {
		t.Errorf("Error publishing message: %v", err)
	}

	events, err := ps.Subscribe(context.Background(), TargetTopic{
		Key: "data-case-test",
	})

	if err != nil {
		t.Errorf("Error subscribing: %v", err)
	}

	for e := range events {

		data, err := e.Data()

		if err != nil {
			continue
		}

		if data.Action() == action {

			s := &CustomStruct{}

			err := data.UnmarshalPayload(s)

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

}

func TestRedisPubSub(t *testing.T) {
	ps := NewPubSub(rdb)

	action := fmt.Sprintf("/custom-struct-%d", rand.Intn(100))
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {

		defer wg.Done()

		events, err := ps.Subscribe(context.Background(), TargetTopic{
			Key: "pubsub-key",
		})

		if err != nil {
			t.Errorf("Error subscribing: %v", err)
		}

		for e := range events {
			data, err := e.Data()

			if err != nil {
				continue
			}

			if data.Action() == action {
				s := &CustomStruct{}

				err := data.UnmarshalPayload(s)

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

	msg := NewOutgoingEvent(&EventID{
		Topic: "pubsub-key",
	}, action, 0, customStruct)

	err := ps.Publish(context.Background(), msg)

	if err != nil {
		t.Errorf("Error publishing message: %v", err)
	}
	wg.Wait()
}
