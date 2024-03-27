package main

import (
	"context"
	"fmt"
	"github.com/edgejumps/sportstalk-common-utils/redisfactory"
	"github.com/edgejumps/sportstalk-pubsub/pubsub"
	"github.com/redis/go-redis/v9"
	"os"
	"os/signal"
	"time"
)

func main() {

	rdb, err := redisfactory.RedisClientFactory(context.Background(), &redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "simonwang",
	})

	if err != nil {
		fmt.Printf("Error creating redis client: %v\n", err)
		return
	}

	defer rdb.Close()

	s := make(chan struct{})

	go func() {
		sema := make(chan os.Signal)

		signal.Notify(sema, os.Interrupt, os.Kill)

		<-sema
		s <- struct{}{}
	}()

	testRedisPubSub(rdb, s)
}

// No need to care about the SyncPoint after stopping a Redis PubSub,
// as it fires and forgets the delivered message, there is no way to persistent messages in Redis PubSub
func testRedisPubSub(rdb *redis.Client, s <-chan struct{}) {

	ps := pubsub.New(rdb)

	go func() {
		err := ps.Subscribe(pubsub.NewTopic("test", ""))

		if err != nil {
			fmt.Printf("Error subscribing to topic [%s]: %v\n", "test", err)
		}
	}()

	go func() {
		err := ps.Subscribe(pubsub.NewTopic("test-2", ""))

		if err != nil {
			fmt.Printf("Error subscribing to topic [%s]: %v\n", "test-2", err)
		}
	}()

	go func() {
		events := ps.Events()

		for event := range events {
			data, err := event.Data()

			if err != nil {
				fmt.Printf("Error getting data: %v\n", err)
				continue
			}

			fmt.Printf("---------")
			fmt.Printf("ID: %s\n", event.ID())
			fmt.Printf("Action: %+v\n", data.Action())
			fmt.Printf("Data: %+v\n", data.RawPayload())
		}
	}()

	<-s

	_, err := ps.Stop()

	if err != nil {
		fmt.Printf("Error stopping worker: %v\n", err)
	}

}

func testRedisStream(rdb *redis.Client, s <-chan struct{}) {
	path := "./test.json"

	ps := pubsub.WithStream(rdb)

	point, err := pubsub.LoadSyncPoint(path)

	if err != nil {
		point = &pubsub.SyncPoint{
			Offsets: map[string]string{
				"stream-test": "0",
			},
		}
	}

	topics := point.AsTopics()

	err = ps.Subscribe(topics...)

	if err != nil {
		fmt.Printf("Error subscribing to topics: %v\n", err)
	}

	go func() {
		events := ps.Events()

		for event := range events {
			data, err := event.Data()

			if err != nil {
				fmt.Printf("Error getting data: %v\n", err)
				continue
			}

			fmt.Printf("---------")
			fmt.Printf("ID: %s\n", event.ID())
			fmt.Printf("Action: %+v\n", data.Action())
			fmt.Printf("Data: %+v\n", data.RawPayload())
		}
	}()

	go func() {
		time.Sleep(5 * time.Second)
		err := ps.Subscribe(pubsub.NewTopic("data-case-test", ""))

		if err != nil {
			fmt.Printf("Error subscribing to topic [%s]: %v\n", "stream-test-2", err)
		}
	}()

	<-s

	lastPoint, err := ps.Stop()

	if err != nil {
		fmt.Printf("Error stopping worker: %v\n", err)
	}

	err = pubsub.DumpSyncPoint(path, &lastPoint)

	if err != nil {
		fmt.Printf("Error dumping sync point: %v\n", err)
	} else {
		fmt.Printf("Sync point dumped\n")
	}
}
