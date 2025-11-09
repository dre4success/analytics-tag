package main

import (
	events "analytics/src"
	"context"
	"fmt"

	"log"
	"net/http"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/segmentio/kafka-go"
)

func logf(msg string, a ...interface{}) {
	fmt.Printf(msg, a...)
	fmt.Println()
}

func main() {

	kw := &kafka.Writer{
		Addr:        kafka.TCP("localhost:9092"),
		Balancer:    &kafka.LeastBytes{},
		Logger:      kafka.LoggerFunc(logf),
		ErrorLogger: kafka.LoggerFunc(logf),
	}

	defer kw.Close()

	cache := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})

	if _, err := cache.Ping(context.Background()).Result(); err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}

	mux := http.NewServeMux()

	events := events.NewHandler(kw, cache)
	events.RegisterRoutes(mux)

	port := "7070"

	fmt.Printf("listening on port: %s\n", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Error starting server, %v", err.Error())
	}
}
