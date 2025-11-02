package main

import (
	"fmt"
	events "learn-go-with-tests/gothings/analytics-tag/src"
	"log"
	"net/http"

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
	mux := http.NewServeMux()

	events := events.NewHandler(kw)
	events.RegisterRoutes(mux)

	port := "7070"

	fmt.Printf("listening on port: %s\n", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Error starting server, %v", err.Error())
	}
}
