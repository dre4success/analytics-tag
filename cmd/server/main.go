package main

import (
	"fmt"
	events "learn-go-with-tests/gothings/analytics-tag/src"
	"log"
	"net/http"
)

func main() {
	mux := http.NewServeMux()

	events := events.NewHandler()
	events.RegisterRoutes(mux)

	port := "7070"

	fmt.Printf("listening on port: %s\n", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Error starting server, %v", err.Error())
	}
}
