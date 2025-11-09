package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type Event struct {
	UserID    string `json:"userId"`
	EventType string `json:"eventType"`
	URL       string `json:"url"`
}

type ReqStats struct {
	successful int64
	failed     int64
}

func main() {
	const (
		url         = "http://localhost:7070/event"
		numRequests = 220
	)

	fmt.Printf("Sending %d requests simultaneously...\n\n", numRequests)

	var wg sync.WaitGroup
	start := time.Now()

	var statsChan = make(chan ReqStats, numRequests)

	for i := range numRequests {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sendRequest(url, id, statsChan)
		}(i)
	}

	// close channel when all goroutines finish
	go func() {
		wg.Wait()
		close(statsChan)
	}()

	var totalStats ReqStats

	for stat := range statsChan {
		totalStats.successful += stat.successful
		totalStats.failed += stat.failed
	}

	elapsed := time.Since(start)

	fmt.Printf("\n✅ Sent %d requests in %v\n", numRequests, elapsed)
	fmt.Printf("✅ Successful requests: %d\n", totalStats.successful)
	fmt.Printf("❌ Failed requests: %d\n", totalStats.failed)
	fmt.Printf("✅ Throughput: %.2f req/s\n", float64(numRequests)/elapsed.Seconds())
}

func sendRequest(url string, id int, statsChan chan ReqStats) {
	event := Event{
		UserID:    fmt.Sprintf("user_%d", rand.Intn(1000)),
		EventType: randomEventType(),
		URL:       randomUrl(),
	}

	body, _ := json.Marshal(event)

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("x Request %d failed: %v\n", id, err)
		statsChan <- ReqStats{
			successful: 0,
			failed:     1,
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusAccepted {
		fmt.Printf("✅ Request %d: %s\n", id, resp.Status)
		statsChan <- ReqStats{
			successful: 1,
			failed:     0,
		}
	} else {
		fmt.Printf("❌ Request %d: %s\n", id, resp.Status)
		statsChan <- ReqStats{
			successful: 0,
			failed:     1,
		}
	}
}

func randomEventType() string {
	types := []string{"click", "view", "submit", "scroll", "hover"}
	return types[rand.Intn(len(types))]
}

func randomUrl() string {
	pages := []string{"/home", "/about", "/product", "/contact", "/pricing", "/blog"}
	return pages[rand.Intn(len(pages))]
}
