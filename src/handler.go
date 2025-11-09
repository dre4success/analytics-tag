package events

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type Handler struct {
	kafka *kafka.Writer
	cache *redis.Client
}

type Event struct {
	UserID    string `json:"userId"`
	EventType string `json:"eventType"`
	URL       string `json:"url"`
}

func NewHandler(kafka *kafka.Writer, cache *redis.Client) *Handler {
	return &Handler{
		kafka: kafka,
		cache: cache,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /event", h.handlerCreateEvent)
}

func (h *Handler) handlerCreateEvent(w http.ResponseWriter, r *http.Request) {

	ip, port, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		log.Printf("Error parsing remote addr: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	log.Printf("ip: %s, port: %s\n", ip, port)

	key := "rate-limit" + ip
	const limit = 200 // 100 requests
	const window = 60 * time.Second

	ctx := r.Context()
	pipe := h.cache.TxPipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, window) // set/refresh the 60s window
	_, err = pipe.Exec(ctx)

	if err != nil {
		log.Printf("Error executing rate-limiter pipeline: %v", err)
	} else {
		count, err := incr.Result()
		log.Println("cache count: ", count)
		if err != nil {
			log.Printf("Error getting rate-limiter count: %v", err)
		}

		if count > limit {
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
	}

	var event Event

	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		log.Printf("invalid request: %v", err.Error())
		return
	}

	val, err := json.Marshal(event)
	if err != nil {
		log.Printf("unable to marshal json event: %v", err.Error())
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if err := h.kafka.WriteMessages(r.Context(), kafka.Message{
		Topic: "events",
		Key:   []byte(event.UserID),
		Value: val,
		Time:  time.Now(),
	}); err != nil {
		log.Printf("failed to write message: %v", err)
	}
	w.WriteHeader(http.StatusAccepted)
	log.Printf("Event: %+v", event)
}
