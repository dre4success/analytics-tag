package events

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

type Handler struct {
	kafka *kafka.Writer
}

type Event struct {
	UserID    string `json:"userId"`
	EventType string `json:"eventType"`
	URL       string `json:"url"`
}

func NewHandler(kafka *kafka.Writer) *Handler {
	return &Handler{
		kafka: kafka,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /event", h.handlerCreateEvent)
}

func (h *Handler) handlerCreateEvent(w http.ResponseWriter, r *http.Request) {
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
		Time: time.Now(),
	}); err != nil {
		log.Printf("failed to write message: %v", err)
	}
	w.WriteHeader(http.StatusAccepted)
	log.Printf("Event: %+v", event)
}
