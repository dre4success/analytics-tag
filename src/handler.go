package events

import (
	"encoding/json"
	"log"
	"net/http"
)

type Handler struct{}

type Event struct {
	UserID    string `json:"userId"`
	EventType string `json:"eventType"`
	URL       string `json:"url"`
}

func NewHandler() *Handler {
	return &Handler{}
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

	w.WriteHeader(http.StatusAccepted)
	log.Printf("Event: %+v", event)
}
