package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type Event struct {
	UserID    string `json:"userId"`
	EventType string `json:"eventType"`
	URL       string `json:"url"`
}

const (
	defaultTopic        = "events"
	defaultGroupID      = "consumer-group-events"
	defaultBatchTimeout = 5 * time.Second
)

type PostgresConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSLMode  string
}

func (cfg *PostgresConfig) Stringify() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode)
}

func (cfg *PostgresConfig) URL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode)
}

func NewPostgresConfig() *PostgresConfig {
	return &PostgresConfig{
		Host:     getEnv("PG_HOST", "localhost"),
		Port:     getEnv("PG_PORT", "5432"),
		User:     getEnv("PG_USER", "user"),
		Password: getEnv("PG_PASSWORD", "password"),
		Database: getEnv("PG_DATABASE", "analytics"),
		SSLMode:  getEnv("PG_SSLMODE", "disable"),
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func (cfg *PostgresConfig) Open() (*sql.DB, error) {
	db, err := sql.Open("postgres", cfg.Stringify())
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return db, nil
}

type EventBatch struct {
	Events   []Event
	Messages []kafka.Message
}

// insertBatch performs a bulk insert of events in a single transaction
func insertBatch(ctx context.Context, db *sql.DB, batch *EventBatch) error {
	if len(batch.Events) == 0 {
		return nil
	}

	start := time.Now()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction error: %w", err)
	}
	defer tx.Rollback()

	// bulk insert query
	// Goal : INSERT INTO events (user_id, event_type, url) VALUES ($1, $2, $3), ($4, $5, $6), ...
	query := `INSERT INTO events (user_id, event_type, url) VALUES `

	// Each event has 3 fields, so we need 3 * batch_size total placeholders
	values := make([]any, 0, len(batch.Events)*3)
	placeholders := make([]string, 0, len(batch.Events))

	paramIndex := 1
	for _, event := range batch.Events {
		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d)", paramIndex, paramIndex+1, paramIndex+2))

		values = append(values, event.UserID, event.EventType, event.URL)

		paramIndex += 3 // Move to next set of parameters
	}

	query += strings.Join(placeholders, ", ")

	_, err = tx.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("exec batch insert error: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction error: %w", err)
	}

	log.Printf("Successfully inserted %d events in %v", len(batch.Events), time.Since(start))
	return nil
}

func processBatch(ctx context.Context, db *sql.DB, reader *kafka.Reader, batch *EventBatch) error {
	if len(batch.Events) == 0 {
		return nil
	}

	if err := insertBatch(ctx, db, batch); err != nil {
		log.Printf("Error inserting batch: %v", err)
		// Don't commit offsets - kafka will redeliver
		return err
	}

	// only commit offsets after successful DB insert
	if err := reader.CommitMessages(ctx, batch.Messages...); err != nil {
		log.Printf("Error committing offsets: %v", err)
		return err
	}
	return nil
}

func runMigrations(pgConn *PostgresConfig) error {
	m, err := migrate.New(
		"file://cmd/worker/db/migrations",
		pgConn.URL())

	if err != nil {
		return fmt.Errorf("error initiating migration: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("migration failed: %w", err)
	}

	log.Println("Database migration successful")
	return nil
}

func main() {
	topic := getEnv("KAFKA_TOPIC", defaultTopic)
	groupID := getEnv("KAFKA_GROUP_ID", defaultGroupID)
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	batchSize := 100
	batchTimeout := defaultBatchTimeout

	pgConn := NewPostgresConfig()
	db, err := pgConn.Open()
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Could not ping DB: %v", err)
	}
	log.Println("Database connection established")

	if err := runMigrations(pgConn); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,
		StartOffset:    kafka.LastOffset,
	})

	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, initiating graceful shutdown...", sig)
		cancel()
	}()

	log.Printf("Starting kafka consumer on topic '%s', consumer groups: '%s'\n", topic, groupID)

	batch := &EventBatch{
		Events:   make([]Event, 0, batchSize),
		Messages: make([]kafka.Message, 0, batchSize),
	}
	lastFlush := time.Now()

	var totalProcessed, totalErrors int64

	for {
		// Check for shutdown
		if ctx.Err() != nil {
			// process remaining messages before shutdown
			if len(batch.Events) > 0 {
				log.Printf("Processing final batch of %d messages...", len(batch.Events))
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := processBatch(shutdownCtx, db, reader, batch); err != nil {
					log.Printf("Error processing final batch: %v", err)
				} else {
					totalProcessed += int64(len(batch.Events))
				}
				shutdownCancel()
			}
			log.Printf("Shutdown complete. Processed: %d, Errors: %d", totalProcessed, totalErrors)
			return
		}

		// read message with timeout
		readCtx, readCancel := context.WithTimeout(ctx, 100*time.Millisecond)
		msg, err := reader.FetchMessage(readCtx)
		readCancel()

		if err != nil {
			if len(batch.Events) > 0 && time.Since(lastFlush) >= batchTimeout {
				if err := processBatch(ctx, db, reader, batch); err != nil {
					totalErrors++
				} else {
					totalProcessed += int64(len(batch.Events))
				}
				batch.Events = batch.Events[:0]
				batch.Messages = batch.Messages[:0]
				lastFlush = time.Now()
			}

			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				log.Printf("Error fetching message: %v", err)
			}
			continue
		}

		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error unmarshalling message (offset %d): %v", msg.Offset, err)

			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Error committing bad message offset: %v", err)
			}
			totalErrors++
			continue
		}

		// Add to batch
		batch.Events = append(batch.Events, event)
		batch.Messages = append(batch.Messages, msg)

		// process batch if full
		if len(batch.Events) >= batchSize {
			if err := processBatch(ctx, db, reader, batch); err != nil {
				totalErrors++
			} else {
				totalProcessed += int64(len(batch.Events))
			}
			batch.Events = batch.Events[:0]
			batch.Messages = batch.Messages[:0]
			lastFlush = time.Now()
		}
	}

}
