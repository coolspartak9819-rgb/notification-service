package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/1oneday2/notification-service/internal/domain"
	"github.com/1oneday2/notification-service/internal/repository"
	"github.com/segmentio/kafka-go"
)

// NotificationSender is an interface for sending notifications.
// This allows for easy mocking and testing.
type NotificationSender interface {
	Send(ctx context.Context, event domain.NotificationEvent) error
}

// KafkaConsumer consumes notification events from Kafka.
type KafkaConsumer struct {
	reader          *kafka.Reader
	idempotencyRepo *repository.IdempotencyRepository
	sender          NotificationSender
	logger          *slog.Logger
}

// NewKafkaConsumer creates a new KafkaConsumer.
func NewKafkaConsumer(
	reader *kafka.Reader,
	idempotencyRepo *repository.IdempotencyRepository,
	sender NotificationSender,
	logger *slog.Logger,
) *KafkaConsumer {
	return &KafkaConsumer{
		reader:          reader,
		idempotencyRepo: idempotencyRepo,
		sender:          sender,
		logger:          logger,
	}
}

// Run starts the consumer loop.
func (c *KafkaConsumer) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	c.logger.Info("Starting Kafka consumer")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Shutting down Kafka consumer")
			return
		default:
			// Use a context with a timeout for fetching the message.
			fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			msg, err := c.reader.FetchMessage(fetchCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					continue // Continue to the next iteration to check for shutdown signal
				}
				c.logger.Error("Failed to fetch message", slog.String("error", err.Error()))
				continue
			}

			c.processMessage(ctx, msg)
		}
	}
}

func (c *KafkaConsumer) processMessage(ctx context.Context, msg kafka.Message) {
	var event domain.NotificationEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		c.logger.Error("Failed to unmarshal message",
			slog.String("error", err.Error()),
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
		)
		// Commit message to avoid reprocessing a malformed message.
		c.commit(ctx, msg)
		return
	}

	log := c.logger.With(slog.String("event_id", event.ID.String()))

	// Idempotency check
	if err := c.idempotencyRepo.TryLock(ctx, event.ID); err != nil {
		if errors.Is(err, repository.ErrEventAlreadyProcessed) {
			log.Warn("Event already processed (idempotency check)")
		} else {
			log.Error("Failed to check idempotency", slog.String("error", err.Error()))
			// Do not commit, so we can retry.
			return
		}
	} else {
		// Process the event only if the lock was acquired.
		if err := c.sender.Send(ctx, event); err != nil {
			log.Error("Failed to send notification", slog.String("error", err.Error()))
			// Do not commit, so we can retry.
			return
		}
		log.Info("Notification sent successfully")
	}

	// Commit the message after processing.
	c.commit(ctx, msg)
}

func (c *KafkaConsumer) commit(ctx context.Context, msg kafka.Message) {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		c.logger.Error("Failed to commit message",
			slog.String("error", err.Error()),
			slog.String("event_id", string(msg.Key)),
		)
	}
}

// MockNotificationSender is a mock implementation of NotificationSender for demonstration.
type MockNotificationSender struct {
	logger *slog.Logger
}

func NewMockNotificationSender(logger *slog.Logger) *MockNotificationSender {
	return &MockNotificationSender{logger: logger}
}

func (s *MockNotificationSender) Send(ctx context.Context, event domain.NotificationEvent) error {
	// In a real application, this would interact with an external service (e.g., SMTP, SMS gateway).
	s.logger.Info("Sending notification",
		slog.String("type", string(event.Type)),
		slog.String("target", event.Target),
	)
	// Simulate network latency
	time.Sleep(100 * time.Millisecond)
	return nil
}
