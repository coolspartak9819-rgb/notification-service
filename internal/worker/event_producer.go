package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/1oneday2/notification-service/internal/domain"
	"github.com/segmentio/kafka-go"
)

const (
	// EventTopic is the main topic for new notification events.
	EventTopic = "notification.events.v1"
)

// EventProducer is responsible for publishing new notification events to Kafka.
type EventProducer struct {
	writer *kafka.Writer
}

// NewEventProducer creates a new EventProducer.
func NewEventProducer(brokers []string) *EventProducer {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Balancer: &kafka.LeastBytes{},
	}
	return &EventProducer{writer: writer}
}

// Publish sends a new notification event to the main event topic.
func (p *EventProducer) Publish(ctx context.Context, event domain.NotificationEvent) error {
	msgBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := kafka.Message{
		Topic: EventTopic,
		Key:   []byte(event.ID.String()),
		Value: msgBytes,
	}

	return p.writer.WriteMessages(ctx, msg)
}

// Close closes the underlying Kafka writer.
func (p *EventProducer) Close() error {
	return p.writer.Close()
}
