package domain

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// NotificationType defines the type of notification.
type NotificationType string

const (
	SMSType NotificationType = "sms"
	EmailType NotificationType = "email"
)

// NotificationEvent represents the data structure for a notification event.
type NotificationEvent struct {
	ID      uuid.UUID        `json:"id"`
	Type    NotificationType `json:"type"`
	Target  string           `json:"target"`
	Payload string           `json:"payload"`
}

// Validate checks if the NotificationEvent has valid data.
func (e *NotificationEvent) Validate() error {
	if e.ID == uuid.Nil {
		return fmt.Errorf("event ID is required")
	}
	switch e.Type {
	case SMSType, EmailType:
		// valid
	default:
		return fmt.Errorf("invalid notification type: %s", e.Type)
	}
	if e.Target == "" {
		return fmt.Errorf("target is required")
	}
	if e.Payload == "" {
		return fmt.Errorf("payload is required")
	}
	return nil
}

// UnmarshalJSON custom unmarshaller to validate the event upon creation.
func (e *NotificationEvent) UnmarshalJSON(data []byte) error {
	type Alias NotificationEvent
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(e),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	return e.Validate()
}
