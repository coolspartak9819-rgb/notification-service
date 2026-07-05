package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	lockTTL = 24 * time.Hour
)

// ErrEventAlreadyProcessed is returned when an event has already been processed.
var ErrEventAlreadyProcessed = errors.New("event already processed")

// IdempotencyRepository provides an interface for checking if an event has been processed.
type IdempotencyRepository struct {
	client *redis.Client
}

// NewIdempotencyRepository creates a new IdempotencyRepository.
func NewIdempotencyRepository(client *redis.Client) *IdempotencyRepository {
	return &IdempotencyRepository{client: client}
}

// TryLock attempts to acquire a lock for a given event ID.
// It returns ErrEventAlreadyProcessed if the lock is already held.
func (r *IdempotencyRepository) TryLock(ctx context.Context, eventID uuid.UUID) error {
	key := fmt.Sprintf("event:%s", eventID.String())

	// SETNX is an atomic operation.
	// It returns true if the key was set, false if the key already existed.
	wasSet, err := r.client.SetNX(ctx, key, "processed", lockTTL).Result()
	if err != nil {
		// Wrap the error to provide more context.
		return fmt.Errorf("failed to execute SETNX for event %s: %w", eventID, err)
	}

	if !wasSet {
		return ErrEventAlreadyProcessed
	}

	return nil
}
