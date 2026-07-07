package server

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/1oneday2/notification-service/internal/domain"
	"github.com/1oneday2/notification-service/internal/repository"
	"github.com/1oneday2/notification-service/internal/worker"
	v1 "github.com/1oneday2/notification-service/pkg/api/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer is the implementation of the gRPC server.
type GRPCServer struct {
	v1.UnimplementedNotificationServiceServer
	eventProducer   *worker.EventProducer
	idempotencyRepo *repository.IdempotencyRepository
	logger          *slog.Logger
}

// NewGRPCServer creates a new GRPCServer.
func NewGRPCServer(
	eventProducer *worker.EventProducer,
	idempotencyRepo *repository.IdempotencyRepository,
	logger *slog.Logger,
) *GRPCServer {
	return &GRPCServer{
		eventProducer:   eventProducer,
		idempotencyRepo: idempotencyRepo,
		logger:          logger,
	}
}

// SendNotification handles the gRPC request to send a notification.
func (s *GRPCServer) SendNotification(ctx context.Context, req *v1.SendNotificationRequest) (*v1.SendNotificationResponse, error) {
	log := s.logger.With(slog.String("idempotency_key", req.IdempotencyKey))

	// 1. Validate request
	if req.IdempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	idempotencyKey, err := uuid.Parse(req.IdempotencyKey)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid idempotency_key format")
	}

	// 2. Idempotency Check
	if err := s.idempotencyRepo.TryLock(ctx, idempotencyKey); err != nil {
		if errors.Is(err, repository.ErrEventAlreadyProcessed) {
			log.Warn("Duplicate request detected")
			return nil, status.Error(codes.AlreadyExists, "request with this idempotency_key has already been processed")
		}
		log.Error("Failed to check idempotency", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to process request")
	}

	// 3. Create domain event
	event := domain.NotificationEvent{
		ID:      uuid.New(), // Generate a new unique ID for the event itself
		Type:    domain.NotificationType(strings.ToLower(req.Type.String())),
		Target:  req.Target,
		Payload: req.Payload,
	}
	if err := event.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid event data: %v", err)
	}

	// 4. Publish to Kafka
	if err := s.eventProducer.Publish(ctx, event); err != nil {
		log.Error("Failed to publish event to Kafka", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to publish event")
	}

	log.Info("Event successfully published to Kafka", slog.String("event_id", event.ID.String()))

	// 5. Return response
	return &v1.SendNotificationResponse{
		EventId: event.ID.String(),
		Status:  "ACCEPTED",
	}, nil
}
