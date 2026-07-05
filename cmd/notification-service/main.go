package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/1oneday2/notification-service/internal/config"
	"github.com/1oneday2/notification-service/internal/repository"
	"github.com/1oneday2/notification-service/internal/worker"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func main() {
	// 1. Initialize structured logger
	logHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	// 2. Load configuration
	cfg := config.New()
	if cfg.LogLevel == "debug" {
		logHandler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
		logger = slog.New(logHandler)
		slog.SetDefault(logger)
	}
	logger.Info("Configuration loaded")

	// 3. Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		logger.Error("Failed to connect to Redis", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("Successfully connected to Redis")
	idempotencyRepo := repository.NewIdempotencyRepository(redisClient)

	// 4. Initialize Kafka Reader
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{cfg.KafkaBrokers},
		GroupID:  "notification-service-group",
		Topic:    "notification.events.v1",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	logger.Info("Kafka reader configured")

	// 5. Initialize notification sender (mock for now)
	sender := worker.NewMockNotificationSender(logger)

	// 6. Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	// 7. Start the Kafka consumer in a goroutine
	consumer := worker.NewKafkaConsumer(kafkaReader, idempotencyRepo, sender, logger)
	wg.Add(1)
	go consumer.Run(ctx, &wg)

	// 8. Wait for shutdown signal
	<-shutdown
	logger.Info("Shutdown signal received. Gracefully shutting down...")
	cancel() // Notify all goroutines to stop

	// 9. Wait for all goroutines to finish with a timeout
	shutdownComplete := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		logger.Info("All workers have stopped.")
	case <-time.After(5 * time.Second):
		logger.Warn("Shutdown timed out. Forcing exit.")
	}

	// 10. Close resources
	if err := kafkaReader.Close(); err != nil {
		logger.Error("Failed to close Kafka reader", slog.String("error", err.Error()))
	}
	if err := redisClient.Close(); err != nil {
		logger.Error("Failed to close Redis client", slog.String("error", err.Error()))
	}

	logger.Info("Shutdown complete.")
}
