package server_test

import (
	"context"
	"log"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/1oneday2/notification-service/internal/repository"
	"github.com/1oneday2/notification-service/internal/server"
	"github.com/1oneday2/notification-service/internal/worker"
	v1 "github.com/1oneday2/notification-service/pkg/api/v1"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	kafka_test "github.com/testcontainers/testcontainers-go/modules/kafka"
	redis_test "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestNotificationService_Integration(t *testing.T) {
	ctx := context.Background()

	// 1. Start Infrastructure using Testcontainers
	kafkaContainer, kafkaBroker := setupKafka(t, ctx)
	defer kafkaContainer.Terminate(ctx)

	redisContainer, redisAddr := setupRedis(t, ctx)
	defer redisContainer.Terminate(ctx)

	// 2. Setup gRPC Server
	grpcServer, lis := setupGRPCServer(t, ctx, kafkaBroker, redisAddr)
	defer grpcServer.GracefulStop()
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// 3. Setup gRPC Client
	conn, client := setupGRPCClient(t, lis.Addr().String())
	defer conn.Close()

	// 4. Run Test Cases
	t.Run("Successful Case", func(t *testing.T) {
		idempotencyKey := uuid.NewString()
		req := &v1.SendNotificationRequest{
			IdempotencyKey: idempotencyKey,
			Type:           v1.NotificationType_SMS,
			Target:         "+1234567890",
			Payload:        "Hello, World!",
		}

		resp, err := client.SendNotification(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, "ACCEPTED", resp.Status)
		assert.NotEmpty(t, resp.EventId)

		// Verify that the message is in Kafka
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{kafkaBroker},
			Topic:       worker.EventTopic,
			StartOffset: kafka.FirstOffset,
		})
		defer reader.Close()

		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		msg, err := reader.ReadMessage(readCtx)
		require.NoError(t, err)
		assert.NotEmpty(t, msg.Value)
	})

	t.Run("Duplicate Case", func(t *testing.T) {
		idempotencyKey := uuid.NewString()
		req := &v1.SendNotificationRequest{
			IdempotencyKey: idempotencyKey,
			Type:           v1.NotificationType_EMAIL,
			Target:         "test@example.com",
			Payload:        "Hello, Again!",
		}

		_, err := client.SendNotification(ctx, req)
		require.NoError(t, err)

		_, err = client.SendNotification(ctx, req)
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.AlreadyExists, st.Code())
	})
}

func setupKafka(t *testing.T, ctx context.Context) (*kafka_test.KafkaContainer, string) {
	clusterID, err := uuid.NewRandom()
	require.NoError(t, err)

	kafkaContainer, err := kafka_test.RunContainer(ctx,
		testcontainers.WithImage("confluentinc/cp-kafka:7.5.0"),
		testcontainers.WithEnv(map[string]string{
			"CLUSTER_ID": clusterID.String(),
		}),
	)
	require.NoError(t, err)

	// Force-create the topic inside the running container.
	_, _, err = kafkaContainer.Exec(ctx, []string{
		"kafka-topics",
		"--create",
		"--topic", worker.EventTopic,
		"--partitions", "1",
		"--replication-factor", "1",
		"--bootstrap-server", "localhost:9092",
	})
	require.NoError(t, err)

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	return kafkaContainer, brokers[0]
}

func setupRedis(t *testing.T, ctx context.Context) (*redis_test.RedisContainer, string) {
	redisContainer, err := redis_test.RunContainer(ctx,
		testcontainers.WithImage("redis:7-alpine"),
	)
	require.NoError(t, err)

	addr, err := redisContainer.Endpoint(ctx, "")
	require.NoError(t, err)

	return redisContainer, addr
}

func setupGRPCServer(t *testing.T, ctx context.Context, kafkaBroker, redisAddr string) (*grpc.Server, net.Listener) {
	lis, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	eventProducer := worker.NewEventProducer([]string{kafkaBroker})
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	idempotencyRepo := repository.NewIdempotencyRepository(redisClient)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	grpcServer := server.NewGRPCServer(eventProducer, idempotencyRepo, logger)
	s := grpc.NewServer()
	v1.RegisterNotificationServiceServer(s, grpcServer)

	return s, lis
}

func setupGRPCClient(t *testing.T, addr string) (*grpc.ClientConn, v1.NotificationServiceClient) {
	dialCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	require.NoError(t, err)
	client := v1.NewNotificationServiceClient(conn)
	return conn, client
}
