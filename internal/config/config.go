package config

import (
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// Config holds all configuration for the application
type Config struct {
	KafkaBrokers string
	RedisAddr    string
	LogLevel     string
}

// New creates a new Config object
func New() *Config {
	// In a real-world scenario, you might not want to ignore this error.
	_ = godotenv.Load()

	return &Config{
		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9093"),
		RedisAddr:    getEnv("REDIS_ADDR", "localhost:6379"),
		LogLevel:     strings.ToLower(getEnv("LOG_LEVEL", "info")),
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	log.Printf("Using default value for %s: %s", key, fallback)
	return fallback
}
