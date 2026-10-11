package config

import (
	"os"
	"strconv"
	"strings"
)

// Config представляет конфигурацию приложения
type Config struct {
	Server     ServerConfig     `json:"server"`
	Database   DatabaseConfig   `json:"database"`
	Redis      RedisConfig      `json:"redis"`
	Kafka      KafkaConfig      `json:"kafka"`
	Logger     LoggerConfig     `json:"logger"`
	Assignment AssignmentConfig `json:"assignment"`
}

// AssignmentConfig представляет конфигурацию алгоритма автоназначения курьеров
type AssignmentConfig struct {
	DistanceWeight   float64 `json:"distance_weight"`   // вес расстояния (по умолчанию 0.4)
	RatingWeight     float64 `json:"rating_weight"`     // вес рейтинга (по умолчанию 0.3)
	WorkloadWeight   float64 `json:"workload_weight"`   // вес загруженности (по умолчанию 0.3)
	MaxDistanceKm    float64 `json:"max_distance_km"`   // радиус нормализации: за его пределами оценка расстояния = 0
	MaxActiveOrders  int     `json:"max_active_orders"` // максимум активных заказов у курьера
	LocationStaleMin int     `json:"location_stale_minutes"`
	DefaultPickupLat float64 `json:"default_pickup_lat"`
	DefaultPickupLon float64 `json:"default_pickup_lon"`
	HasDefaultPickup bool    `json:"has_default_pickup"`
}

// ServerConfig представляет конфигурацию HTTP сервера
type ServerConfig struct {
	Port         string `json:"port"`
	Host         string `json:"host"`
	ReadTimeout  int    `json:"read_timeout"`
	WriteTimeout int    `json:"write_timeout"`
}

// DatabaseConfig представляет конфигурацию базы данных
type DatabaseConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DBName   string `json:"db_name"`
	SSLMode  string `json:"ssl_mode"`
}

// RedisConfig представляет конфигурацию Redis
type RedisConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

// KafkaConfig представляет конфигурацию Kafka
type KafkaConfig struct {
	Brokers []string `json:"brokers"`
	GroupID string   `json:"group_id"`
	Topics  Topics   `json:"topics"`
}

// Topics представляет список топиков Kafka
type Topics struct {
	Orders    string `json:"orders"`
	Couriers  string `json:"couriers"`
	Locations string `json:"locations"`
}

// LoggerConfig представляет конфигурацию логгера
type LoggerConfig struct {
	Level  string `json:"level"`
	Format string `json:"format"`
	File   string `json:"file"`
}

// Load загружает конфигурацию из переменных окружения
func Load() *Config {
	return &Config{
		Server: ServerConfig{
			Port:         getEnv("SERVER_PORT", "8080"),
			Host:         getEnv("SERVER_HOST", "0.0.0.0"),
			ReadTimeout:  getEnvAsInt("SERVER_READ_TIMEOUT", 10),
			WriteTimeout: getEnvAsInt("SERVER_WRITE_TIMEOUT", 10),
		},
		Database: DatabaseConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     getEnv("DB_PORT", "5432"),
			User:     getEnv("DB_USER", "delivery_user"),
			Password: getEnv("DB_PASSWORD", "delivery_pass"),
			DBName:   getEnv("DB_NAME", "delivery_system"),
			SSLMode:  getEnv("DB_SSL_MODE", "disable"),
		},
		Redis: RedisConfig{
			Host:     getEnv("REDIS_HOST", "localhost"),
			Port:     getEnv("REDIS_PORT", "6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getEnvAsInt("REDIS_DB", 0),
		},
		Kafka: KafkaConfig{
			Brokers: strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
			GroupID: getEnv("KAFKA_GROUP_ID", "delivery-service"),
			Topics: Topics{
				Orders:    getEnv("KAFKA_TOPIC_ORDERS", "orders"),
				Couriers:  getEnv("KAFKA_TOPIC_COURIERS", "couriers"),
				Locations: getEnv("KAFKA_TOPIC_LOCATIONS", "locations"),
			},
		},
		Logger: LoggerConfig{
			Level:  getEnv("LOG_LEVEL", "info"),
			Format: getEnv("LOG_FORMAT", "json"),
			File:   getEnv("LOG_FILE", ""),
		}, 
		Assignment: AssignmentConfig{
			DistanceWeight:   getEnvAsFloat("ASSIGN_WEIGHT_DISTANCE", 0.4),
			RatingWeight:     getEnvAsFloat("ASSIGN_WEIGHT_RATING", 0.3),
			WorkloadWeight:   getEnvAsFloat("ASSIGN_WEIGHT_LOAD", 0.3),
			MaxDistanceKm:    getEnvAsFloat("ASSIGN_MAX_DISTANCE_KM", 10),
			MaxActiveOrders:  getEnvAsInt("ASSIGN_MAX_ACTIVE_ORDERS", 2),
			LocationStaleMin: getEnvAsInt("ASSIGN_LOCATION_STALE_MINUTES", 15),
			DefaultPickupLat: getEnvAsFloat("ASSIGN_PICKUP_DEFAULT_LAT", 0),
			DefaultPickupLon: getEnvAsFloat("ASSIGN_PICKUP_DEFAULT_LON", 0),
			HasDefaultPickup: getEnv("ASSIGN_PICKUP_DEFAULT_LAT", "") != "" && getEnv("ASSIGN_PICKUP_DEFAULT_LON", "") != "",
		},
	}
}

// getEnv получает значение переменной окружения с значением по умолчанию
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// getEnvAsInt получает значение переменной окружения как int с значением по умолчанию
func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}

// getEnvAsFloat получает значение переменной окружения как float64 с значением по умолчанию
func getEnvAsFloat(key string, defaultValue float64) float64 {
	valueStr := getEnv(key, "")
	if value, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return value
	}
	return defaultValue
}