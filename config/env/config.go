package env

import (
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type (
	Server struct {
		Mode     string `env:"MODE"`
		HTTPPort string `env:"HTTP_PORT"`
		GRPCPort string `env:"GRPC_PORT"`
	}

	Database struct {
		DBHost     string `env:"DB_HOST"`
		DBPort     string `env:"DB_PORT"`
		DBUser     string `env:"DB_USER"`
		DBPassword string `env:"DB_PASSWORD"`
		DBName     string `env:"DB_NAME"`
	}

	RabbitMQ struct {
		RMQHost        string `env:"RABBITMQ_HOST"`
		RMQPort        string `env:"RABBITMQ_PORT"`
		RMQUser        string `env:"RABBITMQ_USER"`
		RMQPassword    string `env:"RABBITMQ_PASSWORD"`
		RMQVirtualHost string `env:"RABBITMQ_VIRTUAL_HOST"`
	}

	GRPCConfig struct {
		TransactionAddress string `env:"TRANSACTION_ADDRESS"`
	}

	Config struct {
		Server     Server
		Database   Database
		RabbitMQ   RabbitMQ
		GRPCConfig GRPCConfig
	}
)

var Cfg Config

func LoadNative() ([]string, error) {
	var ok bool
	var missing []string

	if _, err := os.Stat("/app/.env"); err == nil {
		if err := godotenv.Load(); err != nil {
			return nil, err
		}
	}

	// ! Load Server configuration ____________________________
	if Cfg.Server.Mode, ok = os.LookupEnv("MODE"); !ok {
		missing = append(missing, "MODE env is not set")
	}
	if Cfg.Server.HTTPPort, ok = os.LookupEnv("HTTP_PORT"); !ok {
		missing = append(missing, "HTTP_PORT env is not set")
	}
	if Cfg.Server.GRPCPort, ok = os.LookupEnv("GRPC_PORT"); !ok {
		missing = append(missing, "GRPC_PORT env is not set")
	}
	// ! ______________________________________________________

	// ! Load Database configuration __________________________
	if Cfg.Database.DBUser, ok = os.LookupEnv("DB_USER"); !ok {
		missing = append(missing, "DB_USER env is not set")
	}
	if Cfg.Database.DBHost, ok = os.LookupEnv("DB_HOST"); !ok {
		missing = append(missing, "DB_HOST env is not set")
	}
	if Cfg.Database.DBPort, ok = os.LookupEnv("DB_PORT"); !ok {
		missing = append(missing, "DB_PORT env is not set")
	}
	if Cfg.Database.DBName, ok = os.LookupEnv("DB_NAME"); !ok {
		missing = append(missing, "DB_NAME env is not set")
	}
	if Cfg.Database.DBPassword, ok = os.LookupEnv("DB_PASSWORD"); !ok {
		missing = append(missing, "DB_PASSWORD env is not set")
	}
	// ! ______________________________________________________

	// ! Load RabbitMQ configuration __________________________
	if Cfg.RabbitMQ.RMQUser, ok = os.LookupEnv("RABBITMQ_USER"); !ok {
		missing = append(missing, "RABBITMQ_USER env is not set")
	}
	if Cfg.RabbitMQ.RMQPassword, ok = os.LookupEnv("RABBITMQ_PASSWORD"); !ok {
		missing = append(missing, "RABBITMQ_PASSWORD env is not set")
	}
	if Cfg.RabbitMQ.RMQHost, ok = os.LookupEnv("RABBITMQ_HOST"); !ok {
		missing = append(missing, "RABBITMQ_HOST env is not set")
	}
	if Cfg.RabbitMQ.RMQPort, ok = os.LookupEnv("RABBITMQ_PORT"); !ok {
		missing = append(missing, "RABBITMQ_PORT env is not set")
	}
	if Cfg.RabbitMQ.RMQVirtualHost, ok = os.LookupEnv("RABBITMQ_VIRTUAL_HOST"); !ok {
		missing = append(missing, "RABBITMQ_VIRTUAL_HOST env is not set")
	}
	// ! ______________________________________________________

	// ! Load gRPC configuration _____________________________
	if Cfg.GRPCConfig.TransactionAddress, ok = os.LookupEnv("TRANSACTION_ADDRESS"); !ok {
		missing = append(missing, "TRANSACTION_ADDRESS env is not set")
	}
	// ! ______________________________________________________

	return missing, nil
}

func LoadByViper() ([]string, error) {
	var missing []string

	config := viper.New()
	if configFile, err := os.Stat("/app/config.json"); err != nil || configFile.IsDir() {
		config.SetConfigFile("config.json")
	} else {
		config.SetConfigFile("/app/config.json")
	}

	if err := config.ReadInConfig(); err != nil {
		return nil, err
	}

	// ! Load Server configuration ____________________________
	if Cfg.Server.Mode = config.GetString("MODE"); Cfg.Server.Mode == "" {
		missing = append(missing, "MODE env is not set")
	}
	if Cfg.Server.HTTPPort = config.GetString("HTTP_PORT"); Cfg.Server.HTTPPort == "" {
		missing = append(missing, "HTTP_PORT env is not set")
	}
	if Cfg.Server.GRPCPort = config.GetString("GRPC_PORT"); Cfg.Server.GRPCPort == "" {
		missing = append(missing, "GRPC_PORT env is not set")
	}
	// ! ______________________________________________________

	// ! Load Database configuration __________________________
	if Cfg.Database.DBUser = config.GetString("DATABASE.POSTGRESQL.USER"); Cfg.Database.DBUser == "" {
		missing = append(missing, "DATABASE.POSTGRESQL.USER env is not set")
	}
	if Cfg.Database.DBHost = config.GetString("DATABASE.POSTGRESQL.HOST"); Cfg.Database.DBHost == "" {
		missing = append(missing, "DATABASE.POSTGRESQL.HOST env is not set")
	}
	if Cfg.Database.DBPort = config.GetString("DATABASE.POSTGRESQL.PORT"); Cfg.Database.DBPort == "" {
		missing = append(missing, "DATABASE.POSTGRESQL.PORT env is not set")
	}
	if Cfg.Database.DBName = config.GetString("DATABASE.POSTGRESQL.NAME"); Cfg.Database.DBName == "" {
		missing = append(missing, "DATABASE.POSTGRESQL.NAME env is not set")
	}
	if Cfg.Database.DBPassword = config.GetString("DATABASE.POSTGRESQL.PASSWORD"); Cfg.Database.DBPassword == "" {
		missing = append(missing, "DATABASE.POSTGRESQL.PASSWORD env is not set")
	}
	// ! ______________________________________________________

	// ! Load RabbitMQ configuration __________________________
	if Cfg.RabbitMQ.RMQUser = config.GetString("MESSAGE-BROKER.RABBITMQ.USER"); Cfg.RabbitMQ.RMQUser == "" {
		missing = append(missing, "MESSAGE-BROKER.RABBITMQ.USER env is not set")
	}
	if Cfg.RabbitMQ.RMQPassword = config.GetString("MESSAGE-BROKER.RABBITMQ.PASSWORD"); Cfg.RabbitMQ.RMQPassword == "" {
		missing = append(missing, "MESSAGE-BROKER.RABBITMQ.PASSWORD env is not set")
	}
	if Cfg.RabbitMQ.RMQHost = config.GetString("MESSAGE-BROKER.RABBITMQ.HOST"); Cfg.RabbitMQ.RMQHost == "" {
		missing = append(missing, "MESSAGE-BROKER.RABBITMQ.HOST env is not set")
	}
	if Cfg.RabbitMQ.RMQPort = config.GetString("MESSAGE-BROKER.RABBITMQ.PORT"); Cfg.RabbitMQ.RMQPort == "" {
		missing = append(missing, "MESSAGE-BROKER.RABBITMQ.PORT env is not set")
	}
	if Cfg.RabbitMQ.RMQVirtualHost = config.GetString("MESSAGE-BROKER.RABBITMQ.VIRTUAL_HOST"); Cfg.RabbitMQ.RMQVirtualHost == "" {
		missing = append(missing, "MESSAGE-BROKER.RABBITMQ.VIRTUAL_HOST env is not set")
	}
	// ! ______________________________________________________

	// ! Load gRPC configuration _____________________________
	if Cfg.GRPCConfig.TransactionAddress = config.GetString("GRPC-CONFIG.TRANSACTION_ADDRESS"); Cfg.GRPCConfig.TransactionAddress == "" {
		missing = append(missing, "GRPC-CONFIG.TRANSACTION_ADDRESS env is not set")
	}
	// ! ______________________________________________________

	return missing, nil
}
