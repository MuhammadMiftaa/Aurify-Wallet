package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"refina-wallet/config/db"
	"refina-wallet/config/env"
	logger "refina-wallet/config/log"
	"refina-wallet/interface/grpc/client"
	grpcserver "refina-wallet/interface/grpc/server"
	"refina-wallet/interface/http/router"
	"refina-wallet/interface/queue"
	"refina-wallet/interface/queue/consumer"
	"refina-wallet/internal/repository"
	"refina-wallet/internal/service"
	"refina-wallet/internal/utils"
	"refina-wallet/internal/utils/data"
)

func init() {
	var err error
	var missing []string
	if missing, err = env.LoadByViper(); err != nil {
		log.Printf("Failed to read JSON config file: %v", err)
		if missing, err = env.LoadNative(); err != nil {
			log.Fatalf("Failed to load environment variables: %v", err)
		}
		log.Printf("Environment variables by .env file loaded successfully")
	} else {
		log.Printf("Environment variables by Viper loaded successfully")
	}

	logger.SetupLogger()

	if len(missing) > 0 {
		for _, envVar := range missing {
			logger.Warn(data.LogEnvVarMissing, map[string]any{"service": data.EnvService, "key": envVar})
		}
	}
}

func main() {
	// Setup Database Connection
	startTime := time.Now()
	dbInstance := db.GetInstance(env.Cfg.Database)
	logger.Info(data.LogDBSetupSuccess, map[string]any{"service": data.DatabaseService, "duration": utils.Ms(time.Since(startTime))})

	// Setup RabbitMQ Connection
	startTime = time.Now()
	queueInstance := queue.GetInstance(env.Cfg.RabbitMQ)
	logger.Info(data.LogRabbitmqSetupSuccess, map[string]any{"service": data.RabbitmqService, "duration": utils.Ms(time.Since(startTime))})

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup Outbox Publisher
	startTime = time.Now()
	outboxRepo := repository.NewOutboxRepository(dbInstance.GetDB())
	outboxPublisher := service.NewOutboxPublisher(outboxRepo, queueInstance)

	// Start outbox publisher worker
	go outboxPublisher.Start(ctx)

	// Start cleanup job (optional)
	go outboxPublisher.StartCleanupJob(ctx)
	logger.Info(data.LogOutboxPublisherStarted, map[string]any{"service": data.OutboxService, "duration": utils.Ms(time.Since(startTime))})

	// Setup Admin Event Consumer
	startTime = time.Now()
	walletTypesRepo := repository.NewWalletTypesRepository(dbInstance.GetDB())
	walletTypesService := service.NewWalletTypesService(repository.NewTxManager(dbInstance.GetDB()), walletTypesRepo)
	adminConsumer := consumer.NewAdminEventConsumer(queueInstance, walletTypesService)
	go adminConsumer.Start(ctx)
	logger.Info(data.LogAdminConsumerStarted, map[string]any{"service": data.AdminConsumerService, "duration": utils.Ms(time.Since(startTime))})

	// Set up the gRPC client
	startTime = time.Now()
	grpcManager := client.GetManager()
	err := grpcManager.SetupGRPCClient()
	if err != nil {
		logger.Fatal(data.LogGRPCClientSetupFailed, map[string]any{"service": data.GRPCClientService, "error": err.Error()})
	}
	logger.Info(data.LogGRPCClientSetupSuccess, map[string]any{"service": data.GRPCClientService, "duration": utils.Ms(time.Since(startTime))})

	// Set up the HTTP server
	startTime = time.Now()
	httpServer := router.SetupHTTPServer(dbInstance, queueInstance)
	if httpServer != nil {
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Fatal(data.LogHTTPServerStartFailed, map[string]any{"service": data.HTTPServerService, "error": err.Error()})
			}
		}()
		logger.Info(data.LogHTTPServerStarted, map[string]any{"service": data.HTTPServerService, "port": env.Cfg.Server.HTTPPort, "duration": utils.Ms(time.Since(startTime))})
	}

	// Set up the gRPC server
	startTime = time.Now()
	grpcServer, lis, err := grpcserver.SetupGRPCServer(dbInstance, queueInstance)
	if err != nil {
		logger.Fatal(data.LogGRPCServerSetupFailed, map[string]any{"service": data.GRPCServerService, "error": err.Error()})
	}
	if grpcServer != nil && lis != nil {
		go func() {
			if err := grpcServer.Serve(*lis); err != nil {
				logger.Fatal(data.LogGRPCServerServeFailed, map[string]any{"service": data.GRPCServerService, "error": err.Error()})
			}
		}()
		logger.Info(data.LogGRPCServerStarted, map[string]any{"service": data.GRPCServerService, "port": env.Cfg.Server.GRPCPort, "duration": utils.Ms(time.Since(startTime))})
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info(data.LogShutdownSignalReceived, map[string]any{"service": data.MainService})

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	startTime = time.Now()
	shutdownErrors := map[string]any{"service": data.MainService}

	// Shutdown HTTP server
	if httpServer != nil {
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error(data.LogHTTPServerShutdownFailed, map[string]any{"service": data.HTTPServerService, "error": err.Error()})
			shutdownErrors["http_error"] = true
		}
	}

	// Cancel context to stop outbox publisher
	cancel()
	time.Sleep(2 * time.Second) // Give some time for outbox publisher to stop

	// Shutdown gRPC server
	if grpcServer != nil {
		grpcServer.GracefulStop()
		if err := grpcManager.Shutdown(shutdownCtx); err != nil {
			logger.Error(data.LogGRPCClientShutdownFailed, map[string]any{"service": data.GRPCClientService, "error": err.Error()})
			shutdownErrors["grpc_error"] = true
		}
	}

	// Close RabbitMQ connection
	if err := queueInstance.Close(); err != nil {
		logger.Error(data.LogRabbitmqCloseFailed, map[string]any{"service": data.RabbitmqService, "error": err.Error()})
		shutdownErrors["rabbitmq_error"] = true
	}

	if err := dbInstance.Close(); err != nil {
		logger.Error(data.LogDBCloseFailed, map[string]any{"service": data.DatabaseService, "error": err.Error()})
		shutdownErrors["database_error"] = true
	}

	if len(shutdownErrors) > 0 {
		logger.Info(data.LogShutdownCompletedWithErrors, shutdownErrors)
	} else {
		logger.Info(data.LogShutdownCompleted, map[string]any{"service": data.MainService, "duration": utils.Ms(time.Since(startTime))})
	}
}
