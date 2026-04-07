package app

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func Run() {
	cfg, err := loadConfig(ConfigPath())
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Sync()
	sugar := logger.Sugar()

	sugar.Infow("Loaded configuration", configLogFields(cfg)...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		sugar.Infof("Received shutdown signal: %v", sig)
		signal.Stop(sigChan)
		cancel()
	}()

	var srClient schemaregistry.Client
	if anyTopicUsesFormat(cfg, "avro") {
		var srCfg *schemaregistry.Config
		if cfg.SchemaRegistryUsername != "" {
			srCfg = schemaregistry.NewConfigWithAuthentication(
				cfg.SchemaRegistry,
				cfg.SchemaRegistryUsername,
				cfg.SchemaRegistryPassword,
			)
		} else {
			srCfg = schemaregistry.NewConfig(cfg.SchemaRegistry)
		}
		srClient, err = schemaregistry.NewClient(srCfg)
		if err != nil {
			sugar.Errorf("Failed to create Schema Registry client: %v", err)
			return
		}
	}

	// Shared ClickHouse connection
	chOptions, err := clickhouse.ParseDSN(cfg.ClickHouseDSN)
	if err != nil {
		sugar.Errorf("Failed to parse ClickHouse DSN: %v", err)
		return
	}
	chConn, err := clickhouse.Open(chOptions)
	if err != nil {
		sugar.Errorf("Failed to open ClickHouse connection: %v", err)
		return
	}
	defer chConn.Close()

	if err := chConn.Ping(context.Background()); err != nil {
		sugar.Errorf("Failed to ping ClickHouse: %v", err)
		return
	}
	sugar.Info("Connected to ClickHouse")

	// Shared DLQ producer
	dlqKafkaConfig := buildKafkaConfig(kafka.ConfigMap{
		"bootstrap.servers": cfg.KafkaBrokers,
	}, cfg)
	dlqProducer, err := kafka.NewProducer(&dlqKafkaConfig)
	if err != nil {
		sugar.Errorf("Failed to create Kafka producer for DLQ: %v", err)
		return
	}
	defer dlqProducer.Close()

	// Drain the producer's Events channel so its internal queue never fills up.
	go func() {
		for range dlqProducer.Events() {
		}
	}()

	// Create and start the task manager
	mgr := NewTaskManager(ctx, cfg, chConn, srClient, dlqProducer, sugar)
	if err := mgr.StartAll(); err != nil {
		sugar.Errorf("Failed to start sink tasks: %v", err)
		return
	}

	// Start metrics, health, and admin server. If it fails to bind (e.g. port conflict),
	// cancel the context so the process exits cleanly rather than running without observability.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	health := NewHealth(sugar, chConn, mgr.Snapshot)
	RegisterHealthEndpoints(health, mux)
	RegisterAdminEndpoints(mgr, mux)

	addr := fmt.Sprintf(":%d", cfg.MetricsPort)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		sugar.Infof("Starting metrics, health, and admin server on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			sugar.Errorf("Server failed: %v — triggering shutdown", err)
			cancel()
		}
	}()

	sugar.Info("All sink tasks started, waiting for shutdown signal")
	mgr.Wait()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		sugar.Errorf("Server shutdown error: %v", err)
	}

	sugar.Info("All sink tasks stopped, exiting")
}
