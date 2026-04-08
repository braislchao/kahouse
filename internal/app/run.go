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
	"go.uber.org/zap/zapcore"
)

func Run() {
	cfg, err := loadConfig(ConfigPath())
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	prodCfg := zap.NewProductionConfig()
	prodCfg.EncoderConfig.TimeKey = "ts"
	prodCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, err := prodCfg.Build()
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
	chOptions.MaxOpenConns = cfg.ClickHouseMaxOpenConns
	chOptions.MaxIdleConns = cfg.ClickHouseMaxIdleConns
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

	if err := validateTables(context.Background(), chConn, cfg.TopicTables, sugar); err != nil {
		sugar.Fatalf("Table validation failed: %v", err)
	}

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

	validateDLQTopics(dlqProducer, cfg, sugar)

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

// validateDLQTopics checks whether the DLQ topics for each configured mapping
// exist in the Kafka cluster. It logs warnings for missing topics but never
// fails startup — Kafka may be configured to auto-create them on first produce.
func validateDLQTopics(producer *kafka.Producer, cfg *Config, sugar *zap.SugaredLogger) {
	adminClient, err := kafka.NewAdminClientFromProducer(producer)
	if err != nil {
		sugar.Warnf("Could not create admin client to validate DLQ topics: %v", err)
		return
	}
	defer adminClient.Close()

	for _, mapping := range cfg.TopicTables {
		dlqTopic := mapping.Topic + cfg.DLQTopicSuffix
		metadata, err := adminClient.GetMetadata(&dlqTopic, false, 5000)
		if err != nil {
			sugar.Warnf("Could not fetch metadata for DLQ topic %q: %v", dlqTopic, err)
			continue
		}
		if topicMeta, exists := metadata.Topics[dlqTopic]; exists {
			if topicMeta.Error.Code() != kafka.ErrNoError {
				sugar.Warnf("DLQ topic %q may not exist (error: %v). Ensure it is pre-created or auto-creation is enabled.", dlqTopic, topicMeta.Error)
			} else {
				sugar.Infof("Validated DLQ topic exists: %s", dlqTopic)
			}
		}
	}
}
