package app

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func Run() {
	cfg, err := loadConfig()
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
		<-sigChan
		sugar.Info("Received shutdown signal")
		cancel()
	}()

	var srClient schemaregistry.Client
	if anyTopicUsesFormat(cfg, "avro") {
		// Shared Schema Registry client
		srClient, err = schemaregistry.NewClient(schemaregistry.NewConfig(cfg.SchemaRegistry))
		if err != nil {
			sugar.Fatalf("Failed to create Schema Registry client: %v", err)
		}
	}

	// Shared ClickHouse connection
	chOptions, err := clickhouse.ParseDSN(cfg.ClickHouseDSN)
	if err != nil {
		sugar.Fatalf("Failed to parse ClickHouse DSN: %v", err)
	}
	chConn, err := clickhouse.Open(chOptions)
	if err != nil {
		sugar.Fatalf("Failed to open ClickHouse connection: %v", err)
	}
	defer chConn.Close()

	if err := chConn.Ping(context.Background()); err != nil {
		sugar.Fatalf("Failed to ping ClickHouse: %v", err)
	}
	sugar.Info("Connected to ClickHouse")

	// Shared DLQ producer
	dlqKafkaConfig := buildKafkaConfig(kafka.ConfigMap{
		"bootstrap.servers": cfg.KafkaBrokers,
	}, cfg)
	dlqProducer, err := kafka.NewProducer(&dlqKafkaConfig)
	if err != nil {
		sugar.Fatalf("Failed to create Kafka producer for DLQ: %v", err)
	}
	defer dlqProducer.Close()

	// Drain the producer's Events channel so its internal queue never fills up.
	go func() {
		for range dlqProducer.Events() {
		}
	}()

	// Create and start sink tasks
	var wg sync.WaitGroup
	var consumers []*kafka.Consumer

	for _, mapping := range cfg.TopicTables {
		task, err := NewSinkTask(mapping, cfg, chConn, srClient, dlqProducer, cancel, sugar)
		if err != nil {
			sugar.Fatalf("Failed to create sink task for topic %s: %v", mapping.Topic, err)
		}

		consumers = append(consumers, task.consumer)
		wg.Add(1)
		go task.Run(ctx, &wg)

		sugar.Infof("Started sink task: topic=%s table=%s format=%s", mapping.Topic, mapping.Table, mapping.Format)
	}

	// Start metrics and health server. If it fails to bind (e.g. port conflict), cancel the
	// context so the process exits cleanly rather than running silently without observability.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	health := NewHealth(sugar, chConn, consumers)
	RegisterHealthEndpoints(health, mux)

	addr := fmt.Sprintf(":%d", cfg.MetricsPort)
	go func() {
		sugar.Infof("Starting metrics and health server on %s", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			sugar.Errorf("Metrics/health server failed: %v — triggering shutdown", err)
			cancel()
		}
	}()

	sugar.Info("All sink tasks started, waiting for shutdown signal")
	wg.Wait()
	sugar.Info("All sink tasks stopped, exiting")
}
