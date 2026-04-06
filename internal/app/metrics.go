package app

import "github.com/prometheus/client_golang/prometheus"

// All metrics are namespaced under "kahouse" to avoid collisions with other applications.
var (
	msgConsumed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "msg_consumed_total",
		Help:      "Total number of messages consumed from Kafka",
	}, []string{"topic"})
	msgProduced = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "msg_produced_total",
		Help:      "Total number of messages written to ClickHouse",
	}, []string{"topic"})
	msgFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "msg_failed_total",
		Help:      "Total number of messages that failed processing (deserialization errors and batch write failures)",
	}, []string{"topic"})
	msgDLQ = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kahouse",
		Name:      "msg_dlq_total",
		Help:      "Total number of messages sent to the dead letter queue",
	}, []string{"topic"})
	batchSizeHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kahouse",
		Name:      "batch_size",
		Help:      "Number of messages per flushed batch",
		Buckets:   prometheus.ExponentialBuckets(10, 2, 10),
	}, []string{"topic"})
	batchDelayHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kahouse",
		Name:      "batch_delay_seconds",
		Help:      "Age of the oldest message in a batch at flush time (wall-clock time from first message arrival to flush)",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 10),
	}, []string{"topic"})
	processLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kahouse",
		Name:      "process_latency_seconds",
		Help:      "Time spent writing a batch to ClickHouse (includes retry backoff)",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 10),
	}, []string{"topic"})
	retryCountHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kahouse",
		Name:      "write_retry_count",
		Help:      "Number of retry attempts per ClickHouse batch write",
		Buckets:   prometheus.LinearBuckets(0, 1, 11),
	}, []string{"topic"})
)

func init() {
	prometheus.MustRegister(
		msgConsumed,
		msgProduced,
		msgFailed,
		msgDLQ,
		batchSizeHist,
		batchDelayHist,
		processLatency,
		retryCountHist,
	)
}
