package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/shawnstephens/badgerbox/cmd/badgerbox-demo/internal/demo"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafka"
	"github.com/shawnstephens/badgerbox/pkg/maintenance"
	"github.com/shawnstephens/badgerbox/pkg/runner"
	"github.com/shawnstephens/badgerbox/pkg/telemetry"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	cli "github.com/urfave/cli/v3"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newBenchmarkCommand() *cli.Command {
	flags := []cli.Flag{
		&cli.IntFlag{Name: "messages", Value: 10000, Usage: "Exact number of messages to enqueue"},
		&cli.IntFlag{Name: "payload-bytes", Value: 1024, Usage: "Exact Kafka value size, at least 20; deterministic incompressible payload including sequence, timestamp and integrity checksum"},
		&cli.Float64Flag{Name: "rate", Usage: "Aggregate offered messages/second; 0 saturates enqueue workers"},
		&cli.DurationFlag{Name: "timeout", Value: 2 * time.Minute, Usage: "Deadline for startup, intake, delivery and drain"},
		&cli.DurationFlag{Name: "sample-interval", Value: 100 * time.Millisecond, Usage: "CPU, RSS, heap and apparent disk sampling interval"},
		&cli.DurationFlag{Name: "delivery-delay", Usage: "Local sink delay per batch (ignored for Kafka)"},
		&cli.DurationFlag{Name: "outage", Usage: "Local sink returns retryable errors for this long after intake starts"},
		&cli.IntFlag{Name: "fail-every", Usage: "Local sink fails every Nth message on its first attempt; 0 disables"},
		&cli.StringFlag{Name: "output", Value: "-", Usage: "JSON report path; - writes stdout, including reports from failed runs"},
		&cli.IntFlag{Name: "topic-partitions", Value: 3, Usage: "Partitions in a newly created unique Kafka benchmark topic"},
	}
	for _, flag := range newProducerCommand().Flags {
		name := flag.Names()[0]
		if strings.HasPrefix(name, "badger-") || name == "brokers" || name == "processor-concurrency" || name == "processor-claim-batch-size" || name == "enqueue-parallelism" || name == "poll-interval" || name == "lease-duration" || name == "publish-timeout" || name == "retry-base-delay" || name == "retry-max-delay" {
			flags = append(flags, flag)
		}
	}
	flags = append(flags, &cli.StringFlag{Name: "db-path", Usage: "Parent directory for a fresh isolated database; blank uses the OS temporary directory. The run directory is retained in the report"})
	return &cli.Command{Name: "benchmark", Usage: "Run a bounded disk-backed outbox benchmark, verify delivery and emit JSON; --brokers enables Kafka consumer verification", Flags: flags, Action: runBenchmark}
}

type benchmarkConfig struct {
	Processor        badgerbox.BatchProcessorOptions `json:"processor"`
	Maintenance      maintenance.Options             `json:"maintenance"`
	Messages         int                             `json:"messages"`
	PayloadBytes     int                             `json:"payload_bytes"`
	Rate             float64                         `json:"offered_messages_per_second"`
	EnqueueWorkers   int                             `json:"enqueue_workers"`
	ProcessorWorkers int                             `json:"processor_workers"`
	ClaimBatchSize   int                             `json:"claim_batch_size"`
	Transport        string                          `json:"transport"`
	Timeout          string                          `json:"timeout"`
	SampleInterval   string                          `json:"sample_interval"`
	DeliveryDelay    string                          `json:"delivery_delay"`
	Outage           string                          `json:"outage"`
	FailEvery        int                             `json:"fail_every"`
	Badger           any                             `json:"badger"`
}
type benchmarkReport struct {
	SchemaVersion       int                     `json:"schema_version"`
	StartedAt           time.Time               `json:"started_at"`
	Environment         map[string]any          `json:"environment"`
	Config              benchmarkConfig         `json:"config"`
	DBPath              string                  `json:"db_path"`
	Topic               string                  `json:"topic,omitempty"`
	Passed              bool                    `json:"passed"`
	Error               string                  `json:"error,omitempty"`
	Accepted            int64                   `json:"accepted"`
	UniqueDelivered     int64                   `json:"unique_delivered"`
	DuplicateDeliveries int64                   `json:"duplicate_deliveries"`
	InvalidDeliveries   int64                   `json:"invalid_deliveries"`
	PublishAttempts     int64                   `json:"publish_attempts"`
	EnqueueSeconds      float64                 `json:"enqueue_seconds"`
	DeliverySeconds     float64                 `json:"delivery_seconds"`
	EnqueuePerSecond    float64                 `json:"enqueue_messages_per_second"`
	DeliveryPerSecond   float64                 `json:"delivery_messages_per_second"`
	PayloadMiBPerSecond float64                 `json:"delivery_payload_mib_per_second"`
	EnqueueLatency      latencySummary          `json:"enqueue_latency_seconds"`
	DeliveryLatency     latencySummary          `json:"delivery_latency_seconds"`
	Resources           benchmarkResources      `json:"resources"`
	Queue               badgerbox.QueueSnapshot `json:"final_queue"`
	Audit               *badgerbox.AuditReport  `json:"final_audit,omitempty"`
	Metrics             map[string]float64      `json:"metric_totals"`
}

func runBenchmark(ctx context.Context, cmd *cli.Command) (resultErr error) {
	count, size := cmd.Int("messages"), cmd.Int("payload-bytes")
	workers, concurrency, batch := cmd.Int("enqueue-parallelism"), cmd.Int("processor-concurrency"), cmd.Int("processor-claim-batch-size")
	rate := cmd.Float64("rate")
	if count < 1 || size < 20 || workers < 1 || concurrency < 1 || batch < 1 || cmd.Int("topic-partitions") < 1 {
		return errors.New("messages, enqueue-parallelism, processor-concurrency, processor-claim-batch-size and topic-partitions must be positive; payload-bytes must be at least 20")
	}
	if rate < 0 || math.IsNaN(rate) || math.IsInf(rate, 0) || cmd.Int("fail-every") < 0 || cmd.Duration("delivery-delay") < 0 || cmd.Duration("outage") < 0 {
		return errors.New("rate, fail-every, delivery-delay and outage must be finite and nonnegative")
	}
	for _, name := range []string{"timeout", "sample-interval", "poll-interval", "lease-duration", "publish-timeout", "retry-base-delay", "retry-max-delay", "badger-gc-interval"} {
		if cmd.Duration(name) <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	if rate > 0 && float64(count-1)/rate >= float64(math.MaxInt64)/float64(time.Second) {
		return errors.New("offered schedule exceeds maximum supported duration; increase rate or reduce messages")
	}
	if cmd.Duration("retry-max-delay") < cmd.Duration("retry-base-delay") {
		return errors.New("retry-max-delay must be at least retry-base-delay")
	}
	if cmd.String("brokers") != "" && (cmd.Duration("delivery-delay") != 0 || cmd.Duration("outage") != 0 || cmd.Int("fail-every") != 0) {
		return errors.New("delivery-delay, outage and fail-every are only supported by the local sink")
	}
	if cmd.String("brokers") != "" && len(demo.ParseBrokers(cmd.String("brokers"))) == 0 {
		return errors.New("brokers must contain at least one nonempty Kafka broker address")
	}
	opts, err := producerBadgerOptions(cmd)
	if err != nil {
		return err
	}
	if opts.NumCompactors == 0 {
		return errors.New("benchmark requires --badger-num-compactors to be at least 2; disabled compaction can stall writes beyond the timeout")
	}
	parent := cmd.String("db-path")
	if parent != "" {
		if err := os.MkdirAll(parent, 0700); err != nil {
			return err
		}
	}
	path, err := os.MkdirTemp(parent, "badgerbox-benchmark-")
	if err != nil {
		return err
	}
	opts = opts.WithDir(path).WithValueDir(path)
	report := benchmarkReport{SchemaVersion: 1, StartedAt: time.Now().UTC(), DBPath: path, Environment: map[string]any{"go_version": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "logical_cpus": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0), "gomemlimit_env": os.Getenv("GOMEMLIMIT"), "gogc_env": os.Getenv("GOGC")}}
	if info, ok := debug.ReadBuildInfo(); ok {
		report.Environment["build_settings"] = info.Settings
		report.Environment["dependencies"] = info.Deps
	}
	report.Config = benchmarkConfig{Messages: count, PayloadBytes: size, Rate: rate, EnqueueWorkers: workers, ProcessorWorkers: concurrency, ClaimBatchSize: batch, Transport: "local-verified-sink", Timeout: cmd.Duration("timeout").String(), SampleInterval: cmd.Duration("sample-interval").String(), DeliveryDelay: cmd.Duration("delivery-delay").String(), Outage: cmd.Duration("outage").String(), FailEvery: cmd.Int("fail-every"), Badger: opts}
	verificationComplete := false
	defer finalizeBenchmarkReport(cmd, &report, &verificationComplete, &resultErr)
	signalCtx, stopSignals := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	runCtx, cancel := context.WithTimeout(signalCtx, cmd.Duration("timeout"))
	defer cancel()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { resultErr = errors.Join(resultErr, provider.Shutdown(context.Background())) }()
	service, err := runner.Open(runCtx, runner.Options{Badger: opts, Telemetry: telemetry.Options{MeterProvider: provider, PollInterval: time.Second}, Maintenance: maintenance.Options{FlattenOnStartup: cmd.Bool("badger-compact-on-startup"), ValueLogGCInterval: cmd.Duration("badger-gc-interval"), ValueLogGCDiscardRatio: demo.DefaultBadgerGCDiscardRatio}})
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, service.Shutdown(shutdownCtx))
		disk, err := benchmarkDiskSize(path)
		report.Resources.FinalDisk = disk
		resultErr = errors.Join(resultErr, err)
	}()
	verifier := newBenchmarkVerifier(count, size)
	var attempts, accepted atomic.Int64
	var started time.Time
	var consumer *kgo.Client
	var producer *kgo.Client
	delivery := badgerbox.BatchProcessFunc[kafka.KafkaMessage, kafka.KafkaDestination](func(ctx context.Context, messages []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], results chan<- badgerbox.BatchProcessResult) error {
		if delay := cmd.Duration("delivery-delay"); delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		for _, msg := range messages {
			attempts.Add(1)
			seq := binary.LittleEndian.Uint64(msg.Payload.Value)
			fail := (cmd.Duration("outage") > 0 && time.Since(started) < cmd.Duration("outage")) || (cmd.Int("fail-every") > 0 && seq%uint64(cmd.Int("fail-every")) == 0 && msg.Attempt == 1)
			result := badgerbox.BatchProcessResult{ID: msg.ID}
			if fail {
				result.Err = errors.New("benchmark injected transient delivery failure")
			} else {
				verifier.receive(msg.Payload.Value, time.Now())
			}
			select {
			case results <- result:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	if brokers := demo.ParseBrokers(cmd.String("brokers")); len(brokers) > 0 {
		report.Config.Transport = "kafka-consumer-verified"
		report.Topic = fmt.Sprintf("badgerbox-benchmark-%d-%d", os.Getpid(), time.Now().UnixNano())
		producer, err = demo.NewKafkaClient(brokers)
		if err != nil {
			return err
		}
		if err = service.RegisterDelivery("kafka", runner.DeliveryHooks{Flush: producer.Flush, Close: func() error { producer.Close(); return nil }}); err != nil {
			producer.Close()
			return err
		}
		if err = demo.CreateTopic(runCtx, producer, report.Topic, int32(cmd.Int("topic-partitions"))); err != nil {
			return err
		}
		partitions := make(map[int32]kgo.Offset)
		for i := range cmd.Int("topic-partitions") {
			partitions[int32(i)] = kgo.NewOffset().AtStart()
		}
		consumer, err = demo.NewKafkaClient(brokers, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{report.Topic: partitions}), kgo.FetchMaxWait(100*time.Millisecond))
		if err != nil {
			return err
		}
		defer consumer.Close()
		send, err := kafka.NewBatchProducerFunc(producer)
		if err != nil {
			return err
		}
		delivery = func(ctx context.Context, messages []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], results chan<- badgerbox.BatchProcessResult) error {
			attempts.Add(int64(len(messages)))
			return send(ctx, messages, results)
		}
	}
	delivery = demo.NewBatchProcessFunc(benchmarkBatchPublisher{fn: delivery}, cmd.Duration("publish-timeout"), nil)
	report.Config.Processor = badgerbox.BatchProcessorOptions{ClaimBatchSize: batch, ProcessorOptions: badgerbox.ProcessorOptions{Concurrency: concurrency, PollInterval: cmd.Duration("poll-interval"), LeaseDuration: cmd.Duration("lease-duration"), RetryBaseDelay: cmd.Duration("retry-base-delay"), RetryMaxDelay: cmd.Duration("retry-max-delay"), MaxAttempts: 36, RequeuePageSize: 64, SettlementTimeout: 10 * time.Second}}
	report.Config.Maintenance = maintenance.Options{FlattenOnStartup: cmd.Bool("badger-compact-on-startup"), ValueLogGCInterval: cmd.Duration("badger-gc-interval"), ValueLogGCDiscardRatio: demo.DefaultBadgerGCDiscardRatio}
	store, err := runner.Register(service, badgerbox.Serde[kafka.KafkaMessage, kafka.KafkaDestination]{}, runner.QueueOptions{Store: badgerbox.Options{Namespace: "benchmark"}, Processor: report.Config.Processor}, delivery)
	if err != nil {
		return err
	}
	defer func() {
		finalCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		snapshot, err := store.QueueSnapshot(finalCtx)
		if err == nil {
			report.Queue = snapshot
		} else {
			resultErr = errors.Join(resultErr, err)
		}
		var metrics metricdata.ResourceMetrics
		if err := reader.Collect(finalCtx, &metrics); err == nil {
			report.Metrics = benchmarkMetricTotals(metrics)
		} else {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	// Include verifier and consumer overhead in the measured process. Database open
	// and close are excluded from throughput; sampled RSS includes their allocations.
	sampler := newBenchmarkSampler(path)
	sampleDone := make(chan struct{})
	sampleStop := make(chan struct{})
	go func() {
		defer close(sampleDone)
		ticker := time.NewTicker(cmd.Duration("sample-interval"))
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sampler.sample()
			case <-sampleStop:
				return
			}
		}
	}()
	defer func() { close(sampleStop); <-sampleDone; report.Resources = sampler.finish() }()
	started = time.Now()
	if err = service.Start(runCtx); err != nil {
		return err
	}
	intakeCtx, stopIntake := context.WithCancel(runCtx)
	defer stopIntake()
	errs := make(chan error, workers+1)
	var consumerWG sync.WaitGroup
	stopConsumer := func() {}
	if consumer != nil {
		consumeCtx, cancelConsumer := context.WithCancel(runCtx)
		stopConsumer = cancelConsumer
		defer func() { stopConsumer(); consumerWG.Wait() }()
		consumerWG.Go(func() {
			for consumeCtx.Err() == nil {
				fetches := consumer.PollFetches(consumeCtx)
				if err := demo.FirstFatalFetchError(fetches.Errors()); err != nil {
					if consumeCtx.Err() == nil {
						errs <- err
					}
					return
				}
				fetches.EachRecord(func(record *kgo.Record) {
					verifier.receive(record.Value, time.Now())
					verifier.mu.Lock()
					verifier.positions[record.Partition] = record.Offset + 1
					verifier.mu.Unlock()
				})
			}
		})
	}
	var next atomic.Int64
	var intake sync.WaitGroup
	for range workers {
		intake.Go(func() {
			for {
				seq := next.Add(1) - 1
				if seq >= int64(count) {
					return
				}
				if rate > 0 {
					wait := time.Until(started.Add(time.Duration(float64(seq) / rate * float64(time.Second))))
					if wait > 0 {
						timer := time.NewTimer(wait)
						select {
						case <-timer.C:
						case <-intakeCtx.Done():
							timer.Stop()
							return
						}
					}
				}
				if intakeCtx.Err() != nil {
					return
				}
				payload := benchmarkPayload(uint64(seq), size, time.Now())
				before := time.Now()
				_, err := store.Enqueue(intakeCtx, badgerbox.EnqueueRequest[kafka.KafkaMessage, kafka.KafkaDestination]{Payload: kafka.KafkaMessage{Key: payload[:8], Value: payload}, Destination: kafka.KafkaDestination{Topic: report.Topic}})
				if err != nil {
					errs <- err
					stopIntake()
					return
				}
				accepted.Add(1)
				verifier.mu.Lock()
				verifier.enqueue.add(time.Since(before))
				verifier.mu.Unlock()
			}
		})
	}
	intakeDone := make(chan struct{})
	go func() { intake.Wait(); close(intakeDone) }()
	defer func() {
		stopIntake()
		<-intakeDone
		stopCtx, cancelStop := context.WithTimeout(context.Background(), 15*time.Second)
		resultErr = errors.Join(resultErr, service.Stop(stopCtx))
		cancelStop()
		stopConsumer()
		consumerWG.Wait()
		report.Accepted = accepted.Load()
		report.PublishAttempts = attempts.Load()
		verifier.mu.Lock()
		defer verifier.mu.Unlock()
		report.UniqueDelivered = verifier.unique
		report.DuplicateDeliveries = verifier.duplicates
		report.InvalidDeliveries = verifier.invalid
		report.EnqueueLatency = verifier.enqueue.summary()
		report.DeliveryLatency = verifier.delivery.summary()
	}()
	select {
	case <-intakeDone:
	case err = <-errs:
		return err
	case err = <-service.Errors():
		if err == nil {
			err = errors.New("runner stopped before intake completed")
		}
		return err
	case <-runCtx.Done():
		return runCtx.Err()
	}
	report.EnqueueSeconds = time.Since(started).Seconds()
	report.EnqueuePerSecond = float64(accepted.Load()) / report.EnqueueSeconds
	var endOffsets map[int32]int64
	var lastSnapshot time.Time
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err = <-errs:
			return err
		case err = <-service.Errors():
			if err == nil {
				err = errors.New("runner stopped before drain")
			}
			return err
		case <-runCtx.Done():
			return runCtx.Err()
		case <-ticker.C:
		}
		verifier.mu.Lock()
		waitingForDelivery := verifier.unique < accepted.Load()
		verifier.mu.Unlock()
		if waitingForDelivery && time.Since(lastSnapshot) < cmd.Duration("sample-interval") {
			continue
		}
		lastSnapshot = time.Now()
		snapshot, err := store.QueueSnapshot(runCtx)
		if err != nil {
			return err
		}
		report.Queue = snapshot
		if snapshot.DeadLetterDepth > 0 {
			return fmt.Errorf("benchmark dead-lettered %d messages", snapshot.DeadLetterDepth)
		}
		if snapshot.ReadyDepth+snapshot.ProcessingDepth != 0 {
			continue
		}
		if producer != nil && endOffsets == nil {
			if err = producer.Flush(runCtx); err != nil {
				return err
			}
			endOffsets, err = benchmarkEndOffsets(runCtx, producer, report.Topic, cmd.Int("topic-partitions"))
			if err != nil {
				return err
			}
		}
		verifier.mu.Lock()
		unique, invalid := verifier.unique, verifier.invalid
		consumed := true
		for partition, end := range endOffsets {
			if verifier.positions[partition] < end {
				consumed = false
			}
		}
		verifier.mu.Unlock()
		if invalid > 0 {
			return fmt.Errorf("benchmark received %d corrupt or foreign messages", invalid)
		}
		if !consumed {
			continue
		}
		if unique != accepted.Load() {
			return fmt.Errorf("delivery conservation failed: accepted=%d unique_delivered=%d", accepted.Load(), unique)
		}
		if accepted.Load() != int64(count) {
			return fmt.Errorf("intake incomplete: accepted=%d requested=%d", accepted.Load(), count)
		}
		report.DeliverySeconds = time.Since(started).Seconds()
		report.DeliveryPerSecond = float64(unique) / report.DeliverySeconds
		report.PayloadMiBPerSecond = float64(unique) * float64(size) / (1 << 20) / report.DeliverySeconds
		audit, err := store.Audit(runCtx, badgerbox.AuditOptions{})
		report.Audit = &audit
		if err != nil {
			return err
		}
		if !audit.Complete || audit.LiveRows != 0 || audit.DeadLetters.Rows != 0 || len(audit.Samples.Anomalies) != 0 {
			return errors.New("final database audit did not prove an empty consistent queue")
		}
		var metrics metricdata.ResourceMetrics
		if err = reader.Collect(runCtx, &metrics); err != nil {
			return err
		}
		report.Metrics = benchmarkMetricTotals(metrics)
		if report.Metrics["badgerbox_enqueue_total"] != float64(count) {
			return errors.New("enqueue metric does not match accepted count")
		}
		verificationComplete = true
		return nil
	}
}

// This must be deferred directly so it observes panics from the run or its
// cleanup. Preserve the panic after writing failure evidence instead of treating
// a still-nil named return error as successful verification.
func finalizeBenchmarkReport(cmd *cli.Command, report *benchmarkReport, verificationComplete *bool, resultErr *error) {
	if recovered := recover(); recovered != nil {
		*resultErr = errors.Join(*resultErr, fmt.Errorf("benchmark panicked: %v", recovered))
		defer func() { panic(recovered) }()
	}
	report.Passed = *verificationComplete && *resultErr == nil
	if *resultErr != nil {
		report.Error = (*resultErr).Error()
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		*resultErr = errors.Join(*resultErr, err)
		return
	}
	data = append(data, '\n')
	if cmd.String("output") == "-" {
		_, err = cmd.Root().Writer.Write(data)
	} else {
		err = os.WriteFile(cmd.String("output"), data, 0600)
	}
	*resultErr = errors.Join(*resultErr, err)
}

func benchmarkMetricTotals(metrics metricdata.ResourceMetrics) map[string]float64 {
	result := make(map[string]float64)
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			switch data := metric.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					result[metric.Name] += float64(point.Value)
				}
			case metricdata.Sum[float64]:
				for _, point := range data.DataPoints {
					result[metric.Name] += point.Value
				}
			}
		}
	}
	return result
}
func benchmarkEndOffsets(ctx context.Context, client *kgo.Client, topic string, partitions int) (map[int32]int64, error) {
	request := kmsg.NewPtrListOffsetsRequest()
	t := kmsg.NewListOffsetsRequestTopic()
	t.Topic = topic
	for i := range partitions {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition = int32(i)
		p.Timestamp = -1
		t.Partitions = append(t.Partitions, p)
	}
	request.Topics = append(request.Topics, t)
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return nil, err
	}
	offsets := make(map[int32]int64)
	for _, t := range response.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				return nil, fmt.Errorf("kafka list offsets partition %d: error code %d", p.Partition, p.ErrorCode)
			}
			offsets[p.Partition] = p.Offset
		}
	}
	if len(offsets) != partitions {
		return nil, errors.New("kafka did not return every partition end offset")
	}
	return offsets, nil
}

// The demo adapter owns callback forwarding and publish-timeout cancellation.
type benchmarkBatchPublisher struct {
	fn badgerbox.BatchProcessFunc[kafka.KafkaMessage, kafka.KafkaDestination]
}

func (p benchmarkBatchPublisher) Deliver(ctx context.Context, messages []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], results chan<- badgerbox.BatchProcessResult) error {
	return p.fn(ctx, messages, results)
}
func (benchmarkBatchPublisher) Flush(context.Context) error { return nil }
func (benchmarkBatchPublisher) Close() error                { return nil }
