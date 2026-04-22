package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/demo/internal/demo"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafkaoutbox"
	"github.com/twmb/franz-go/pkg/kgo"
	cli "github.com/urfave/cli/v3"
)

func main() {
	if err := newRootCommand().Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "badgerbox-demo: %v\n", err)
		os.Exit(1)
	}
}

func newRootCommand() *cli.Command {
	return &cli.Command{
		Name:  "badgerbox-demo",
		Usage: "Run a local badgerbox + Kafka demo with producer and consumer processes",
		Commands: []*cli.Command{
			newKafkaCommand(),
			newProducerCommand(),
			newRepairQueueStateCommand(),
			newConsumerCommand(),
		},
	}
}

func newKafkaCommand() *cli.Command {
	return &cli.Command{
		Name:  "kafka",
		Usage: "Start a Kafka broker in Testcontainers and write shared state for producer and consumer processes",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "state-file",
				Usage:   "Path to the shared demo state file",
				Value:   demo.DefaultStateFile,
				Sources: cli.EnvVars("BADGERBOX_DEMO_STATE_FILE"),
			},
			&cli.StringFlag{
				Name:    "topic",
				Usage:   "Kafka topic for the demo",
				Value:   demo.DefaultTopic,
				Sources: cli.EnvVars("BADGERBOX_DEMO_TOPIC"),
			},
			&cli.IntFlag{
				Name:    "topic-partitions",
				Usage:   "Number of partitions to create for the demo topic",
				Value:   demo.DefaultTopicPartitions,
				Sources: cli.EnvVars("BADGERBOX_DEMO_TOPIC_PARTITIONS"),
			},
			&cli.StringFlag{
				Name:    "kafka-image",
				Usage:   "Kafka container image",
				Value:   demo.DefaultKafkaImage,
				Sources: cli.EnvVars("BADGERBOX_DEMO_KAFKA_IMAGE"),
			},
			&cli.StringFlag{
				Name:    "cluster-id",
				Usage:   "Kafka cluster ID for the Testcontainers broker",
				Value:   demo.DefaultClusterID,
				Sources: cli.EnvVars("BADGERBOX_DEMO_KAFKA_CLUSTER_ID"),
			},
			colorFlag(demo.ColorAuto),
		},
		Action: runKafka,
	}
}

func newProducerCommand() *cli.Command {
	badgerDefaults := demo.DefaultBadgerOptions("")

	return &cli.Command{
		Name:  "producer",
		Usage: "Run badgerbox enqueueing and processing in one process, publishing to Kafka or demo logs until stopped",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "state-file",
				Usage:   "Path to the shared demo state file",
				Value:   demo.DefaultStateFile,
				Sources: cli.EnvVars("BADGERBOX_DEMO_STATE_FILE"),
			},
			&cli.StringFlag{
				Name:    "brokers",
				Usage:   "Comma-separated Kafka broker list; overrides the shared state file",
				Sources: cli.EnvVars("BADGERBOX_DEMO_BROKERS"),
			},
			&cli.StringFlag{
				Name:    "topic",
				Usage:   "Kafka topic; defaults to the shared state file, then badgerbox-demo",
				Sources: cli.EnvVars("BADGERBOX_DEMO_TOPIC"),
			},
			&cli.BoolFlag{
				Name:    "logging-producer",
				Usage:   "Log produced messages instead of publishing to Kafka; demo-only bottleneck check",
				Sources: cli.EnvVars("BADGERBOX_DEMO_LOGGING_PRODUCER"),
			},
			&cli.StringFlag{
				Name:    "db-path",
				Usage:   "Badger directory path",
				Value:   demo.DefaultBadgerPath,
				Sources: cli.EnvVars("BADGERBOX_DEMO_DB_PATH"),
			},
			&cli.StringFlag{
				Name:        "badger-memtable-size",
				Usage:       "Override Badger MemTableSize with a human-readable size like 32MiB or 128MB",
				DefaultText: demo.FormatBytes(badgerDefaults.MemTableSize),
				Sources:     cli.EnvVars("BADGERBOX_DEMO_BADGER_MEMTABLE_SIZE"),
			},
			&cli.IntFlag{
				Name:        "badger-num-memtables",
				Usage:       "Override Badger NumMemtables",
				DefaultText: fmt.Sprintf("%d", badgerDefaults.NumMemtables),
				Sources:     cli.EnvVars("BADGERBOX_DEMO_BADGER_NUM_MEMTABLES"),
			},
			&cli.IntFlag{
				Name:        "badger-num-level-zero-tables",
				Usage:       "Override Badger NumLevelZeroTables",
				DefaultText: fmt.Sprintf("%d", badgerDefaults.NumLevelZeroTables),
				Sources:     cli.EnvVars("BADGERBOX_DEMO_BADGER_NUM_LEVEL_ZERO_TABLES"),
			},
			&cli.IntFlag{
				Name:        "badger-num-level-zero-tables-stall",
				Usage:       "Override Badger NumLevelZeroTablesStall",
				DefaultText: fmt.Sprintf("%d", badgerDefaults.NumLevelZeroTablesStall),
				Sources:     cli.EnvVars("BADGERBOX_DEMO_BADGER_NUM_LEVEL_ZERO_TABLES_STALL"),
			},
			&cli.IntFlag{
				Name:        "badger-num-compactors",
				Usage:       "Override Badger NumCompactors",
				DefaultText: fmt.Sprintf("%d", badgerDefaults.NumCompactors),
				Sources:     cli.EnvVars("BADGERBOX_DEMO_BADGER_NUM_COMPACTORS"),
			},
			&cli.StringFlag{
				Name:        "badger-base-table-size",
				Usage:       "Override Badger BaseTableSize with a human-readable size like 2MiB",
				DefaultText: demo.FormatBytes(badgerDefaults.BaseTableSize),
				Sources:     cli.EnvVars("BADGERBOX_DEMO_BADGER_BASE_TABLE_SIZE"),
			},
			&cli.StringFlag{
				Name:        "badger-value-log-file-size",
				Usage:       "Override Badger ValueLogFileSize with a human-readable size like 512MiB or 1GiB",
				DefaultText: fmt.Sprintf("%d bytes (~%s)", badgerDefaults.ValueLogFileSize, demo.FormatBytes(badgerDefaults.ValueLogFileSize)),
				Sources:     cli.EnvVars("BADGERBOX_DEMO_BADGER_VALUE_LOG_FILE_SIZE"),
			},
			&cli.StringFlag{
				Name:        "badger-block-cache-size",
				Usage:       "Override Badger BlockCacheSize with a human-readable size like 64MiB",
				DefaultText: demo.FormatBytes(badgerDefaults.BlockCacheSize),
				Sources:     cli.EnvVars("BADGERBOX_DEMO_BADGER_BLOCK_CACHE_SIZE"),
			},
			&cli.StringFlag{
				Name:        "badger-index-cache-size",
				Usage:       "Override Badger IndexCacheSize with a human-readable size like 128MiB; 0 keeps all indices in memory",
				DefaultText: "0",
				Sources:     cli.EnvVars("BADGERBOX_DEMO_BADGER_INDEX_CACHE_SIZE"),
			},
			&cli.StringFlag{
				Name:        "badger-value-threshold",
				Usage:       "Override Badger ValueThreshold with a human-readable size like 64KiB or 1MiB",
				DefaultText: demo.FormatBytes(badgerDefaults.ValueThreshold),
				Sources:     cli.EnvVars("BADGERBOX_DEMO_BADGER_VALUE_THRESHOLD"),
			},
			&cli.BoolFlag{
				Name:        "badger-sync-writes",
				Usage:       "Enable Badger SyncWrites for the extra-durable fsync-on-write path",
				DefaultText: fmt.Sprintf("%t", badgerDefaults.SyncWrites),
				Sources:     cli.EnvVars("BADGERBOX_DEMO_BADGER_SYNC_WRITES"),
			},
			&cli.StringFlag{
				Name:    "namespace",
				Usage:   "Badgerbox namespace",
				Value:   demo.DefaultNamespace,
				Sources: cli.EnvVars("BADGERBOX_DEMO_NAMESPACE"),
			},
			&cli.IntFlag{
				Name:    "enqueue-parallelism",
				Usage:   "Number of enqueue goroutines",
				Value:   1,
				Sources: cli.EnvVars("BADGERBOX_DEMO_ENQUEUE_PARALLELISM"),
			},
			&cli.DurationFlag{
				Name:    "message-interval",
				Usage:   "Wait time after each enqueue per worker; use 0ms for no delay",
				Value:   500 * time.Millisecond,
				Sources: cli.EnvVars("BADGERBOX_DEMO_MESSAGE_INTERVAL"),
			},
			&cli.IntFlag{
				Name:    "processor-concurrency",
				Usage:   "Number of badgerbox processor workers",
				Value:   4,
				Sources: cli.EnvVars("BADGERBOX_DEMO_PROCESSOR_CONCURRENCY"),
			},
			&cli.IntFlag{
				Name:    "processor-claim-batch-size",
				Usage:   "Maximum number of ready records to claim in one processor batch",
				Value:   32,
				Sources: cli.EnvVars("BADGERBOX_DEMO_PROCESSOR_CLAIM_BATCH_SIZE"),
			},
			&cli.DurationFlag{
				Name:    "retry-base-delay",
				Usage:   "Retry delay after the first failed publish",
				Value:   demo.DefaultRetryBaseDelay,
				Sources: cli.EnvVars("BADGERBOX_DEMO_RETRY_BASE_DELAY"),
			},
			&cli.DurationFlag{
				Name:    "retry-max-delay",
				Usage:   "Maximum retry delay after repeated publish failures",
				Value:   demo.DefaultRetryMaxDelay,
				Sources: cli.EnvVars("BADGERBOX_DEMO_RETRY_MAX_DELAY"),
			},
			&cli.DurationFlag{
				Name:    "poll-interval",
				Usage:   "Processor poll interval for ready and expired records",
				Value:   demo.DefaultPollInterval,
				Sources: cli.EnvVars("BADGERBOX_DEMO_POLL_INTERVAL"),
			},
			&cli.DurationFlag{
				Name:    "lease-duration",
				Usage:   "Lease duration for in-flight processing",
				Value:   demo.DefaultLeaseDuration,
				Sources: cli.EnvVars("BADGERBOX_DEMO_LEASE_DURATION"),
			},
			&cli.DurationFlag{
				Name:    "publish-timeout",
				Usage:   "Timeout for each Kafka publish attempt",
				Value:   demo.DefaultPublishTimeout,
				Sources: cli.EnvVars("BADGERBOX_DEMO_PUBLISH_TIMEOUT"),
			},
			&cli.DurationFlag{
				Name:    "badger-gc-interval",
				Usage:   "How often to run Badger value-log GC",
				Value:   demo.DefaultBadgerGCInterval,
				Sources: cli.EnvVars("BADGERBOX_DEMO_BADGER_GC_INTERVAL"),
			},
			&cli.BoolFlag{
				Name:    "badger-compact-on-startup",
				Usage:   "Block startup to run Badger Flatten plus one-shot value-log GC; use only when this producer is the sole owner of the DB",
				Sources: cli.EnvVars("BADGERBOX_DEMO_BADGER_COMPACT_ON_STARTUP"),
			},
			&cli.StringFlag{
				Name:    "producer-id",
				Usage:   "Logical producer identifier used in keys and logs",
				Sources: cli.EnvVars("BADGERBOX_DEMO_PRODUCER_ID"),
			},
			&cli.StringFlag{
				Name:    "otel-endpoint",
				Usage:   "OTLP/HTTP collector endpoint host:port; blank disables observability export",
				Sources: cli.EnvVars("BADGERBOX_DEMO_OTEL_ENDPOINT"),
			},
			&cli.StringFlag{
				Name:    "otel-service-name",
				Usage:   "OTEL service.name for demo telemetry",
				Value:   demo.DefaultOTelServiceName,
				Sources: cli.EnvVars("BADGERBOX_DEMO_OTEL_SERVICE_NAME"),
			},
			&cli.StringFlag{
				Name:    "expvar-listen-addr",
				Usage:   "Listen address for the demo expvar endpoint; blank disables the expvar server",
				Value:   demo.DefaultExpvarListenAddr,
				Sources: cli.EnvVars("BADGERBOX_DEMO_EXPVAR_LISTEN_ADDR"),
			},
			colorFlag(demo.ColorAuto),
		},
		Action: runProducer,
	}
}

func newConsumerCommand() *cli.Command {
	return &cli.Command{
		Name:  "consumer",
		Usage: "Consume Kafka messages for the demo and print them to the console",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "state-file",
				Usage:   "Path to the shared demo state file",
				Value:   demo.DefaultStateFile,
				Sources: cli.EnvVars("BADGERBOX_DEMO_STATE_FILE"),
			},
			&cli.StringFlag{
				Name:    "brokers",
				Usage:   "Comma-separated Kafka broker list; overrides the shared state file",
				Sources: cli.EnvVars("BADGERBOX_DEMO_BROKERS"),
			},
			&cli.StringFlag{
				Name:    "topic",
				Usage:   "Kafka topic; defaults to the shared state file, then badgerbox-demo",
				Sources: cli.EnvVars("BADGERBOX_DEMO_TOPIC"),
			},
			&cli.BoolFlag{
				Name:    "from-beginning",
				Usage:   "Consume from the beginning of the topic",
				Value:   true,
				Sources: cli.EnvVars("BADGERBOX_DEMO_FROM_BEGINNING"),
			},
			colorFlag(demo.ColorAuto),
		},
		Action: runConsumer,
	}
}

func newRepairQueueStateCommand() *cli.Command {
	return &cli.Command{
		Name:  "repair-queue-state",
		Usage: "Rebuild badgerbox queue-state metadata for an existing namespace; stop producers and processors first",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "db-path",
				Usage:   "Badger directory path",
				Value:   demo.DefaultBadgerPath,
				Sources: cli.EnvVars("BADGERBOX_DEMO_DB_PATH"),
			},
			&cli.StringFlag{
				Name:    "namespace",
				Usage:   "Badgerbox namespace",
				Value:   demo.DefaultNamespace,
				Sources: cli.EnvVars("BADGERBOX_DEMO_NAMESPACE"),
			},
			colorFlag(demo.ColorAuto),
		},
		Action: runRepairQueueState,
	}
}

func colorFlag(defaultMode demo.ColorMode) *cli.StringFlag {
	return &cli.StringFlag{
		Name:    "color",
		Usage:   "Color mode: auto, always, or never",
		Value:   string(defaultMode),
		Sources: cli.EnvVars("BADGERBOX_DEMO_COLOR"),
		Validator: func(mode string) error {
			switch demo.ColorMode(strings.ToLower(mode)) {
			case demo.ColorAuto, demo.ColorAlways, demo.ColorNever:
				return nil
			default:
				return fmt.Errorf("invalid color mode %q", mode)
			}
		},
	}
}

func runKafka(ctx context.Context, cmd *cli.Command) error {
	runCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := demo.NewLogger(os.Stdout, cmd.String("color"))
	stateFile := cmd.String("state-file")
	topic := cmd.String("topic")
	topicPartitions := cmd.Int("topic-partitions")
	if topicPartitions < 1 {
		return errors.New("topic-partitions must be at least 1")
	}
	image := cmd.String("kafka-image")
	clusterID := cmd.String("cluster-id")

	logger.Printf("startup", "command=kafka state_file=%s topic=%s topic_partitions=%d image=%s cluster_id=%s", stateFile, topic, topicPartitions, image, clusterID)

	container, brokers, err := demo.StartKafka(runCtx, image, clusterID)
	if err != nil {
		return fmt.Errorf("start kafka container: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), demo.DefaultKafkaTimeout())
		defer cancel()

		logger.Printf("shutdown", "command=kafka stopping_container=true container_id=%s", container.GetContainerID())
		if err := container.Terminate(shutdownCtx); err != nil {
			logger.Printf("error", "command=kafka terminate_error=%q", err)
		}
		logger.Printf("shutdown", "command=kafka state_file_preserved=true state_file=%s", stateFile)
	}()

	client, err := demo.NewKafkaClient(brokers)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()

	topicCtx, cancel := context.WithTimeout(runCtx, demo.DefaultKafkaTimeout())
	defer cancel()
	if err := demo.CreateTopic(topicCtx, client, topic, int32(topicPartitions)); err != nil {
		return err
	}

	state := demo.State{
		Version:     1,
		Brokers:     brokers,
		Topic:       topic,
		KafkaImage:  image,
		StartedAt:   time.Now().UTC(),
		ContainerID: container.GetContainerID(),
	}
	if err := demo.WriteState(stateFile, state); err != nil {
		return err
	}

	logger.Printf("ready", "command=kafka brokers=%s topic=%s topic_partitions=%d state_file=%s container_id=%s", demo.ShortBrokerList(brokers), topic, topicPartitions, stateFile, container.GetContainerID())
	logger.Printf("ready", "command=kafka next=\"go run ./demo producer\"")
	logger.Printf("ready", "command=kafka next=\"go run ./demo consumer\"")

	<-runCtx.Done()
	return nil
}

func runProducer(ctx context.Context, cmd *cli.Command) error {
	runCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := demo.NewLogger(os.Stdout, cmd.String("color"))
	stateFile := cmd.String("state-file")
	loggingProducer := cmd.Bool("logging-producer")

	target, err := resolveProducerTarget(cmd.String("brokers"), cmd.String("topic"), stateFile, loggingProducer)
	if err != nil {
		return err
	}

	dbPath := cmd.String("db-path")
	namespace := cmd.String("namespace")
	enqueueParallelism := cmd.Int("enqueue-parallelism")
	if enqueueParallelism < 1 {
		return errors.New("enqueue-parallelism must be at least 1")
	}
	messageInterval := cmd.Duration("message-interval")
	if messageInterval < 0 {
		return errors.New("message-interval must be greater than or equal to 0")
	}
	processorConcurrency := cmd.Int("processor-concurrency")
	if processorConcurrency < 1 {
		return errors.New("processor-concurrency must be at least 1")
	}
	processorClaimBatchSize := cmd.Int("processor-claim-batch-size")
	if processorClaimBatchSize < 1 {
		return errors.New("processor-claim-batch-size must be at least 1")
	}
	retryBaseDelay := cmd.Duration("retry-base-delay")
	if retryBaseDelay <= 0 {
		return errors.New("retry-base-delay must be greater than 0")
	}
	retryMaxDelay := cmd.Duration("retry-max-delay")
	if retryMaxDelay <= 0 {
		return errors.New("retry-max-delay must be greater than 0")
	}
	if retryMaxDelay < retryBaseDelay {
		return errors.New("retry-max-delay must be greater than or equal to retry-base-delay")
	}
	pollInterval := cmd.Duration("poll-interval")
	if pollInterval <= 0 {
		return errors.New("poll-interval must be greater than 0")
	}
	leaseDuration := cmd.Duration("lease-duration")
	if leaseDuration <= 0 {
		return errors.New("lease-duration must be greater than 0")
	}
	publishTimeout := cmd.Duration("publish-timeout")
	if publishTimeout <= 0 {
		return errors.New("publish-timeout must be greater than 0")
	}
	badgerGCInterval := cmd.Duration("badger-gc-interval")
	if badgerGCInterval <= 0 {
		return errors.New("badger-gc-interval must be greater than 0")
	}
	badgerCompactOnStartup := cmd.Bool("badger-compact-on-startup")
	producerID := cmd.String("producer-id")
	if producerID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "localhost"
		}
		producerID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	otelConfig := demo.OTelConfig{
		Endpoint:    cmd.String("otel-endpoint"),
		ServiceName: cmd.String("otel-service-name"),
	}
	expvarListenAddr := cmd.String("expvar-listen-addr")

	badgerOpts, err := demo.BuildBadgerOptions(dbPath, demo.BadgerOptionsOverrides{
		SyncWritesSet:              cmd.IsSet("badger-sync-writes"),
		SyncWrites:                 cmd.Bool("badger-sync-writes"),
		MemTableSizeSet:            cmd.IsSet("badger-memtable-size"),
		MemTableSize:               cmd.String("badger-memtable-size"),
		NumMemtablesSet:            cmd.IsSet("badger-num-memtables"),
		NumMemtables:               cmd.Int("badger-num-memtables"),
		NumLevelZeroTablesSet:      cmd.IsSet("badger-num-level-zero-tables"),
		NumLevelZeroTables:         cmd.Int("badger-num-level-zero-tables"),
		NumLevelZeroTablesStallSet: cmd.IsSet("badger-num-level-zero-tables-stall"),
		NumLevelZeroTablesStall:    cmd.Int("badger-num-level-zero-tables-stall"),
		NumCompactorsSet:           cmd.IsSet("badger-num-compactors"),
		NumCompactors:              cmd.Int("badger-num-compactors"),
		BaseTableSizeSet:           cmd.IsSet("badger-base-table-size"),
		BaseTableSize:              cmd.String("badger-base-table-size"),
		ValueLogFileSizeSet:        cmd.IsSet("badger-value-log-file-size"),
		ValueLogFileSize:           cmd.String("badger-value-log-file-size"),
		BlockCacheSizeSet:          cmd.IsSet("badger-block-cache-size"),
		BlockCacheSize:             cmd.String("badger-block-cache-size"),
		IndexCacheSizeSet:          cmd.IsSet("badger-index-cache-size"),
		IndexCacheSize:             cmd.String("badger-index-cache-size"),
		ValueThresholdSet:          cmd.IsSet("badger-value-threshold"),
		ValueThreshold:             cmd.String("badger-value-threshold"),
	})
	if err != nil {
		return fmt.Errorf("build badger options: %w", err)
	}

	publishTransport := "kafka"
	if loggingProducer {
		publishTransport = "logging"
	}
	brokerSummary := demo.ShortBrokerList(target.Brokers)
	if brokerSummary == "" {
		brokerSummary = "-"
	}

	logger.Printf("startup", "command=producer publish_transport=%s brokers=%s brokers_source=%s topic=%s topic_source=%s db_path=%s namespace=%s enqueue_parallelism=%d processor_concurrency=%d processor_claim_batch_size=%d interval=%s retry_base_delay=%s retry_max_delay=%s poll_interval=%s lease_duration=%s publish_timeout=%s badger_gc_interval=%s badger_gc_discard_ratio=%.2f badger_compact_on_startup=%t badger_sync_writes=%t badger_memtable_size=%s badger_num_memtables=%d badger_num_level_zero_tables=%d badger_num_level_zero_tables_stall=%d badger_num_compactors=%d badger_base_table_size=%s badger_value_log_file_size=%s badger_block_cache_size=%s badger_index_cache_size=%s badger_value_threshold=%s producer_id=%s otel_endpoint=%s expvar_listen_addr=%s", publishTransport, brokerSummary, target.BrokersSource, target.Topic, target.TopicSource, dbPath, namespace, enqueueParallelism, processorConcurrency, processorClaimBatchSize, messageInterval, retryBaseDelay, retryMaxDelay, pollInterval, leaseDuration, publishTimeout, badgerGCInterval, demo.DefaultBadgerGCDiscardRatio, badgerCompactOnStartup, badgerOpts.SyncWrites, demo.FormatBytes(badgerOpts.MemTableSize), badgerOpts.NumMemtables, badgerOpts.NumLevelZeroTables, badgerOpts.NumLevelZeroTablesStall, badgerOpts.NumCompactors, demo.FormatBytes(badgerOpts.BaseTableSize), demo.FormatBytes(badgerOpts.ValueLogFileSize), demo.FormatBytes(badgerOpts.BlockCacheSize), demo.FormatBytes(badgerOpts.IndexCacheSize), demo.FormatBytes(badgerOpts.ValueThreshold), producerID, otelConfig.Endpoint, expvarListenAddr)

	observability, shutdownOTel, err := demo.SetupOTel(runCtx, otelConfig)
	if err != nil {
		return fmt.Errorf("setup otel: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownOTel(shutdownCtx); err != nil {
			logger.Printf("warning", "event=otel_shutdown_failed err=%q", err)
		}
	}()
	if otelConfig.Enabled() {
		logger.Printf("ready", "event=otel_export enabled=true endpoint=%s service_name=%s", otelConfig.Endpoint, otelConfig.ServiceName)
	}

	expvarServer, expvarAddr, expvarErrCh, err := demo.StartExpvarServer(expvarListenAddr)
	if err != nil {
		return fmt.Errorf("start expvar server: %w", err)
	}
	if expvarServer != nil {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := expvarServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Printf("warning", "event=expvar_shutdown_failed err=%q", err)
			}
		}()
		logger.Printf("ready", "event=expvar enabled=true addr=%s paths=/debug/vars,/metrics", expvarAddr)
	}

	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return fmt.Errorf("create badger directory: %w", err)
	}

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return fmt.Errorf("open badger: %w", err)
	}
	defer db.Close()

	if badgerCompactOnStartup {
		if err := demo.RunStartupCompaction(db, dbPath, badgerOpts.NumCompactors, logger); err != nil {
			return fmt.Errorf("run startup compaction: %w", err)
		}
	}

	go demo.RunValueLogGC(runCtx, db, badgerGCInterval, demo.DefaultBadgerGCDiscardRatio, logger)

	store, err := badgerbox.New[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination](
		db,
		badgerbox.Serde[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{},
		badgerbox.Options{
			Namespace:     namespace,
			Observability: observability,
		},
	)
	if err != nil {
		return fmt.Errorf("new store: %w", err)
	}
	defer store.Close()

	logger.Printf("ready", "event=backlog_preflight skipped=true namespace=%s db_path=%s note=%q", namespace, dbPath, "exact startup backlog scans are disabled; run `badgerbox-demo repair-queue-state` before large legacy DB startups")

	var publisher demo.Publisher
	if loggingProducer {
		publisher = demo.NewLoggingPublisher(logger)
	} else {
		publisher = demo.NewReloadingPublisher(stateFile, target.Brokers, target.Topic, logger)
	}
	defer publisher.Close()

	processFn := func(ctx context.Context, msg badgerbox.Message[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]) error {
		key := string(msg.Payload.Key)
		logger.Printf("process", "event=start msg_id=%d key=%s topic=%s attempt=%d", msg.ID, key, msg.Destination.Topic, msg.Attempt)
		publishCtx, cancel := context.WithTimeout(ctx, publishTimeout)
		err := publisher.Publish(publishCtx, msg)
		cancel()
		if err != nil {
			logProcessFailure(logger, time.Now().UTC(), msg, err, retryBaseDelay, retryMaxDelay)
			return err
		}
		if !loggingProducer {
			logger.Printf("publish", "event=success msg_id=%d key=%s topic=%s", msg.ID, key, msg.Destination.Topic)
		}
		return nil
	}

	processor, err := badgerbox.NewProcessor(store, processFn, badgerbox.ProcessorOptions{
		Concurrency:    processorConcurrency,
		ClaimBatchSize: processorClaimBatchSize,
		PollInterval:   pollInterval,
		LeaseDuration:  leaseDuration,
		RetryBaseDelay: retryBaseDelay,
		RetryMaxDelay:  retryMaxDelay,
	})
	if err != nil {
		return fmt.Errorf("new processor: %w", err)
	}

	processorCtx, processorCancel := context.WithCancel(runCtx)
	defer processorCancel()

	processorDone := make(chan error, 1)
	go func() {
		processorDone <- processor.Run(processorCtx)
	}()

	var sequence atomic.Uint64
	var wg sync.WaitGroup
	enqueueErrCh := make(chan error, enqueueParallelism)

	for worker := 0; worker < enqueueParallelism; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			first := true

			for {
				if !first && messageInterval > 0 {
					timer := time.NewTimer(messageInterval)
					select {
					case <-runCtx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
				first = false

				if err := runCtx.Err(); err != nil {
					return
				}

				seq := sequence.Add(1)
				payload, destination, err := demo.BuildKafkaMessage(target.Topic, producerID, worker, seq)
				if err != nil {
					select {
					case enqueueErrCh <- fmt.Errorf("build message: %w", err):
					default:
					}
					return
				}

				id, err := store.Enqueue(runCtx, badgerbox.EnqueueRequest[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{
					Payload:     payload,
					Destination: destination,
				})
				if err != nil {
					if context.Canceled == err || errors.Is(err, context.Canceled) {
						return
					}
					select {
					case enqueueErrCh <- fmt.Errorf("enqueue worker %d: %w", worker, err):
					default:
					}
					return
				}

				logger.Printf("enqueue", "worker=%d msg_id=%d seq=%d key=%s topic=%s", worker, id, seq, string(payload.Key), target.Topic)
			}
		}()
	}

	select {
	case err := <-enqueueErrCh:
		processorCancel()
		stop()
		wg.Wait()
		return err
	case err := <-processorDone:
		stop()
		wg.Wait()
		if err != nil {
			return fmt.Errorf("processor stopped: %w", err)
		}
		return nil
	case err := <-expvarErrCh:
		if err != nil {
			processorCancel()
			stop()
			wg.Wait()
			return fmt.Errorf("expvar server stopped: %w", err)
		}
	case <-runCtx.Done():
	}

	wg.Wait()
	processorCancel()
	select {
	case err := <-processorDone:
		if err != nil {
			return fmt.Errorf("processor stopped: %w", err)
		}
	case <-time.After(10 * time.Second):
		return errors.New("timed out waiting for processor shutdown")
	}

	logger.Printf("shutdown", "command=producer produced=%d", sequence.Load())
	return nil
}

func runRepairQueueState(ctx context.Context, cmd *cli.Command) error {
	runCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := demo.NewLogger(os.Stdout, cmd.String("color"))
	dbPath := cmd.String("db-path")
	namespace := cmd.String("namespace")

	logger.Printf("startup", "command=repair-queue-state db_path=%s namespace=%s", dbPath, namespace)

	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return fmt.Errorf("create badger directory: %w", err)
	}

	db, err := badger.Open(demo.DefaultBadgerOptions(dbPath))
	if err != nil {
		return fmt.Errorf("open badger: %w", err)
	}
	defer db.Close()

	store, err := badgerbox.New[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination](
		db,
		badgerbox.Serde[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{},
		badgerbox.Options{Namespace: namespace},
	)
	if err != nil {
		return fmt.Errorf("new store: %w", err)
	}
	defer store.Close()

	if err := store.RepairQueueState(runCtx); err != nil {
		return fmt.Errorf("repair queue state: %w", err)
	}

	logger.Printf("ready", "command=repair-queue-state repaired=true db_path=%s namespace=%s", dbPath, namespace)
	return nil
}

func runConsumer(ctx context.Context, cmd *cli.Command) error {
	runCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := demo.NewLogger(os.Stdout, cmd.String("color"))
	stateFile := cmd.String("state-file")

	target, err := resolveKafkaTarget(cmd.String("brokers"), cmd.String("topic"), stateFile)
	if err != nil {
		return err
	}

	opts := []kgo.Opt{
		kgo.ConsumeTopics(target.Topic),
	}
	if cmd.Bool("from-beginning") {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	client, err := demo.NewKafkaClient(target.Brokers, opts...)
	if err != nil {
		return fmt.Errorf("create consumer client: %w", err)
	}
	defer client.Close()

	logger.Printf("startup", "command=consumer brokers=%s brokers_source=%s topic=%s topic_source=%s from_beginning=%t", demo.ShortBrokerList(target.Brokers), target.BrokersSource, target.Topic, target.TopicSource, cmd.Bool("from-beginning"))

	for {
		select {
		case <-runCtx.Done():
			logger.Printf("shutdown", "command=consumer")
			return nil
		default:
		}

		pollCtx, cancel := context.WithTimeout(runCtx, 5*time.Second)
		records, err := demo.PollForRecords(pollCtx, client)
		cancel()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			return fmt.Errorf("poll kafka: %w", err)
		}
		if len(records) == 0 {
			continue
		}

		for _, record := range records {
			logger.Printf("consume", "topic=%s partition=%d offset=%d key=%s headers=%s value=%s", record.Topic, record.Partition, record.Offset, string(record.Key), headerSummary(record.Headers), prettyValue(record.Value))
		}
	}
}

type kafkaTarget struct {
	Brokers       []string
	Topic         string
	State         *demo.State
	BrokersSource string
	TopicSource   string
}

func resolveProducerTarget(brokersValue, topicValue, stateFile string, loggingProducer bool) (kafkaTarget, error) {
	if loggingProducer {
		return resolveLoggingProducerTarget(topicValue, stateFile)
	}
	return resolveKafkaTarget(brokersValue, topicValue, stateFile)
}

func resolveKafkaTarget(brokersValue, topicValue, stateFile string) (kafkaTarget, error) {
	target := kafkaTarget{}
	var state *demo.State

	if brokersValue == "" || topicValue == "" {
		resolvedState, err := demo.ReadState(stateFile)
		if err == nil {
			state = &resolvedState
		} else if !errors.Is(err, os.ErrNotExist) {
			return target, fmt.Errorf("read state file %q: %w", stateFile, err)
		}
	}

	brokers := demo.ParseBrokers(brokersValue)
	brokersSource := "flags-env"
	if len(brokers) == 0 && state != nil {
		brokers = append(brokers, state.Brokers...)
		brokersSource = "state-file"
	}
	if len(brokers) == 0 {
		return target, fmt.Errorf("no Kafka brokers resolved; run `badgerbox-demo kafka` first or set --brokers / BADGERBOX_DEMO_BROKERS")
	}

	topic := topicValue
	topicSource := "flags-env"
	if topic == "" && state != nil {
		topic = state.Topic
		topicSource = "state-file"
	}
	if topic == "" {
		topic = demo.DefaultTopic
		topicSource = "default"
	}

	target.Brokers = brokers
	target.Topic = topic
	target.State = state
	target.BrokersSource = brokersSource
	target.TopicSource = topicSource
	return target, nil
}

func resolveLoggingProducerTarget(topicValue, stateFile string) (kafkaTarget, error) {
	target := kafkaTarget{
		BrokersSource: "disabled",
	}

	topic := topicValue
	topicSource := "flags-env"
	if topic == "" {
		stateTopic, found, err := readOptionalStateTopic(stateFile)
		if err != nil {
			return target, fmt.Errorf("read state file %q: %w", stateFile, err)
		}
		if found && stateTopic != "" {
			topic = stateTopic
			topicSource = "state-file"
		}
	}
	if topic == "" {
		topic = demo.DefaultTopic
		topicSource = "default"
	}

	target.Topic = topic
	target.TopicSource = topicSource
	return target, nil
}

func readOptionalStateTopic(path string) (string, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("read state file: %w", err)
	}

	var state demo.State
	if err := json.Unmarshal(data, &state); err != nil {
		return "", false, fmt.Errorf("decode state file: %w", err)
	}

	return state.Topic, true, nil
}

func prettyValue(data []byte) string {
	var decoded any
	if err := json.Unmarshal(data, &decoded); err == nil {
		if formatted, err := json.Marshal(decoded); err == nil {
			return string(formatted)
		}
	}
	return string(data)
}

func headerSummary(headers []kgo.RecordHeader) string {
	if len(headers) == 0 {
		return "-"
	}

	parts := make([]string, 0, len(headers))
	for _, header := range headers {
		parts = append(parts, fmt.Sprintf("%s=%s", header.Key, string(header.Value)))
	}
	return strings.Join(parts, ",")
}
