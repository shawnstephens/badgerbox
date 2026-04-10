package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/internal/demo"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafkaoutbox"
	"github.com/twmb/franz-go/pkg/kgo"
	cli "github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:  "badgerbox-demo",
		Usage: "Run a local badgerbox + Kafka demo with producer and consumer processes",
		Commands: []*cli.Command{
			newKafkaCommand(),
			newProducerCommand(),
			newConsumerCommand(),
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "badgerbox-demo: %v\n", err)
		os.Exit(1)
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
	return &cli.Command{
		Name:  "producer",
		Usage: "Run badgerbox enqueueing and processing in one process, publishing to Kafka until stopped",
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
			&cli.IntFlag{
				Name:    "enqueue-parallelism",
				Usage:   "Number of enqueue goroutines",
				Value:   1,
				Sources: cli.EnvVars("BADGERBOX_DEMO_ENQUEUE_PARALLELISM"),
			},
			&cli.DurationFlag{
				Name:    "message-interval",
				Usage:   "Wait time after each enqueue per worker",
				Value:   500 * time.Millisecond,
				Sources: cli.EnvVars("BADGERBOX_DEMO_MESSAGE_INTERVAL"),
			},
			&cli.IntFlag{
				Name:    "processor-concurrency",
				Usage:   "Number of badgerbox processor workers",
				Value:   4,
				Sources: cli.EnvVars("BADGERBOX_DEMO_PROCESSOR_CONCURRENCY"),
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
			&cli.StringFlag{
				Name:    "producer-id",
				Usage:   "Logical producer identifier used in keys and logs",
				Sources: cli.EnvVars("BADGERBOX_DEMO_PRODUCER_ID"),
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
	logger.Printf("ready", "command=kafka next=\"go run ./cmd/badgerbox-demo producer\"")
	logger.Printf("ready", "command=kafka next=\"go run ./cmd/badgerbox-demo consumer\"")

	<-runCtx.Done()
	return nil
}

func runProducer(ctx context.Context, cmd *cli.Command) error {
	runCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := demo.NewLogger(os.Stdout, cmd.String("color"))
	stateFile := cmd.String("state-file")

	target, err := resolveKafkaTarget(cmd.String("brokers"), cmd.String("topic"), stateFile)
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
	if messageInterval <= 0 {
		return errors.New("message-interval must be greater than 0")
	}
	processorConcurrency := cmd.Int("processor-concurrency")
	if processorConcurrency < 1 {
		return errors.New("processor-concurrency must be at least 1")
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
	producerID := cmd.String("producer-id")
	if producerID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "localhost"
		}
		producerID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}

	logger.Printf("startup", "command=producer brokers=%s brokers_source=%s topic=%s topic_source=%s db_path=%s namespace=%s enqueue_parallelism=%d processor_concurrency=%d interval=%s retry_base_delay=%s retry_max_delay=%s poll_interval=%s lease_duration=%s producer_id=%s", demo.ShortBrokerList(target.Brokers), target.BrokersSource, target.Topic, target.TopicSource, dbPath, namespace, enqueueParallelism, processorConcurrency, messageInterval, retryBaseDelay, retryMaxDelay, pollInterval, leaseDuration, producerID)

	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return fmt.Errorf("create badger directory: %w", err)
	}

	db, err := badger.Open(badger.DefaultOptions(dbPath).WithLogger(nil))
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

	readyCount, err := demo.CountReadyMessages(db, namespace)
	if err != nil {
		return err
	}
	if readyCount > 0 {
		logger.Printf("warning", "event=backlog_detected pending_ready=%d namespace=%s db_path=%s note=%q", readyCount, namespace, dbPath, "older ready records will be processed before newly enqueued ones")
	} else {
		logger.Printf("ready", "event=backlog pending_ready=0 namespace=%s db_path=%s", namespace, dbPath)
	}

	publisher := demo.NewReloadingPublisher(stateFile, target.Brokers, target.Topic, logger)
	defer publisher.Close()

	processFn := func(ctx context.Context, msg badgerbox.Message[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]) error {
		key := string(msg.Payload.Key)
		logger.Printf("process", "event=start msg_id=%d key=%s topic=%s attempt=%d", msg.ID, key, msg.Destination.Topic, msg.Attempt)
		err := publisher.Publish(ctx, msg)
		if err != nil {
			logProcessFailure(logger, time.Now().UTC(), msg, err, retryBaseDelay, retryMaxDelay)
			return err
		}
		logger.Printf("publish", "event=success msg_id=%d key=%s topic=%s", msg.ID, key, msg.Destination.Topic)
		return nil
	}

	processor, err := badgerbox.NewProcessor(store, processFn, badgerbox.ProcessorOptions{
		Concurrency:    processorConcurrency,
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
				if !first {
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
