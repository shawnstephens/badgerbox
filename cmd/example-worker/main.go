package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"badgerbox/pkg/badgerbox"
	"badgerbox/pkg/kafkaoutbox"
	"github.com/dgraph-io/badger/v4"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var (
		dbPath      = flag.String("db-path", "", "path to the Badger DB directory (must be exclusively owned by this process)")
		brokersFlag = flag.String("brokers", "", "comma-separated Kafka brokers; falls back to KAFKA_BROKERS")
		namespace   = flag.String("namespace", "", "outbox namespace")
		concurrency = flag.Int("concurrency", 4, "processor concurrency")
	)
	flag.Parse()

	if *dbPath == "" {
		log.Fatal("db-path is required")
	}

	brokers := brokerList(*brokersFlag)
	if len(brokers) == 0 {
		log.Fatal("at least one Kafka broker is required via -brokers or KAFKA_BROKERS")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := badger.Open(badger.DefaultOptions(*dbPath).WithLogger(nil))
	if err != nil {
		log.Fatalf("open badger db: %v", err)
	}
	defer db.Close()

	store, err := badgerbox.New[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination](
		db,
		badgerbox.Serde[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{},
		badgerbox.Options{Namespace: *namespace},
	)
	if err != nil {
		log.Fatalf("create store: %v", err)
	}
	defer store.Close()

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		log.Fatalf("create kafka client: %v", err)
	}
	defer client.Close()

	processor, err := badgerbox.NewProcessor(
		store,
		kafkaoutbox.NewProcessFunc(client, kafkaoutbox.Options{}),
		badgerbox.ProcessorOptions{Concurrency: *concurrency},
	)
	if err != nil {
		log.Fatalf("create processor: %v", err)
	}

	log.Printf("starting processor on namespace %q against brokers %s", storeNamespace(*namespace), strings.Join(brokers, ","))
	if err := processor.Run(ctx); err != nil {
		log.Fatalf("processor exited with error: %v", err)
	}
}

func brokerList(flagValue string) []string {
	raw := flagValue
	if raw == "" {
		raw = os.Getenv("KAFKA_BROKERS")
	}
	if raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			brokers = append(brokers, part)
		}
	}
	return brokers
}

func storeNamespace(namespace string) string {
	if namespace == "" {
		return fmt.Sprintf("%s (default)", "default")
	}
	return namespace
}
