package demo

import "time"

const (
	DefaultStateDir        = ".demo/badgerbox-demo"
	DefaultStateFile       = DefaultStateDir + "/state.json"
	DefaultBadgerPath      = DefaultStateDir + "/badger"
	DefaultTopic           = "badgerbox-demo"
	DefaultTopicPartitions = 10
	DefaultNamespace       = "demo"
	DefaultKafkaImage      = "confluentinc/confluent-local:7.5.0"
	DefaultClusterID       = "badgerbox-demo"

	DefaultRetryBaseDelay   = 1 * time.Second
	DefaultRetryMaxDelay    = 5 * time.Second
	DefaultPollInterval     = 250 * time.Millisecond
	DefaultLeaseDuration    = 30 * time.Second
	DefaultPublishTimeout   = 2 * time.Second
	DefaultBadgerGCInterval = time.Minute

	DefaultBadgerGCDiscardRatio = 0.1
)
