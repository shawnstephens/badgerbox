package kafkaoutbox

type KafkaDestination struct {
	Topic     string `json:"topic"`
	Partition *int32 `json:"partition,omitempty"`
}

type KafkaMessage struct {
	Key     []byte            `json:"key,omitempty"`
	Headers map[string][]byte `json:"headers,omitempty"`
	Value   []byte            `json:"value"`
}

type Options struct{}
