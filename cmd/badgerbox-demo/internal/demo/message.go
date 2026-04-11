package demo

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/shawnstephens/badgerbox/pkg/kafkaoutbox"
)

type Payload struct {
	Sequence   uint64    `json:"sequence"`
	ProducerID string    `json:"producer_id"`
	Worker     int       `json:"worker"`
	Message    string    `json:"message"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

func BuildKafkaMessage(topic, producerID string, worker int, sequence uint64) (kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination, error) {
	payload := Payload{
		Sequence:   sequence,
		ProducerID: producerID,
		Worker:     worker,
		Message:    fmt.Sprintf("badgerbox demo message %d", sequence),
		EnqueuedAt: time.Now().UTC(),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return kafkaoutbox.KafkaMessage{}, kafkaoutbox.KafkaDestination{}, err
	}

	key := fmt.Sprintf("%s-%06d", producerID, sequence)
	return kafkaoutbox.KafkaMessage{
			Key:   []byte(key),
			Value: data,
			Headers: map[string][]byte{
				"demo-source":    []byte("badgerbox-demo"),
				"producer-id":    []byte(producerID),
				"enqueue-worker": []byte(fmt.Sprintf("%d", worker)),
				"sequence":       []byte(fmt.Sprintf("%d", sequence)),
			},
		},
		kafkaoutbox.KafkaDestination{Topic: topic},
		nil
}
