package main

import (
	"errors"
	"time"
)

func resolveRecordDeliveryTimeout(record, publish time.Duration) (time.Duration, error) {
	if record < 0 {
		return 0, errors.New("record-delivery-timeout must not be negative")
	}
	if record == 0 {
		record = publish / 2
	}
	if record <= 0 || record >= publish {
		return 0, errors.New("record-delivery-timeout must be positive and shorter than publish-timeout")
	}
	if record < time.Second {
		return 0, errors.New("record-delivery-timeout must be at least 1s for Kafka; increase it and publish-timeout")
	}
	return record, nil
}
