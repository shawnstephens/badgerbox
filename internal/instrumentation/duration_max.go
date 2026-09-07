package instrumentation

import (
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

type queueMetricKey struct{ namespace, outcome, mode, failure string }
type durationMaxPoint struct {
	attrs []attribute.KeyValue
	value float64
}

// Readers share time windows, never consumption state. Each attribute set keeps
// at most two maxima, regardless of observation rate or number of readers.
type durationMaxTracker struct {
	mu                sync.Mutex
	now               func() time.Time
	window            time.Duration
	start             time.Time
	current, previous map[queueMetricKey]durationMaxPoint
}

func newDurationMaxTracker(window time.Duration, now func() time.Time) durationMaxTracker {
	return durationMaxTracker{window: window, now: now, start: now()}
}

// advance runs under mu. Backward clock movement does not restore expired data.
func (t *durationMaxTracker) advance() {
	elapsed := t.now().Sub(t.start)
	if elapsed < t.window {
		return
	}
	steps := elapsed / t.window
	t.previous = nil
	if steps == 1 {
		t.previous = t.current
	}
	t.current = nil
	t.start = t.start.Add(steps * t.window)
}

func (t *durationMaxTracker) record(key queueMetricKey, attrs []attribute.KeyValue, duration time.Duration) {
	if duration <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.advance()
	value := duration.Seconds()
	if current, ok := t.current[key]; ok && current.value >= value {
		return
	}
	if t.current == nil {
		t.current = make(map[queueMetricKey]durationMaxPoint)
	}
	t.current[key] = durationMaxPoint{attrs: append([]attribute.KeyValue(nil), attrs...), value: value}
}

func (t *durationMaxTracker) snapshot() []durationMaxPoint {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.advance()
	points := make([]durationMaxPoint, 0, len(t.current)+len(t.previous))
	for key, point := range t.current {
		if prior, ok := t.previous[key]; ok && prior.value > point.value {
			point = prior
		}
		points = append(points, point)
	}
	for key, point := range t.previous {
		if _, ok := t.current[key]; !ok {
			points = append(points, point)
		}
	}
	return points
}
