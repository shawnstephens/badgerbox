package main

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"sync"
	"time"
)

type latencySummary struct {
	Count uint64  `json:"count"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50_upper_bound"`
	P95   float64 `json:"p95_upper_bound"`
	P99   float64 `json:"p99_upper_bound"`
	Max   float64 `json:"max"`
}

// Constant memory, logarithmic buckets with <=2% upper-bound error above 1 ns.
// Unlike retaining all samples, this does not make benchmark memory grow with load.
type latencyHistogram struct {
	buckets    [2048]uint64
	count      uint64
	total, max float64
}

func (h *latencyHistogram) add(d time.Duration) {
	ns := max(float64(d.Nanoseconds()), 1)
	bucket := min(int(math.Ceil(math.Log(ns)/math.Log(1.02))), len(h.buckets)-1)
	h.buckets[bucket]++
	h.count++
	h.total += d.Seconds()
	h.max = max(h.max, d.Seconds())
}
func (h *latencyHistogram) summary() latencySummary {
	if h.count == 0 {
		return latencySummary{}
	}
	quantile := func(q float64) float64 {
		target := uint64(math.Ceil(float64(h.count) * q))
		var count uint64
		for i, n := range h.buckets {
			count += n
			if count >= target {
				return min(math.Pow(1.02, float64(i))/1e9, h.max)
			}
		}
		return h.max
	}
	return latencySummary{Count: h.count, Mean: h.total / float64(h.count), P50: quantile(.5), P95: quantile(.95), P99: quantile(.99), Max: h.max}
}

type benchmarkVerifier struct {
	mu                          sync.Mutex
	seen                        []uint64
	messages, size              int
	unique, duplicates, invalid int64
	enqueue, delivery           latencyHistogram
	positions                   map[int32]int64
}

func newBenchmarkVerifier(messages, size int) *benchmarkVerifier {
	return &benchmarkVerifier{messages: messages, size: size, seen: make([]uint64, (messages-1)/64+1), positions: make(map[int32]int64)}
}
func benchmarkPayload(sequence uint64, size int, now time.Time) []byte {
	value := make([]byte, size)
	binary.LittleEndian.PutUint64(value, sequence)
	binary.LittleEndian.PutUint64(value[8:], uint64(now.UnixNano()))
	fillBenchmarkPayload(value[16:len(value)-4], sequence)
	binary.LittleEndian.PutUint32(value[len(value)-4:], crc32.ChecksumIEEE(value[:len(value)-4]))
	return value
}
func fillBenchmarkPayload(dst []byte, sequence uint64) {
	state := sequence + 0x9e3779b97f4a7c15
	for i := range dst {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		dst[i] = byte(state)
	}
}
func (v *benchmarkVerifier) receive(value []byte, now time.Time) {
	valid := len(value) == v.size && len(value) >= 20
	var seq uint64
	if valid {
		seq = binary.LittleEndian.Uint64(value)
		valid = seq < uint64(v.messages) && binary.LittleEndian.Uint32(value[len(value)-4:]) == crc32.ChecksumIEEE(value[:len(value)-4])
	}
	if valid {
		state := seq + 0x9e3779b97f4a7c15
		for _, b := range value[16 : len(value)-4] {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			if b != byte(state) {
				valid = false
				break
			}
		}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if !valid {
		v.invalid++
		return
	}
	if v.seen[seq/64]&(uint64(1)<<(seq%64)) != 0 {
		v.duplicates++
		return
	}
	v.seen[seq/64] |= uint64(1) << (seq % 64)
	v.unique++
	v.delivery.add(now.Sub(time.Unix(0, int64(binary.LittleEndian.Uint64(value[8:])))))
}
