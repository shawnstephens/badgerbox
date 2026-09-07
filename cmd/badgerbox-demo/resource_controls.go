package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dustin/go-humanize"
	"github.com/shawnstephens/badgerbox/pkg/admission"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	cli "github.com/urfave/cli/v3"
)

func resourceControlFlags() []cli.Flag {
	return []cli.Flag{
		&cli.Uint64Flag{Name: "max-retained-messages", Usage: "Persisted namespace message quota, including retries and dead letters; 0 is unlimited", Sources: cli.EnvVars("BADGERBOX_DEMO_MAX_RETAINED_MESSAGES")},
		&cli.StringFlag{Name: "max-retained-bytes", Value: "0", Usage: "Persisted namespace logical-byte quota, including metadata; e.g. 64MiB. 0 is unlimited; this is not a disk or RAM cap", Sources: cli.EnvVars("BADGERBOX_DEMO_MAX_RETAINED_BYTES")},
		&cli.StringFlag{Name: "processor-claim-max-bytes", Value: "0", Usage: "Maximum stored source bytes read per claim; e.g. 2MiB. 0 disables this limit; individually oversized rows are quarantined", Sources: cli.EnvVars("BADGERBOX_DEMO_PROCESSOR_CLAIM_MAX_BYTES")},
		&cli.StringFlag{Name: "min-free-disk-bytes", Value: "0", Usage: "Advisory free-space margin checked before enqueue on Badger directories; e.g. 1GiB. 0 disables the guard", Sources: cli.EnvVars("BADGERBOX_DEMO_MIN_FREE_DISK_BYTES")},
		&cli.DurationFlag{Name: "disk-check-interval", Value: 100 * time.Millisecond, Usage: "Maximum age of shared free-space samples; 0 probes each check", Sources: cli.EnvVars("BADGERBOX_DEMO_DISK_CHECK_INTERVAL")},
		&cli.DurationFlag{Name: "admission-retry-interval", Value: 10 * time.Millisecond, Usage: "Positive cancellable wait between retries of namespace quota or disk pressure rejection", Sources: cli.EnvVars("BADGERBOX_DEMO_ADMISSION_RETRY_INTERVAL")},
	}
}

type resourceControls struct {
	AdmissionLimits        badgerbox.AdmissionLimits `json:"admission_limits"`
	ClaimMaxBytes          int64                     `json:"claim_max_bytes"`
	MinFreeDiskBytes       int64                     `json:"min_free_disk_bytes"`
	DiskCheckInterval      time.Duration             `json:"disk_check_interval_nanoseconds"`
	AdmissionRetryInterval time.Duration             `json:"admission_retry_interval_nanoseconds"`
}

func parseResourceControls(cmd *cli.Command) (resourceControls, error) {
	controls := resourceControls{AdmissionLimits: badgerbox.AdmissionLimits{MaxRetainedMessages: cmd.Uint64("max-retained-messages")}, DiskCheckInterval: cmd.Duration("disk-check-interval"), AdmissionRetryInterval: cmd.Duration("admission-retry-interval")}
	if controls.DiskCheckInterval < 0 || controls.AdmissionRetryInterval <= 0 {
		return controls, errors.New("disk-check-interval must be nonnegative and admission-retry-interval must be positive")
	}
	for _, field := range []struct {
		name string
		set  func(int64)
	}{
		{"max-retained-bytes", func(value int64) { controls.AdmissionLimits.MaxRetainedBytes = uint64(value) }},
		{"processor-claim-max-bytes", func(value int64) { controls.ClaimMaxBytes = value }},
		{"min-free-disk-bytes", func(value int64) { controls.MinFreeDiskBytes = value }},
	} {
		raw := strings.TrimSpace(cmd.String(field.name))
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			value, err = humanize.ParseBytes(raw)
		}
		if err != nil || raw == "" || value > math.MaxInt64 || (value == 0 && strings.ContainsAny(raw, "123456789")) {
			return controls, fmt.Errorf("%s must be zero or a byte size from 1 through MaxInt64", field.name)
		}
		field.set(int64(value))
	}
	return controls, nil
}

func (c resourceControls) storeOptions(namespace string, opts badger.Options) (badgerbox.Options, error) {
	options := badgerbox.Options{Namespace: namespace, AdmissionLimits: c.AdmissionLimits}
	if c.MinFreeDiskBytes != 0 {
		guard, err := admission.NewDiskGuard(admission.DiskGuardOptions{Paths: []string{opts.Dir, opts.ValueDir}, MinFreeBytes: c.MinFreeDiskBytes, RefreshInterval: c.DiskCheckInterval})
		if err != nil {
			return options, err
		}
		options.EnqueueGuard = guard.Check
	}
	return options, nil
}

type admissionRejections struct {
	quota, disk atomic.Int64
}

func (r *admissionRejections) record(reason string) {
	switch reason {
	case "namespace_quota":
		r.quota.Add(1)
	case "disk_pressure":
		r.disk.Add(1)
	}
}
func (r *admissionRejections) total() int64 { return r.quota.Load() + r.disk.Load() }
func (r *admissionRejections) snapshot() map[string]int64 {
	return map[string]int64{"namespace_quota": r.quota.Load(), "disk_pressure": r.disk.Load()}
}

// Keep the same logical message until admission succeeds. Invalid requests and
// failed filesystem probes are errors, not pressure that an indefinite retry
// should hide. A request larger than the whole byte quota cannot fit even when
// every retained message is acknowledged, so it fails without waiting.
func enqueueWithAdmissionRetry(ctx context.Context, interval time.Duration, enqueue func() (badgerbox.MessageID, error), rejected func(string)) (badgerbox.MessageID, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		id, err := enqueue()
		if err == nil {
			return id, nil
		}
		reason := ""
		switch {
		case errors.Is(err, badgerbox.ErrAdmissionLimit):
			reason = "namespace_quota"
		case errors.Is(err, admission.ErrDiskPressure):
			reason = "disk_pressure"
		default:
			return 0, err
		}
		if rejected != nil {
			rejected(reason)
		}
		var limit *badgerbox.AdmissionLimitError
		if errors.As(err, &limit) && limit.Requested > limit.Limit {
			return 0, err
		}
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		}
	}
}
