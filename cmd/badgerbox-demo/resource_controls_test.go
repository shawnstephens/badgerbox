package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/shawnstephens/badgerbox/pkg/admission"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	cli "github.com/urfave/cli/v3"
)

func readResourceControls(t *testing.T, args ...string) (resourceControls, error) {
	t.Helper()
	var result resourceControls
	command := &cli.Command{Name: "test", Flags: resourceControlFlags(), Writer: io.Discard, ErrWriter: io.Discard, Action: func(_ context.Context, cmd *cli.Command) error {
		var err error
		result, err = parseResourceControls(cmd)
		return err
	}}
	err := command.Run(context.Background(), append([]string{"test"}, args...))
	return result, err
}

func TestResourceControlDefaultsAndByteSizes(t *testing.T) {
	defaults, err := readResourceControls(t)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.AdmissionLimits != (badgerbox.AdmissionLimits{}) || defaults.ClaimMaxBytes != 0 || defaults.MinFreeDiskBytes != 0 || defaults.DiskCheckInterval != 100*time.Millisecond || defaults.AdmissionRetryInterval != 10*time.Millisecond {
		t.Fatalf("defaults: %+v", defaults)
	}
	controls, err := readResourceControls(t, "--max-retained-messages", "17", "--max-retained-bytes", "1.5MiB", "--processor-claim-max-bytes", "2MiB", "--min-free-disk-bytes", "1GiB", "--disk-check-interval", "0s", "--admission-retry-interval", "2ms")
	if err != nil {
		t.Fatal(err)
	}
	if controls.AdmissionLimits.MaxRetainedMessages != 17 || controls.AdmissionLimits.MaxRetainedBytes != 1572864 || controls.ClaimMaxBytes != 2097152 || controls.MinFreeDiskBytes != 1073741824 || controls.DiskCheckInterval != 0 || controls.AdmissionRetryInterval != 2*time.Millisecond {
		t.Fatalf("controls: %+v", controls)
	}
}

func TestResourceControlEnvironment(t *testing.T) {
	t.Setenv("BADGERBOX_DEMO_MAX_RETAINED_MESSAGES", "23")
	t.Setenv("BADGERBOX_DEMO_MAX_RETAINED_BYTES", "32KiB")
	t.Setenv("BADGERBOX_DEMO_PROCESSOR_CLAIM_MAX_BYTES", "16KiB")
	t.Setenv("BADGERBOX_DEMO_MIN_FREE_DISK_BYTES", "2GiB")
	t.Setenv("BADGERBOX_DEMO_DISK_CHECK_INTERVAL", "25ms")
	t.Setenv("BADGERBOX_DEMO_ADMISSION_RETRY_INTERVAL", "3ms")
	c, err := readResourceControls(t)
	if err != nil {
		t.Fatal(err)
	}
	if c.AdmissionLimits.MaxRetainedMessages != 23 || c.AdmissionLimits.MaxRetainedBytes != 32768 || c.ClaimMaxBytes != 16384 || c.MinFreeDiskBytes != 2147483648 || c.DiskCheckInterval != 25*time.Millisecond || c.AdmissionRetryInterval != 3*time.Millisecond {
		t.Fatalf("controls: %+v", c)
	}
}

func TestResourceControlInvalidValues(t *testing.T) {
	invalid := [][]string{{"--max-retained-messages", "-1"}, {"--max-retained-messages", "18446744073709551616"}, {"--disk-check-interval", "-1ms"}, {"--admission-retry-interval", "0s"}, {"--admission-retry-interval", "-1ms"}}
	for _, flag := range []string{"--max-retained-bytes", "--processor-claim-max-bytes", "--min-free-disk-bytes"} {
		for _, value := range []string{"", "-1", "-1KiB", "garbage", "8EiB", "NaN", "0.5B"} {
			invalid = append(invalid, []string{flag, value})
		}
	}
	for _, args := range invalid {
		if _, err := readResourceControls(t, args...); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

func TestAdmissionRetryPreservesRequestAndDistinguishesErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var calls int
	var rejections admissionRejections
	id, err := enqueueWithAdmissionRetry(ctx, time.Millisecond, func() (badgerbox.MessageID, error) {
		calls++
		switch calls {
		case 1:
			return 0, &badgerbox.AdmissionLimitError{Resource: "messages", Limit: 1, Used: 1, Requested: 1}
		case 2:
			return 0, admission.ErrDiskPressure
		default:
			return 37, nil
		}
	}, rejections.record)
	if err != nil || id != 37 || calls != 3 || rejections.quota.Load() != 1 || rejections.disk.Load() != 1 {
		t.Fatalf("id=%d err=%v calls=%d rejections=%v", id, err, calls, rejections.snapshot())
	}
	for _, failure := range []error{admission.ErrDiskProbe, errors.New("codec failure"), &badgerbox.AdmissionLimitError{Resource: "bytes", Limit: 1, Requested: 2}} {
		calls = 0
		_, err = enqueueWithAdmissionRetry(ctx, time.Hour, func() (badgerbox.MessageID, error) { calls++; return 0, failure }, nil)
		if !errors.Is(err, failure) || calls != 1 {
			t.Fatalf("failure=%v err=%v calls=%d", failure, err, calls)
		}
	}
}

func TestAdmissionRetryCancellationInterruptsWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	_, err := enqueueWithAdmissionRetry(ctx, time.Hour, func() (badgerbox.MessageID, error) { calls++; return 0, admission.ErrDiskPressure }, func(string) { cancel() })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestBenchmarkQuotaPressureConservesAllAcceptedMessages(t *testing.T) {
	report, err := runBenchmarkTest(t, "--messages", "40", "--max-retained-messages", "2", "--max-retained-bytes", "16KiB", "--processor-claim-max-bytes", "16KiB", "--admission-retry-interval", "1ms", "--delivery-delay", "5ms", "--timeline-interval", "10ms")
	if err != nil {
		t.Fatal(err)
	}
	if !report.Passed || report.Accepted != 40 || report.UniqueDelivered != 40 || report.AdmissionRejectedAttempts == 0 || report.AdmissionRejections["namespace_quota"] != report.AdmissionRejectedAttempts || report.Usage == nil || report.Usage.RetainedMessages != 0 || report.Usage.RetainedBytes != 0 {
		t.Fatalf("conservation under quota pressure: %+v", report)
	}
	if report.Config.ResourceControls.AdmissionLimits.MaxRetainedMessages != 2 || report.Config.ResourceControls.AdmissionLimits.MaxRetainedBytes != 16384 || report.Config.Processor.ClaimMaxBytes != 16384 {
		t.Fatalf("settings: %+v", report.Config)
	}
	for _, point := range report.Timeline.Points {
		if point.RetainedMessages > 2 || point.RetainedBytes > 16384 {
			t.Fatalf("quota exceeded: %+v", point)
		}
	}
}

func TestBenchmarkImpossibleQuotaFailsWithoutRetryingUntilDeadline(t *testing.T) {
	report, err := runBenchmarkTest(t, "--max-retained-bytes", "1", "--timeout", "5s")
	if !errors.Is(err, badgerbox.ErrAdmissionLimit) || report.Passed || report.Accepted != 0 || report.AdmissionRejectedAttempts == 0 {
		t.Fatalf("err=%v report=%+v", err, report)
	}
}

func TestBenchmarkDiskPressureTimesOutAndReportsRejectedAttempts(t *testing.T) {
	report, err := runBenchmarkTest(t, "--min-free-disk-bytes", "1EiB", "--timeout", "150ms")
	if !errors.Is(err, context.DeadlineExceeded) || report.Passed || report.Accepted != 0 || report.AdmissionRejectedAttempts == 0 || report.AdmissionRejections["disk_pressure"] != report.AdmissionRejectedAttempts || report.Usage == nil || report.Usage.RetainedMessages != 0 {
		t.Fatalf("err=%v report=%+v", err, report)
	}
}

func TestBenchmarkQuarantineWithFullQuotaFailsPromptly(t *testing.T) {
	report, err := runBenchmarkTest(t, "--max-retained-messages", "1", "--processor-claim-max-bytes", "1", "--timeout", "5s")
	if err == nil || errors.Is(err, context.DeadlineExceeded) || !strings.Contains(report.Error, "dead-lettered") || report.Passed || report.Accepted != 1 || report.UniqueDelivered != 0 || report.AdmissionRejectedAttempts == 0 || report.Queue.DeadLetterDepth != 1 || report.Usage == nil || report.Usage.RetainedMessages != 1 {
		t.Fatalf("err=%v report=%+v", err, report)
	}
}
