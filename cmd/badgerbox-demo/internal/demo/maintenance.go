package demo

import (
	"context"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const startupBadgerGCDiscardRatio = 0.5

type valueLogGCDB interface {
	RunValueLogGC(discardRatio float64) error
}

type startupCompactionDB interface {
	Flatten(workers int) error
	RunValueLogGC(discardRatio float64) error
	Size() (lsm, vlog int64)
}

func RunValueLogGC(ctx context.Context, db valueLogGCDB, interval time.Duration, discardRatio float64, logger *Logger) {
	if interval <= 0 || db == nil {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runValueLogGCPass(db, discardRatio, logger)
		}
	}
}

func RunStartupCompaction(db startupCompactionDB, dbPath string, compactors int, logger *Logger) error {
	if db == nil {
		return nil
	}

	workers := max(1, compactors)
	beforeLSM, beforeVlog := db.Size()
	if logger != nil {
		logger.Printf(
			"maintenance",
			"event=startup_compaction_start db_path=%s flatten_workers=%d gc_discard_ratio=%.2f before_lsm=%s before_vlog=%s",
			dbPath,
			workers,
			startupBadgerGCDiscardRatio,
			FormatBytes(beforeLSM),
			FormatBytes(beforeVlog),
		)
	}

	if err := db.Flatten(workers); err != nil {
		if logger != nil {
			logger.Printf(
				"warning",
				"event=startup_compaction_failed stage=flatten db_path=%s flatten_workers=%d err=%q",
				dbPath,
				workers,
				err,
			)
		}
		return err
	}

	rewrites, err := runStartupValueLogGCPass(db, startupBadgerGCDiscardRatio)
	if err != nil {
		if logger != nil {
			logger.Printf(
				"warning",
				"event=startup_compaction_failed stage=value_log_gc db_path=%s flatten_workers=%d gc_discard_ratio=%.2f rewrites=%d err=%q",
				dbPath,
				workers,
				startupBadgerGCDiscardRatio,
				rewrites,
				err,
			)
		}
		return err
	}

	afterLSM, afterVlog := db.Size()
	if logger != nil {
		logger.Printf(
			"maintenance",
			"event=startup_compaction_complete db_path=%s flatten_workers=%d gc_discard_ratio=%.2f rewrites=%d before_lsm=%s before_vlog=%s after_lsm=%s after_vlog=%s",
			dbPath,
			workers,
			startupBadgerGCDiscardRatio,
			rewrites,
			FormatBytes(beforeLSM),
			FormatBytes(beforeVlog),
			FormatBytes(afterLSM),
			FormatBytes(afterVlog),
		)
	}

	return nil
}

func runValueLogGCPass(db valueLogGCDB, discardRatio float64, logger *Logger) {
	var rewrites int
	for {
		err := db.RunValueLogGC(discardRatio)
		switch {
		case err == nil:
			rewrites++
			continue
		case errors.Is(err, badger.ErrNoRewrite):
			if logger != nil {
				logger.Printf("maintenance", "event=value_log_gc rewrites=%d discard_ratio=%.2f result=no_rewrite", rewrites, discardRatio)
			}
			return
		case errors.Is(err, badger.ErrRejected):
			if logger != nil {
				logger.Printf("warning", "event=value_log_gc_rejected rewrites=%d discard_ratio=%.2f err=%q", rewrites, discardRatio, err)
			}
			return
		default:
			if logger != nil {
				logger.Printf("warning", "event=value_log_gc_failed rewrites=%d discard_ratio=%.2f err=%q", rewrites, discardRatio, err)
			}
			return
		}
	}
}

func runStartupValueLogGCPass(db valueLogGCDB, discardRatio float64) (int, error) {
	var rewrites int
	for {
		err := db.RunValueLogGC(discardRatio)
		switch {
		case err == nil:
			rewrites++
			continue
		case errors.Is(err, badger.ErrNoRewrite):
			return rewrites, nil
		default:
			return rewrites, err
		}
	}
}
