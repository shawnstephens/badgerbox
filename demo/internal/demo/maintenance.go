package demo

import (
	"context"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type valueLogGCDB interface {
	RunValueLogGC(discardRatio float64) error
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
