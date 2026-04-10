package demo

import "time"

func RetryDelay(base, max time.Duration, attempt int) time.Duration {
	if attempt <= 1 {
		return base
	}

	delay := base
	for i := 1; i < attempt && delay < max; i++ {
		if delay > max/2 {
			return max
		}
		delay *= 2
	}

	if delay > max {
		return max
	}
	return delay
}
