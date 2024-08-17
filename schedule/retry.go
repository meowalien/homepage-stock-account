package schedule

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Retry attempts to execute the provided function `f` multiple times until it succeeds,
// the maximum number of retries is reached, or the context is cancelled. The retry
// interval between each attempt is specified by `retryInterval`.
//
// Parameters:
//   - ctx: The context that can be used to cancel the retry process early. If ctx.Done() is triggered,
//     the function returns with the context's error.
//   - retryCount: The maximum number of retries. If set to -1, the function will retry indefinitely
//     until the context is cancelled or the function succeeds. If set to 0, the function will immediately
//     return an error indicating that the retry limit has been reached.
//   - retryInterval: The duration to wait between each retry attempt.
//   - f: A function that takes the current retry round (starting from 1) and returns a boolean indicating
//     whether the function succeeded.
//
// Returns:
//   - err: Returns nil if the function `f` succeeds during one of the retry attempts.
//     Returns a context error if the context is cancelled, or an error indicating that the retry limit
//     has been reached if all retries are exhausted.
//
// Errors:
//   - ReachRetryLimit: Returned when the maximum number of retries is reached without success.
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//
//	err := Retry(ctx, 5, 2*time.Second, func(round int) bool {
//	    fmt.Println("Attempt", round)
//	    // Replace with your actual retry logic here.
//	    return round == 3
//	})
//
//	if err == nil {
//	    fmt.Println("Operation succeeded")
//	} else if errors.Is(err, ReachRetryLimit) {
//	    fmt.Println("Operation failed after reaching the retry limit")
//	} else {
//	    fmt.Println("Operation failed or was cancelled:", err)
//	}
func Retry(ctx context.Context, retryCount int, retryInterval time.Duration, f func(round int) (success bool)) (err error) {
	if retryCount == 0 {
		return fmt.Errorf("%w: retryCount: %d, retryInterval:%s", ReachRetryLimit, retryCount, retryInterval.String())
	}

	for round := 1; retryCount == -1 || round <= retryCount; round++ {
		if f(round) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
			// Continue to the next round
		}
	}

	return fmt.Errorf("%w: retryCount: %d, retryInterval:%s", ReachRetryLimit, retryCount, retryInterval.String())
}

// ReachRetryLimit is returned when the maximum number of retries is reached without success.
var ReachRetryLimit = errors.New("reach retry limit")
