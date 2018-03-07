// Package semaphore implements a lockless counting semaphore which maintains a
// set of permits.
package semaphore

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"time"
)

// Semaphore is a lockless counting semaphore. It maintains a set of permits.
// Each Acquire() call spins in a tight loop which yields the CPU time of the
// goroutine until a permit is available, and then takes it.
// Each Release() call adds a permit, potentially releasing a spinning acquirer.
// No actual permit objects are used. The Semaphore just keeps a count of the
// available permits.
//
// Semaphores are often used to restrict the number of threads that can access
// some resource.
type Semaphore struct {
	permits, turn uint64
}

// New creates a Semaphore with the given number of permits.
func New(permits uint64) *Semaphore {
	return &Semaphore{permits: permits}
}

// Acquire acquires the given number of permits from this semaphore, blocking
// the current goroutine until all are available.
func (s *Semaphore) Acquire(n uint64) {
	for {
		if atomic.CompareAndSwapUint64(&s.turn, 0, 1) {
			if s.AvailablePermits() >= n {
				atomic.AddUint64(&s.permits, ^uint64(n-1))
				atomic.StoreUint64(&s.turn, 0)
				return
			}
			atomic.StoreUint64(&s.turn, 0)
		}
		runtime.Gosched()
	}
}

// AvailablePermits returns the current number of permits available in this
// semaphore. This method is typically used for debugging and testing purposes.
func (s *Semaphore) AvailablePermits() uint64 {
	return atomic.LoadUint64(&s.permits)
}

// DrainPermits acquires and returns all permits that are immediately available.
func (s *Semaphore) DrainPermits() uint64 {
	for {
		n := s.AvailablePermits()
		if n == 0 || atomic.CompareAndSwapUint64(&s.permits, n, 0) {
			return n
		}
		runtime.Gosched()
	}
}

// Release releases the given number of permits, returning them to the
// semaphore.
func (s *Semaphore) Release(n uint64) {
	atomic.AddUint64(&s.permits, n)
}

// TryAcquire acquires the given number of permits from this semaphore, only
// if all are available at the time of invocation. If successful, reduces the
// number of available permits by the given number and returns nil, representing
// success. Otherwise, the number of available permits stays unchanged and an
// error is returned.
func (s *Semaphore) TryAcquire(n uint64) error {
	for {
		if atomic.CompareAndSwapUint64(&s.turn, 0, 1) {
			defer atomic.StoreUint64(&s.turn, 0)
			if s.AvailablePermits() >= n {
				atomic.AddUint64(&s.permits, ^uint64(n-1))
				return nil
			}
			return errors.New("not enough available permits")
		}
		runtime.Gosched()
	}
}

// TryAcquireWithContext acquires the given number of permits from this
// semaphore, if all become available while the context is valid. If successful,
// reduces the number of available permits by the given number and returns nil,
// representing success. Otherwise, the number of available permits stays
// unchanged and an error is returned.
func (s *Semaphore) TryAcquireWithContext(ctx context.Context, n uint64) error {
	success := make(chan struct{}, 1)
	signal := make(chan bool, 1)

	go func() {
		s.Acquire(n)
		success <- struct{}{}
		if <-signal {
			s.Release(n)
		}
	}()

	select {
	case <-success:
		signal <- false
		return nil
	case <-ctx.Done():
		signal <- true
		return ctx.Err()
	}
}

// TryAcquireWithTimeout acquires the given number of permits from this
// semaphore, if all become available within the given waiting time. If
// successful, reduces the number of available permits by the given number and
// returns nil, representing success. Otherwise, the number of available permits
// stays unchanged and an error is returned.
func (s *Semaphore) TryAcquireWithTimeout(n uint64, t time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()
	return s.TryAcquireWithContext(ctx, n)
}
