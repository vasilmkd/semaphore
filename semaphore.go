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
	permits, turn int64
}

// New creates a Semaphore with the given number of permits.
func New(permits int64) *Semaphore {
	return &Semaphore{permits: permits}
}

// Acquire acquires the given number of permits from this semaphore, blocking
// the current goroutine until all are available.
func (s *Semaphore) Acquire(n int64) {
	if n < 0 {
		panic("cannot acquire a negative number of permits")
	}
	for {
		if atomic.CompareAndSwapInt64(&s.turn, 0, 1) {
			if s.AvailablePermits() >= n {
				atomic.AddInt64(&s.permits, -n)
				atomic.SwapInt64(&s.turn, 0)
				return
			}
			atomic.SwapInt64(&s.turn, 0)
		}
		runtime.Gosched()
	}
}

// AvailablePermits returns the current number of permits available in this
// semaphore. This method is typically used for debugging and testing purposes.
func (s *Semaphore) AvailablePermits() int64 {
	if n := atomic.LoadInt64(&s.permits); n > 0 {
		return n
	}
	return 0
}

// DrainPermits acquires and returns all permits that are immediately available.
func (s *Semaphore) DrainPermits() int64 {
	for {
		n := s.AvailablePermits()
		if n == 0 || atomic.CompareAndSwapInt64(&s.permits, n, 0) {
			return n
		}
		runtime.Gosched()
	}
}

// ReducePermits shrinks the number of available permits by the indicated
// reduction. This method differs from Acquire in that it does not block
// waiting for permits to become available.
func (s *Semaphore) ReducePermits(by int64) {
	if by < 0 {
		panic("cannot reduce the number of permits by a negative number")
	}
	atomic.AddInt64(&s.permits, -by)
}

// Release releases the given number of permits, returning them to the
// semaphore.
func (s *Semaphore) Release(n int64) {
	if n < 0 {
		panic("cannot release a negative number of permits")
	}
	atomic.AddInt64(&s.permits, n)
}

// TryAcquire acquires the given number of permits from this semaphore, only
// if all are available at the time of invocation. If successful, reduces the
// number of available permits by the given number and returns nil, representing
// success. Otherwise, the number of available permits stays unchanged and an
// error is returned.
func (s *Semaphore) TryAcquire(n int64) error {
	if n < 0 {
		panic("cannot acquire a negative number of permits")
	}
	for {
		if atomic.CompareAndSwapInt64(&s.turn, 0, 1) {
			defer atomic.SwapInt64(&s.turn, 0)
			if s.AvailablePermits() >= n {
				atomic.AddInt64(&s.permits, -n)
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
func (s *Semaphore) TryAcquireWithContext(ctx context.Context, n int64) error {
	if n < 0 {
		panic("cannot acquire a negative number of permits")
	}
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
func (s *Semaphore) TryAcquireWithTimeout(n int64, t time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()
	return s.TryAcquireWithContext(ctx, n)
}
