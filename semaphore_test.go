package semaphore

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

const (
	maxSleep   = 1 * time.Millisecond
	totalTimes = 10000
)

func TestAcquireStress(t *testing.T) {
	n := runtime.GOMAXPROCS(0)
	times := 10000 / n
	s := New(int64(n))
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < times; j++ {
				s.Acquire(int64(n))
				sleep()
				s.Release(int64(n))
			}
		}()
	}
	wg.Wait()
}

func TestAcquirePanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("acquire negative number of permits did not panic")
		}
	}()

	s := New(1)
	s.Acquire(-1)
}

func TestDrainPermitsStress(t *testing.T) {
	n := runtime.GOMAXPROCS(0)
	times := 10000 / n
	s := New(int64(n))
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < times; j++ {
				acq := s.DrainPermits()
				sleep()
				s.Release(acq)
			}
		}()
	}
	wg.Wait()
}

func TestReducePermits(t *testing.T) {
	for _, c := range []struct {
		s        *Semaphore
		by, want int64
	}{
		{
			s:    New(10),
			by:   5,
			want: 5,
		},
		{
			s:    New(0),
			by:   1,
			want: 0,
		},
		{
			s:    New(0),
			by:   0,
			want: 0,
		},
		{
			s:    New(-5),
			by:   8,
			want: 0,
		},
		{
			s:    New(-5),
			by:   5,
			want: 0,
		},
		{
			s:    New(-5),
			by:   2,
			want: 0,
		},
	} {
		c.s.ReducePermits(c.by)
		if got := c.s.AvailablePermits(); got != c.want {
			t.Errorf("Release(%d) = %d, want %d", c.by, got, c.want)
		}
	}
}

func TestReducePermitsPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("reduce permits by a negative number did not panic")
		}
	}()

	s := New(1)
	s.ReducePermits(-1)
}

func TestRelease(t *testing.T) {
	for _, c := range []struct {
		s         *Semaphore
		rel, want int64
	}{
		{
			s:    New(10),
			rel:  5,
			want: 15,
		},
		{
			s:    New(0),
			rel:  1,
			want: 1,
		},
		{
			s:    New(0),
			rel:  0,
			want: 0,
		},
		{
			s:    New(-5),
			rel:  8,
			want: 3,
		},
		{
			s:    New(-5),
			rel:  5,
			want: 0,
		},
		{
			s:    New(-5),
			rel:  2,
			want: 0,
		},
	} {
		c.s.Release(c.rel)
		if got := c.s.AvailablePermits(); got != c.want {
			t.Errorf("Release(%d) = %d, want %d", c.rel, got, c.want)
		}
	}
}

func TestReleasePanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("release negative number of permits did not panic")
		}
	}()

	s := New(1)
	s.Release(-1)
}

func TestTryAcquire(t *testing.T) {
	s := New(2) // Semaphore with 2 permits.

	tryAcquire := func(n int64) bool {
		return s.TryAcquire(n) == nil
	}

	var tries []bool
	s.Acquire(1)                         // 1 permit available.
	tries = append(tries, tryAcquire(1)) // Succeeds. 0 permits available.
	tries = append(tries, tryAcquire(1)) // Fails. 0 permits available.

	s.Release(2) // 2 permits available.

	tries = append(tries, tryAcquire(1)) // Succeeds. 1 permit available.
	s.Acquire(1)                         // 0 permit available.
	tries = append(tries, tryAcquire(1)) // Fails. 0 permits available.

	s.Release(2) // 2 permits available.

	tries = append(tries, tryAcquire(3)) // Fails. 2 permits available.

	want := []bool{true, false, true, false, false}
	for i := range tries {
		if tries[i] != want[i] {
			t.Errorf("tries[%d] = %t, want %t", i, tries[i], want[i])
		}
	}
}

func TestTryAcquireStress(t *testing.T) {
	n := runtime.GOMAXPROCS(0)
	times := 10000 / n
	s := New(int64(n))
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < times; j++ {
				if err := s.TryAcquire(int64(n)); err == nil {
					sleep()
					s.Release(int64(n))
				}
			}
		}()
	}
	wg.Wait()
}

func TestTryAcquirePanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("acquire negative number of permits did not panic")
		}
	}()

	s := New(1)
	s.TryAcquire(-1)
}

func TestTryAcquireWithTimeout(t *testing.T) {
	s := New(2) // Semaphore with 2 permits.

	tryAcquireWithTimeout := func(n int64) bool {
		return s.TryAcquireWithTimeout(n, 10*time.Millisecond) == nil
	}

	var tries []bool
	s.Acquire(1)                                    // 1 permit available.
	tries = append(tries, tryAcquireWithTimeout(1)) // Succeeds. 0 permits available.
	tries = append(tries, tryAcquireWithTimeout(1)) // Fails. 0 permits available.

	s.Release(2) // 2 permits available.

	tries = append(tries, tryAcquireWithTimeout(1)) // Succeeds. 1 permit available.
	s.Acquire(1)                                    // 0 permits available.
	tries = append(tries, tryAcquireWithTimeout(1)) // Fails. 0 permits available.

	s.Release(2) // 2 permits available.

	tries = append(tries, tryAcquireWithTimeout(3)) // Fails. 2 permits available.

	want := []bool{true, false, true, false, false}
	for i := range tries {
		if tries[i] != want[i] {
			t.Errorf("tries[%d] = %t, want %t", i, tries[i], want[i])
		}
	}
}

func TestTryAcquireWithTimeoutPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("acquire negative number of permits did not panic")
		}
	}()

	s := New(1)
	s.TryAcquireWithTimeout(-1, 1*time.Millisecond)
}

func TestLargeAcquireDoesNotStarve(t *testing.T) {
	ctx := context.Background()
	n := runtime.GOMAXPROCS(0)
	s := New(int64(n))
	running := true

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		s.TryAcquireWithContext(ctx, 1)
		go func() {
			defer func() {
				s.Release(1)
				wg.Done()
			}()
			for running {
				time.Sleep(1 * time.Millisecond)
				s.Release(1)
				s.TryAcquireWithContext(ctx, 1)
			}
		}()
	}

	s.TryAcquireWithContext(ctx, int64(n))
	running = false
	s.Release(int64(n))
	wg.Wait()
}

func sleep() {
	time.Sleep(time.Duration(rand.Int63n(int64(maxSleep/time.Nanosecond))) * time.Nanosecond)
}
