// Package semaphore implements a lockless counting semaphore which maintains a
// set of permits.
package semaphore

import (
	"container/list"
	"context"
	"errors"
	"time"
)

// Semaphore is a lockless counting semaphore. It maintains a set of permits.
// No actual permit objects are used. The Semaphore just keeps a count of the
// available permits. Semaphores are often used to restrict the number of
// threads that can access some resource.
//
// The semaphore is completely fair, acquire requests are served in a FIFO
// manner. There is no hard limit on the number of permits that the semaphore
// maintains, i.e. more permits can be released, even if all of the initial
// permits are available. Some care should be taken, as to not try to acquire
// a number of permits which cannot be satisfied with calls to Release(), which
// might lead to starvation of later, smaller Acquire() calls. On the other
// hand, this gives more flexibility for different use cases, e.g. a semaphore
// with 0 initial permits is akin to an already locked mutex (which first needs
// to be released), or similar to sync.WaitGroup (a goroutine can wait on n
// other goroutines by acquiring n permits, each of which needs to release a
// single permit).
type Semaphore struct {
	permits   uint64
	acq       chan req
	rel       chan uint64
	drain     chan drainReq
	try       chan tryReq
	available chan availableReq
	queue     list.List
}

// Request for permits.
type req struct {
	n      uint64        // Number of permits requested.
	notify chan struct{} // Get notified when enough permits are available.
}

// Request to drain the semaphore.
type drainReq struct {
	notify chan uint64 // How many permits were acquired.
}

// Non blocking acquire request.
type tryReq struct {
	n      uint64
	notify chan error
}

// A query for the number of available permits in the semaphore.
type availableReq drainReq

// New creates a Semaphore with the given initial number of permits.
func New(permits uint64) *Semaphore {
	s := &Semaphore{
		permits:   permits,
		acq:       make(chan req),
		rel:       make(chan uint64),
		drain:     make(chan drainReq),
		try:       make(chan tryReq),
		available: make(chan availableReq),
	}
	go s.serve()
	return s
}

// Serves the different types of requests that can be sent to the semaphore. It
// is designed to be invoked as a separate goroutine.
func (s *Semaphore) serve() {
	for {
		select {
		case r := <-s.acq:
			s.serveAcquire(r)
		case n := <-s.rel:
			s.serveRelease(n)
		case dr := <-s.drain:
			s.serveDrain(dr)
		case tr := <-s.try:
			s.serveTry(tr)
		case ar := <-s.available:
			s.serveAvailable(ar)
		}
	}
}

// Serve a request to acquire permits.
func (s *Semaphore) serveAcquire(r req) {
	if s.permits >= r.n && s.queue.Len() == 0 {
		s.permits -= r.n // Acquire can be immediately served.
		close(r.notify)  // There is no need to queue the request.
	} else {
		// Acquire is blocked and queued, not enough available permits.
		s.queue.PushBack(r)
	}
}

// Serve a request to release permits.
func (s *Semaphore) serveRelease(n uint64) {
	s.permits += n // Permits are released immediately.
	// Proceed to unblock the next queued requests which can be
	// fulfilled.
	for s.queue.Len() > 0 {
		h := s.queue.Front() // Get the next queued acquire req.
		r := h.Value.(req)
		if s.permits < r.n {
			// The next req cannot be fulfilled. Must stop here,
			// otherwise we risk starving requests for many permits
			// at once.
			break
		}
		// Acquire the permits.
		s.permits -= r.n
		s.queue.Remove(h) // Remove the req from the queue.
		close(r.notify)   // Notify the requester.
	}
}

// Serve a request to drain the semaphore.
func (s *Semaphore) serveDrain(dr drainReq) {
	// All of the available permits are acquired by the requester.
	dr.notify <- s.permits
	s.permits = 0    // No more permits are left in the semaphore.
	close(dr.notify) // No more communication over the channel.
}

// Serve a TryAcquire request.
func (s *Semaphore) serveTry(tr tryReq) {
	if s.permits >= tr.n && s.queue.Len() == 0 {
		// The semaphore can be acquired without blocking. This can happen if
		// the semaphore has enough available permits and there are no other
		// blocked requests.
		s.permits -= tr.n
		tr.notify <- nil
	} else {
		tr.notify <- errors.New("not enough available permits")
	}
	close(tr.notify)
}

// Serve a query for the available permits in the semaphore.
func (s *Semaphore) serveAvailable(ar availableReq) {
	ar.notify <- s.permits
	close(ar.notify)
}

// Acquire acquires the given number of permits from this semaphore, blocking
// the current goroutine until all are available.
func (s *Semaphore) Acquire(n uint64) {
	r := req{
		n:      n,
		notify: make(chan struct{}),
	}
	s.acq <- r
	<-r.notify
}

// AvailablePermits returns the current number of permits available in this
// semaphore. This method is typically used for debugging and testing purposes.
func (s *Semaphore) AvailablePermits() uint64 {
	ar := availableReq{notify: make(chan uint64)}
	s.available <- ar
	return <-ar.notify
}

// DrainPermits acquires all permits that are immediately available and returns
// the number of acquired permits.
func (s *Semaphore) DrainPermits() uint64 {
	dr := drainReq{notify: make(chan uint64)}
	s.drain <- dr
	return <-dr.notify
}

// Release releases the given number of permits, returning them to the
// semaphore.
func (s *Semaphore) Release(n uint64) {
	s.rel <- n
}

// TryAcquire acquires the given number of permits from this semaphore, only
// if all are available at the time of invocation. If successful, reduces the
// number of available permits by the given number and returns nil, representing
// success. Otherwise, the number of available permits stays unchanged and an
// error is returned.
func (s *Semaphore) TryAcquire(n uint64) error {
	tr := tryReq{
		n:      n,
		notify: make(chan error),
	}
	s.try <- tr
	return <-tr.notify
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
