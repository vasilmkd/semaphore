package semaphore_test

import (
	"github.com/vasilmkd/semaphore"
)

const (
	rounds  = 1000 // Number of rounds to be served.
	glasses = 1000 // Number of spots at the bar.
)

var (
	// The bar as a slice of semaphores, initially with 0 permits.
	// Releasing a permit means filling up the glass.
	// Acquiring a permit means drinking.
	bar []*semaphore.Semaphore

	// Semaphore used to signal that the whole bar needs refilling.
	barSem = semaphore.New(1)

	// Number of full glasses left at the bar.
	left int

	// Binary semaphore used as a mutual exclusion lock when accessing
	// the left variable.
	leftSem = semaphore.New(1)

	// Semaphore used as a waiting group.
	wg = semaphore.New(0)
)

// ExampleSemaphore models a producer consumer relationship between one producer
// and many consumers. The producer (bartender) fills up all of the glasses at
// the bar, only when all of the glasses have been emptied by the consumers.
func ExampleSemaphore() {
	bar = make([]*semaphore.Semaphore, glasses, glasses)
	for g := 0; g < glasses; g++ {
		// Initialize the glasses at the bar, initially empty.
		bar[g] = semaphore.New(0)
	}

	for g := 0; g < glasses; g++ {
		go consumer(g) // Occupy the bar.
	}
	go producer() // The bartender arrives at the bar, ready to serve.

	wg.Acquire(glasses + 1) // Wait for all consumers and the producer.
}

func producer() {
	defer wg.Release(1) // Notify the semaphore used as a waiting group.

	for r := 0; r < rounds; r++ {
		barSem.Acquire(1)  // Obtain permission to serve the bar.
		leftSem.Acquire(1) // Obtain permission to change the left variable.
		for g := 0; g < glasses; g++ {
			bar[g].Release(1) // Fill up each glass.
		}
		left = glasses
		leftSem.Release(1) // Done changing the left variable.
	}
}

func consumer(glass int) {
	defer wg.Release(1) // Notify the semaphore used as a waiting group.

	for r := 0; r < rounds; r++ {
		bar[glass].Acquire(1) // Drink from your own glass.
		leftSem.Acquire(1)    // Obtain permission to change the left variable.
		left--
		if left == 0 {
			// Notify the producer that everyone has finished drinking.
			barSem.Release(1)
		}
		leftSem.Release(1) // Done changing the left variable.
	}
}
