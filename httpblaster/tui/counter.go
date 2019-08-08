package tui

import (
	"sync/atomic"
)

type Counter struct {
	count    int64
	chValues chan int64
}

func (sc *Counter) Add(value int64) {
	sc.chValues <- value
}

func (sc *Counter) GetValue() int64 {
	value := atomic.LoadInt64(&sc.count)
	return value
}

func (sc *Counter) start() chan int64 {
	go func() {
		for v := range sc.chValues {
			atomic.AddInt64(&sc.count, v)
		}
	}()
	return sc.chValues
}

func NewCounter() *Counter {
	counter := &Counter{
		chValues: make(chan int64, 5000),
	}
	counter.start()
	return counter
}
