package tui

import (
	"sync/atomic"
)

//Counter with ch
type Counter struct {
	count    int64
	chValues chan int64
}

//Add : add value to counter
func (sc *Counter) Add(value int64) {
	sc.chValues <- value
}

//GetValue : get counter value
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

//NewCounter : return new counter
func NewCounter() *Counter {
	counter := &Counter{
		chValues: make(chan int64, 5000),
	}
	counter.start()
	return counter
}
