package tui

import (
	"sync/atomic"
)

type Counter struct {
	count     int64
	ch_values chan int64
}

func (sc *Counter) Add(value int64) {
	sc.ch_values <- value
}

func (sc *Counter) GetValue() int64 {
	value := atomic.LoadInt64(&sc.count)
	return value
}

func (self *Counter) start() chan int64 {
	go func() {
		for v := range self.ch_values {
			atomic.AddInt64(&self.count, v)
		}
	}()
	return self.ch_values
}

func NewCounter() *Counter {
	counter := &Counter{
		ch_values: make(chan int64, 5000),
	}
	counter.start()
	return counter
}
