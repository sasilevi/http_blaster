package tui

import (
	"github.com/sasile/gohistogram"
)

// StatusesCollector obj
type StatusesCollector struct {
	WeighHist *gohistogram.NumericHistogram
	chValues  chan int
}

//New : return new status collector
func (s *StatusesCollector) New(n int, alpha float64) chan int {
	s.WeighHist = gohistogram.NewHistogram(10)
	s.chValues = make(chan int, 400000)
	go func() {
		for v := range s.chValues {
			s.WeighHist.Add(float64(v))
		}
	}()
	return s.chValues
}

// Get : return hist from collector
func (s *StatusesCollector) Get() ([]string, []int) {
	return s.WeighHist.BarArray()

}
