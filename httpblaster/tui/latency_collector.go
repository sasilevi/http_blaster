package tui

import (
	"time"

	"github.com/sasile/gohistogram"
)

//LatencyCollector : collect latancy values for ui
type LatencyCollector struct {
	WeighHist *gohistogram.NumericHistogram
	chValues  chan time.Duration
}

//New : return new latency collector
func (r *LatencyCollector) New(n int, alpha float64) chan time.Duration {
	r.WeighHist = gohistogram.NewHistogram(50)
	r.chValues = make(chan time.Duration, 400000)
	go func() {
		for v := range r.chValues {
			r.WeighHist.Add(float64(v.Nanoseconds() / 1000))
		}
	}()
	return r.chValues
}

//Add : add value to lactency collector
func (r *LatencyCollector) Add(v time.Duration) {
	r.chValues <- v
}

//Get : return latency bar
func (r *LatencyCollector) Get() ([]string, []int) {
	return r.WeighHist.BarArray()
}

// GetResults : return hist as array
func (r *LatencyCollector) GetResults() ([]string, []float64) {
	return r.WeighHist.GetHistAsArray()

}

// func (r *LatencyCollector) GetQuantile(q float64) float64 {
// 	return r.WeighHist.CDF(q)
// }

// GetCount : latency collector hist count
func (r *LatencyCollector) GetCount() float64 {
	return r.WeighHist.Count()

}

// String : latency collector hist string
func (r *LatencyCollector) String() string {
	return r.WeighHist.String()

}
