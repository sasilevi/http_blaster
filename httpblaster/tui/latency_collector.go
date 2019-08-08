package tui

import (
	"time"

	"github.com/sasile/gohistogram"
)

type LatencyCollector struct {
	WeighHist *gohistogram.NumericHistogram
	ch_values chan time.Duration
}

func (r *LatencyCollector) New(n int, alpha float64) chan time.Duration {
	r.WeighHist = gohistogram.NewHistogram(50)
	r.ch_values = make(chan time.Duration, 400000)
	go func() {
		for v := range r.ch_values {
			r.WeighHist.Add(float64(v.Nanoseconds() / 1000))
		}
	}()
	return r.ch_values
}

func (r *LatencyCollector) Add(v time.Duration) {
	r.ch_values <- v
}

func (r *LatencyCollector) Get() ([]string, []int) {
	return r.WeighHist.BarArray()
}

func (r *LatencyCollector) GetResults() ([]string, []float64) {
	return r.WeighHist.GetHistAsArray()

}

func (r *LatencyCollector) GetQuantile(q float64) float64 {
	return r.WeighHist.CDF(q)

}

func (r *LatencyCollector) GetCount() float64 {
	return r.WeighHist.Count()

}

func (r *LatencyCollector) String() string {
	return r.WeighHist.String()

}
