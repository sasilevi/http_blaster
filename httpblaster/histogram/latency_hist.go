package histogram

import (
	"fmt"
	"sort"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type LatencyHist struct {
	chValues chan time.Duration
	hist     map[int64]int
	count    int64
	size     int64
	wg       sync.WaitGroup
}

func (l *LatencyHist) Add(v time.Duration) {
	l.chValues <- v
	l.size++
}

func (l *LatencyHist) Close() {
	close(l.chValues)
}

func (l *LatencyHist) place(v int64) {
	l.hist[v/100]++
}

func (l *LatencyHist) New() chan time.Duration {
	l.hist = make(map[int64]int)
	l.wg.Add(1)

	l.chValues = make(chan time.Duration, 10000)

	go func() {
		defer l.wg.Done()
		for v := range l.chValues {
			l.count++
			l.place(v.Nanoseconds() / 1000)
		}
	}()
	return l.chValues
}

func (l *LatencyHist) GetResults() ([]string, []float64) {
	log.Debugln("get latency hist")
	l.wg.Wait()
	var keys []int
	for k := range l.hist {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	resStrings := []string{}
	resValues := []float64{}
	for _, k := range keys {
		v := l.hist[int64(k)]
		resStrings = append(resStrings, fmt.Sprintf("%5d - %5d",
			k*100, (k+1)*100))
		value := float64(v*100) / float64(l.count)
		resValues = append(resValues, value)
	}
	return resStrings, resValues
}

func (l *LatencyHist) GetHistMap() map[int64]int {
	l.wg.Wait()
	return l.hist
}
