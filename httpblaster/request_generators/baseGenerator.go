package request_generators

import (
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/tui"
)

// BaseGenerator : base generatoe impliment run
type BaseGenerator struct {
}

// Run : generator run func
func (b *BaseGenerator) Run(global config.Global, workload config.Workload, TLSMode bool, host string, retCh chan *Response, workerQD int, counter *tui.Counter, generator Generator) chan *Request {
	chMidRequest := make(chan *Request)
	go func() {
		defer close(chMidRequest)
		requestCh := generator.GenerateRequests(global, workload, TLSMode, host, retCh, workerQD)
		for m := range requestCh {
			counter.Add(1)
			chMidRequest <- m
		}
	}()
	return chMidRequest

}
