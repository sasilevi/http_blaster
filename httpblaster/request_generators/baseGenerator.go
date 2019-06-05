package request_generators

import (
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/tui"
)

type BaseGenerator struct {
}

func (b *BaseGenerator) Run(global config.Global, workload config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int, counter *tui.Counter, generator Generator) chan *Request {
	chMidRequest := make(chan *Request)
	go func() {
		defer close(chMidRequest)
		requestCh := generator.GenerateRequests(global, workload, tls_mode, host, ret_ch, worker_qd)
		for m := range requestCh {
			counter.Add(1)
			chMidRequest <- m
		}
	}()
	return chMidRequest

}
