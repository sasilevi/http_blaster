package requestgenerators

import (
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/tui"
)

// BaseGenerator : base generatoe impliment run
type BaseGenerator struct {
}

// Run : generator run func
func (b *BaseGenerator) Run(global config.Global, workload config.Workload, TLSMode bool, host string, retCh chan *Response, workerQD int, counter *tui.Counter, generator Generator) chan *Request {
	chMidRequest := make(chan *Request, workerQD)
	go func() {
		defer close(chMidRequest)
		requestCh := generator.GenerateRequests(global, workload, TLSMode, host, retCh, workerQD)
		counterCh := counter.Chan()
		for m := range requestCh {
			counterCh <- 1
			chMidRequest <- m
		}
		log.Info("closing chMidRequest")
	}()

	return chMidRequest

}
