package responsehandlers

import (
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/requestgenerators"
	"github.com/v3io/http_blaster/httpblaster/tui"
)

//BaseResponseHandler : base handler runner
type BaseResponseHandler struct {
}

//Run : base runner function
func (b *BaseResponseHandler) Run(global config.Global, workload config.Workload, respoCh chan *requestgenerators.Response, wg *sync.WaitGroup, handler IResponseHandler, counter *tui.Counter) {
	defer log.Println("Terminating response handler")
	defer wg.Done()
	chMidResponse := make(chan *requestgenerators.Response)

	go func() {
		defer close(chMidResponse)
		counter_ch := counter.Chan()
		for m := range respoCh {
			counter_ch <- 1
			chMidResponse <- m
		}
	}()

	handler.HandlerResponses(global, workload, chMidResponse)
}
