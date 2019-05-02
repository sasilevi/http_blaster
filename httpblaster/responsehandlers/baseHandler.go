package responsehandlers

import (
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
)

//BaseResponseHandler : base handler runner
type BaseResponseHandler struct {
}

//Run : base runner function
func (b *BaseResponseHandler) Run(global config.Global, workload config.Workload, respoCh chan *request_generators.Response, wg *sync.WaitGroup, handler IResponseHandler) {
	defer log.Println("Terminating response handler")
	handler.HandlerResponses(global, workload, respoCh)
	wg.Done()
}
