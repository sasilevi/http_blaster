package responsehandlers

import (
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
)

// Default : Default handler that only release the response
type Default struct {
}

// HandlerResponses :  handler function to habdle responses
func (r *Default) HandlerResponses(global config.Global, workload config.Workload, respCh chan *request_generators.Response, wg *sync.WaitGroup) {
	defer log.Println("Terminating response handler")
	defer wg.Done()
	if respCh == nil {
		log.Println("response handler not available for this generator, all responses will be ignored. only statuses and latency are collected")
	}
	for resp := range respCh {
		request_generators.ReleaseResponse(resp)
	}
}

// Report : report redirect responses assertions
func (r *Default) Report() string {
	return ""
}
