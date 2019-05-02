package responsehandlers

import (
	"sync"

	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
)

// NoneHandler : will not do anything, just interface impl
type NoneHandler struct {
}

// HandlerResponses :  handler function to habdle responses
func (r *NoneHandler) HandlerResponses(global config.Global, workload config.Workload, respCh chan *request_generators.Response, wg *sync.WaitGroup) {
	defer wg.Done()
}

// Report : report redirect responses assertions
func (r *NoneHandler) Report() string {
	return ""
}
