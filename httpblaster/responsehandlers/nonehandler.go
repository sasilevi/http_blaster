package responsehandlers

import (
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
)

// NoneHandler : will not do anything, just interface impl
type NoneHandler struct {
	BaseResponseHandler
}

// HandlerResponses :  handler function to habdle responses
func (r *NoneHandler) HandlerResponses(global config.Global, workload config.Workload, respCh chan *request_generators.Response) {
}

// Report : report redirect responses assertions
func (r *NoneHandler) Report() string {
	return ""
}
