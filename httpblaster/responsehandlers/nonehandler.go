package responsehandlers

import (
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/requestgenerators"
)

// NoneHandler : will not do anything, just interface impl
type NoneHandler struct {
	BaseResponseHandler
}

// HandlerResponses :  handler function to habdle responses
func (r *NoneHandler) HandlerResponses(global config.Global, workload config.Workload, respCh chan *requestgenerators.Response) {
}

// Report : report redirect responses assertions
func (r *NoneHandler) Report() string {
	return ""
}

func (r *NoneHandler) Counters() map[string]int64 {
	return nil
}

func (r *NoneHandler) Error() error {
	return nil
}
