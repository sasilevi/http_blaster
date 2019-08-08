package responsehandlers

import (
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/requestgenerators"
)

// Default : Default handler that only release the response
type Default struct {
	BaseResponseHandler
}

// HandlerResponses :  handler function to habdle responses
func (r *Default) HandlerResponses(global config.Global, workload config.Workload, respCh chan *requestgenerators.Response) {
	if respCh == nil {
		log.Println("response handler not available for this generator, all responses will be ignored. only statuses and latency are collected")
	}
	for resp := range respCh {
		requestgenerators.ReleaseResponse(resp)
	}
}

// Report : report redirect responses assertions
func (r *Default) Report() string {
	return ""
}

// Counters : currenly return nil
func (r *Default) Counters() map[string]int64 {
	return nil
}

func (r *Default) Error() error {
	return nil
}
