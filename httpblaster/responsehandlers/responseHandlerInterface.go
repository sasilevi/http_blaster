package responsehandlers

import (
	"sync"

	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
)

type IResponseHandler interface {
	HandlerResponses(global config.Global, workload config.Workload, respoCh chan *request_generators.Response, wg *sync.WaitGroup)
	Report() string
}
