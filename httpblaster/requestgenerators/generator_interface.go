package requestgenerators

import (
	"github.com/v3io/http_blaster/httpblaster/config"
)

// Generator : request generator interface
type Generator interface {
	useCommon(c RequestCommon)
	GenerateRequests(global config.Global, workload config.Workload, TLSMode bool, host string, chRet chan *Response, workerQD int) chan *Request
}
