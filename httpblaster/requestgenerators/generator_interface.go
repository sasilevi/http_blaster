package requestgenerators

import (
	"github.com/v3io/http_blaster/httpblaster/config"
)

type Generator interface {
	UseCommon(c RequestCommon)
	GenerateRequests(global config.Global, workload config.Workload, TLSMode bool, host string, chRet chan *Response, workerQD int) chan *Request
}
