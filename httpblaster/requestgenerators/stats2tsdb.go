package requestgenerators

import (
	"runtime"
	"strconv"
	"sync"

	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/datagenerator"
)

var gen = datagenerator.MemoryGenerator{}

//Stats2TSDB : Stats2TSDB generator
type Stats2TSDB struct {
	workload config.Workload
	RequestCommon
}

func (r *Stats2TSDB) useCommon(c RequestCommon) {

}

func (r *Stats2TSDB) generateRequest(chRecords chan []string, chReq chan *Request, host string,
	wg *sync.WaitGroup, cpuNumber int, wl config.Workload) {
	defer wg.Done()
	for i := 0; i < wl.Count; i++ {

		var contentType = "text/html"
		JSONPayload := gen.GenerateRandomData(strconv.FormatInt(int64(i), 10))
		for _, payload := range JSONPayload {
			req := AcquireRequest()
			r.prepareRequest(contentType, r.workload.Header, "PUT",
				r.baseURI, payload, host, req.Request)
			chReq <- req
		}
	}
}

func (r *Stats2TSDB) generate(chReq chan *Request, payload string, host string, wl config.Workload) {
	defer close(chReq)
	var chRecords = make(chan []string, 1000)
	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go r.generateRequest(chRecords, chReq, host, &wg, c, wl)
	}

	close(chRecords)
	wg.Wait()
}

//GenerateRequests : GenerateRequests impl
func (r *Stats2TSDB) GenerateRequests(global config.Global, wl config.Workload, TLSMode bool, host string, chRet chan *Response, workerQD int) chan *Request {

	r.workload = wl

	if r.workload.Header == nil {
		r.workload.Header = make(map[string]string)
	}
	r.workload.Header["X-v3io-function"] = "PutItem"

	r.setBaseURI(TLSMode, host, r.workload.Container, r.workload.Target)

	chReq := make(chan *Request, workerQD)

	go r.generate(chReq, r.workload.Payload, host, wl)

	return chReq
}
