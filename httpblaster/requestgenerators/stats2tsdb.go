package requestgenerators

import (
	"runtime"
	"strconv"
	"sync"

	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/datagenerator"
)

var gen = datagenerator.MemoryGenerator{}

type Stats2TSDB struct {
	workload config.Workload
	RequestCommon
}

func (r *Stats2TSDB) UseCommon(c RequestCommon) {

}

func (r *Stats2TSDB) generateRequest(ch_records chan []string, ch_req chan *Request, host string,
	wg *sync.WaitGroup, cpuNumber int, wl config.Workload) {
	defer wg.Done()
	for i := 0; i < wl.Count; i++ {

		var contentType string = "text/html"
		json_payload := gen.GenerateRandomData(strconv.FormatInt(int64(i), 10))
		for _, payload := range json_payload {
			req := AcquireRequest()
			r.PrepareRequest(contentType, r.workload.Header, "PUT",
				r.baseURI, payload, host, req.Request)
			ch_req <- req
		}
	}
}

func (r *Stats2TSDB) generate(ch_req chan *Request, payload string, host string, wl config.Workload) {
	defer close(ch_req)
	var ch_records chan []string = make(chan []string, 1000)
	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go r.generateRequest(ch_records, ch_req, host, &wg, c, wl)
	}

	close(ch_records)
	wg.Wait()
}

func (r *Stats2TSDB) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request {

	r.workload = wl

	if r.workload.Header == nil {
		r.workload.Header = make(map[string]string)
	}
	r.workload.Header["X-v3io-function"] = "PutItem"

	r.SetBaseUri(tls_mode, host, r.workload.Container, r.workload.Target)

	ch_req := make(chan *Request, worker_qd)

	go r.generate(ch_req, r.workload.Payload, host, wl)

	return ch_req
}
