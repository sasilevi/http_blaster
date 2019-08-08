package requestgenerators

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
)

type Replay struct {
	workload config.Workload
	RequestCommon
}

func (r *Replay) UseCommon(c RequestCommon) {

}

func (r *Replay) generateRequest(ch_records chan []byte, ch_req chan *Request, host string,
	wg *sync.WaitGroup) {
	defer wg.Done()
	for rec := range ch_records {
		req_dump := &RequestDump{}
		json.Unmarshal(rec, req_dump)

		req := AcquireRequest()
		r.PrepareRequest(contentType, req_dump.Headers,
			req_dump.Method,
			req_dump.URI,
			req_dump.Body,
			req_dump.Host,
			req.Request)
		ch_req <- req
	}
}

func (r *Replay) generate(ch_req chan *Request, payload string, host string) {
	defer close(ch_req)
	var ch_records chan []byte = make(chan []byte, 10000)

	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go r.generateRequest(ch_records, ch_req, host, &wg)
	}
	r_count := 0
	ch_files := r.FilesScan(r.workload.Payload)

	for f := range ch_files {
		if file, err := os.Open(f); err == nil {
			r_count++
			reader := bufio.NewReader(file)
			data, err := ioutil.ReadAll(reader)
			if err != nil {
				log.Errorf("Fail to read file %v:%v", f, err.Error())
			} else {
				ch_records <- data
			}
			log.Println(fmt.Sprintf("Finish file scaning, generated %d requests ", r_count))
		} else {
			panic(err)
		}
	}
	close(ch_records)
	wg.Wait()
}

func (r *Replay) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request {
	r.workload = wl

	ch_req := make(chan *Request, worker_qd)

	r.SetBaseUri(tls_mode, host, r.workload.Container, r.workload.Target)

	go r.generate(ch_req, r.workload.Payload, host)

	return ch_req
}
