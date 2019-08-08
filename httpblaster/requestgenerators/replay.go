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

//Replay : Replay generator
type Replay struct {
	workload config.Workload
	RequestCommon
}

func (r *Replay) useCommon(c RequestCommon) {

}

func (r *Replay) generateRequest(chRecords chan []byte, chReq chan *Request, host string,
	wg *sync.WaitGroup) {
	defer wg.Done()
	for rec := range chRecords {
		reqDump := &RequestDump{}
		json.Unmarshal(rec, reqDump)

		req := AcquireRequest()
		r.prepareRequest(contentType, reqDump.Headers,
			reqDump.Method,
			reqDump.URI,
			reqDump.Body,
			reqDump.Host,
			req.Request)
		chReq <- req
	}
}

func (r *Replay) generate(chReq chan *Request, payload string, host string) {
	defer close(chReq)
	var chRecords = make(chan []byte, 10000)

	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go r.generateRequest(chRecords, chReq, host, &wg)
	}
	rCount := 0
	chFiles := r.filesScan(r.workload.Payload)

	for f := range chFiles {
		if file, err := os.Open(f); err == nil {
			rCount++
			reader := bufio.NewReader(file)
			data, err := ioutil.ReadAll(reader)
			if err != nil {
				log.Errorf("Fail to read file %v:%v", f, err.Error())
			} else {
				chRecords <- data
			}
			log.Println(fmt.Sprintf("Finish file scaning, generated %d requests ", rCount))
		} else {
			panic(err)
		}
	}
	close(chRecords)
	wg.Wait()
}

//GenerateRequests : GenerateRequests impl
func (r *Replay) GenerateRequests(global config.Global, wl config.Workload, TLSMode bool, host string, chRet chan *Response, workerQD int) chan *Request {
	r.workload = wl

	chReq := make(chan *Request, workerQD)

	r.setBaseURI(TLSMode, host, r.workload.Container, r.workload.Target)

	go r.generate(chReq, r.workload.Payload, host)

	return chReq
}
