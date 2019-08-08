package requestgenerators

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/igzdata"
)

//JSON2Kv : JSON2Kv generator
type JSON2Kv struct {
	workload config.Workload
	RequestCommon
}

func (j *JSON2Kv) useCommon(c RequestCommon) {

}

func (j *JSON2Kv) generateRequest(chRecords chan []byte, chReq chan *Request, host string,
	wg *sync.WaitGroup) {
	defer wg.Done()
	parser := igzdata.EmdSchemaParser{}
	var contentType = "text/html"
	e := parser.LoadSchema(j.workload.Schema, "", "")
	if e != nil {
		panic(e)
	}
	for r := range chRecords {
		JSONPayload, err := parser.EmdFromJSONRecord(r)
		if err != nil {
			panic(err)
		}
		req := AcquireRequest()
		j.prepareRequest(contentType, j.workload.Header, "PUT",
			j.baseURI, JSONPayload, host, req.Request)
		chReq <- req
	}
}

func (j *JSON2Kv) generate(chReq chan *Request, payload string, host string) {
	defer close(chReq)
	var chRecords = make(chan []byte, 10000)

	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go j.generateRequest(chRecords, chReq, host, &wg)
	}
	chFiles := j.filesScan(j.workload.Payload)

	for f := range chFiles {
		if file, err := os.Open(f); err == nil {
			reader := bufio.NewReader(file)
			var lineCount = 0
			for {
				line, err := reader.ReadBytes('\n')
				if err == nil {
					chRecords <- line
					lineCount++
					if lineCount%1024 == 0 {
						log.Printf("line: %d from file %s was submitted", lineCount, f)
					}
				} else if err == io.EOF {
					break
				} else {
					log.Fatal(err)
				}
			}

			log.Println(fmt.Sprintf("Finish file scaning, generated %d records", lineCount))
		} else {
			panic(err)
		}
	}
	close(chRecords)
	wg.Wait()
}

//GenerateRequests : GenerateRequests impl
func (j *JSON2Kv) GenerateRequests(global config.Global, wl config.Workload, TLSMode bool, host string, chRet chan *Response, workerQD int) chan *Request {
	j.workload = wl
	if j.workload.Header == nil {
		j.workload.Header = make(map[string]string)
	}
	j.workload.Header["X-v3io-function"] = "PutItem"

	chReq := make(chan *Request, workerQD)

	j.setBaseURI(TLSMode, host, j.workload.Container, j.workload.Target)

	go j.generate(chReq, j.workload.Payload, host)

	return chReq
}
