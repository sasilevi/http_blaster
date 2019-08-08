package requestgenerators

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
)

type Line2KvGenerator struct {
	RequestCommon
	workload config.Workload
}

func (l *Line2KvGenerator) UseCommon(c RequestCommon) {

}

func (l *Line2KvGenerator) generateRequest(chRecords chan []string,
	chReq chan *Request,
	host string, wg *sync.WaitGroup) {
	defer wg.Done()
	for r := range chRecords {
		req := AcquireRequest()
		l.prepareRequest(contentType, l.workload.Header, "PUT",
			r[0], r[1], host, req.Request)
		chReq <- req
	}
	log.Println("generateRequest Done")
}

func (l *Line2KvGenerator) generate(chReq chan *Request, payload string, host string) {
	defer close(chReq)
	var chRecords = make(chan []string)
	wg := sync.WaitGroup{}
	chFiles := l.filesScan(l.workload.Payload)

	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go l.generateRequest(chRecords, chReq, host, &wg)
	}

	for f := range chFiles {
		if file, err := os.Open(f); err == nil {
			reader := bufio.NewReader(file)
			var i = 0
			for {
				address, addrErr := reader.ReadString('\n')
				payload, payloadErr := reader.ReadString('\n')

				if addrErr == nil && payloadErr == nil {
					chRecords <- []string{strings.TrimSpace(address), string(payload)}
					i++
				} else if addrErr == io.EOF || payloadErr == io.EOF {
					break
				} else {
					log.Fatal(err)
				}
			}

			log.Println(fmt.Sprintf("Finish file scaning, generated %d records", i))
		} else {
			panic(err)
		}
	}
	close(chRecords)
	log.Println("Waiting for generators to finish")
	wg.Wait()
	log.Println("generators done")
}

func (l *Line2KvGenerator) GenerateRequests(global config.Global, wl config.Workload, TLSMode bool, host string, chRet chan *Response, workerQD int) chan *Request {
	l.workload = wl
	if l.workload.Header == nil {
		l.workload.Header = make(map[string]string)
	}
	l.workload.Header["X-v3io-function"] = "PutItem"

	l.setBaseURI(TLSMode, host, l.workload.Container, l.workload.Target)

	chReq := make(chan *Request, workerQD)

	go l.generate(chReq, l.workload.Payload, host)

	return chReq
}
