package requestgenerators

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"

	uuid "github.com/nu7hatch/gouuid"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/igzdata"
)

//Line2StreamGenerator : Line2StreamGenerator generator
type Line2StreamGenerator struct {
	RequestCommon
	workload config.Workload
}

func (l *Line2StreamGenerator) useCommon(c RequestCommon) {

}

func (l *Line2StreamGenerator) generateRequest(chRecords chan string,
	chReq chan *Request,
	host string, wg *sync.WaitGroup) {
	defer wg.Done()
	var contentType = "application/json"
	u, _ := uuid.NewV4()
	for r := range chRecords {
		sr := igzdata.NewStreamRecord("client", r, u.String(), 0, true)
		r := igzdata.NewStreamRecords(sr)
		req := AcquireRequest()
		l.prepareRequest(contentType, l.workload.Header, "PUT",
			l.baseURI, r.ToJSONString(), host, req.Request)
		chReq <- req
	}
	log.Println("generateRequest Done")
}

func (l *Line2StreamGenerator) generate(chReq chan *Request, payload string, host string) {
	defer close(chReq)
	var chRecords = make(chan string, 10000)
	wg := sync.WaitGroup{}
	chFiles := l.filesScan(l.workload.Payload)

	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go l.generateRequest(chRecords, chReq, host, &wg)
	}

	for f := range chFiles {
		if file, err := os.Open(f); err == nil {
			reader := bufio.NewReader(file)
			var lineCount = 0
			for {
				line, err := reader.ReadString('\n')
				if err == nil {
					chRecords <- strings.TrimSpace(line)
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
	log.Println("Waiting for generators to finish")
	wg.Wait()
	log.Println("generators done")
}

//GenerateRequests : GenerateRequests impl
func (l *Line2StreamGenerator) GenerateRequests(global config.Global, wl config.Workload, TLSMode bool, host string, chRet chan *Response, workerQD int) chan *Request {
	l.workload = wl
	if l.workload.Header == nil {
		l.workload.Header = make(map[string]string)
	}
	l.workload.Header["X-v3io-function"] = "PutRecords"

	l.setBaseURI(TLSMode, host, l.workload.Container, l.workload.Target)

	chReq := make(chan *Request, workerQD)

	go l.generate(chReq, l.workload.Payload, host)

	return chReq
}
