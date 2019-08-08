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

type Line2StreamGenerator struct {
	RequestCommon
	workload config.Workload
}

func (l *Line2StreamGenerator) UseCommon(c RequestCommon) {

}

func (l *Line2StreamGenerator) generateRequest(ch_records chan string,
	ch_req chan *Request,
	host string, wg *sync.WaitGroup) {
	defer wg.Done()
	var contentType string = "application/json"
	u, _ := uuid.NewV4()
	for r := range ch_records {
		sr := igzdata.NewStreamRecord("client", r, u.String(), 0, true)
		r := igzdata.NewStreamRecords(sr)
		req := AcquireRequest()
		l.PrepareRequest(contentType, l.workload.Header, "PUT",
			l.baseURI, r.ToJsonString(), host, req.Request)
		ch_req <- req
	}
	log.Println("generateRequest Done")
}

func (l *Line2StreamGenerator) generate(ch_req chan *Request, payload string, host string) {
	defer close(ch_req)
	var ch_records chan string = make(chan string, 10000)
	wg := sync.WaitGroup{}
	ch_files := l.FilesScan(l.workload.Payload)

	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go l.generateRequest(ch_records, ch_req, host, &wg)
	}

	for f := range ch_files {
		if file, err := os.Open(f); err == nil {
			reader := bufio.NewReader(file)
			var line_count int = 0
			for {
				line, err := reader.ReadString('\n')
				if err == nil {
					ch_records <- strings.TrimSpace(line)
					line_count++
					if line_count%1024 == 0 {
						log.Printf("line: %d from file %s was submitted", line_count, f)
					}
				} else if err == io.EOF {
					break
				} else {
					log.Fatal(err)
				}
			}

			log.Println(fmt.Sprintf("Finish file scaning, generated %d records", line_count))
		} else {
			panic(err)
		}
	}
	close(ch_records)
	log.Println("Waiting for generators to finish")
	wg.Wait()
	log.Println("generators done")
}

func (l *Line2StreamGenerator) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request {
	l.workload = wl
	if l.workload.Header == nil {
		l.workload.Header = make(map[string]string)
	}
	l.workload.Header["X-v3io-function"] = "PutRecords"

	l.SetBaseUri(tls_mode, host, l.workload.Container, l.workload.Target)

	ch_req := make(chan *Request, worker_qd)

	go l.generate(ch_req, l.workload.Payload, host)

	return ch_req
}
