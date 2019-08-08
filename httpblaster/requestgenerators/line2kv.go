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

func (l *Line2KvGenerator) generateRequest(ch_records chan []string,
	ch_req chan *Request,
	host string, wg *sync.WaitGroup) {
	defer wg.Done()
	for r := range ch_records {
		req := AcquireRequest()
		l.PrepareRequest(contentType, l.workload.Header, "PUT",
			r[0], r[1], host, req.Request)
		ch_req <- req
	}
	log.Println("generateRequest Done")
}

func (l *Line2KvGenerator) generate(ch_req chan *Request, payload string, host string) {
	defer close(ch_req)
	var ch_records chan []string = make(chan []string)
	wg := sync.WaitGroup{}
	ch_files := l.FilesScan(l.workload.Payload)

	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go l.generateRequest(ch_records, ch_req, host, &wg)
	}

	for f := range ch_files {
		if file, err := os.Open(f); err == nil {
			reader := bufio.NewReader(file)
			var i int = 0
			for {
				address, addr_err := reader.ReadString('\n')
				payload, payload_err := reader.ReadString('\n')

				if addr_err == nil && payload_err == nil {
					ch_records <- []string{strings.TrimSpace(address), string(payload)}
					i++
				} else if addr_err == io.EOF || payload_err == io.EOF {
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
	close(ch_records)
	log.Println("Waiting for generators to finish")
	wg.Wait()
	log.Println("generators done")
}

func (l *Line2KvGenerator) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request {
	l.workload = wl
	if l.workload.Header == nil {
		l.workload.Header = make(map[string]string)
	}
	l.workload.Header["X-v3io-function"] = "PutItem"

	l.SetBaseUri(tls_mode, host, l.workload.Container, l.workload.Target)

	ch_req := make(chan *Request, worker_qd)

	go l.generate(ch_req, l.workload.Payload, host)

	return ch_req
}
