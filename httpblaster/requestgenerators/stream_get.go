package requestgenerators

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	uuid "github.com/nu7hatch/gouuid"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/igzdata"
)

type StreamGetGenerator struct {
	RequestCommon
	workload config.Workload
}

func (sg *StreamGetGenerator) UseCommon(c RequestCommon) {

}

func (sg *StreamGetGenerator) generateRequest(ch_records chan string,
	ch_req chan *Request,
	host string, wg *sync.WaitGroup) {
	defer wg.Done()
	var contentType string = "application/json"
	u, _ := uuid.NewV4()
	for r := range ch_records {
		sr := igzdata.NewStreamRecord("client", r, u.String(), 0, true)
		r := igzdata.NewStreamRecords(sr)
		req := AcquireRequest()
		sg.PrepareRequest(contentType, sg.workload.Header, "PUT",
			sg.baseURI, r.ToJsonString(), host, req.Request)
		ch_req <- req
	}
	log.Println("generateRequest Done")
}

func (sg *StreamGetGenerator) generate(ch_req chan *Request, payload string, host string) {
	defer close(ch_req)
	var ch_records chan string = make(chan string)
	wg := sync.WaitGroup{}
	ch_files := sg.FilesScan(sg.workload.Payload)

	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go sg.generateRequest(ch_records, ch_req, host, &wg)
	}

	for f := range ch_files {
		if file, err := os.Open(f); err == nil {
			reader := bufio.NewReader(file)
			var i int
			for {
				line, err := reader.ReadString('\n')
				if err == nil {
					ch_records <- strings.TrimSpace(line)
					i++
				} else if err == io.EOF {
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

func (sg *StreamGetGenerator) NextLocationFromResponse(response *Response) interface{} {
	return 0
}

func (sg *StreamGetGenerator) Consumer(return_ch chan *Response) chan interface{} {
	ch_location := make(chan interface{}, 1000)
	go func() {
		for {
			select {
			case response := <-return_ch:
				loc := sg.NextLocationFromResponse(response)
				ch_location <- loc
			case <-time.After(time.Second * 30):
				log.Println("didn't get location for more then 30 seconds, exit now")
				return
			}
		}
	}()
	return ch_location
}

func (sg *StreamGetGenerator) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request {
	sg.workload = wl
	if sg.workload.Header == nil {
		sg.workload.Header = make(map[string]string)
	}
	sg.workload.Header["X-v3io-function"] = "PutRecords"

	sg.SetBaseUri(tls_mode, host, sg.workload.Container, sg.workload.Target)

	ch_req := make(chan *Request, worker_qd)

	go sg.generate(ch_req, sg.workload.Payload, host)

	return ch_req
}
