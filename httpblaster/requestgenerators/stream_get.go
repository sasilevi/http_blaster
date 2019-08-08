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

func (sg *StreamGetGenerator) generateRequest(chRecords chan string,
	chReq chan *Request,
	host string, wg *sync.WaitGroup) {
	defer wg.Done()
	var contentType string = "application/json"
	u, _ := uuid.NewV4()
	for r := range chRecords {
		sr := igzdata.NewStreamRecord("client", r, u.String(), 0, true)
		r := igzdata.NewStreamRecords(sr)
		req := AcquireRequest()
		sg.PrepareRequest(contentType, sg.workload.Header, "PUT",
			sg.baseURI, r.ToJsonString(), host, req.Request)
		chReq <- req
	}
	log.Println("generateRequest Done")
}

func (sg *StreamGetGenerator) generate(chReq chan *Request, payload string, host string) {
	defer close(chReq)
	var chRecords chan string = make(chan string)
	wg := sync.WaitGroup{}
	chFiles := sg.FilesScan(sg.workload.Payload)

	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go sg.generateRequest(chRecords, chReq, host, &wg)
	}

	for f := range chFiles {
		if file, err := os.Open(f); err == nil {
			reader := bufio.NewReader(file)
			var i int
			for {
				line, err := reader.ReadString('\n')
				if err == nil {
					chRecords <- strings.TrimSpace(line)
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
	close(chRecords)
	log.Println("Waiting for generators to finish")
	wg.Wait()
	log.Println("generators done")
}

func (sg *StreamGetGenerator) NextLocationFromResponse(response *Response) interface{} {
	return 0
}

func (sg *StreamGetGenerator) Consumer(returnCh chan *Response) chan interface{} {
	chLocation := make(chan interface{}, 1000)
	go func() {
		for {
			select {
			case response := <-returnCh:
				loc := sg.NextLocationFromResponse(response)
				chLocation <- loc
			case <-time.After(time.Second * 30):
				log.Println("didn't get location for more then 30 seconds, exit now")
				return
			}
		}
	}()
	return chLocation
}

func (sg *StreamGetGenerator) GenerateRequests(global config.Global, wl config.Workload, TLSMode bool, host string, chRet chan *Response, workerQD int) chan *Request {
	sg.workload = wl
	if sg.workload.Header == nil {
		sg.workload.Header = make(map[string]string)
	}
	sg.workload.Header["X-v3io-function"] = "PutRecords"

	sg.SetBaseUri(TLSMode, host, sg.workload.Container, sg.workload.Target)

	chReq := make(chan *Request, workerQD)

	go sg.generate(chReq, sg.workload.Payload, host)

	return chReq
}
