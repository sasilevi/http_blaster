package request_generators

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/valyala/fasthttp"
)

type Onelink struct {
	workload config.Workload
	RequestCommon
	Host string
}

func (self *Onelink) UseCommon(c RequestCommon) {

}

func (self *Onelink) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, retChan chan *Response, workerQD int) chan *Request {
	go self.responseHandler(retChan)
	self.workload = wl
	self.Host = host
	self.SetBaseUri(tls_mode, host, self.workload.Container, self.workload.Target)
	var contentType = "text/html"
	var payload []byte
	var DataBfr []byte
	var ferr error
	if self.workload.Payload != "" {
		payload, ferr = ioutil.ReadFile(self.workload.Payload)
		if ferr != nil {
			log.Fatal(ferr)
		}
	} else {
		if self.workload.Type == http.MethodPut || self.workload.Type == http.MethodPost {
			DataBfr = make([]byte, global.Block_size, global.Block_size)
			for i := range DataBfr {
				DataBfr[i] = byte(rand.Int())
			}

			payload = bytes.NewBuffer(DataBfr).Bytes()

		}
	}
	req := AcquireRequest()
	self.PrepareRequest(contentType, self.workload.Header, string(self.workload.Type),
		self.base_uri, string(payload), host, req.Request)

	done := make(chan struct{})
	go func() {
		select {
		case <-time.After(self.workload.Duration.Duration):
			close(done)
		}
	}()

	ch_req := make(chan *Request, workerQD)
	go func() {
		if self.workload.FileIndex == 0 && self.workload.FilesCount == 0 {
			self.single_file_submitter(ch_req, req.Request, done)
		} else {
			self.multi_file_submitter(ch_req, req.Request, done)
		}
	}()
	return ch_req
}

func (self *Onelink) clone_request(req *fasthttp.Request) *Request {
	newReq := AcquireRequest()
	req.Header.CopyTo(&newReq.Request.Header)
	newReq.Request.AppendBody(req.Body())
	return newReq
}

func (self *Onelink) single_file_submitter(ch_req chan *Request, req *fasthttp.Request, done chan struct{}) {
	var generated int
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		default:
			request := self.clone_request(req)
			request.Request.SetHost(self.Host)
			if self.workload.Count == 0 {
				ch_req <- request
				generated++
			} else if generated < self.workload.Count {
				ch_req <- request
				generated++
			} else {
				break LOOP
			}
		}
	}
	close(ch_req)
}

func (self *Onelink) gen_files_uri(file_index int, count int, random bool) chan string {
	ch := make(chan string, 1000)
	go func() {
		if random {
			for {
				n := rand.Intn(count)
				ch <- fmt.Sprintf("%s_%d", self.base_uri, n+file_index)
			}
		} else {
			filePref := file_index
			for {
				if filePref == file_index+count {
					filePref = file_index
				}
				ch <- fmt.Sprintf("%s_%d", self.base_uri, filePref)
				filePref++
			}
		}
	}()
	return ch
}

func (self *Onelink) multi_file_submitter(ch_req chan *Request, req *fasthttp.Request, done chan struct{}) {
	chURI := self.gen_files_uri(self.workload.FileIndex, self.workload.FilesCount, self.workload.Random)
	var generated int
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		default:
			uri := <-chURI
			request := self.clone_request(req)
			request.Request.SetRequestURI(uri)
			if self.workload.Count == 0 {
				ch_req <- request
				generated++
			} else if generated < self.workload.Count {
				ch_req <- request
				generated++
			} else {
				break LOOP
			}
		}
	}
	close(ch_req)
}

func (self *Onelink) responseHandler(retChan chan *Response) {
	log.Println("Starting return channel thread")
	defer log.Println("Terminating response handler")
	for r := range retChan {
		log.Println(r.Response.StatusCode(), r.Duration)

	}
}
