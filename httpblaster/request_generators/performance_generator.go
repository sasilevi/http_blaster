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

type PerformanceGenerator struct {
	workload config.Workload
	RequestCommon
	Host string
}

func (self *PerformanceGenerator) UseCommon(c RequestCommon) {

}

func (self *PerformanceGenerator) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request {
	self.workload = wl
	self.Host = host
	self.SetBaseUri(tls_mode, host, self.workload.Container, self.workload.Target)
	var contentType string = "text/html"
	var payload []byte
	var Data_bfr []byte
	var ferr error
	if self.workload.Payload != "" {
		payload, ferr = ioutil.ReadFile(self.workload.Payload)
		if ferr != nil {
			log.Fatal(ferr)
		}
	} else {
		if self.workload.Type == http.MethodPut || self.workload.Type == http.MethodPost {
			Data_bfr = make([]byte, global.BlockSize, global.BlockSize)
			for i, _ := range Data_bfr {
				Data_bfr[i] = byte(rand.Int())
			}

			payload = bytes.NewBuffer(Data_bfr).Bytes()

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

	ch_req := make(chan *Request, worker_qd)
	go func() {
		if self.workload.FileIndex == 0 && self.workload.FilesCount == 0 {
			self.single_file_submitter(ch_req, req.Request, done)
		} else {
			self.multi_file_submitter(ch_req, req.Request, done)
		}
	}()
	return ch_req
}

func (self *PerformanceGenerator) clone_request(req *fasthttp.Request) *Request {
	new_req := AcquireRequest()
	req.Header.CopyTo(&new_req.Request.Header)
	new_req.Request.AppendBody(req.Body())
	return new_req
}

func (self *PerformanceGenerator) single_file_submitter(ch_req chan *Request, req *fasthttp.Request, done chan struct{}) {
	var generated int = 0
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
				generated += 1
			} else if generated < self.workload.Count {
				ch_req <- request
				generated += 1
			} else {
				break LOOP
			}
		}
	}
	close(ch_req)
}

func (self *PerformanceGenerator) gen_files_uri(file_index int, count int, random bool) chan string {
	ch := make(chan string, 1000)
	go func() {
		if random {
			for {
				n := rand.Intn(count)
				ch <- fmt.Sprintf("%s_%d", self.base_uri, n+file_index)
			}
		} else {
			file_pref := file_index
			for {
				if file_pref == file_index+count {
					file_pref = file_index
				}
				ch <- fmt.Sprintf("%s_%d", self.base_uri, file_pref)
				file_pref += 1
			}
		}
	}()
	return ch
}

func (self *PerformanceGenerator) multi_file_submitter(ch_req chan *Request, req *fasthttp.Request, done chan struct{}) {
	ch_uri := self.gen_files_uri(self.workload.FileIndex, self.workload.FilesCount, self.workload.Random)
	var generated int = 0
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		default:
			uri := <-ch_uri
			request := self.clone_request(req)
			request.Request.SetRequestURI(uri)
			if self.workload.Count == 0 {
				ch_req <- request
				generated += 1
			} else if generated < self.workload.Count {
				ch_req <- request
				generated += 1
			} else {
				break LOOP
			}
		}
	}
	close(ch_req)
}
