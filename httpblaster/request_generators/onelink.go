package request_generators

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/memqueue"
	"github.com/valyala/fasthttp"
)

type Onelink struct {
	workload config.Workload
	RequestCommon
	Host   string
	errors int64
}

func (self *Onelink) UseCommon(c RequestCommon) {

}

func (self *Onelink) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, retChan chan *Response, workerQD int) chan *Request {
	go self.responseHandler(retChan)
	rabbitmq := memqueue.New("hello", "localhost", "5672", "guest")
	chUsrAgent := rabbitmq.NewClient()
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

	chRequsets := make(chan *Request, workerQD)

	go func() {
		self.userAgentSubmitter(chRequsets, chUsrAgent, done)
	}()
	return chRequsets
}

func (self *Onelink) clone_request(req *fasthttp.Request) *Request {
	newReq := AcquireRequest()
	req.Header.CopyTo(&newReq.Request.Header)
	newReq.Request.AppendBody(req.Body())
	return newReq
}

func (self *Onelink) userAgentSubmitter(ch_req chan *Request, usrAgentch chan string, done chan struct{}) {
	var generated int
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		case userAgent, ok := <-usrAgentch:
			if !ok {
				break LOOP
			}
			request := AcquireRequest()
			request.Request.SetHost(self.Host)
			request.Request.SetRequestURI(self.base_uri)
			request.Request.Header.Set("User-Agent", userAgent)
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
		if r.Response.StatusCode() != http.StatusOK {
			self.errors++
		}
		// log.Println(r.Response.StatusCode(), "\t", r.Duration, "\t", r.ID)
		ReleaseResponse(r)
	}
}

func (self *Onelink) CheckResponse() {

}
