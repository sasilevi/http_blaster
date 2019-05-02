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
)

// Onelink : Generator for onlelink testing
type Onelink struct {
	workload config.Workload
	RequestCommon
	Host   string
	errors int64
}

//UseCommon : force abstract use
func (ol *Onelink) UseCommon(c RequestCommon) {

}

// GenerateRequests : impliment abs generate request
func (ol *Onelink) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, retChan chan *Response, workerQD int) chan *Request {
	rabbitmq := memqueue.New(wl.Topic, "localhost", "5672", "guest")
	chUsrAgent := rabbitmq.NewClient()
	ol.workload = wl
	ol.Host = host
	ol.SetBaseUri(tls_mode, host, ol.workload.Container, ol.workload.Target)
	var contentType = "text/html"
	var payload []byte
	var DataBfr []byte
	var ferr error
	if ol.workload.Payload != "" {
		payload, ferr = ioutil.ReadFile(ol.workload.Payload)
		if ferr != nil {
			log.Fatal(ferr)
		}
	} else {
		if ol.workload.Type == http.MethodPut || ol.workload.Type == http.MethodPost {
			DataBfr = make([]byte, global.Block_size, global.Block_size)
			for i := range DataBfr {
				DataBfr[i] = byte(rand.Int())
			}

			payload = bytes.NewBuffer(DataBfr).Bytes()

		}
	}
	req := AcquireRequest()
	ol.PrepareRequest(contentType, ol.workload.Header, string(ol.workload.Type),
		ol.base_uri, string(payload), host, req.Request)

	done := make(chan struct{})
	go func() {
		select {
		case <-time.After(ol.workload.Duration.Duration):
			close(done)
		}
	}()

	chRequsets := make(chan *Request, workerQD)

	go func() {
		ol.userAgentSubmitter(chRequsets, chUsrAgent, done)
	}()
	return chRequsets
}

func (ol *Onelink) userAgentSubmitter(chReq chan *Request, usrAgentch chan string, done chan struct{}) {
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
			request.Request.SetHost(ol.Host)
			request.Request.SetRequestURI(ol.base_uri)
			request.Request.Header.Set("User-Agent", userAgent)
			request.Cookie = userAgent
			if ol.workload.Count == 0 {
				chReq <- request
				generated++
			} else if generated < ol.workload.Count {
				chReq <- request
				generated++
			} else {
				break LOOP
			}
		}
	}
	close(chReq)
}
