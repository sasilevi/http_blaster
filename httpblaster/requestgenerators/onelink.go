package requestgenerators

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/v3io/http_blaster/httpblaster/bquery"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/dto"
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

// // GenerateRequests : impliment abs generate request
// func (ol *Onelink) GenerateRequests(global config.Global, wl config.Workload, tlsMode bool, host string, retChan chan *Response, workerQD int) chan *Request {
// 	rabbitmq := memqueue.New(wl.Topic, global.RbmqAddr, global.RbmqPort, global.RbmqUser)
// 	chUsrAgent := rabbitmq.NewClient()
// 	ol.workload = wl
// 	ol.Host = host
// 	ol.SetBaseURI(tlsMode, host, ol.workload.Container, ol.workload.Target)
// 	var contentType = "text/html"
// 	var payload []byte
// 	var DataBfr []byte
// 	var ferr error
// 	if ol.workload.Payload != "" {
// 		payload, ferr = ioutil.ReadFile(ol.workload.Payload)
// 		if ferr != nil {
// 			log.Fatal(ferr)
// 		}
// 	} else {
// 		if ol.workload.Type == http.MethodPut || ol.workload.Type == http.MethodPost {
// 			DataBfr = make([]byte, global.Block_size, global.Block_size)
// 			for i := range DataBfr {
// 				DataBfr[i] = byte(rand.Int())
// 			}

// 			payload = bytes.NewBuffer(DataBfr).Bytes()

// 		}
// 	}
// 	req := AcquireRequest()
// 	ol.PrepareRequest(contentType, ol.workload.Header, string(ol.workload.Type),
// 		ol.base_uri, string(payload), host, req.Request)

// 	done := make(chan struct{})
// 	go func() {
// 		select {
// 		case <-time.After(ol.workload.Duration.Duration):
// 			close(done)
// 		}
// 	}()

// 	chRequsets := make(chan *Request, workerQD)

// 	go func() {
// 		ol.userAgentSubmitter(chRequsets, chUsrAgent, done)
// 	}()
// 	return chRequsets
// }

// GenerateRequests : impliment abs generate request
func (ol *Onelink) GenerateRequests(global config.Global, wl config.Workload, tlsMode bool, host string, retChan chan *Response, workerQD int) chan *Request {
	if strings.Compare(global.BqueryCert, "") == 0 {
		panic("Missing certificate for bigquery DB")
	} else {
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", global.BqueryCert)
	}
	bquery := &bquery.Bquery{}
	ctx := context.Background()
	chUsrAgent := bquery.Query(ctx, wl.BQProjectID, wl.Query, global.BqueryCert)

	// chUsrAgent := rabbitmq.NewClient()
	ol.workload = wl
	ol.Host = host
	if len(ol.workload.Targets) > 0 {
		ol.setBaseURI(tlsMode, host, "", "")
	} else {
		ol.setBaseURI(tlsMode, host, ol.workload.Container, ol.workload.Target)
	}

	var contentType = "text/html"
	var payload []byte

	req := AcquireRequest()
	ol.prepareRequest(contentType, ol.workload.Header, string(ol.workload.Type),
		ol.baseURI, string(payload), host, req.Request)

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

func (ol *Onelink) userAgentSubmitter(chReq chan *Request, usrAgentCh chan interface{}, done chan struct{}) {
	var generated int
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		case userAgent, ok := <-usrAgentCh:
			if !ok {
				break LOOP
			}
			for k := range ol.workload.Targets {
				ua := userAgent.(string)
				request := AcquireRequest()
				request.Request.SetHost(ol.Host)
				request.Request.SetRequestURI(ol.getURI(k, ""))
				request.Request.Header.Set("User-Agent", ua)
				request.Cookie = dto.AcquireUaObj(ua, k)
				chReq <- request
			}
			if ol.workload.Count == 0 {
				generated++
			} else if generated < ol.workload.Count {
				generated++
			} else {
				break LOOP
			}
		}
	}
	close(chReq)
}
