package requestgenerators

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/v3io/http_blaster/httpblaster/bquery"
	"github.com/v3io/http_blaster/httpblaster/config"
)

// Compare : Generator for onlelink testing
type Compare struct {
	workload config.Workload
	RequestCommon
	Host   string
	errors int64
}

//UseCommon : force abstract use
func (ol *Compare) useCommon(c RequestCommon) {

}

// GenerateRequests : impliment abs generate request
func (ol *Compare) GenerateRequests(global config.Global, wl config.Workload, tlsMode bool, host string, retChan chan *Response, workerQD int) chan *Request {
	if strings.Compare(global.BqueryCert, "") == 0 {
		panic("Missing certificate for bigquery DB")
	} else {
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", global.BqueryCert)
	}
	bquery := &bquery.Bquery{}
	ctx := context.Background()
	chRequestUrls := bquery.Query(ctx, wl.BQProjectID, wl.Query, global.BqueryCert, "request_url")

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
		ol.endpointSubmitter(chRequsets, chRequestUrls, done)
	}()
	return chRequsets
}

func (ol *Compare) endpointSubmitter(chReq chan *Request, endpoint chan interface{}, done chan struct{}) {
	var generated int
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		case ep, ok := <-endpoint:
			if !ok {
				break LOOP
			}
			if ep != nil {
				ep1 := ep.(string)
				ep2 := strings.Replace(ep1, "v3", "v4.0", 1)
				ep2 = strings.Replace(ep2, "api", "gcdsdk", 1)

				request := AcquireRequest()
				coockie := ep1
				request.Request.SetHost(ol.Host)
				request.Request.SetRequestURI(ep1)
				request.Cookie = coockie
				chReq <- request
				request2 := AcquireRequest()
				request2.Request.SetHost(ol.Host)
				request2.Request.SetRequestURI(ep2)
				request2.Cookie = coockie
				chReq <- request2
				if ol.workload.Count == 0 {
					generated++
				} else if generated < ol.workload.Count {
					generated++
				} else {
					break LOOP
				}
			}
		}
	}
	close(chReq)
}
