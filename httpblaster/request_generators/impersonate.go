package request_generators

import (
	"errors"
	"time"

	"github.com/labstack/gommon/log"
	"github.com/v3io/http_blaster/httpblaster/config"
)

// Impersonate : Generator for onlelink testing
type Impersonate struct {
	workload config.Workload
	RequestCommon
	Host   string
	errors int64
}

type TestHost struct {
	Host    string
	Success bool
}

//UseCommon : force abstract use
func (ol *Impersonate) UseCommon(c RequestCommon) {

}

//GenerateHosts : will iterate over the hosts list
func (ol *Impersonate) GenerateHosts(wl config.Workload, done chan struct{}) (chan TestHost, error) {
	chHosts := make(chan TestHost)
	log.Info("GenerateHosts")

	go func() {
		defer close(chHosts)
		for {
			select {
			case <-done:
				break
			default:
				for host, succeess := range wl.ImpersonateHosts {
					chHosts <- TestHost{Host: host, Success: succeess}
				}

			}
		}
	}()
	if len(wl.ImpersonateHosts) == 0 {
		return chHosts, errors.New("missing impersonate hosts list in the config file")
	}
	return chHosts, nil
}

// GenerateRequests : impliment abs generate request
func (ol *Impersonate) GenerateRequests(global config.Global, wl config.Workload, tlsMode bool, host string, retChan chan *Response, workerQD int) chan *Request {
	// chUsrAgent := rabbitmq.NewClient()
	ol.workload = wl
	ol.Host = host
	if len(ol.workload.Targets) > 0 {
		ol.SetBaseUri(tlsMode, host, "", "")
	} else {
		ol.SetBaseUri(tlsMode, host, ol.workload.Container, ol.workload.Target)
	}

	var contentType = "text/html"
	var payload []byte

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
	chHosts, err := ol.GenerateHosts(wl, done)
	if err != nil {
		panic(err.Error())
	}

	chRequsets := make(chan *Request, workerQD)

	go func() {
		ol.hostSubmitter(chRequsets, chHosts, done)
	}()
	return chRequsets
}

func (ol *Impersonate) hostSubmitter(chReq chan *Request, hostsCh chan TestHost, done chan struct{}) {
	var generated int
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		case host, ok := <-hostsCh:
			if !ok {
				break LOOP
			}
			request := AcquireRequest()

			request.Request.Header.Add("Host", host.Host)
			request.Request.Header.Add("User-Agent", "http_blaster")
			request.Request.SetRequestURI(ol.GetUri("", ol.workload.Args))
			request.Host = host.Host
			request.ExpectedConnectionStatus = host.Success
			request.ResetConnection = true

			chReq <- request

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
