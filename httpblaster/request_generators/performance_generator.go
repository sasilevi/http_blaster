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

// PerformanceGenerator : performance generator for high load
type PerformanceGenerator struct {
	workload config.Workload
	RequestCommon
	Host string
}

// UseCommon : force use abs
func (p *PerformanceGenerator) UseCommon(c RequestCommon) {

}

// GenerateRequests : generate function
func (p *PerformanceGenerator) GenerateRequests(global config.Global, wl config.Workload, TLSMode bool, host string, retCh chan *Response, worker_qd int) chan *Request {
	p.workload = wl
	p.Host = host
	p.SetBaseUri(TLSMode, host, p.workload.Container, p.workload.Target)
	contentType := "text/html"
	var payload []byte
	var DataBfr []byte
	var ferr error
	if p.workload.Payload != "" {
		payload, ferr = ioutil.ReadFile(p.workload.Payload)
		if ferr != nil {
			log.Fatal(ferr)
		}
	} else {
		if p.workload.Type == http.MethodPut || p.workload.Type == http.MethodPost {
			DataBfr = make([]byte, global.BlockSize, global.BlockSize)
			for i := range DataBfr {
				DataBfr[i] = byte(rand.Int())
			}

			payload = bytes.NewBuffer(DataBfr).Bytes()

		}
	}
	req := AcquireRequest()
	p.PrepareRequest(contentType, p.workload.Header, string(p.workload.Type),
		p.base_uri, string(payload), host, req.Request)

	done := make(chan struct{})
	go func() {
		select {
		case <-time.After(p.workload.Duration.Duration):
			close(done)
		}
	}()

	chReq := make(chan *Request, worker_qd)
	go func() {
		if p.workload.FileIndex == 0 && p.workload.FilesCount == 0 {
			p.single_file_submitter(chReq, req.Request, done)
		} else {
			p.multi_file_submitter(chReq, req.Request, done)
		}
	}()
	return chReq
}

func (p *PerformanceGenerator) clone_request(req *fasthttp.Request) *Request {
	new_req := AcquireRequest()
	req.Header.CopyTo(&new_req.Request.Header)
	new_req.Request.AppendBody(req.Body())
	return new_req
}

func (p *PerformanceGenerator) single_file_submitter(ch_req chan *Request, req *fasthttp.Request, done chan struct{}) {
	var generated int = 0
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		default:
			request := p.clone_request(req)
			request.Request.SetHost(p.Host)
			if p.workload.Count == 0 {
				ch_req <- request
				generated += 1
			} else if generated < p.workload.Count {
				ch_req <- request
				generated += 1
			} else {
				break LOOP
			}
		}
	}
	close(ch_req)
}

func (p *PerformanceGenerator) gen_files_uri(file_index int, count int, random bool) chan string {
	ch := make(chan string, 1000)
	go func() {
		if random {
			for {
				n := rand.Intn(count)
				ch <- fmt.Sprintf("%s_%d", p.base_uri, n+file_index)
			}
		} else {
			file_pref := file_index
			for {
				if file_pref == file_index+count {
					file_pref = file_index
				}
				ch <- fmt.Sprintf("%s_%d", p.base_uri, file_pref)
				file_pref += 1
			}
		}
	}()
	return ch
}

func (p *PerformanceGenerator) multi_file_submitter(ch_req chan *Request, req *fasthttp.Request, done chan struct{}) {
	ch_uri := p.gen_files_uri(p.workload.FileIndex, p.workload.FilesCount, p.workload.Random)
	var generated int = 0
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		default:
			uri := <-ch_uri
			request := p.clone_request(req)
			request.Request.SetRequestURI(uri)
			if p.workload.Count == 0 {
				ch_req <- request
				generated += 1
			} else if generated < p.workload.Count {
				ch_req <- request
				generated += 1
			} else {
				break LOOP
			}
		}
	}
	close(ch_req)
}
