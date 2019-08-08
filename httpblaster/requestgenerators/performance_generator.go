package requestgenerators

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
func (p *PerformanceGenerator) GenerateRequests(global config.Global, wl config.Workload, TLSMode bool, host string, retCh chan *Response, workerQD int) chan *Request {
	p.workload = wl
	p.Host = host
	p.SetBaseURI(TLSMode, host, p.workload.Container, p.workload.Target)
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
		p.baseURI, string(payload), host, req.Request)
	req.Request.Header.Add("Host", p.Host)
	req.Request.Header.Add("User-Agent", "http_blaster")
	done := make(chan struct{})
	go func() {
		select {
		case <-time.After(p.workload.Duration.Duration):
			close(done)
		}
	}()

	chReq := make(chan *Request, workerQD)
	go func() {
		if p.workload.FileIndex == 0 && p.workload.FilesCount == 0 {
			p.singleFileSubmitter(chReq, req.Request, done)
		} else {
			p.multiFileSubmitter(chReq, req.Request, done)
		}
	}()
	return chReq
}

func (p *PerformanceGenerator) cloneRequest(req *fasthttp.Request) *Request {
	newReq := AcquireRequest()
	req.Header.CopyTo(&newReq.Request.Header)
	newReq.Request.AppendBody(req.Body())
	return newReq
}

func (p *PerformanceGenerator) singleFileSubmitter(chReq chan *Request, req *fasthttp.Request, done chan struct{}) {
	var generated = 0
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		default:
			request := p.cloneRequest(req)
			request.Request.SetHost(p.Host)
			request.Request.SetRequestURI(p.GetURI("", p.workload.Args))
			if p.workload.Count == 0 {
				chReq <- request
				generated++
			} else if generated < p.workload.Count {
				chReq <- request
				generated++
			} else {
				break LOOP
			}
		}
	}
	close(chReq)
}

func (p *PerformanceGenerator) genFilesURI(fileIndex int, count int, random bool) chan string {
	ch := make(chan string, 1000)
	go func() {
		if random {
			for {
				n := rand.Intn(count)
				ch <- fmt.Sprintf("%s_%d", p.baseURI, n+fileIndex)
			}
		} else {
			filePref := fileIndex
			for {
				if filePref == fileIndex+count {
					filePref = fileIndex
				}
				ch <- fmt.Sprintf("%s_%d", p.baseURI, filePref)
				filePref++
			}
		}
	}()
	return ch
}

func (p *PerformanceGenerator) multiFileSubmitter(chReq chan *Request, req *fasthttp.Request, done chan struct{}) {
	chURI := p.genFilesURI(p.workload.FileIndex, p.workload.FilesCount, p.workload.Random)
	var generated = 0
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		default:
			uri := <-chURI
			request := p.cloneRequest(req)
			request.Request.SetRequestURI(uri)
			if p.workload.Count == 0 {
				chReq <- request
				generated++
			} else if generated < p.workload.Count {
				chReq <- request
				generated++
			} else {
				break LOOP
			}
		}
	}
	close(chReq)
}
