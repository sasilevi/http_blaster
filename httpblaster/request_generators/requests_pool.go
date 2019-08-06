package request_generators

import (
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

type Request struct {
	Cookie                   interface{}
	ID                       int
	Request                  *fasthttp.Request
	Host                     string
	ExpectedConnectionStatus bool
}

type Response struct {
	Cookie     interface{}
	ID         int
	Response   *fasthttp.Response
	Duration   time.Duration
	RequestURI string
	Endpoint   string
}

var (
	requestPool  sync.Pool
	responsePool sync.Pool
	ids          int
)

func AcquireRequest() *Request {
	v := requestPool.Get()
	if v == nil {
		ids++
		return &Request{Request: fasthttp.AcquireRequest(), ID: ids}
	}
	return v.(*Request)
}

func ReleaseRequest(req *Request) {
	req.Request.Reset()
	req.Cookie = nil
	requestPool.Put(req)
}

func AcquireResponse() *Response {
	v := responsePool.Get()
	if v == nil {
		return &Response{Response: fasthttp.AcquireResponse()}
	}
	return v.(*Response)
}

func ReleaseResponse(resp *Response) {
	resp.Response.Reset()
	responsePool.Put(resp)
}
