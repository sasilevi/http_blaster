package requestgenerators

import (
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

//Request : blaster request obj
type Request struct {
	Cookie                   interface{}
	ID                       int
	Request                  *fasthttp.Request
	Host                     string
	ExpectedConnectionStatus bool
}

//Response : blaster Response obj
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

//AcquireRequest : get request from pool
func AcquireRequest() *Request {
	v := requestPool.Get()
	if v == nil {
		ids++
		return &Request{Request: fasthttp.AcquireRequest(), ID: ids, ExpectedConnectionStatus: true}
	}
	return v.(*Request)
}

//ReleaseRequest : release request back to pool
func ReleaseRequest(req *Request) {
	req.Request.Reset()
	req.Cookie = nil
	requestPool.Put(req)
}

//AcquireResponse : get response object from pool
func AcquireResponse() *Response {
	v := responsePool.Get()
	if v == nil {
		return &Response{Response: fasthttp.AcquireResponse()}
	}
	return v.(*Response)
}

//ReleaseResponse : release response obj back to pool
func ReleaseResponse(resp *Response) {
	resp.Response.Reset()
	responsePool.Put(resp)
}
