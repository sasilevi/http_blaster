package requestgenerators

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const (
	PERFORMANCE = "performance"
	LINE2STREAM = "line2stream"
	CSV2KV      = "csv2kv"
	CSVUPDATEKV = "csvupdatekv"
	CSV2STREAM  = "csv2stream"
	JSON2KV     = "json2kv"
	STREAMGET   = "stream_get"
	LINE2KV     = "line2kv"
	RESTORE     = "restore"
	LINE2HTTP   = "line2http"
	REPLAY      = "replay"
	CSV2TSDB    = "csv2tsdb"
	STATS2TSDB  = "stats2tsdb"
	ONELINK     = "onelink"
	IMPERSONATE = "impersonate"
)

type RequestCommon struct {
	chFiles chan string
	baseURI string
}

var (
	contentType string = "application/json"
)

func (r *RequestCommon) PrepareRequest(contentType string,
	headerArgs map[string]string,
	method string, uri string,
	body string, host string, req *fasthttp.Request) {
	u := url.URL{Path: uri}
	req.Header.SetContentType(contentType)
	req.Header.SetMethod(method)
	req.Header.SetRequestURI(u.EscapedPath())
	req.Header.SetHost(host)
	for k, v := range headerArgs {
		req.Header.Set(k, v)
	}
	req.AppendBodyString(body)
}

func (r *RequestCommon) PrepareRequestBytes(contentType string,
	headerArgs map[string]string,
	method string, uri string,
	body []byte, host string, req *fasthttp.Request) {
	u := url.URL{Path: uri}
	req.Header.SetContentType(contentType)
	req.Header.SetMethod(method)
	req.Header.SetRequestURI(u.EscapedPath())
	req.Header.SetHost(host)
	for k, v := range headerArgs {
		req.Header.Set(k, v)
	}
	req.AppendBody(body)
}

func (r *RequestCommon) SubmitFiles(path string, info os.FileInfo, err error) error {
	log.Print(path)
	if err != nil {
		log.Print(err)
		return nil
	}
	if !info.IsDir() {
		r.chFiles <- path
	}
	fmt.Println(path)
	return nil
}

func (r *RequestCommon) FilesScan(path string) chan string {
	r.chFiles = make(chan string)
	go func() {
		err := filepath.Walk(path, r.SubmitFiles)
		if err != nil {
			log.Fatal(err)
		}
		close(r.chFiles)
	}()
	return r.chFiles
}

func (r *RequestCommon) SetBaseUri(TLSMode bool, host string, container string, target string) {
	http := "http"
	if TLSMode {
		http += "s"
	}

	r.baseURI = fmt.Sprintf("%s://%s", http, host)

	if len(container) > 0 {
		r.baseURI += fmt.Sprintf("/%s", container)

	}
	if len(target) > 0 {
		r.baseURI += fmt.Sprintf("/%s", target)
	}
}

func (r *RequestCommon) GetUri(target string, params string) string {
	if len(target) > 0 {
		u := url.URL{Path: fmt.Sprintf("%s/%s", r.baseURI, target)}
		return u.EscapedPath() + params
	}
	u := url.URL{Path: r.baseURI}
	return u.EscapedPath() + params
}
