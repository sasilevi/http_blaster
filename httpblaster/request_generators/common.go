package request_generators

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
	STREAM_GET  = "stream_get"
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
	ch_files chan string
	base_uri string
}

var (
	contentType string = "application/json"
)

func (self *RequestCommon) PrepareRequest(content_type string,
	header_args map[string]string,
	method string, uri string,
	body string, host string, req *fasthttp.Request) {
	u := url.URL{Path: uri}
	req.Header.SetContentType(content_type)
	req.Header.SetMethod(method)
	req.Header.SetRequestURI(u.EscapedPath())
	req.Header.SetHost(host)
	for k, v := range header_args {
		req.Header.Set(k, v)
	}
	req.AppendBodyString(body)
}

func (self *RequestCommon) PrepareRequestBytes(content_type string,
	header_args map[string]string,
	method string, uri string,
	body []byte, host string, req *fasthttp.Request) {
	u := url.URL{Path: uri}
	req.Header.SetContentType(content_type)
	req.Header.SetMethod(method)
	req.Header.SetRequestURI(u.EscapedPath())
	req.Header.SetHost(host)
	for k, v := range header_args {
		req.Header.Set(k, v)
	}
	req.AppendBody(body)
}

func (self *RequestCommon) SubmitFiles(path string, info os.FileInfo, err error) error {
	log.Print(path)
	if err != nil {
		log.Print(err)
		return nil
	}
	if !info.IsDir() {
		self.ch_files <- path
	}
	fmt.Println(path)
	return nil
}

func (self *RequestCommon) FilesScan(path string) chan string {
	self.ch_files = make(chan string)
	go func() {
		err := filepath.Walk(path, self.SubmitFiles)
		if err != nil {
			log.Fatal(err)
		}
		close(self.ch_files)
	}()
	return self.ch_files
}

func (self *RequestCommon) SetBaseUri(tls_mode bool, host string, container string, target string) {
	http := "http"
	if tls_mode {
		http += "s"
	}

	self.base_uri = fmt.Sprintf("%s://%s", http, host)

	if len(container) > 0 {
		self.base_uri += fmt.Sprintf("/%s", container)

	}
	if len(target) > 0 {
		self.base_uri += fmt.Sprintf("/%s", target)
	}
}

func (self *RequestCommon) GetUri(target string, params string) string {
	if len(target) > 0 {
		u := url.URL{Path: fmt.Sprintf("%s/%s", self.base_uri, target)}
		return u.EscapedPath() + params
	}
	u := url.URL{Path: self.base_uri}
	return u.EscapedPath() + params
}
