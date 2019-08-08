package requestgenerators

import (
	"encoding/csv"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/igzdata"
)

type Csv2KV struct {
	workload config.Workload
	RequestCommon
}

func (c *Csv2KV) UseCommon(rc RequestCommon) {

}

func (c *Csv2KV) generateRequest(chRecords chan []string, chReq chan *Request, host string,
	wg *sync.WaitGroup) {
	defer wg.Done()
	parser := igzdata.EmdSchemaParser{}
	var contentType = "text/html"
	e := parser.LoadSchema(c.workload.Schema, "", "")
	if e != nil {
		panic(e)
	}
	for r := range chRecords {
		JSONpayload := parser.EmdFromCSVRecord(r)
		req := AcquireRequest()
		c.PrepareRequest(contentType, c.workload.Header, "PUT",
			c.baseURI, JSONpayload, host, req.Request)
		chReq <- req
	}
}

func (c *Csv2KV) generate(chReq chan *Request, payload string, host string) {
	defer close(chReq)
	var chRecords chan []string = make(chan []string, 1000)
	parser := igzdata.EmdSchemaParser{}
	e := parser.LoadSchema(c.workload.Schema, "", "")
	if e != nil {
		panic(e)
	}

	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for cpu := 0; cpu < runtime.NumCPU(); cpu++ {
		go c.generateRequest(chRecords, chReq, host, &wg)
	}

	chFiles := c.FilesScan(c.workload.Payload)

	for f := range chFiles {
		fp, err := os.Open(f)
		if err != nil {
			panic(err)
		}

		r := csv.NewReader(fp)
		r.Comma = parser.JSONSchema.Settings.Separator.Rune
		var lineCount = 0
		for {
			record, err := r.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}

			if strings.HasPrefix(record[0], "#") {
				log.Println("Skipping scv header ", strings.Join(record[:], ","))
			} else {
				chRecords <- record
				lineCount++
				if lineCount%1024 == 0 {
					log.Printf("line: %d from file %s was submitted", lineCount, f)
				}
			}
		}
		fp.Close()
	}

	close(chRecords)
	wg.Wait()
}

func (c *Csv2KV) GenerateRequests(global config.Global, wl config.Workload, TLSMode bool, host string, chRet chan *Response, workerQD int) chan *Request {
	c.workload = wl
	if c.workload.Header == nil {
		c.workload.Header = make(map[string]string)
	}
	c.workload.Header["X-v3io-function"] = "PutItem"

	c.SetBaseUri(TLSMode, host, c.workload.Container, c.workload.Target)

	chReq := make(chan *Request, workerQD)

	go c.generate(chReq, c.workload.Payload, host)

	return chReq
}
