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

type CsvUpdateKV struct {
	workload config.Workload
	RequestCommon
}

func (cu *CsvUpdateKV) UseCommon(c RequestCommon) {

}

func (cu *CsvUpdateKV) generateRequest(chRecords chan []string, chReq chan *Request, host string,
	wg *sync.WaitGroup) {
	defer wg.Done()
	parser := igzdata.EmdSchemaParser{}
	var contentType = "application/json"
	e := parser.LoadSchema(cu.workload.Schema, cu.workload.UpdateMode, cu.workload.UpdateExpression)
	if e != nil {
		panic(e)
	}
	for r := range chRecords {
		JSONPayload := parser.EmdUpdateFromCSVRecord(r)
		key := parser.KeyFromCSVRecord(r)
		req := AcquireRequest()
		cu.prepareRequest(contentType, cu.workload.Header, "POST",
			cu.baseURI+key, JSONPayload, host, req.Request)
		//panic(fmt.Sprintf("%+v", req))
		chReq <- req
	}
}

func (cu *CsvUpdateKV) generate(chReq chan *Request, payload string, host string) {
	defer close(chReq)
	var chRecords = make(chan []string, 1000)
	parser := igzdata.EmdSchemaParser{}
	e := parser.LoadSchema(cu.workload.Schema, cu.workload.UpdateMode, cu.workload.UpdateExpression)
	if e != nil {
		panic(e)
	}

	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go cu.generateRequest(chRecords, chReq, host, &wg)
	}

	chFiles := cu.filesScan(cu.workload.Payload)

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

func (cu *CsvUpdateKV) GenerateRequests(global config.Global, wl config.Workload, TLSMode bool, host string, retCh chan *Response, workerQD int) chan *Request {
	cu.workload = wl
	if cu.workload.Header == nil {
		cu.workload.Header = make(map[string]string)
	}
	cu.workload.Header["X-v3io-function"] = "UpdateItem"

	cu.setBaseURI(TLSMode, host, cu.workload.Container, cu.workload.Target)

	chReq := make(chan *Request, workerQD)

	go cu.generate(chReq, cu.workload.Payload, host)

	return chReq
}

/*
Content-Type="application/json"
X-v3io-function="UpdateItem"
{
    "UpdateMode" : "CreateOrReplaceAttributes",
    "UpdateExpression":  "if_not_exists( hits , 0 ); hits=hits+1; subcustomer_id='GMMNLZNRUNEB'; geographic_region=666; billing_flag=6; ip_address='169.163.9.26' "
}

*/
