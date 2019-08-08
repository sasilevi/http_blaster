package requestgenerators

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"

	uuid "github.com/nu7hatch/gouuid"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/igzdata"
)

//CSV2StreamGenerator : CSV2StreamGenerator generator
type CSV2StreamGenerator struct {
	RequestCommon
	workload config.Workload
}

func (c *CSV2StreamGenerator) useCommon(rc RequestCommon) {

}

//Hash32 : Hash32 calc hash 32 from given string
func (c *CSV2StreamGenerator) Hash32(line string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(line))
	return h.Sum32()
}

func (c *CSV2StreamGenerator) generateRequest(chRecords chan string,
	chReq chan *Request,
	host string, wg *sync.WaitGroup) {
	defer wg.Done()
	u, _ := uuid.NewV4()
	for r := range chRecords {
		columns := strings.Split(r, c.workload.Separator)
		shardID := c.Hash32(columns[c.workload.ShardColumn]) % c.workload.ShardCount
		sr := igzdata.NewStreamRecord("client", r, u.String(), int(shardID), true)
		r := igzdata.NewStreamRecords(sr)
		req := AcquireRequest()
		c.prepareRequest(contentType, c.workload.Header, "PUT",
			c.baseURI, r.ToJSONString(), host, req.Request)
		chReq <- req
	}
	log.Println("generateRequest Done")
}

func (c *CSV2StreamGenerator) generate(chReq chan *Request, payload string, host string) {
	defer close(chReq)
	var chRecords = make(chan string)
	wg := sync.WaitGroup{}
	chFiles := c.filesScan(c.workload.Payload)

	wg.Add(runtime.NumCPU())
	for cpu := 0; cpu < runtime.NumCPU(); cpu++ {
		go c.generateRequest(chRecords, chReq, host, &wg)
	}

	for f := range chFiles {
		if file, err := os.Open(f); err == nil {
			reader := bufio.NewReader(file)
			var i = 0
			for {
				line, err := reader.ReadString('\n')
				if err == nil {
					chRecords <- strings.TrimSpace(line)
					i++
				} else if err == io.EOF {
					break
				} else {
					log.Fatal(err)
				}
			}

			log.Println(fmt.Sprintf("Finish file scaning, generated %d records", i))
		} else {
			panic(err)
		}
	}
	close(chRecords)
	log.Println("Waiting for generators to finish")
	wg.Wait()
	log.Println("generators done")
}

//GenerateRequests : GenerateRequests impl
func (c *CSV2StreamGenerator) GenerateRequests(global config.Global, wl config.Workload, TLSMode bool, host string, chRet chan *Response, workerQD int) chan *Request {
	c.workload = wl
	if c.workload.Header == nil {
		c.workload.Header = make(map[string]string)
	}
	c.workload.Header["X-v3io-function"] = "PutRecords"

	c.setBaseURI(TLSMode, host, c.workload.Container, c.workload.Target)

	chReq := make(chan *Request, workerQD)

	go c.generate(chReq, c.workload.Payload, host)

	return chReq
}
