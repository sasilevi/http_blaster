package requestgenerators

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"sync"

	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
)

//RestoreGenerator : RestoreGenerator generator
type RestoreGenerator struct {
	RequestCommon
	workload       config.Workload
	reItem         *regexp.Regexp
	reItems        *regexp.Regexp
	reName         *regexp.Regexp
	reCollectionID *regexp.Regexp
	reRemoveItems  *regexp.Regexp
	emdIgnoreAttrs []string
}

type backupItem struct {
	Payload []byte
	URI     string
}

func (r *RestoreGenerator) useCommon(c RequestCommon) {

}

// LoadSchema : load schema from file
func (r *RestoreGenerator) LoadSchema(filePath string) (map[string]interface{}, error) {
	type backupSchema struct {
		records map[interface{}]interface{}
		inode   map[interface{}]interface{}
		shards  []interface{}
		dir     map[interface{}]map[interface{}]interface{}
	}

	plan, _ := ioutil.ReadFile(filePath)

	var data interface{}
	err := jsoniter.Unmarshal(plan, &data)

	if err != nil {
		panic(err)
	}
	if val, ok := data.(map[string]interface{})["inode"]; ok {
		return val.(map[string]interface{}), nil
	}
	return nil, errors.New("fail to get inode table")

}

type itemsS struct {
	LastItemIncluded interface{}
	NextKey          string
	EvaluatedItems   int
	NumItems         int
	NextMarker       string
	Items            []map[string]map[string]interface{}
}

func (r *RestoreGenerator) generateItems(chLines chan []byte, collectionIds map[string]interface{}) chan *backupItem {
	chItems := make(chan *backupItem, 100000)
	wg := sync.WaitGroup{}
	routines := 1 //runtime.NumCPU()/2
	wg.Add(routines)
	go func() {
		for i := 0; i < routines; i++ {
			go func() {
				defer wg.Done()
				for line := range chLines {
					var itemsJ itemsS
					err := jsoniter.Unmarshal(line, &itemsJ)
					if err != nil {
						log.Println("Unable to Unmarshal line:", string(line))
						panic(err)
					}
					items := itemsJ.Items
					for _, i := range items {
						itemName := i["__name"]["S"]
						collectionID := i["__collectionID"]["N"]
						dirName := collectionIds[collectionID.(string)]
						if dirName == nil {
							log.Errorf("Fail to get dir name for collection id: %v", collectionID)
							continue
						}
						for _, attr := range r.emdIgnoreAttrs {
							delete(i, attr)
						}

						j, e := jsoniter.Marshal(i)
						if e != nil {
							log.Println("Unable to Marshal json:", i)
							panic(e)
						}
						var payload bytes.Buffer
						if len(i) != 0 {
							payload.WriteString(`{"Item": `)
							payload.Write(j)
							payload.WriteString(`}`)
							chItems <- &backupItem{URI: r.baseURI + dirName.(string) + itemName.(string),
								Payload: payload.Bytes()}
						}
					}
				}
			}()
		}
		wg.Wait()
		close(chItems)
	}()
	return chItems
}

func (r *RestoreGenerator) generate(chReq chan *Request,
	chItems chan *backupItem, host string) {
	defer close(chReq)
	wg := sync.WaitGroup{}

	routines := 1 //runtime.NumCPU()
	wg.Add(routines)
	for i := 0; i < routines; i++ {
		go func() {
			defer wg.Done()
			for item := range chItems {
				req := AcquireRequest()
				r.prepareRequestBytes(contentType, r.workload.Header, "PUT",
					item.URI, item.Payload, host, req.Request)
				chReq <- req
			}
		}()
	}
	log.Println("Waiting for generators to finish")
	wg.Wait()
	log.Println("generators done")
}

func (r *RestoreGenerator) lineReader() chan []byte {
	chLines := make(chan []byte, 24)
	chFiles := r.filesScan(r.workload.Payload)
	go func() {
		for f := range chFiles {
			if file, err := os.Open(f); err == nil {
				reader := bufio.NewReader(file)
				var i = 0
				for {
					line, lineErr := reader.ReadBytes('\n')
					if lineErr == nil {
						chLines <- line
						i++
					} else if lineErr == io.EOF {
						break
					} else {
						log.Fatal(err)
					}
				}

				log.Println(fmt.Sprintf("Finish file scaning %v, generated %d records", f, i))
			} else {
				panic(err)
			}
		}
		close(chLines)
	}()
	log.Println("finish line generation")
	return chLines
}

//GenerateRequests : GenerateRequests impl
func (r *RestoreGenerator) GenerateRequests(global config.Global, wl config.Workload, TLSMode bool, host string, chRet chan *Response, workerQD int) chan *Request {
	r.workload = wl
	chReq := make(chan *Request, workerQD)

	if r.workload.Header == nil {
		r.workload.Header = make(map[string]string)
	}
	r.emdIgnoreAttrs = global.IgnoreAttrs

	r.workload.Header["X-v3io-function"] = "PutItem"

	r.setBaseURI(TLSMode, host, r.workload.Container, r.workload.Target)

	inodeMap, err := r.LoadSchema(wl.Schema)

	if err != nil {
		panic(err)
	}

	chLines := r.lineReader()

	chItems := r.generateItems(chLines, inodeMap)

	go r.generate(chReq, chItems, host)

	return chReq
}
