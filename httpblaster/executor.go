/*
Copyright 2016 Iguazio.io Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/
package httpblaster

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
	responsehandlers "github.com/v3io/http_blaster/httpblaster/responseHandlers"
	"github.com/v3io/http_blaster/httpblaster/tui"
	"github.com/v3io/http_blaster/httpblaster/worker"
)

type executorResults struct {
	Total                  uint64
	Duration               time.Duration
	Min                    time.Duration
	Max                    time.Duration
	Avg                    time.Duration
	Iops                   uint64
	Latency                map[int]int64
	Statuses               map[int]uint64
	Errors                 map[string]int
	ErrorsCount            uint32
	ConRestarts            uint32
	ResponseHandlerResults interface{}
	Counters               map[string]int64
	ResponseErrors         error
}

// Executor : executor is workload execution intity which responsible for workers, generators and response handlers
type Executor struct {
	connections int32
	Workload    config.Workload
	Globals     config.Global
	//host                  string
	//port                  string
	//tlsMode              bool
	Host  string
	Hosts []string
	//Port                  string
	TLSMode      bool
	results      executorResults
	workers      []worker.Worker
	StartTime    time.Time
	DataBfr      []byte
	WorkerQd     int
	TermUI       *tui.Term_ui
	ChGetLatency chan time.Duration
	ChPutLatency chan time.Duration
	//Ch_statuses    chan int
	DumpFailures bool
	DumpLocation string
}

func (ex *Executor) loadResponseHandler(resp chan *request_generators.Response, wg *sync.WaitGroup) responsehandlers.IResponseHandler {
	handlerType := strings.ToLower(ex.Workload.ResponseHandler)
	var rh responsehandlers.IResponseHandler

	switch handlerType {
	case "":
		rh = &responsehandlers.NoneHandler{}
	case responsehandlers.REDIRECT:
		rh = &responsehandlers.RedirectResponseHandler{}
	case responsehandlers.DEFAULT:
		rh = &responsehandlers.Default{}
	default:
		log.Println("No response handler was selected")
	}
	go rh.Run(ex.Globals, ex.Workload, resp, wg, rh)
	return rh
}

func (ex *Executor) loadRequestGenerator() (chan *request_generators.Request,
	bool, chan *request_generators.Response) {
	var reqGen request_generators.Generator
	var releaseReq = true
	var chResponse chan *request_generators.Response = nil

	genType := strings.ToLower(ex.Workload.Generator)
	switch genType {
	case request_generators.PERFORMANCE:
		reqGen = &request_generators.PerformanceGenerator{}
		if ex.Workload.FilesCount == 0 {
			releaseReq = false
		}
		break

	case request_generators.LINE2STREAM:
		reqGen = &request_generators.Line2StreamGenerator{}
		break
	case request_generators.CSV2KV:
		reqGen = &request_generators.Csv2KV{}
		break
	case request_generators.CSVUPDATEKV:
		reqGen = &request_generators.CsvUpdateKV{}
		break
	case request_generators.JSON2KV:
		reqGen = &request_generators.Json2KV{}
		break
	case request_generators.LINE2KV:
		reqGen = &request_generators.Line2KvGenerator{}
		break
	case request_generators.RESTORE:
		reqGen = &request_generators.RestoreGenerator{}
		break
	case request_generators.CSV2STREAM:
		reqGen = &request_generators.CSV2StreamGenerator{}
		break
	case request_generators.LINE2HTTP:
		reqGen = &request_generators.Line2HttpGenerator{}
		break
	case request_generators.REPLAY:
		reqGen = &request_generators.Replay{}
		break
	case request_generators.STREAM_GET:
		reqGen = &request_generators.StreamGetGenerator{}
		chResponse = make(chan *request_generators.Response)
	case request_generators.CSV2TSDB:
		reqGen = &request_generators.Csv2TSDB{}
		break
	case request_generators.STATS2TSDB:
		reqGen = &request_generators.Stats2TSDB{}
		break
	case request_generators.ONELINK:
		reqGen = &request_generators.Onelink{}
		chResponse = make(chan *request_generators.Response)
		break
	default:
		panic(fmt.Sprintf("unknown request generator %s", ex.Workload.Generator))
	}
	var host string
	if len(ex.Hosts) > 0 {
		host = ex.Hosts[0]
	} else {
		host = ex.Host
	}

	ch_req := reqGen.GenerateRequests(ex.Globals, ex.Workload, ex.TLSMode, host, chResponse, ex.WorkerQd)
	return ch_req, releaseReq, chResponse
}

func (ex *Executor) GetWorkerType() worker.WorkerType {
	genType := strings.ToLower(ex.Workload.Generator)
	if genType == request_generators.PERFORMANCE {
		return worker.PERFORMANCE_WORKER
	}
	return worker.INGESTION_WORKER
}

func (ex *Executor) GetType() string {
	return ex.Workload.Type
}

func (ex *Executor) run(wg *sync.WaitGroup) error {
	defer wg.Done()
	ex.StartTime = time.Now()
	workers_wg := sync.WaitGroup{}
	rhWg := sync.WaitGroup{}
	workers_wg.Add(ex.Workload.Workers)

	ch_req, releaseReq_flag, chResponse := ex.loadRequestGenerator()
	rhWg.Add(1)
	rh := ex.loadResponseHandler(chResponse, &rhWg)

	for i := 0; i < ex.Workload.Workers; i++ {
		var host_address string
		if len(ex.Hosts) > 0 {
			server_id := (i) % len(ex.Hosts)
			host_address = ex.Hosts[server_id]
		} else {
			host_address = ex.Host
		}

		server := fmt.Sprintf("%s:%s", host_address, ex.Globals.Port)
		w := worker.NewWorker(ex.GetWorkerType(),
			server, ex.Globals.TLSMode, ex.Workload.Lazy,
			ex.Globals.RetryOnStatusCodes,
			ex.Globals.RetryCount, ex.Globals.PemFile, i, ex.Workload.Name)
		ex.workers = append(ex.workers, w)
		//var ch_latency chan time.Duration
		//if ex.Workload.Type == "GET" {
		//	ch_latency = ex.ChGetLatency
		//} else {
		//	ch_latency = ex.ChPutLatency
		//}

		go w.RunWorker(chResponse, ch_req,
			&workers_wg, releaseReq_flag, // ch_latency,
			//ex.Ch_statuses,
			ex.DumpFailures,
			ex.DumpLocation)
	}
	ended := make(chan bool)
	go func() {
		workers_wg.Wait()
		close(ended)
	}()
	tick := time.Tick(time.Millisecond * 500)
LOOP:
	for {
		select {
		case <-ended:
			break LOOP
		case <-tick:
			if ex.TermUI != nil {
				var put_req_count uint64 = 0
				var get_req_count uint64 = 0
				for _, w := range ex.workers {
					wresults := w.GetResults()
					if w.GetResults().Method == `PUT` {
						put_req_count += wresults.Count
					} else {
						get_req_count += wresults.Count
					}
				}
				ex.TermUI.Update_requests(time.Now().Sub(ex.StartTime), put_req_count, get_req_count)
			}
		}
	}

	ex.results.Duration = time.Now().Sub(ex.StartTime)
	ex.results.Min = time.Duration(time.Second * 10)
	ex.results.Max = 0
	ex.results.Avg = 0
	ex.results.Total = 0
	ex.results.Iops = 0
	log.Println("close response channel")
	if chResponse != nil {
		close(chResponse)
	}

	log.Println("Waiting for response handler to finish")
	rhWg.Wait()
	ex.results.ResponseErrors = rh.Error()
	ex.results.Counters = rh.Counters()

	log.Println(rh.Report())
	for _, w := range ex.workers {
		wresults := w.GetResults()
		ex.results.ConRestarts += wresults.ConnectionRestarts
		ex.results.ErrorsCount += wresults.ErrorCount

		ex.results.Total += wresults.Count
		if w.GetResults().Min < ex.results.Min {
			ex.results.Min = wresults.Min
		}
		if w.GetResults().Max > ex.results.Max {
			ex.results.Max = wresults.Max
		}

		ex.results.Avg +=
			time.Duration(float64(wresults.Count) / float64(ex.results.Total) * float64(wresults.Avg))
		for k, v := range wresults.Codes {
			ex.results.Statuses[k] += v
		}
	}

	seconds := uint64(ex.results.Duration.Seconds())
	if seconds == 0 {
		seconds = 1
	}
	ex.results.Iops = ex.results.Total / seconds

	log.Info("Ending ", ex.Workload.Name)

	return nil
}

func (ex *Executor) Start(wg *sync.WaitGroup) error {
	ex.results.Statuses = make(map[int]uint64)
	log.Info("at executor start ", ex.Workload)
	go func() {
		ex.run(wg)
	}()
	return nil
}

func (ex *Executor) Stop() error {
	return errors.New("Not Implimented!!!")
}

func (ex *Executor) Report() (executorResults, error) {
	log.Info("report for wl ", ex.Workload.ID, ":")
	log.Info("Total Requests ", ex.results.Total)
	log.Info("Min: ", ex.results.Min)
	log.Info("Max: ", ex.results.Max)
	log.Info("Avg: ", ex.results.Avg)
	log.Info("Connection Restarts: ", ex.results.ConRestarts)
	log.Info("Error Count: ", ex.results.ErrorsCount)
	log.Info("Statuses: ")
	for k, v := range ex.results.Statuses {
		log.Println(fmt.Sprintf("%d - %d", k, v))
	}

	log.Info("iops: ", ex.results.Iops)
	for err_code, err_count := range ex.results.Statuses {
		if max_errors, ok := ex.Globals.StatusCodesAcceptance[strconv.Itoa(err_code)]; ok {
			if ex.results.Total > 0 && err_count > 0 {
				err_percent := (float64(err_count) * float64(100)) / float64(ex.results.Total)
				log.Infof("status code %d occured %f%% during the test \"%s\"",
					err_code, err_percent, ex.Workload.Name)
				if float64(err_percent) > float64(max_errors) {
					return ex.results,
						errors.New(fmt.Sprintf("Executor %s completed with errors: %+v",
							ex.Workload.Name, ex.results.Statuses))
				}
			}
		} else {
			return ex.results, errors.New(fmt.Sprintf("Executor %s completed with errors: %+v",
				ex.Workload.Name, ex.results.Statuses))
		}
	}
	if ex.results.ErrorsCount > 0 {
		return ex.results, errors.New("executor completed with errors")
	}
	return ex.results, nil
}

func (ex *Executor) LatencyHist() map[int64]int {
	res := make(map[int64]int)
	for _, w := range ex.workers {
		hist := w.GetHist()
		for k, v := range hist {
			res[k] += v
		}
	}
	return res
}
