package worker

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

import (
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/requestgenerators"
	"github.com/v3io/http_blaster/httpblaster/tui"
	//"time"
)

//PerfWorker : worker focused on performance
type PerfWorker struct {
	Base
}

//UseBase : for to use abs
func (w *PerfWorker) UseBase(c Base) {

}

// RunWorker :  worker run method
func (w *PerfWorker) RunWorker(chResp chan *requestgenerators.Response,
	chReq chan *requestgenerators.Request,
	wg *sync.WaitGroup, releaseReq bool,
	countSubmitted *tui.Counter,
	//ch_latency chan time.Duration,
	//ch_statuses chan int,
	dumpRequests bool,
	dumpLocation string) {
	defer wg.Done()
	var reqType sync.Once
	w.countSubmitted = countSubmitted
	doOnce.Do(func() {
		log.Info("Running Performance workers")
	})

	response := requestgenerators.AcquireResponse()
	defer requestgenerators.ReleaseResponse(response)
	for req := range chReq {
		reqType.Do(func() {
			w.Results.Method = string(req.Request.Header.Method())
		})
		req.Request.SetHost(w.host)
		log.Infoln(w.host)
		_, err := w.sendRequest(req, response)

		if err != nil {
			log.Errorf("send request failed %s", err.Error())
		}

		//ch_statuses <- response.Response.StatusCode()
		//ch_latency <- d
		requestgenerators.ReleaseRequest(req)
		response.Response.Reset()
	}
	log.Debugln("closing hist")
	w.hist.Close()
	w.closeConnection()
}
