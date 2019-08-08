package worker

import (
	"sync"

	"github.com/v3io/http_blaster/httpblaster/requestgenerators"
	"github.com/v3io/http_blaster/httpblaster/tui"
	//"time"
)

// Worker : worker interface
type Worker interface {
	UseBase(c Base)
	Init(lazy int)
	GetResults() Results
	GetHist() map[int64]int
	RunWorker(chResp chan *requestgenerators.Response,
		chReq chan *requestgenerators.Request,
		wg *sync.WaitGroup,
		releaseReq bool,
		countSubmitted *tui.Counter,
		//ch_latency chan time.Duration,
		//ch_statuses chan int,
		dumpRequests bool,
		dumpLocation string)
}
