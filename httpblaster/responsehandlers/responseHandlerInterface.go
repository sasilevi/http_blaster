package responsehandlers

import (
	"sync"

	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/requestgenerators"
	"github.com/v3io/http_blaster/httpblaster/tui"
)

//IResponseHandler : response handler interface
type IResponseHandler interface {
	//Impliment this method in your handler
	HandlerResponses(global config.Global, workload config.Workload, respoCh chan *requestgenerators.Response)
	//This methos is in base object, do not impliment
	Run(global config.Global, workload config.Workload, respoCh chan *requestgenerators.Response, wg *sync.WaitGroup, rh IResponseHandler, counter *tui.Counter)
	//Impliment this method to report the log
	Report() string
	//Return countes collected by the executor
	Counters() map[string]int64
	//Return error in case of failure else return nil
	Error() error
}
