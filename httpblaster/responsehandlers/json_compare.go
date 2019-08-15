package responsehandlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/nsf/jsondiff"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/requestgenerators"
)

// JSONCompareResponseHandler : response handler for redirect test
type JSONCompareResponseHandler struct {
	BaseResponseHandler
	pendingResponses map[interface{}]*requestgenerators.Response
	ErrorCounters    map[string]int64
	Errors           int64
	results          map[string]*errorInfo
	logfile          *os.File
}

// type errorInfo struct {
// 	Status       int
// 	url1         string
// 	url2         string
// 	compareError bool
// 	requestError bool
// 	statusCode   int
// }

func (r *JSONCompareResponseHandler) startCompareLog() {
	filename := "compare_out.html"
	var e error
	r.logfile, e = os.Create(filename)
	if e != nil {
		panic(e.Error())
	}
	r.logfile.WriteString("<html>")
	r.logfile.WriteString("\n<body>")
	r.logfile.WriteString("\n<pre>")
	r.logfile.WriteString("\n<code>")

}

type matchResponces struct {
	r1 *requestgenerators.Response
	r2 *requestgenerators.Response
}

func (r *JSONCompareResponseHandler) stopCompareLog() {

	r.logfile.WriteString("\n</code>")
	r.logfile.WriteString("\n</pre>")
	r.logfile.WriteString("\n</html>")
	r.logfile.Close()
}

func (r *JSONCompareResponseHandler) writeCompareDiff(v1, v2, diff, errorStr string) {
	r.logfile.WriteString(fmt.Sprintf("\nSource:%v\nCompare:%v\nDiff:%v\n%v", v1, v2, diff, errorStr))
}

// HandlerResponses :  handler function to habdle responses
func (r *JSONCompareResponseHandler) HandlerResponses(global config.Global, workload config.Workload, respCh chan *requestgenerators.Response) {
	r.pendingResponses = make(map[interface{}]*requestgenerators.Response)
	r.ErrorCounters = make(map[string]int64)
	chMatchResponces := make(chan *matchResponces)
	wg := sync.WaitGroup{}

	r.startCompareLog()
	defer r.stopCompareLog()

	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go r.compareJSONResponces(chMatchResponces, &wg)
	}

	for resp := range respCh {
		if resp.Response.StatusCode() != http.StatusOK {
			r.Errors++
		}
		if _, ok := r.pendingResponses[resp.Cookie]; ok {
			chMatchResponces <- &matchResponces{r1: r.pendingResponses[resp.Cookie], r2: resp}
			// r.compareJSONResponses(r.pendingResponses[resp.Cookie], resp)
			delete(r.pendingResponses, resp.Cookie)
		} else {
			r.pendingResponses[resp.Cookie] = resp
		}
	}
	close(chMatchResponces)
	wg.Wait()
}

func (r *JSONCompareResponseHandler) compareJSONResponces(responce chan *matchResponces, wg *sync.WaitGroup) {
	defer wg.Done()
	for c := range responce {
		r.compareJSONResponses(c.r1, c.r2)
	}
}

func (r *JSONCompareResponseHandler) compareJSONResponses(r1, r2 *requestgenerators.Response) error {
	var o1 interface{}
	var o2 interface{}

	defer requestgenerators.ReleaseResponse(r1)
	defer requestgenerators.ReleaseResponse(r2)

	json.Unmarshal(r1.Response.Body(), &o1)
	json.Unmarshal(r2.Response.Body(), &o2)
	var first = r1
	var second = r2
	ops := jsondiff.DefaultHTMLOptions()
	if !strings.Contains(r1.RequestURI, "v3") {
		first = r2
		second = r1
	}

	if !reflect.DeepEqual(o1, o2) {
		diff, err := jsondiff.Compare(first.Response.Body(), second.Response.Body(), &ops)
		r.writeCompareDiff(first.RequestURI, second.RequestURI, diff.String(), err)
		r.Errors++
	}

	return nil
}

// Report : report redirect responses assertions
func (r *JSONCompareResponseHandler) Report() string {
	return "TODO: implement JSON Compare report"
}

// Counters : returns counters for wrong link and not found
func (r *JSONCompareResponseHandler) Counters() map[string]int64 {
	return r.ErrorCounters
}

func (r *JSONCompareResponseHandler) Error() error {
	if r.Errors > 0 {
		return fmt.Errorf("Response handler %v completed with %v errors", reflect.TypeOf(r).Elem().Name(), r.Errors)
	}
	return nil
}
