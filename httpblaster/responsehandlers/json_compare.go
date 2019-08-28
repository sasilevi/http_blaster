package responsehandlers

import (
	"fmt"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sasilevi/jsondiff"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/db"
	"github.com/v3io/http_blaster/httpblaster/requestgenerators"
)

type benchamrkResults struct {
	Min   time.Duration
	Max   time.Duration
	Avg   time.Duration
	Count uint64
}

// JSONCompareResponseHandler : response handler for redirect test
type JSONCompareResponseHandler struct {
	BaseResponseHandler
	pendingResponses          map[interface{}]*requestgenerators.Response
	ErrorCounters             map[string]int64
	Errors                    int64
	results                   map[string]*errorInfo
	logfile                   *os.File
	v3Benchmark               benchamrkResults
	v4Benchmark               benchamrkResults
	v3NonAttributionBenchmark benchamrkResults
	v4NonAttributionBenchmark benchamrkResults
	psql                      *db.PostgresDB
	reportDb                  bool
	recordFile                bool
}

func (r *JSONCompareResponseHandler) startCompareLog() {
	if !r.recordFile {
		return
	}
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
	if !r.recordFile {
		return
	}
	r.logfile.WriteString("\n</code>")
	r.logfile.WriteString("\n</pre>")
	r.logfile.WriteString("\n</html>")
	r.logfile.Close()
}

func (r *JSONCompareResponseHandler) calcBenchmark(resDuration chan time.Duration, benchmark *benchamrkResults) {
	for d := range resDuration {
		atomic.AddUint64(&benchmark.Count, 1)
		benchmark.Count++
		benchmark.Avg = benchmark.Avg + (d-benchmark.Avg)/time.Duration(benchmark.Count)
		if benchmark.Min == 0 {
			benchmark.Min = d
		}
		if d > benchmark.Max {
			benchmark.Max = d
		} else if d < benchmark.Min {
			benchmark.Min = d
		}
	}
}

func (r *JSONCompareResponseHandler) writeCompareDiff(v1, v2, diff, errorStr string, diffMap jsondiff.DiffMap) {
	if !r.recordFile {
		return
	}
	r.logfile.WriteString(fmt.Sprintf("\nSource:%v\nCompare:%v\nDiff:%v\n%v", v1, v2, diff, errorStr))
	r.logfile.WriteString(fmt.Sprintf("\nAdded:%v", diffMap.GetAdded()))
	r.logfile.WriteString(fmt.Sprintf("\nRemoved:%v", diffMap.GetRemoved()))
	r.logfile.WriteString(fmt.Sprintf("\nChanged:%v", diffMap.GetChanged()))

}

// HandlerResponses :  handler function to habdle responses
func (r *JSONCompareResponseHandler) HandlerResponses(global config.Global, workload config.Workload, respCh chan *requestgenerators.Response) {
	r.pendingResponses = make(map[interface{}]*requestgenerators.Response)
	r.ErrorCounters = make(map[string]int64)
	r.recordFile = workload.ResponseHandlerDumpToFile
	chMatchResponces := make(chan *matchResponces)
	if global.DbHost != "" {
		r.psql = db.New(global.DbHost, global.DbPort, global.DbName, global.DbUser, global.DbPassword)
		r.reportDb = true
		defer r.psql.Close()
	}
	wg := sync.WaitGroup{}

	v3DurationCh := make(chan time.Duration)
	defer close(v3DurationCh)
	v4DurationCh := make(chan time.Duration)
	defer close(v4DurationCh)
	v3NonAttributionDurationCh := make(chan time.Duration)
	defer close(v3NonAttributionDurationCh)
	v4NonAttributionDurationCh := make(chan time.Duration)
	defer close(v4NonAttributionDurationCh)

	go r.calcBenchmark(v3DurationCh, &r.v3Benchmark)
	go r.calcBenchmark(v4DurationCh, &r.v4Benchmark)
	go r.calcBenchmark(v3NonAttributionDurationCh, &r.v3NonAttributionBenchmark)
	go r.calcBenchmark(v4NonAttributionDurationCh, &r.v4NonAttributionBenchmark)

	r.startCompareLog()
	defer r.stopCompareLog()

	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go r.compareJSONResponces(v3DurationCh, v4DurationCh, v3NonAttributionDurationCh, v4NonAttributionDurationCh, chMatchResponces, &wg)
	}

	for resp := range respCh {
		if resp.Response.StatusCode() != http.StatusOK {
			log.Println("Failed on uri:", resp.RequestURI)
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

func (r *JSONCompareResponseHandler) compareJSONResponces(v3DurationCh, v4DurationCh, v3NonAttributionDurationCh, v4NonAttributionDurationCh chan time.Duration, responce chan *matchResponces, wg *sync.WaitGroup) {
	defer wg.Done()
	for c := range responce {
		r.compareJSONResponses(v3DurationCh, v4DurationCh, v3NonAttributionDurationCh, v4NonAttributionDurationCh, c.r1, c.r2)
	}
}

func (r *JSONCompareResponseHandler) compareJSONResponses(v3DurationCh, v4DurationCh, v3NonAttributionDurationCh, v4NonAttributionDurationCh chan time.Duration, r1, r2 *requestgenerators.Response) error {
	defer requestgenerators.ReleaseResponse(r1)
	defer requestgenerators.ReleaseResponse(r2)
	var first = r1
	var second = r2
	// ops := jsondiff.DefaultHTMLOptions()

	ops := jsondiff.Options{
		Added:   jsondiff.Tag{Begin: `<span style="background-color: #8bff7f">Added:`, End: `</span>`},
		Removed: jsondiff.Tag{Begin: `<span style="background-color: #fd7f7f">Removed`, End: `</span>`},
		Changed: jsondiff.Tag{Begin: `<span style="background-color: #fcff7f">Changed`, End: `</span>`},
		Indent:  "    ",
	}
	if !strings.Contains(r1.RequestURI, "v3") {
		first = r2
		second = r1
	}
	if second.Response.StatusCode() == http.StatusOK {
		v3DurationCh <- first.Duration
		v4DurationCh <- second.Duration
	} else {
		v3NonAttributionDurationCh <- first.Duration
		v4NonAttributionDurationCh <- second.Duration
	}
	if second.Duration > time.Duration(time.Second*8) {
		log.Errorln(second.RequestURI, " duration:", second.Duration)
	}

	diff, err, diffMap := jsondiff.Compare(first.Response.Body(), second.Response.Body(), &ops)
	if diff != jsondiff.FullMatch {
		r.writeCompareDiff(first.RequestURI, second.RequestURI, diff.String(), err, diffMap)
		added := diffMap.GetAdded()
		removed := diffMap.GetRemoved()
		changed := diffMap.GetChanged()
		if r.reportDb {
			r.psql.InserAPICompareInfo(first.RequestURI, second.RequestURI, diff.String(), err,
				first.Response.StatusCode(), second.Response.StatusCode(), first.Duration, second.Duration, added, removed, changed)
		}
	}

	return nil
}

// Report : report redirect responses assertions
func (r *JSONCompareResponseHandler) Report() string {
	log.Println("v3 benchmark:\nMin", r.v3Benchmark.Min, "\nMax:", r.v3Benchmark.Max, "\nAvg:", r.v3Benchmark.Avg)
	log.Println("v4 benchmark:\nMin", r.v4Benchmark.Min, "\nMax:", r.v4Benchmark.Max, "\nAvg:", r.v4Benchmark.Avg)
	log.Println("v3 non attribution benchmark:\nMin", r.v3NonAttributionBenchmark.Min, "\nMax:", r.v3NonAttributionBenchmark.Max, "\nAvg:", r.v3NonAttributionBenchmark.Avg)
	log.Println("v4 non attribution benchmark:\nMin", r.v4NonAttributionBenchmark.Min, "\nMax:", r.v4NonAttributionBenchmark.Max, "\nAvg:", r.v4NonAttributionBenchmark.Avg)
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
