package responsehandlers

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"regexp"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/db"
	"github.com/v3io/http_blaster/httpblaster/dto"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
)

// RedirectResponseHandler : response handler for redirect test
type RedirectResponseHandler struct {
	BaseResponseHandler
	notFound       *regexp.Regexp
	Errors         int64
	results        map[string]*errorInfo
	ErrorCounters  map[string]int64
	Checks         map[string]*regexp.Regexp
	responsesHash  map[uint32]string
	dumpDir        string
	psql           *db.PostgresDB
	RecordFile     bool
	positiveHash   uint32
	recordPositive bool
}

type errorInfo struct {
	// UA        string
	Status    int
	NotFound  bool
	WrongLink bool
}

type errorsCounters struct {
	WrongLink int64
	NotFound  int64
}

// HandlerResponses :  handler function to habdle responses
func (r *RedirectResponseHandler) HandlerResponses(global config.Global, workload config.Workload, respCh chan *request_generators.Response) {
	r.dumpDir = "WrongLinksDumps"
	r.positiveHash = r.hash("")
	os.MkdirAll(r.dumpDir, os.ModePerm)
	r.results = make(map[string]*errorInfo)
	r.Checks = make(map[string]*regexp.Regexp)
	r.responsesHash = make(map[uint32]string)
	r.ErrorCounters = make(map[string]int64)
	r.psql = db.New(global.DbHost, global.DbPort, global.DbName, global.DbUser, global.DbPassword)
	r.RecordFile = workload.UADumpToFile
	r.recordPositive = global.DbRecordAll
	defer r.psql.Close()

	for k, v := range workload.Targets {
		log.Println(v)
		r.Checks[k] = regexp.MustCompile(v)
	}
	r.notFound = regexp.MustCompile("THE APP YOU ARE LOOKING FOR IS NOT AVAILABLE IN THE MARKET YET")
	for resp := range respCh {
		if resp.Response.StatusCode() != http.StatusOK && resp.Response.StatusCode() != http.StatusFound {
			log.Errorln(resp.Response.StatusCode(), resp.Cookie.(*dto.UserAgentMessage).UserAgent)
			r.recordResult(resp, false, true, "", r.RecordFile, true, true)
			r.Errors++
		} else {
			r.checkResponse(resp)
		}
		dto.ReleaseUaObj(resp.Cookie.(*dto.UserAgentMessage))
		request_generators.ReleaseResponse(resp)
	}
}
func (r *RedirectResponseHandler) hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (r *RedirectResponseHandler) dumpResponseToFile(hash uint32, body []byte) {
	if _, ok := r.responsesHash[hash]; !ok {
		r.responsesHash[hash] = fmt.Sprintf("%d.html", hash)
		filePath := path.Join(r.dumpDir, r.responsesHash[hash])

		err := ioutil.WriteFile(filePath, body, 0644)
		if err != nil {
			panic(err.Error())
		}
		log.Println("Dumping file:", filePath)
	}
}

func (r *RedirectResponseHandler) dumpUserAgentToFile(hash uint32, userAgent string, wrongLink bool, notFound bool, target string) {
	filePath := path.Join(r.dumpDir, fmt.Sprintf("%d.users", hash))

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		panic(err.Error())
	}
	f.WriteString(userAgent + "\n")
	f.Close()

	// log.Println("Dumping file:", filePath)
}

func (r *RedirectResponseHandler) recordResult(response *request_generators.Response, wrongLink bool, notFound bool, expectedStoreLink string, file bool, db bool, saveBody bool) {
	body := response.Response.Body()
	userAgent := response.Cookie.(*dto.UserAgentMessage).UserAgent
	target := response.Cookie.(*dto.UserAgentMessage).Target
	hash := r.hash(string(body))

	if file {
		r.dumpResponseToFile(hash, body)
		r.dumpUserAgentToFile(hash, userAgent, false, false, target)
	}
	if db {
		if saveBody {
			r.psql.InsertResponseBody(body, hash)
			r.psql.InsertUserAgentInfo(userAgent, hash, target, wrongLink, notFound, response.Response.StatusCode(), expectedStoreLink)
		} else {
			// in case of positive we would not save the body its too much data
			r.psql.InsertResponseBody([]byte(""), r.positiveHash)
			r.psql.InsertUserAgentInfo(userAgent, r.positiveHash, target, wrongLink, notFound, response.Response.StatusCode(), expectedStoreLink)
		}
	}
}

func (r *RedirectResponseHandler) checkResponse(response *request_generators.Response) {
	err := &errorInfo{Status: response.Response.StatusCode()}
	expectedStoreLink := r.Checks[response.Cookie.(*dto.UserAgentMessage).Target].String()
	if response.Response.StatusCode() == http.StatusOK {
		if r.Checks[response.Cookie.(*dto.UserAgentMessage).Target].Match(response.Response.Body()) && r.recordPositive {
			r.recordResult(response, err.WrongLink, err.NotFound, expectedStoreLink, false, true, false) //record positive results
			return
		}
	}
	body, _ := url.PathUnescape(response.Response.String())
	if r.Checks[response.Cookie.(*dto.UserAgentMessage).Target].Match([]byte(body)) { //check with unescape body
		return
	} else if r.checkNotFoundResponse(response) {
		err.NotFound = true
		r.ErrorCounters["NotFound"]++
	} else {
		err.WrongLink = true
		r.ErrorCounters["WrongLink"]++
	}
	r.recordResult(response, err.WrongLink, err.NotFound, expectedStoreLink, r.RecordFile, true, true)
}

func (r *RedirectResponseHandler) checkNotFoundResponse(response *request_generators.Response) bool {
	if r.notFound.Match([]byte(response.Response.String())) {
		return true
	}
	return false
}

// Report : report redirect responses assertions
func (r *RedirectResponseHandler) Report() string {
	j, _ := json.MarshalIndent(r.results, "", "  ")
	w, _ := json.MarshalIndent(r.responsesHash, "", "  ")
	return string(j) + "\n" + string(w)
}

// Counters : returns counters for wrong link and not found
func (r *RedirectResponseHandler) Counters() map[string]int64 {
	return r.ErrorCounters
}

func (r *RedirectResponseHandler) Error() error {
	if r.Errors > 0 {
		return fmt.Errorf("Response handler %v completed with %v errors", reflect.TypeOf(r).Elem().Name(), r.Errors)
	}
	return nil
}
