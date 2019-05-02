package responsehandlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
)

// RedirectResponseHandler : response handler for redirect test
type RedirectResponseHandler struct {
	r200     *regexp.Regexp
	r302     *regexp.Regexp
	notFound *regexp.Regexp
	Errors   int64
	results  map[string]*errorInfo
}

type errorInfo struct {
	// UA        string
	Status    int
	NoFound   bool
	WrongLink bool
}

// HandlerResponses :  handler function to habdle responses
func (r *RedirectResponseHandler) HandlerResponses(global config.Global, workload config.Workload, respCh chan *request_generators.Response, wg *sync.WaitGroup) {
	r.results = make(map[string]*errorInfo)
	defer log.Println("Terminating response handler")
	defer wg.Done()
	r.r200 = regexp.MustCompile(fmt.Sprintf("store_link = %s", workload.ExpectedStoreLink))
	r.r302 = regexp.MustCompile(fmt.Sprintf("af_android_custom_url=%s", workload.ExpectedStoreLink))
	r.notFound = regexp.MustCompile("THE APP YOU ARE LOOKING FOR IS NOT AVAILABLE IN THE MARKET YET")

	for resp := range respCh {
		if resp.Response.StatusCode() != http.StatusOK && resp.Response.StatusCode() != http.StatusFound {
			log.Errorln(resp.Response.StatusCode(), resp.Cookie)
			r.Errors++
		} else {
			r.checkResponse(resp)
		}
		// log.Println(r.Response.StatusCode(), "\t", r.Duration, "\t", r.ID)
		// if r.Response.StatusCode() == http.StatusOK {
		// 	log.Println(r.Response.StatusCode(), r.Cookie)
		// }
		request_generators.ReleaseResponse(resp)
	}
}

func (r *RedirectResponseHandler) checkResponse(response *request_generators.Response) {
	err := &errorInfo{Status: response.Response.StatusCode()}
	if r.findMatches(response) == false {
		// log.Println(fmt.Sprintf("%s %s", r.workload.MatchExpression, r.workload.ExpectedStoreLink))
		// log.Errorln("Body: ", response.Response.String())
		// log.Errorln("User-Agent: ", response.Cookie)
		// log.Println("Request: ", response.RequestURI)
		if r.checkNotFoundResponse(response) {
			err.NoFound = true
			r.results[response.Cookie.(string)] = err
		} else {
			err.WrongLink = true
			r.results[response.Cookie.(string)] = err
		}
	}
}

func (r *RedirectResponseHandler) checkNotFoundResponse(response *request_generators.Response) bool {
	if r.notFound.Match([]byte(response.Response.String())) {
		return true
	}
	return false
}

func (r *RedirectResponseHandler) findMatches(response *request_generators.Response) bool {

	if response.Response.StatusCode() == http.StatusFound { //302
		body, _ := url.PathUnescape(response.Response.String())
		return r.r302.Match([]byte(body))
	}

	return r.r200.Match(response.Response.Body())
}

// Report : report redirect responses assertions
func (r *RedirectResponseHandler) Report() string {
	j, _ := json.MarshalIndent(r.results, "", "  ")
	return string(j)
}
