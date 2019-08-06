package worker

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/histogram"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
	"github.com/v3io/http_blaster/httpblaster/tui"
	"github.com/valyala/fasthttp"
)

//DialTimeout : connection timeout
const DialTimeout = 600 * time.Second

//RequestTimeout : send timeout
const RequestTimeout = 600 * time.Second

var doOnce sync.Once

// Base : worker abs
type Base struct {
	host            string
	domain          string
	conn            net.Conn
	Results         Results
	isTLSClient     bool
	pemFile         string
	br              *bufio.Reader
	bw              *bufio.Writer
	chDuration      chan time.Duration
	chError         chan error
	lazySleep       time.Duration
	retryCodes      map[int]interface{}
	retryCount      int
	timer           *time.Timer
	id              int
	hist            *histogram.LatencyHist
	executorName    string
	countSubmitted  *tui.Counter
	resetConnection bool
}

func (w *Base) openConnection(host string) error {
	log.Debug("open connection")
	conn, err := fasthttp.DialTimeout(w.host, DialTimeout)
	if err != nil {
		panic(err)
		// log.Printf("open connection error: %s\n", err)
	}
	if w.isTLSClient {
		w.conn, err = w.openSecureConnection(conn, host)
		if err != nil {
			return err
		}
	} else {
		w.conn = conn
	}
	w.br = bufio.NewReader(w.conn)
	if w.br == nil {
		log.Errorf("Reader is nil, conn: %v", conn)
	}
	w.bw = bufio.NewWriter(w.conn)
	return nil
}

func (w *Base) openSecureConnection(conn net.Conn, host string) (*tls.Conn, error) {
	log.Debug("open secure connection")
	var conf *tls.Config
	if w.pemFile != "" {
		var pemData []byte
		fp, err := os.Open(w.pemFile)
		if err != nil {
			panic(err)
		} else {
			defer fp.Close()
			pemData, err = ioutil.ReadAll(fp)
			if err != nil {
				panic(err)
			}
		}
		block, _ := pem.Decode([]byte(pemData))
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			panic(err)
			// log.Fatal(err)
		}
		clientCertPool := x509.NewCertPool()
		clientCertPool.AddCert(cert)

		conf = &tls.Config{
			ServerName:         host,
			ClientAuth:         tls.RequireAndVerifyClientCert,
			InsecureSkipVerify: true,
			ClientCAs:          clientCertPool,
		}
	} else {
		conf = &tls.Config{
			ServerName:         host,
			ClientAuth:         tls.RequireAndVerifyClientCert,
			InsecureSkipVerify: false,
		}
	}
	c := tls.Client(conn, conf)

	// log.Info(c.Handshake())
	return c, c.Handshake()
}

func (w *Base) closeConnection() {
	if w.conn != nil {
		w.conn.Close()
	}
}

func (w *Base) restartConnection(err error, host string) error {
	log.Debugln("restart connection with ", host)
	w.closeConnection()
	oerr := w.openConnection(host)
	w.Results.ConnectionRestarts++
	return oerr
}

func (w *Base) send(req *fasthttp.Request, resp *fasthttp.Response,
	timeout time.Duration) (time.Duration, error) {
	var err error
	go func() {
		start := time.Now()
		if err = req.Write(w.bw); err != nil {
			log.Debugf("send write error: %s\n", err)
			log.Debugln(fmt.Sprintf("%+v", req))
			w.chError <- err
			return
		} else if err = w.bw.Flush(); err != nil {
			log.Debugf("send flush error: %s\n", err)
			w.chError <- err
			return
		} else if err = resp.Read(w.br); err != nil {
			log.Debugf("send read error: %s\n", err)
			w.chError <- err
			return
		}
		end := time.Now()
		w.chDuration <- end.Sub(start)
	}()
	w.countSubmitted.Add(1)
	w.timer.Reset(timeout)
	select {
	case duration := <-w.chDuration:
		w.hist.Add(duration)
		return duration, nil
	case err := <-w.chError:
		log.Debugf("request completed with error:%s", err.Error())
		if strings.Contains(err.Error(), "does not look like a TLS handshake") {
			panic(err.Error())
		}
		return timeout, err
	case <-w.timer.C:
		log.Printf("Error: request didn't complete on timeout url:%s", req.URI().String())
		return timeout, fmt.Errorf("request timedout url:%s", req.URI().String())
	}
	// return timeout, nil
}

func (w *Base) sendRequest(req *request_generators.Request, response *request_generators.Response) (time.Duration, error) {
	var (
		code     int
		err      error
		duration time.Duration
	)
	host := w.domain
	if w.lazySleep > 0 {
		time.Sleep(w.lazySleep)
	}
	if w.resetConnection {
		// log.Debugln("Restart Connection")

		if req.Host != "" {
			host = req.Host
		}
		// log.Debugln("host = ", host)
		if w.restartConnection(errors.New(""), host) != nil {
			if req.ExpectedConnectionStatus {
				log.Errorln("connection error with host", host)
				w.Results.ConnectionErrors++
				return 1, err
			}
			log.Debug("connection error with host as expected ", host)
			return 0, nil
		}
		if req.ExpectedConnectionStatus == false {
			log.Errorln("connection success for unregistered domain ", host)
			w.Results.ConnectionErrors++
			return 1, errors.New("connection success for unregistered domain")
		}
	}
	log.Debugln("Send request")
	duration, err = w.send(req.Request, response.Response, RequestTimeout)

	if err == nil {
		code = response.Response.StatusCode()
		w.Results.Codes[code]++
		w.Results.Count++
		if duration < w.Results.Min {
			w.Results.Min = duration
		}
		if duration > w.Results.Max {
			w.Results.Max = duration
		}
		w.Results.Avg = w.Results.Avg + (duration-w.Results.Avg)/time.Duration(w.Results.Count)
	} else {
		w.Results.ErrorCount++
		log.Debugln(err.Error())
		w.restartConnection(err, host)
	}
	if response.Response.ConnectionClose() {
		w.restartConnection(err, host)
	}

	return duration, err
}

// Init : init worker vars
func (w *Base) Init(lazy int) {
	w.Results.Codes = make(map[int]uint64)
	w.Results.Min = time.Duration(time.Second * 10)
	err := w.openConnection(w.domain)
	if err != nil {
		panic(w.domain)
	}
	w.chDuration = make(chan time.Duration, 1)
	w.chError = make(chan error, 1)
	w.lazySleep = time.Duration(lazy) * time.Millisecond
	w.timer = time.NewTimer(time.Second * 120)
}

// GetResults : return worker results struct
func (w *Base) GetResults() Results {
	return w.Results
}

// GetHist : return worker latency hist
func (w *Base) GetHist() map[int64]int {
	return w.hist.GetHistMap()
}

//NewWorker : new worker object
func NewWorker(workerType Type,
	host string,
	domian string,
	tlsClient bool,
	lazy int,
	retryCodes []int,
	retryCount int,
	pemFile string,
	id int,
	executorName string,
	resetConnection bool) Worker {
	if host == "" {
		return nil
	}
	retryCodesMap := make(map[int]interface{})
	for _, c := range retryCodes {
		retryCodesMap[c] = true

	}
	if retryCount == 0 {
		retryCount = 1
	}
	var worker Worker
	hist := &histogram.LatencyHist{}
	hist.New()
	if workerType == Performance {
		worker = &PerfWorker{Base{host: host, domain: domian, isTLSClient: tlsClient, retryCodes: retryCodesMap,
			retryCount: retryCount, pemFile: pemFile, id: id, hist: hist, executorName: executorName, resetConnection: resetConnection}}
	} else {
		worker = &IngestWorker{Base{host: host, domain: domian, isTLSClient: tlsClient, retryCodes: retryCodesMap,
			retryCount: retryCount, pemFile: pemFile, id: id, hist: hist, executorName: executorName, resetConnection: resetConnection}}
	}
	worker.Init(lazy)
	return worker
}
