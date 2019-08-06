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

const DialTimeout = 600 * time.Second
const RequestTimeout = 600 * time.Second

var do_once sync.Once

type WorkerBase struct {
	host           string
	conn           net.Conn
	Results        worker_results
	is_tls_client  bool
	pem_file       string
	br             *bufio.Reader
	bw             *bufio.Writer
	ch_duration    chan time.Duration
	ch_error       chan error
	lazy_sleep     time.Duration
	retry_codes    map[int]interface{}
	retry_count    int
	timer          *time.Timer
	id             int
	hist           *histogram.LatencyHist
	executor_name  string
	countSubmitted *tui.Counter
}

func (w *WorkerBase) open_connection(host string) error {
	log.Debug("open connection")
	conn, err := fasthttp.DialTimeout(w.host, DialTimeout)
	if err != nil {
		panic(err)
		// log.Printf("open connection error: %s\n", err)
	}
	if w.is_tls_client {
		w.conn, err = w.open_secure_connection(conn, host)
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

func (w *WorkerBase) open_secure_connection(conn net.Conn, host string) (*tls.Conn, error) {
	log.Debug("open secure connection")
	var conf *tls.Config
	if w.pem_file != "" {
		var pem_data []byte
		fp, err := os.Open(w.pem_file)
		if err != nil {
			panic(err)
		} else {
			defer fp.Close()
			pem_data, err = ioutil.ReadAll(fp)
			if err != nil {
				panic(err)
			}
		}
		block, _ := pem.Decode([]byte(pem_data))
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			panic(err)
			log.Fatal(err)
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

func (w *WorkerBase) close_connection() {
	if w.conn != nil {
		w.conn.Close()
	}
}

func (w *WorkerBase) restart_connection(err error, host string) error {
	log.Debugln("restart connection with ", host)
	w.close_connection()
	oerr := w.open_connection(host)
	w.Results.ConnectionRestarts++
	return oerr
}

func (w *WorkerBase) send(req *fasthttp.Request, resp *fasthttp.Response,
	timeout time.Duration) (error, time.Duration) {
	var err error
	go func() {
		start := time.Now()
		if err = req.Write(w.bw); err != nil {
			log.Debugf("send write error: %s\n", err)
			log.Debugln(fmt.Sprintf("%+v", req))
			w.ch_error <- err
			return
		} else if err = w.bw.Flush(); err != nil {
			log.Debugf("send flush error: %s\n", err)
			w.ch_error <- err
			return
		} else if err = resp.Read(w.br); err != nil {
			log.Debugf("send read error: %s\n", err)
			w.ch_error <- err
			return
		}
		end := time.Now()
		w.ch_duration <- end.Sub(start)
	}()
	w.countSubmitted.Add(1)
	w.timer.Reset(timeout)
	select {
	case duration := <-w.ch_duration:
		w.hist.Add(duration)
		return nil, duration
	case err := <-w.ch_error:
		log.Debugf("request completed with error:%s", err.Error())
		if strings.Contains(err.Error(), "does not look like a TLS handshake") {
			panic(err.Error())
		}
		return err, timeout
	case <-w.timer.C:
		log.Printf("Error: request didn't complete on timeout url:%s", req.URI().String())
		return errors.New(fmt.Sprintf("request timedout url:%s", req.URI().String())), timeout
	}
	return nil, timeout
}

func (w *WorkerBase) send_request(req *request_generators.Request, response *request_generators.Response) (error, time.Duration) {
	var (
		code     int
		err      error
		duration time.Duration
	)
	if w.lazy_sleep > 0 {
		time.Sleep(w.lazy_sleep)
	}

	if req.ResetConnection {
		if w.restart_connection(errors.New(""), req.Host) != nil {
			if req.ExpectedSuccess {
				log.Errorln("connection error with host", req.Host)
				w.Results.ConnectionErrors++
				return err, 1
			} else {
				log.Debug("connection error with host as expected ", req.Host)
				return nil, 0
			}
		} else {
			if req.ExpectedSuccess == false {
				log.Errorln("connection success for unregistered domain ", req.Host)
				w.Results.ConnectionErrors++
				return errors.New("connection success for unregistered domain"), 1
			}
		}
	}

	err, duration = w.send(req.Request, response.Response, RequestTimeout)

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
	}
	if response.Response.ConnectionClose() {
		w.restart_connection(err, string(req.Host))
	}

	return err, duration
}

func (w *WorkerBase) Init(lazy int) {
	w.Results.Codes = make(map[int]uint64)
	w.Results.Min = time.Duration(time.Second * 10)
	w.open_connection(w.host)
	w.ch_duration = make(chan time.Duration, 1)
	w.ch_error = make(chan error, 1)
	w.lazy_sleep = time.Duration(lazy) * time.Millisecond
	w.timer = time.NewTimer(time.Second * 120)
}

func (w *WorkerBase) GetResults() worker_results {
	return w.Results
}

func (w *WorkerBase) GetHist() map[int64]int {
	return w.hist.GetHistMap()
}

func NewWorker(worker_type WorkerType, host string,
	tls_client bool, lazy int, retry_codes []int,
	retry_count int, pem_file string, id int, executor_name string) Worker {
	if host == "" {
		return nil
	}
	retry_codes_map := make(map[int]interface{})
	for _, c := range retry_codes {
		retry_codes_map[c] = true

	}
	if retry_count == 0 {
		retry_count = 1
	}
	var worker Worker
	hist := &histogram.LatencyHist{}
	hist.New()
	if worker_type == PERFORMANCE_WORKER {
		worker = &PerfWorker{WorkerBase{host: host, is_tls_client: tls_client, retry_codes: retry_codes_map,
			retry_count: retry_count, pem_file: pem_file, id: id, hist: hist, executor_name: executor_name}}
	} else {
		worker = &IngestWorker{WorkerBase{host: host, is_tls_client: tls_client, retry_codes: retry_codes_map,
			retry_count: retry_count, pem_file: pem_file, id: id, hist: hist, executor_name: executor_name}}
	}
	worker.Init(lazy)
	return worker
}
