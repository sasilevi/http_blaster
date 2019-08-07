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
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	logrus_stack "github.com/Gurpartap/logrus-stack"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/tui"
)

var (
	startTime             time.Time
	endTime               time.Time
	wlID                  int32 = -1
	confFile              string
	resultsFile           string
	showVersion           bool
	dataBfr               []byte
	cpuProfile            = false
	memProfile            = false
	cfg                   config.TomlConfig
	executors             []*httpblaster.Executor
	exGroup               sync.WaitGroup
	enableLog             bool
	logFile               *os.File
	workerQD              = 10000
	verbose               = false
	enableUI              bool
	enableRequestCounters bool
	chPutLatency          chan time.Duration
	chGetLatency          chan time.Duration
	countGenerated        *tui.Counter
	countSubmitted        *tui.Counter
	countAnalyzed         *tui.Counter
	//LatencyCollectorGet histogram.LatencyHist// tui.LatencyCollector
	//LatencyCollectorPut histogram.LatencyHist//tui.LatencyCollector
	//StatusesCollector   tui.StatusesCollector
	termUI                 *tui.TermUI
	dumpFailures           = true
	dumpLocation           = "."
	maxConcurrentWorkloads = 1000
)

const appVersion = "4.0.0"

func init() {
	const (
		defaultConf                   = "example.toml"
		usageConf                     = "conf file path"
		usageVersion                  = "show version"
		defaultShowVersion            = false
		usageResultsFile              = "results file path"
		defaultResultsFile            = "example.results"
		usageLogFile                  = "enable stdout to log"
		defaultLogFile                = true
		defaultWorkerQD               = 10000
		usageWorkerQD                 = "queue depth for worker requests"
		usageVerbose                  = "print debug logs"
		defaultVerbose                = false
		usageMemprofile               = "write mem profile to file"
		defaultMemprofile             = false
		usageCPUProfile               = "write cpu profile to file"
		defaultCPUProfile             = false
		usageEnableUI                 = "enable terminal ui"
		defaultEnableUI               = false
		usageDumpFailures             = "enable 4xx status requests dump to file"
		defauleDumpFailures           = false
		usageDumpLocation             = "location of dump requests"
		defaultDumpLocation           = "."
		defaultMaxConcurrentWorkloads = 1000
		usageMaxConcurrentWorkloads   = "max concurrent workloads"
	)
	flag.StringVar(&confFile, "conf", defaultConf, usageConf)
	flag.StringVar(&confFile, "c", defaultConf, usageConf+" (shorthand)")
	flag.StringVar(&resultsFile, "o", defaultResultsFile, usageResultsFile+" (shorthand)")
	flag.BoolVar(&showVersion, "version", defaultShowVersion, usageVersion)
	flag.BoolVar(&cpuProfile, "p", defaultCPUProfile, usageCPUProfile)
	flag.BoolVar(&memProfile, "m", defaultMemprofile, usageMemprofile)
	flag.BoolVar(&enableLog, "d", defaultLogFile, usageLogFile)
	flag.BoolVar(&verbose, "v", defaultVerbose, usageVerbose)
	flag.IntVar(&workerQD, "q", defaultWorkerQD, usageWorkerQD)
	flag.BoolVar(&enableUI, "u", defaultEnableUI, usageEnableUI)
	flag.BoolVar(&dumpFailures, "f", defauleDumpFailures, usageDumpFailures)
	flag.BoolVar(&enableRequestCounters, "enable-counters", true, "enable counters logging during run")
	flag.StringVar(&dumpLocation, "l", defaultDumpLocation, usageDumpLocation)
	flag.IntVar(&maxConcurrentWorkloads, "n", defaultMaxConcurrentWorkloads, usageMaxConcurrentWorkloads)
}

func getWorkloadID() int {
	return int(atomic.AddInt32(&wlID, 1))
}

func startCPUProfile() {
	if cpuProfile {
		log.Println("CPU Profile enabled")
		f, err := os.Create("cpuProfile")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}
}

func stopCPUProfile() {
	if cpuProfile {
		pprof.StopCPUProfile()
	}
}

func writeMemProfile() {
	if memProfile {
		log.Println("MEM Profile enabled")
		f, err := os.Create("memProfile")
		defer f.Close()
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
	}
}

func parseCmdlineargs() {
	flag.Parse()
	if showVersion {
		fmt.Println(appVersion)
		os.Exit(0)
	}
}

func loadConfig() {
	var err error
	cfg, err = config.LoadConfig(confFile)
	if err != nil {
		log.Println(err)
		log.Fatalln("Failed to parse config file")
	}
	log.Printf("Running test on %s:%s, tls mode=%v, block size=%d, test timeout %v",
		cfg.Global.Servers, cfg.Global.Port, cfg.Global.TLSMode,
		cfg.Global.BlockSize, cfg.Global.Duration)
	dataBfr = make([]byte, cfg.Global.BlockSize, cfg.Global.BlockSize)
	for i := range dataBfr {
		dataBfr[i] = byte(rand.Int())
	}

}

func generateExecutors(termUI *tui.TermUI) {
	//chPutLatency = LatencyCollectorPut.New()
	//chGetLatency = LatencyCollectorGet.New()
	//ch_statuses := StatusesCollector.New(160, 1)
	countSubmitted = tui.NewCounter()
	countGenerated = tui.NewCounter()
	countAnalyzed = tui.NewCounter()

	for Name, workload := range cfg.Workloads {
		log.Println("Adding executor for ", Name)
		workload.ID = getWorkloadID()

		e := &httpblaster.Executor{
			Globals:          cfg.Global,
			Workload:         workload,
			Host:             cfg.Global.Server,
			Hosts:            cfg.Global.Servers,
			TLSMode:          cfg.Global.TLSMode,
			DataBfr:          dataBfr,
			TermUI:           termUI,
			ChGetLatency:     chGetLatency,
			ChPutLatency:     chPutLatency,
			CounterSubmitter: countSubmitted,
			CounterGenerated: countGenerated,
			CounterAnalyzed:  countAnalyzed,
			//Ch_statuses:    ch_statuses,
			DumpFailures: dumpFailures,
			DumpLocation: dumpLocation}
		executors = append(executors, e)
	}
}

func startExecutors() {
	startTime = time.Now()
	for i, e := range executors {
		exGroup.Add(1)
		e.Start(&exGroup)
		if i > 0 && i%maxConcurrentWorkloads == 0 {
			waitForCompletion()
		}
	}
}

func waitForCompletion() {
	log.Println("Wait for executors to finish")
	exGroup.Wait()
	endTime = time.Now()
	//close(chGetLatency)
	///close(chPutLatency)
}

func waitUICompletion(chDone chan struct{}) {
	if enableUI {
		select {
		case <-chDone:
			break
		case <-time.After(time.Second * 10):
			close(chDone)
			break
		}
	}
}

func reportExecutorResult(file string) {
	fname := fmt.Sprintf("%s.executors", file)
	f, err := os.Create(fname)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	for _, executor := range executors {
		r, e := executor.Report()
		if e != nil {
			f.WriteString(e.Error())
		} else {
			f.WriteString("======================================\n")
			f.WriteString(fmt.Sprintf("Duration = %v\n", r.Duration))
			f.WriteString(fmt.Sprintf("Iops = %v\n", r.Iops))
			f.WriteString(fmt.Sprintf("Statuses = %v\n", r.Statuses))

			f.WriteString(fmt.Sprintf("Avg = %v\n", r.Avg))
			f.WriteString(fmt.Sprintf("Max = %v\n", r.Max))
			f.WriteString(fmt.Sprintf("Min = %v\n", r.Min))
			f.WriteString(fmt.Sprintf("Latency = %v\n", r.Latency))

			f.WriteString(fmt.Sprintf("Total = %v\n", r.Total))
			f.WriteString(fmt.Sprintf("Errors = %v\n", r.Errors))
		}
	}
}

func report() int {
	var overallRequests uint64
	var overallGetRequests uint64
	var overallPutRequests uint64
	var overallGetLatMax time.Duration
	var overallGetLatMin time.Duration
	var overallPutLatMax time.Duration
	var overallPutLatMin time.Duration
	var overallIOps uint64
	var overallGetIOps uint64
	var overallPutIOps uint64
	var overallGetAVGLat time.Duration
	var overallPutAVGLat time.Duration
	var overallGetExecutors int
	var overallPutExecutors int
	var overallStatuses = make(map[int]uint64)
	var overallCounters = make(map[string]int64)
	var overallResponseErrors = make([]error, 0)
	var overallConnectionErrors uint32

	errors := make([]error, 0)
	duration := endTime.Sub(startTime)
	for _, executor := range executors {
		results, err := executor.Report()
		overallConnectionErrors += results.ConnectionErrors
		if err != nil {
			errors = append(errors, err)
		}
		if results.ResponseErrors != nil {
			overallResponseErrors = append(overallResponseErrors, results.ResponseErrors)
		}
		for k, v := range results.Counters {
			overallCounters[k] += v
		}
		for k, v := range results.Statuses {
			overallStatuses[k] += v
		}
		overallRequests += results.Total
		if executor.Workload.Type == "GET" {
			overallGetExecutors++
			overallGetRequests += results.Total
			overallGetIOps += results.Iops
			overallGetAVGLat += results.Avg
			if overallGetLatMax < results.Max {
				overallGetLatMax = results.Max
			}
			if overallGetLatMin == 0 {
				overallGetLatMin = results.Min
			}
			if overallGetLatMin > results.Min {
				overallGetLatMin = results.Min
			}
		} else {
			overallPutExecutors++
			overallPutRequests += results.Total
			overallPutIOps += results.Iops
			overallPutAVGLat += results.Avg
			if overallPutLatMax < results.Max {
				overallPutLatMax = results.Max
			}
			if overallPutLatMin == 0 {
				overallPutLatMin = results.Min
			}
			if overallPutLatMin > results.Min {
				overallPutLatMin = results.Min
			}
		}

		overallIOps += results.Iops
	}
	if overallGetRequests != 0 {
		overallGetAVGLat = time.Duration(float64(overallGetAVGLat) / float64(overallGetExecutors))
	}
	if overallPutRequests != 0 {
		overallPutAVGLat = time.Duration(float64(overallPutAVGLat) / float64(overallPutExecutors))
	}

	reportExecutorResult(resultsFile)

	log.Println("Duration: ", duration)
	log.Println("Overall Results: ")
	log.Println("Overall Requests: ", overallRequests)
	log.Println("Overall GET Requests: ", overallGetRequests)
	log.Println("Overall GET Min Latency: ", overallGetLatMin)
	log.Println("Overall GET Max Latency: ", overallGetLatMax)
	log.Println("Overall GET Avg Latency: ", overallGetAVGLat)
	log.Println("Overall PUT Requests: ", overallPutRequests)
	log.Println("Overall PUT Min Latency: ", overallPutLatMin)
	log.Println("Overall PUT Max Latency: ", overallPutLatMax)
	log.Println("Overall PUT Avg Latency: ", overallPutAVGLat)
	log.Println("Overall IOPS: ", overallIOps)
	log.Println("Overall GET IOPS: ", overallGetIOps)
	log.Println("Overall PUT IOPS: ", overallPutIOps)
	log.Println("Overall Statuses: ", overallStatuses)
	countersStr, err := json.MarshalIndent(overallCounters, "", " ")
	log.Println("Overall counters: \n", string(countersStr))

	f, err := os.Create(resultsFile)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	f.WriteString(fmt.Sprintf("[global]\n"))
	f.WriteString(fmt.Sprintf("overall_requests=%v\n", overallRequests))
	f.WriteString(fmt.Sprintf("overall_iops=%v\n", overallIOps))
	f.WriteString(fmt.Sprintf("\n[get]\n"))
	f.WriteString(fmt.Sprintf("overall_requests=%v\n", overallGetRequests))
	f.WriteString(fmt.Sprintf("overall_iops=%v\n", overallGetIOps))
	f.WriteString(fmt.Sprintf("overall_lat_min=%vusec\n", overallGetLatMin.Nanoseconds()/1e3))
	f.WriteString(fmt.Sprintf("overall_lat_max=%vusec\n", overallGetLatMax.Nanoseconds()/1e3))
	f.WriteString(fmt.Sprintf("overall_lat_avg=%vusec\n", overallGetAVGLat.Nanoseconds()/1e3))
	f.WriteString(fmt.Sprintf("\n[put]\n"))
	f.WriteString(fmt.Sprintf("overall_requests=%v\n", overallPutRequests))
	f.WriteString(fmt.Sprintf("overall_iops=%v\n", overallPutIOps))
	f.WriteString(fmt.Sprintf("overall_lat_min=%vusec\n", overallPutLatMin.Nanoseconds()/1e3))
	f.WriteString(fmt.Sprintf("overall_lat_max=%vusec\n", overallPutLatMax.Nanoseconds()/1e3))
	f.WriteString(fmt.Sprintf("overall_lat_avg=%vusec\n", overallPutAVGLat.Nanoseconds()/1e3))

	getKeys, getValues, putKeys, putVlaues := dumpLatenciesHistograms()
	f.WriteString(fmt.Sprintf("\n[get hist]\n"))
	for i, v := range getKeys {
		f.WriteString(fmt.Sprintf("%s=%3.4f\n", strings.TrimSpace(v), getValues[i]))
	}
	f.WriteString(fmt.Sprintf("\n[put hist]\n"))
	for i, v := range putKeys {
		f.WriteString(fmt.Sprintf("%s=%3.4f\n", v, putVlaues[i]))
	}

	if len(errors) > 0 {
		for _, e := range errors {
			log.Errorln(e)
		}
		return 2
	}
	if len(overallResponseErrors) > 0 {
		for _, e := range overallResponseErrors {
			log.Errorln(e)
		}
		return 3
	}
	if overallConnectionErrors > 0 {
		log.Errorln("Test had ", overallConnectionErrors, " connection errors")
		return 4
	}
	return 0
}

func configureLog() {

	log.SetFormatter(&log.TextFormatter{ForceColors: true,
		FullTimestamp:   true,
		TimestampFormat: "2006/01/02-15:04:05"})
	if verbose {
		log.SetLevel(log.DebugLevel)
		log.AddHook(logrus_stack.StandardHook())
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if enableLog {
		filename := fmt.Sprintf("%s.log", resultsFile)
		var err error
		logFile, err = os.Create(filename)
		if err != nil {
			log.Fatalln("failed to open log file")
		} else {
			var logWriters io.Writer
			if enableUI {
				logWriters = io.MultiWriter(logFile, termUI)
			} else {
				logWriters = io.MultiWriter(os.Stdout, logFile)
			}
			log.SetOutput(logWriters)
		}
	}
}

func closeLogFile() {
	if logFile != nil {
		logFile.Close()
	}
}

func exit(errCode int) {
	if errCode != 0 {
		log.Errorln("Test failed with error")
		os.Exit(errCode)
	}
	log.Println("Test completed successfully")
}

func handleExit() {
	if err := recover(); err != nil {
		log.Println(err)
		log.Exit(1)
	}
}

func enableTUI() chan struct{} {
	if enableUI {
		termUI = &tui.TermUI{}
		chDone := termUI.InitTerminamlUI(&cfg)
		go func() {
			defer termUI.Terminate_ui()
			tick := time.Tick(time.Millisecond * 500)
			for {
				select {
				case <-chDone:
					return
				case <-tick:
					//termUI.Update_put_latency_chart(LatencyCollectorPut.Get())
					//termUI.Update_get_latency_chart(LatencyCollectorGet.Get())
					//termUI.Update_status_codes(StatusesCollector.Get())
					termUI.Refresh_log()
					termUI.Render()
				}
			}
		}()
		return chDone
	}
	return nil
}

func enableCounters() chan struct{} {
	chDone := make(chan struct{})
	if enableRequestCounters {
		go func() {
			tick := time.Tick(time.Second * 1)
			for {
				select {
				case <-chDone:
					return
				case <-tick:
					log.Info("\nSubmitted: ", countSubmitted.GetValue(),
						"\nGenerated: ", countGenerated.GetValue(),
						"\nAnalayzed: ", countAnalyzed.GetValue(),
					)
				}
			}
		}()
		return chDone
	}
	return nil
}
func dumpLatenciesHistograms() ([]string, []float64, []string, []float64) {
	latencyGet := make(map[int64]int)
	latencyPut := make(map[int64]int)
	totalGet := 0
	totalPut := 0

	for _, e := range executors {
		hist := e.LatencyHist()
		if e.GetType() == "GET" {
			for k, v := range hist {
				latencyGet[k] += v
				totalGet += v
			}
		} else {
			for k, v := range hist {
				latencyPut[k] += v
				totalPut += v
			}
		}
	}
	getKeys, getValues := dumpLatencyHistogram(latencyGet, totalGet, "GET")
	putKeys, putValues := dumpLatencyHistogram(latencyPut, totalPut, "PUT")
	return getKeys, getValues, putKeys, putValues

}

func remapLatencyHistogram(hist map[int64]int) map[int64]int {
	res := make(map[int64]int)
	for k, v := range hist {
		if k > 10000 { //1 sec
			res[10000] += v
		} else if k > 5000 { //500 mili
			res[5000] += v
		} else if k > 1000 { // 100mili
			res[1000] += v
		} else if k > 100 { //10 mili
			res[100] += v
		} else if k > 50 { //5 mili
			res[50] += v
		} else if k > 20 { //2 mili
			res[20] += v
		} else if k > 10 { //1 mili
			res[10] += v
		} else { //below 1 mili
			res[k] += v
		}
	}
	return res
}

func dumpLatencyHistogram(histogram map[int64]int, total int, reqType string) ([]string, []float64) {
	var keys []int
	var prefix string
	title := "type \t usec \t\t percentage\n"
	if reqType == "GET" {
		prefix = "GetHist"
	} else {
		prefix = "PutHist"
	}
	strout := fmt.Sprintf("%s Latency Histograms:\n", prefix)
	hist := remapLatencyHistogram(histogram)
	for k := range hist {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	log.Debugln("latency hist wait released")
	resStrings := []string{}
	resValues := []float64{}

	for _, k := range keys {
		v := hist[int64(k)]
		resStrings = append(resStrings, fmt.Sprintf("%d", k*100))
		value := float64(v*100) / float64(total)
		resValues = append(resValues, value)
	}

	if len(resStrings) > 0 {
		strout += title
		for i, v := range resStrings {
			strout += fmt.Sprintf("%s: %5s \t\t %3.4f%%\n", prefix, v, resValues[i])
		}
	}
	log.Println(strout)
	return resStrings, resValues
}

func main() {
	parseCmdlineargs()
	log.Println("Started http_blaster version:", appVersion)
	loadConfig()
	chDone := enableTUI()
	configureLog()
	log.Println("Starting http_blaster")

	//defer handleExit()
	//defer closeLogFile()
	defer stopCPUProfile()
	defer writeMemProfile()

	startCPUProfile()
	enableCounters()
	generateExecutors(termUI)
	startExecutors()
	waitForCompletion()
	log.Println("Executors done!")
	//dump_status_code_histogram()
	errCode := report()
	log.Println("Done with error code ", errCode)
	waitUICompletion(chDone)
	exit(errCode)
}
