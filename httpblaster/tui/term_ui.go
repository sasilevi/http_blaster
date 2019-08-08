package tui

import (
	"runtime"
	"strconv"
	"sync"
	"time"

	ui "github.com/sasile/termui"
	"github.com/v3io/http_blaster/httpblaster/config"
)

// TermUI : ui obj
type TermUI struct {
	cfg                   *config.TomlConfig
	widgetTitle           ui.GridBufferer
	widgetSysInfo         ui.GridBufferer
	widgetServerInfo      ui.GridBufferer
	widgetPutIopsChart    *ui.LineChart
	widgetGetIopsChart    *ui.LineChart
	widgetLogs            *ui.List
	widgetProgress        ui.GridBufferer
	widgetRequestBarChart *ui.BarChart
	widgetPutLatency      *ui.BarChart
	widgetGetLatency      *ui.BarChart

	iopsGetFifo *Float64Fifo
	iopsPutFifo *Float64Fifo
	logsFifo    *StringsFifo
	statuses    map[int]uint64
	M           sync.RWMutex
	chDone      chan struct{}
}

// StringsFifo : strings fifo
type StringsFifo struct {
	string
	Length     int
	Items      []string
	chMessages chan string
	lock       sync.Mutex
}

// Init : StringsFifo init
func (t *StringsFifo) Init(length int) {
	t.Length = length
	t.Items = make([]string, length)
	t.chMessages = make(chan string, 100)
	go func() {
		for msg := range t.chMessages {
			func() {
				t.lock.Lock()
				defer t.lock.Unlock()
				if len(t.Items) < t.Length {
					t.Items = append(t.Items, msg)
				} else {
					t.Items = t.Items[1:]
					t.Items = append(t.Items, msg)
				}
			}()
		}
	}()
}

// Insert : StringsFifo insert
func (t *StringsFifo) Insert(msg string) {
	t.chMessages <- msg
}

//Get : StringsFifo get all strings
func (t *StringsFifo) Get() []string {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.Items
}

//Float64Fifo :Float64Fifo
type Float64Fifo struct {
	int
	Length int
	index  int
	Items  []float64
}

//Init :Float64Fifo init
func (t *Float64Fifo) Init(length int) {
	t.Length = length
	t.index = 0
	t.Items = make([]float64, length)
}

//Insert :Float64Fifo Insert
func (t *Float64Fifo) Insert(i float64) {
	if t.index < t.Length {
		t.Items[t.index] = i
		t.index++
	} else {
		t.Items = t.Items[1:]
		t.Items = append(t.Items, i)
	}
}

//Get :Float64Fifo get array
func (t *Float64Fifo) Get() []float64 {
	return t.Items
}

func (t *TermUI) uiSetTitle(x, y, w, h int) ui.GridBufferer {
	uiTitilePar := ui.NewPar("Running " + t.cfg.Title + " : PRESS q TO QUIT")
	uiTitilePar.Height = h
	uiTitilePar.X = x
	uiTitilePar.Y = y
	uiTitilePar.Width = w
	uiTitilePar.TextFgColor = ui.ColorWhite
	uiTitilePar.BorderLabel = "Title"
	uiTitilePar.BorderFg = ui.ColorCyan
	ui.Handle("/sys/kbd/q", func(ui.Event) {
		// press q to quit
		ui.StopLoop()
		ui.Close()
		close(t.chDone)
	})
	ui.Render(uiTitilePar)
	return uiTitilePar
}

func (t *TermUI) uiSetServersInfo(x, y, w, h int) ui.GridBufferer {
	table1 := ui.NewTable()
	var rows [][]string
	rows = append(rows, []string{"Server", "Port", "TLS Mode"})
	table1.Height++
	if len(t.cfg.Global.Servers) == 0 {
		rows = append(rows, []string{t.cfg.Global.Server,
			t.cfg.Global.Port,
			strconv.FormatBool(t.cfg.Global.TLSMode)})
		table1.Height += 2
	} else {
		for _, s := range t.cfg.Global.Servers {
			rows = append(rows, []string{s,
				t.cfg.Global.Port,
				strconv.FormatBool(t.cfg.Global.TLSMode)})
			table1.Height += 2
		}
	}

	table1.Rows = rows
	table1.FgColor = ui.ColorWhite
	table1.BgColor = ui.ColorDefault
	table1.Y = y
	table1.X = x
	table1.Width = w
	table1.BorderLabel = "Servers"
	return table1
}

func (t *TermUI) uiSetSystemInfo(x, y, w, h int) ui.GridBufferer {
	table1 := ui.NewTable()
	var rows [][]string
	var memStat runtime.MemStats
	runtime.ReadMemStats(&memStat)
	rows = append(rows, []string{"OS", "CPU's", "Memory"})
	rows = append(rows, []string{runtime.GOOS, strconv.Itoa(runtime.NumCPU()), strconv.FormatInt(int64(memStat.Sys), 10)})
	table1.Rows = rows
	table1.FgColor = ui.ColorWhite
	table1.BgColor = ui.ColorDefault
	table1.Y = y
	table1.X = x
	table1.Width = w
	table1.Height = h
	table1.BorderLabel = "Syetem Info"
	return table1
}

func (t *TermUI) uiSetLogList(x, y, w, h int) *ui.List {
	list := ui.NewList()
	list.ItemFgColor = ui.ColorYellow
	list.BorderLabel = "Log"
	list.Height = h
	list.Width = w
	list.Y = y
	list.X = x
	list.Items = t.logsFifo.Get()
	return list
}

func (t *TermUI) uiSetRequestsBarChart(x, y, w, h int) *ui.BarChart {
	bc := ui.NewBarChart()
	bc.BarGap = 3
	bc.BarWidth = 8
	data := []int{}
	bclabels := []string{}
	bc.BorderLabel = "Status codes"
	bc.Data = data
	bc.Width = w
	bc.Height = h
	bc.DataLabels = bclabels
	bc.TextColor = ui.ColorGreen
	bc.BarColor = ui.ColorGreen
	bc.NumColor = ui.ColorYellow
	return bc
}

func (t *TermUI) uiSetPutLatencyBarChart(x, y, w, h int) *ui.BarChart {
	bc := ui.NewBarChart()
	bc.BarGap = 3
	bc.BarWidth = 8
	data := []int{}
	bclabels := []string{}
	bc.BorderLabel = "put Latency us"
	bc.Data = data
	bc.Width = w
	bc.Height = h
	bc.DataLabels = bclabels
	bc.TextColor = ui.ColorGreen
	bc.BarColor = ui.ColorGreen
	bc.NumColor = ui.ColorYellow
	return bc
}

func (t *TermUI) uiSetGetLatencyBarChart(x, y, w, h int) *ui.BarChart {
	bc := ui.NewBarChart()
	bc.BarGap = 3
	bc.BarWidth = 8
	data := []int{}
	bclabels := []string{}
	bc.BorderLabel = "get Latency us"
	bc.Data = data
	bc.Width = w
	bc.Height = h
	bc.DataLabels = bclabels
	bc.TextColor = ui.ColorGreen
	bc.BarColor = ui.ColorGreen
	bc.NumColor = ui.ColorYellow
	return bc
}

func (t *TermUI) uiGetIops(x, y, w, h int) *ui.LineChart {
	lc2 := ui.NewLineChart()
	lc2.BorderLabel = "Get iops chart"
	lc2.Mode = "braille"

	lc2.Width = w
	lc2.Height = h
	lc2.X = x
	lc2.Y = y
	lc2.AxesColor = ui.ColorWhite
	lc2.LineColor = ui.ColorCyan | ui.AttrBold
	lc2.Data = t.iopsGetFifo.Get()
	return lc2
}

func (t *TermUI) uiPutIops(x, y, w, h int) *ui.LineChart {
	lc2 := ui.NewLineChart()
	lc2.BorderLabel = "Put iops chart"
	lc2.Mode = "braille"
	lc2.Data = t.iopsPutFifo.Get()
	lc2.Width = w
	lc2.Height = h
	lc2.X = x
	lc2.Y = y
	lc2.AxesColor = ui.ColorWhite
	lc2.LineColor = ui.ColorCyan | ui.AttrBold
	return lc2
}

//UpdateRequests : update the put/get bar chars
func (t *TermUI) UpdateRequests(duration time.Duration, putCount, getCount uint64) {

	seconds := uint64(duration.Seconds())
	if seconds == 0 {
		seconds = 1
	}
	putIops := putCount / seconds
	getIops := getCount / seconds
	if putIops > 0 {
		t.iopsPutFifo.Insert(float64(putIops) / 1000)
	}
	if getIops > 0 {
		t.iopsGetFifo.Insert(float64(getIops) / 1000)
	}
	t.widgetPutIopsChart.Data = t.iopsPutFifo.Get()
	t.widgetGetIopsChart.Data = t.iopsGetFifo.Get()

}

//RefreshLog : Refresh Log
func (t *TermUI) RefreshLog() {
	t.widgetLogs.Items = t.logsFifo.Get()
	ui.Render(t.widgetLogs)
}

//UpdateStatusCodes : Update Status Codes bar
func (t *TermUI) UpdateStatusCodes(labels []string, values []int) {
	t.widgetRequestBarChart.Data = values
	t.widgetRequestBarChart.DataLabels = labels
}

//UpdatePutLatencyChart :Update Put Latency Chart
func (t *TermUI) UpdatePutLatencyChart(labels []string, values []int) {
	t.widgetPutLatency.Data = values
	t.widgetPutLatency.DataLabels = labels
}

//UpdateGetLatencyChart :Update Get Latency Chart
func (t *TermUI) UpdateGetLatencyChart(labels []string, values []int) {
	t.widgetGetLatency.Data = values
	t.widgetGetLatency.DataLabels = labels
}

func percentage(value, total int) int {
	return value * total / 100
}

//InitTerminamlUI : init ui
func (t *TermUI) InitTerminamlUI(cfg *config.TomlConfig) chan struct{} {
	t.cfg = cfg
	t.chDone = make(chan struct{})
	t.iopsGetFifo = &Float64Fifo{}
	t.iopsGetFifo.Init(150)
	t.iopsPutFifo = &Float64Fifo{}
	t.iopsPutFifo.Init(150)
	t.logsFifo = &StringsFifo{}
	t.logsFifo.Init(10)
	t.statuses = make(map[int]uint64)
	err := ui.Init()
	if err != nil {
		panic(err)
	}
	termHight := ui.TermHeight()

	t.widgetTitle = t.uiSetTitle(0, 0, 50, percentage(7, termHight))
	t.widgetServerInfo = t.uiSetServersInfo(0, 0, 0, 0)
	t.widgetSysInfo = t.uiSetSystemInfo(0, 0, 0, t.widgetServerInfo.GetHeight())
	t.widgetPutIopsChart = t.uiPutIops(0, 0, 0, percentage(30, termHight))
	t.widgetGetIopsChart = t.uiGetIops(0, 0, 0, percentage(30, termHight))
	t.widgetPutLatency = t.uiSetPutLatencyBarChart(0, 0, 0, percentage(30, termHight))
	t.widgetGetLatency = t.uiSetGetLatencyBarChart(0, 0, 0, percentage(30, termHight))
	t.widgetRequestBarChart = t.uiSetRequestsBarChart(0, 0, 0, percentage(20, termHight))
	t.widgetLogs = t.uiSetLogList(0, 0, 0, percentage(20, termHight))

	ui.Body.AddRows(
		ui.NewRow(
			ui.NewCol(12, 0, t.widgetTitle),
		),
		ui.NewRow(
			ui.NewCol(6, 0, t.widgetSysInfo),
			ui.NewCol(6, 0, t.widgetServerInfo),
		),
		ui.NewRow(
			ui.NewCol(6, 0, t.widgetPutIopsChart),
			ui.NewCol(6, 0, t.widgetPutLatency),
		),
		ui.NewRow(
			ui.NewCol(6, 0, t.widgetGetIopsChart),
			ui.NewCol(6, 0, t.widgetGetLatency),
		),
		ui.NewRow(
			ui.NewCol(6, 0, t.widgetLogs),
			ui.NewCol(6, 0, t.widgetRequestBarChart),
		),
	)

	ui.Body.Align()
	ui.Render(ui.Body)
	go ui.Loop()
	return t.chDone
}

//Render : Render
func (t *TermUI) Render() {
	ui.Render(ui.Body)
}

//TerminateUI : Terminate UI
func (t *TermUI) TerminateUI() {
	ui.StopLoop()
	ui.Close()
}

//Write :Write
func (t *TermUI) Write(p []byte) (n int, err error) {
	if p == nil {
		return 0, nil
	}
	t.logsFifo.Insert(string(p))
	if t.widgetLogs != nil {
		t.widgetLogs.Items = t.logsFifo.Get()
	}
	ui.Render(t.widgetLogs)
	return len(p), nil
}
