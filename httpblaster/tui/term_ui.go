package tui

import (
	"runtime"
	"strconv"
	"sync"
	"time"

	ui "github.com/sasile/termui"
	"github.com/v3io/http_blaster/httpblaster/config"
)

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

type StringsFifo struct {
	string
	Length     int
	Items      []string
	chMessages chan string
	lock       sync.Mutex
}

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

func (t *StringsFifo) Insert(msg string) {
	t.chMessages <- msg
}
func (t *StringsFifo) Get() []string {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.Items
}

type Float64Fifo struct {
	int
	Length int
	index  int
	Items  []float64
}

func (t *Float64Fifo) Init(length int) {
	t.Length = length
	t.index = 0
	t.Items = make([]float64, length)
}

func (t *Float64Fifo) Insert(i float64) {
	if t.index < t.Length {
		t.Items[t.index] = i
		t.index++
	} else {
		t.Items = t.Items[1:]
		t.Items = append(t.Items, i)
	}
}
func (t *Float64Fifo) Get() []float64 {
	return t.Items
}

func (t *TermUI) ui_set_title(x, y, w, h int) ui.GridBufferer {
	ui_titile_par := ui.NewPar("Running " + t.cfg.Title + " : PRESS q TO QUIT")
	ui_titile_par.Height = h
	ui_titile_par.X = x
	ui_titile_par.Y = y
	ui_titile_par.Width = w
	ui_titile_par.TextFgColor = ui.ColorWhite
	ui_titile_par.BorderLabel = "Title"
	ui_titile_par.BorderFg = ui.ColorCyan
	ui.Handle("/sys/kbd/q", func(ui.Event) {
		// press q to quit
		ui.StopLoop()
		ui.Close()
		close(t.chDone)
	})
	ui.Render(ui_titile_par)
	return ui_titile_par
}

func (t *TermUI) ui_set_servers_info(x, y, w, h int) ui.GridBufferer {
	table1 := ui.NewTable()
	var rows [][]string
	rows = append(rows, []string{"Server", "Port", "TLS Mode"})
	table1.Height += 1
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

func (t *TermUI) ui_set_system_info(x, y, w, h int) ui.GridBufferer {
	table1 := ui.NewTable()
	var rows [][]string
	var mem_stat runtime.MemStats
	runtime.ReadMemStats(&mem_stat)
	rows = append(rows, []string{"OS", "CPU's", "Memory"})
	rows = append(rows, []string{runtime.GOOS, strconv.Itoa(runtime.NumCPU()), strconv.FormatInt(int64(mem_stat.Sys), 10)})
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

func (t *TermUI) ui_set_log_list(x, y, w, h int) *ui.List {
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

func (t *TermUI) ui_set_requests_bar_chart(x, y, w, h int) *ui.BarChart {
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

func (t *TermUI) ui_set_put_latency_bar_chart(x, y, w, h int) *ui.BarChart {
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

func (t *TermUI) ui_set_get_latency_bar_chart(x, y, w, h int) *ui.BarChart {
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

func (t *TermUI) ui_get_iops(x, y, w, h int) *ui.LineChart {
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

func (t *TermUI) Update_requests(duration time.Duration, put_count, get_count uint64) {

	seconds := uint64(duration.Seconds())
	if seconds == 0 {
		seconds = 1
	}
	put_iops := put_count / seconds
	get_iops := get_count / seconds
	if put_iops > 0 {
		t.iopsPutFifo.Insert(float64(put_iops) / 1000)
	}
	if get_iops > 0 {
		t.iopsGetFifo.Insert(float64(get_iops) / 1000)
	}
	t.widgetPutIopsChart.Data = t.iopsPutFifo.Get()
	t.widgetGetIopsChart.Data = t.iopsGetFifo.Get()

}

func (t *TermUI) Refresh_log() {
	t.widgetLogs.Items = t.logsFifo.Get()
	ui.Render(t.widgetLogs)
}

func (t *TermUI) Update_status_codes(labels []string, values []int) {
	t.widgetRequestBarChart.Data = values
	t.widgetRequestBarChart.DataLabels = labels
}

func (t *TermUI) Update_put_latency_chart(labels []string, values []int) {
	t.widgetPutLatency.Data = values
	t.widgetPutLatency.DataLabels = labels
}

func (t *TermUI) Update_get_latency_chart(labels []string, values []int) {
	t.widgetGetLatency.Data = values
	t.widgetGetLatency.DataLabels = labels
}

func Percentage(value, total int) int {
	return value * total / 100
}

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
	term_hight := ui.TermHeight()

	t.widgetTitle = t.ui_set_title(0, 0, 50, Percentage(7, term_hight))
	t.widgetServerInfo = t.ui_set_servers_info(0, 0, 0, 0)
	t.widgetSysInfo = t.ui_set_system_info(0, 0, 0, t.widgetServerInfo.GetHeight())
	t.widgetPutIopsChart = t.uiPutIops(0, 0, 0, Percentage(30, term_hight))
	t.widgetGetIopsChart = t.ui_get_iops(0, 0, 0, Percentage(30, term_hight))
	t.widgetPutLatency = t.ui_set_put_latency_bar_chart(0, 0, 0, Percentage(30, term_hight))
	t.widgetGetLatency = t.ui_set_get_latency_bar_chart(0, 0, 0, Percentage(30, term_hight))
	t.widgetRequestBarChart = t.ui_set_requests_bar_chart(0, 0, 0, Percentage(20, term_hight))
	t.widgetLogs = t.ui_set_log_list(0, 0, 0, Percentage(20, term_hight))

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

func (t *TermUI) Render() {
	ui.Render(ui.Body)
}

func (t *TermUI) Terminate_ui() {
	ui.StopLoop()
	ui.Close()
}

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
