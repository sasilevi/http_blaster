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
	cfg                      *config.TomlConfig
	widget_title             ui.GridBufferer
	widget_sys_info          ui.GridBufferer
	widget_server_info       ui.GridBufferer
	widget_put_iops_chart    *ui.LineChart
	widget_get_iops_chart    *ui.LineChart
	widget_logs              *ui.List
	widget_progress          ui.GridBufferer
	widget_request_bar_chart *ui.BarChart
	widget_put_latency       *ui.BarChart
	widget_get_latency       *ui.BarChart

	iops_get_fifo *Float64Fifo
	iops_put_fifo *Float64Fifo
	logs_fifo     *StringsFifo
	statuses      map[int]uint64
	M             sync.RWMutex
	ch_done       chan struct{}
}

type StringsFifo struct {
	string
	Length      int
	Items       []string
	ch_messages chan string
	lock        sync.Mutex
}

func (t *StringsFifo) Init(length int) {
	t.Length = length
	t.Items = make([]string, length)
	t.ch_messages = make(chan string, 100)
	go func() {
		for msg := range t.ch_messages {
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
	t.ch_messages <- msg
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
		close(t.ch_done)
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
	list.Items = t.logs_fifo.Get()
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
	lc2.Data = t.iops_get_fifo.Get()
	return lc2
}

func (t *TermUI) ui_put_iops(x, y, w, h int) *ui.LineChart {
	lc2 := ui.NewLineChart()
	lc2.BorderLabel = "Put iops chart"
	lc2.Mode = "braille"
	lc2.Data = t.iops_put_fifo.Get()
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
		t.iops_put_fifo.Insert(float64(put_iops) / 1000)
	}
	if get_iops > 0 {
		t.iops_get_fifo.Insert(float64(get_iops) / 1000)
	}
	t.widget_put_iops_chart.Data = t.iops_put_fifo.Get()
	t.widget_get_iops_chart.Data = t.iops_get_fifo.Get()

}

func (t *TermUI) Refresh_log() {
	t.widget_logs.Items = t.logs_fifo.Get()
	ui.Render(t.widget_logs)
}

func (t *TermUI) Update_status_codes(labels []string, values []int) {
	t.widget_request_bar_chart.Data = values
	t.widget_request_bar_chart.DataLabels = labels
}

func (t *TermUI) Update_put_latency_chart(labels []string, values []int) {
	t.widget_put_latency.Data = values
	t.widget_put_latency.DataLabels = labels
}

func (t *TermUI) Update_get_latency_chart(labels []string, values []int) {
	t.widget_get_latency.Data = values
	t.widget_get_latency.DataLabels = labels
}

func Percentage(value, total int) int {
	return value * total / 100
}

func (t *TermUI) InitTerminamlUI(cfg *config.TomlConfig) chan struct{} {
	t.cfg = cfg
	t.ch_done = make(chan struct{})
	t.iops_get_fifo = &Float64Fifo{}
	t.iops_get_fifo.Init(150)
	t.iops_put_fifo = &Float64Fifo{}
	t.iops_put_fifo.Init(150)
	t.logs_fifo = &StringsFifo{}
	t.logs_fifo.Init(10)
	t.statuses = make(map[int]uint64)
	err := ui.Init()
	if err != nil {
		panic(err)
	}
	term_hight := ui.TermHeight()

	t.widget_title = t.ui_set_title(0, 0, 50, Percentage(7, term_hight))
	t.widget_server_info = t.ui_set_servers_info(0, 0, 0, 0)
	t.widget_sys_info = t.ui_set_system_info(0, 0, 0, t.widget_server_info.GetHeight())
	t.widget_put_iops_chart = t.ui_put_iops(0, 0, 0, Percentage(30, term_hight))
	t.widget_get_iops_chart = t.ui_get_iops(0, 0, 0, Percentage(30, term_hight))
	t.widget_put_latency = t.ui_set_put_latency_bar_chart(0, 0, 0, Percentage(30, term_hight))
	t.widget_get_latency = t.ui_set_get_latency_bar_chart(0, 0, 0, Percentage(30, term_hight))
	t.widget_request_bar_chart = t.ui_set_requests_bar_chart(0, 0, 0, Percentage(20, term_hight))
	t.widget_logs = t.ui_set_log_list(0, 0, 0, Percentage(20, term_hight))

	ui.Body.AddRows(
		ui.NewRow(
			ui.NewCol(12, 0, t.widget_title),
		),
		ui.NewRow(
			ui.NewCol(6, 0, t.widget_sys_info),
			ui.NewCol(6, 0, t.widget_server_info),
		),
		ui.NewRow(
			ui.NewCol(6, 0, t.widget_put_iops_chart),
			ui.NewCol(6, 0, t.widget_put_latency),
		),
		ui.NewRow(
			ui.NewCol(6, 0, t.widget_get_iops_chart),
			ui.NewCol(6, 0, t.widget_get_latency),
		),
		ui.NewRow(
			ui.NewCol(6, 0, t.widget_logs),
			ui.NewCol(6, 0, t.widget_request_bar_chart),
		),
	)

	ui.Body.Align()
	ui.Render(ui.Body)
	go ui.Loop()
	return t.ch_done
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
	t.logs_fifo.Insert(string(p))
	if t.widget_logs != nil {
		t.widget_logs.Items = t.logs_fifo.Get()
	}
	ui.Render(t.widget_logs)
	return len(p), nil
}
