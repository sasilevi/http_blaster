package datagenerator

import (
	"encoding/json"
	"fmt"
	"http_blaster/httpblaster/igz_data"
	"strings"

	//"github.com/shirou/gopsutil/cpu"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/shirou/gopsutil/mem"
)

// MemoryGenerator MemoryGenerator
type MemoryGenerator struct {
	Total     uint64
	Availible uint64
	Active    uint64
}

//GenerateRandomData GenerateRandomData
func (m *MemoryGenerator) GenerateRandomData(cpuNumber string) []string {
	//stats, _ := cpu.Info()
	//fmt.Println(stats)
	v, _ := mem.VirtualMemory()
	payloads := m.GenerateJSONArray(v, cpuNumber)
	fmt.Println(strings.Join(payloads, ", "))
	return payloads
}

//GenerateJSONByVal GenerateJSONByVal
func (m *MemoryGenerator) GenerateJSONByVal(timestamp string, colName string, val float64, cpuNumber string) string {
	//item :=igz_data.IgzTSDBItem{}
	item := igz_data.IgzTSDBItem{}
	item.InsertMetric("memory")
	item.InsertLable("type", colName)
	item.InsertLable("hostname", GetHostname())
	item.InsertLable("cpu", string(cpuNumber))

	item.InsertSample(timestamp, val)
	return item.ToJsonString()
}

//GenerateJSONArray GenerateJSONArray
func (m *MemoryGenerator) GenerateJSONArray(v *mem.VirtualMemoryStat, cpuNumber string) []string {
	timestamp := NowAsUnixMilli()
	arr := []string{}
	val := make(map[string]interface{})
	if err := json.Unmarshal([]byte(v.String()), &val); err != nil {
		panic(err)
	}
	for s, vl := range val {
		f, _ := getFloat(vl)
		arr = append(arr, m.GenerateJSONByVal(timestamp, s, f, cpuNumber))
	}
	return arr
}

//GetHostname get host name
func GetHostname() string {
	name, err := os.Hostname()
	if err != nil {
		panic(err)
	} else {
		return name
	}
}

var floatType = reflect.TypeOf(float64(0))

func getFloat(unk interface{}) (float64, error) {
	v := reflect.ValueOf(unk)
	v = reflect.Indirect(v)
	if !v.Type().ConvertibleTo(floatType) {
		return 0, fmt.Errorf("cannot convert %v to float64", v.Type())
	}
	fv := v.Convert(floatType)
	return fv.Float(), nil
}

// NowAsUnixMilli : now as unix mili
func NowAsUnixMilli() string {
	ts := time.Now().UnixNano() / 1e6
	tsStr := strconv.FormatInt(ts, 10)
	return tsStr
}
